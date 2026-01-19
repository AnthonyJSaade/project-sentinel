#!/usr/bin/env python3
"""
==============================================================================
AGENT_NEWS: Independence-Aware News Ingestion Engine for Project Sentinel
==============================================================================

Purpose:
    Fetches, cleans, deduplicates, and classifies news from multiple RSS feeds.
    Designed to combat "Fake Consensus" (syndication) and "Data Drift" by
    tracking source independence through a tiered registry system.

Architectural Design:
    -------------------------------------------------------------------------
    The Source Registry Pattern addresses key OSINT challenges:
    
    1. FAKE CONSENSUS: Wire services (Reuters, AP) are syndicated by hundreds
       of outlets. If we count each outlet as a "source", we falsely inflate
       corroboration. Solution: Tier 1 sources are primary/wire services.
       
    2. DATA DRIFT: Regional and state media may delay, translate, or editorialize
       wire content. We track source type (Western, Regional, State_Affiliated)
       to weight reliability appropriately.
       
    3. DEDUPLICATION: Running the script multiple times shouldn't create
       duplicates. We generate deterministic IDs by hashing URLs.
    -------------------------------------------------------------------------

Source Tiers:
    - Tier 1: Wire/Primary sources (Reuters, AP, AFP) - First to report
    - Tier 2: Mainstream international (BBC, Al Jazeera) - Independent reporting
    - Tier 3: Local/State-affiliated (JPost, TASS) - May have editorial bias

Source Types:
    - Western: NATO-aligned perspective
    - Regional: Middle Eastern perspective  
    - State_Affiliated: Government-linked media (weight accordingly)

Author: Project Sentinel Team
Created: 2026
==============================================================================
"""

import hashlib
import html
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional
from email.utils import parsedate_to_datetime

import feedparser
import requests

# ==============================================================================
# SOURCE REGISTRY CONFIGURATION
# ==============================================================================
# Each source has: url, tier (1-3), type (Western/Regional/State_Affiliated)
# This registry enables source-aware analysis downstream

SOURCE_CONFIG: Dict[str, Dict[str, Any]] = {
    # -------------------------------------------------------------------------
    # TIER 1: Wire Services / Primary Sources
    # These are the origin of most breaking news. High reliability, fast.
    # -------------------------------------------------------------------------
    "reuters_world": {
        "url": "https://feeds.reuters.com/Reuters/worldNews",
        "tier": 1,
        "type": "Western",
        "name": "Reuters World News",
    },
    
    # -------------------------------------------------------------------------
    # TIER 2: Mainstream International Media
    # Independent editorial, may lag wire services but add analysis/context.
    # -------------------------------------------------------------------------
    "bbc_world": {
        "url": "https://feeds.bbci.co.uk/news/world/rss.xml",
        "tier": 2,
        "type": "Western",
        "name": "BBC World News",
    },
    "aljazeera_english": {
        "url": "https://www.aljazeera.com/xml/rss/all.xml",
        "tier": 2,
        "type": "Regional",
        "name": "Al Jazeera English",
    },
    
    # -------------------------------------------------------------------------
    # TIER 3: Local/Regional/State-Affiliated Media
    # Valuable for local detail, but may have editorial bias or state influence.
    # -------------------------------------------------------------------------
    "jerusalem_post": {
        "url": "https://www.jpost.com/rss/rssfeedsfrontpage.aspx",
        "tier": 3,
        "type": "Regional",
        "name": "Jerusalem Post",
    },
    "tass_world": {
        "url": "https://tass.com/rss/v2.xml",
        "tier": 3,
        "type": "State_Affiliated",
        "name": "TASS (Russia)",
        "notes": "Russian state media, often covers Syria/Middle East",
    },
}

# Output configuration
OUTPUT_DIR = Path(__file__).parent.parent.parent / "data"
OUTPUT_FILE = OUTPUT_DIR / "news_feed.json"

# HTTP configuration
USER_AGENT = (
    "Mozilla/5.0 (compatible; ProjectSentinel/1.0; "
    "+https://github.com/project-sentinel)"
)
REQUEST_TIMEOUT = 15  # seconds


# ==============================================================================
# UTILITY FUNCTIONS
# ==============================================================================

def generate_article_id(url: str) -> str:
    """
    Generate a deterministic, unique article ID by hashing the URL.
    
    Using SHA256 for collision resistance. The URL is the most reliable
    unique identifier for articles (titles can be edited, GUIDs may not exist).
    
    Args:
        url: Article URL
        
    Returns:
        First 16 characters of SHA256 hash (64-bit identifier)
    """
    # Normalize URL: strip whitespace, lowercase for consistency
    normalized = url.strip().lower()
    hash_obj = hashlib.sha256(normalized.encode('utf-8'))
    return hash_obj.hexdigest()[:16]


def strip_html_tags(text: str) -> str:
    """
    Remove HTML tags and decode HTML entities from text.
    
    RSS feeds often contain HTML markup in summaries. We need clean text
    for downstream NLP processing.
    
    Args:
        text: Raw text potentially containing HTML
        
    Returns:
        Clean plain text
    """
    if not text:
        return ""
    
    # Remove HTML tags using regex
    # This handles <p>, <br>, <a href="...">, etc.
    clean = re.sub(r'<[^>]+>', '', text)
    
    # Decode HTML entities like &amp;, &quot;, &#39;
    clean = html.unescape(clean)
    
    # Normalize whitespace
    clean = re.sub(r'\s+', ' ', clean).strip()
    
    return clean


def parse_published_date(entry: Dict) -> Optional[str]:
    """
    Extract and standardize the published date to UTC ISO format.
    
    RSS feeds use various date formats. feedparser provides parsed_date
    as a struct_time, but we also handle string parsing as fallback.
    
    Args:
        entry: feedparser entry dict
        
    Returns:
        ISO 8601 UTC timestamp string, or None if unparseable
    """
    # Try feedparser's pre-parsed date first
    if hasattr(entry, 'published_parsed') and entry.published_parsed:
        try:
            # struct_time to datetime
            dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            return dt.isoformat().replace('+00:00', 'Z')
        except (ValueError, TypeError):
            pass
    
    # Fallback: try parsing the raw published string
    published_str = entry.get('published') or entry.get('updated', '')
    if published_str:
        try:
            # parsedate_to_datetime handles RFC 2822 format (common in RSS)
            dt = parsedate_to_datetime(published_str)
            # Convert to UTC if timezone-aware
            if dt.tzinfo:
                dt = dt.astimezone(timezone.utc)
            else:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat().replace('+00:00', 'Z')
        except (ValueError, TypeError):
            pass
    
    # Last resort: use current time
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


# ==============================================================================
# FEED FETCHING AND PROCESSING
# ==============================================================================

def fetch_feed(source_id: str, config: Dict) -> List[Dict[str, Any]]:
    """
    Fetch and parse a single RSS feed.
    
    Uses requests library for HTTP fetching (more reliable than feedparser's
    internal urllib) and feedparser for XML parsing only.
    
    Args:
        source_id: Unique identifier for this source
        config: Source configuration dict
        
    Returns:
        List of parsed article dicts
    """
    url = config["url"]
    tier = config["tier"]
    source_type = config["type"]
    name = config.get("name", source_id)
    
    print(f"   📰 {name} (Tier {tier}, {source_type})...")
    
    try:
        # Use requests for HTTP fetching (more reliable than feedparser's urllib)
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/rss+xml, application/xml, text/xml, */*"
        }
        
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        
        # Parse the content with feedparser (XML parsing only)
        feed = feedparser.parse(response.content)
        
        # Check for parse errors
        if feed.bozo and feed.bozo_exception:
            print(f"      ⚠️  Parse warning: {type(feed.bozo_exception).__name__}")
        
        if not feed.entries:
            print(f"      ⚠️  No entries found")
            return []
        
        articles = []
        for entry in feed.entries:
            # Extract article URL (feedparser normalizes this)
            link = entry.get('link', '')
            if not link:
                continue
            
            # Generate deterministic ID from URL
            article_id = generate_article_id(link)
            
            # Extract and clean fields
            title = strip_html_tags(entry.get('title', 'No Title'))
            summary = strip_html_tags(
                entry.get('summary') or 
                entry.get('description') or 
                ''
            )
            published = parse_published_date(entry)
            
            article = {
                "id": article_id,
                "source_id": source_id,
                "source_tier": tier,
                "source_type": source_type,
                "title": title,
                "summary": summary[:500] if summary else "",  # Truncate long summaries
                "link": link,
                "published_utc": published,
            }
            
            articles.append(article)
        
        print(f"      ✓ Fetched {len(articles)} articles")
        return articles
    
    except requests.exceptions.Timeout:
        print(f"      ❌ Timeout: Feed took too long to respond")
        return []
    except requests.exceptions.HTTPError as e:
        print(f"      ❌ HTTP Error: {e.response.status_code}")
        return []
    except requests.exceptions.RequestException as e:
        print(f"      ❌ Request Error: {type(e).__name__}")
        return []
    except Exception as e:
        print(f"      ❌ Error: {type(e).__name__}: {e}")
        return []


def deduplicate_articles(
    new_articles: List[Dict], 
    existing_ids: set
) -> tuple[List[Dict], int]:
    """
    Remove duplicate articles based on their deterministic IDs.
    
    Args:
        new_articles: List of newly fetched articles
        existing_ids: Set of already-seen article IDs
        
    Returns:
        Tuple of (unique articles list, count of duplicates skipped)
    """
    unique = []
    duplicates = 0
    
    for article in new_articles:
        article_id = article["id"]
        if article_id in existing_ids:
            duplicates += 1
        else:
            unique.append(article)
            existing_ids.add(article_id)
    
    return unique, duplicates


def load_existing_articles(filepath: Path) -> tuple[List[Dict], set]:
    """
    Load existing articles from output file to enable deduplication.
    
    Args:
        filepath: Path to existing JSON file
        
    Returns:
        Tuple of (existing articles list, set of existing IDs)
    """
    if not filepath.exists():
        return [], set()
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            articles = data.get("articles", [])
            ids = {a["id"] for a in articles}
            return articles, ids
    except (json.JSONDecodeError, KeyError):
        return [], set()


def save_articles(articles: List[Dict], filepath: Path) -> None:
    """
    Save articles to JSON file with metadata.
    
    Args:
        articles: List of article dicts
        filepath: Output file path
    """
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    # Count by source for metadata
    by_source = {}
    for article in articles:
        source = article["source_id"]
        by_source[source] = by_source.get(source, 0) + 1
    
    output = {
        "metadata": {
            "last_updated": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            "total_articles": len(articles),
            "sources": list(SOURCE_CONFIG.keys()),
            "articles_by_source": by_source,
        },
        "articles": articles,
    }
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Saved to {filepath}")


# ==============================================================================
# MAIN ENTRY POINT
# ==============================================================================

def main():
    """
    Main entry point for Agent_News.
    
    Orchestrates the news ingestion pipeline:
    1. Load existing articles for deduplication
    2. Fetch each registered source
    3. Clean and standardize articles
    4. Deduplicate across sources and runs
    5. Save to output file
    """
    print("=" * 70)
    print("📰 AGENT_NEWS: Independence-Aware News Ingestion Engine")
    print("   Project Sentinel - Multi-Source News Aggregation")
    print("=" * 70)
    
    print(f"\n📋 Source Registry: {len(SOURCE_CONFIG)} sources configured")
    for source_id, config in SOURCE_CONFIG.items():
        print(f"   └─ {source_id}: Tier {config['tier']}, {config['type']}")
    
    # Load existing articles for cross-run deduplication
    existing_articles, existing_ids = load_existing_articles(OUTPUT_FILE)
    initial_count = len(existing_articles)
    print(f"\n📂 Loaded {initial_count} existing articles from cache")
    
    # Fetch all sources
    print(f"\n🔄 Fetching feeds...")
    all_new_articles = []
    failed_sources = []
    
    for source_id, config in SOURCE_CONFIG.items():
        articles = fetch_feed(source_id, config)
        if articles:
            all_new_articles.extend(articles)
        else:
            failed_sources.append(source_id)
    
    # Deduplicate
    unique_articles, duplicates_skipped = deduplicate_articles(
        all_new_articles, 
        existing_ids
    )
    
    # Merge with existing (newest first)
    # Sort all articles by published date, newest first
    merged = existing_articles + unique_articles
    merged.sort(key=lambda x: x.get('published_utc', ''), reverse=True)
    
    # Save results
    save_articles(merged, OUTPUT_FILE)
    
    # Print summary
    print("\n" + "=" * 70)
    print("📊 INGESTION SUMMARY")
    print("=" * 70)
    print(f"   Sources fetched:        {len(SOURCE_CONFIG) - len(failed_sources)}/{len(SOURCE_CONFIG)}")
    if failed_sources:
        print(f"   Failed sources:         {', '.join(failed_sources)}")
    print(f"   New articles fetched:   {len(all_new_articles)}")
    print(f"   Duplicates skipped:     {duplicates_skipped}")
    print(f"   Unique new articles:    {len(unique_articles)}")
    print(f"   Total in database:      {len(merged)}")
    print("=" * 70)
    
    # Required output format
    print(f"\n🎯 Fetched {len(unique_articles)} articles. Skipped {duplicates_skipped} duplicates.")


if __name__ == "__main__":
    main()
