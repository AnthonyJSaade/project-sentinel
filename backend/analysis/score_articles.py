#!/usr/bin/env python3
"""
==============================================================================
ARTICLE CREDIBILITY SCORING - Project Sentinel
==============================================================================

Purpose:
    Score news articles based on source credibility and cross-reference detection.
    This is the core of the "verified news" experience.

Scoring Algorithm:
    -------------------------------------------------------------------------
    BASE SCORE (by source tier):
        Tier 1 (Wire services: NPR, BBC, Reuters):     70 points
        Tier 2 (International: Al Jazeera, J. Post):   50 points
        Tier 3 (Local/Social: Telegram):               30 points
    
    CROSS-REFERENCE BONUS:
        Same story reported by 2+ sources:             +15 points
        Same story reported by 3+ sources:             +25 points
        Tier 1 source confirms:                        +10 points
    
    RECENCY BONUS:
        Published within last 6 hours:                 +5 points
    
    STATUS CLASSIFICATION:
        >= 70: "Verified"   (green)
        >= 50: "Plausible"  (yellow)
        <  50: "Unverified" (red)
    -------------------------------------------------------------------------

Author: Project Sentinel Team
==============================================================================
"""

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from collections import defaultdict

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent  # Go up to OverWatch/
DATA_DIR = PROJECT_ROOT / "data"
NEWS_FEED_PATH = DATA_DIR / "news_feed.json"
TELEGRAM_FEED_PATH = DATA_DIR / "telegram_feed.json"
OUTPUT_PATH = DATA_DIR / "scored_articles.json"

# Base scores by tier
TIER_SCORES = {
    1: 70,  # Wire services (NPR, BBC, Reuters)
    2: 50,  # International mainstream (Al Jazeera, Jerusalem Post)
    3: 30,  # Local/social media (Telegram)
}

# Cross-reference bonuses
CROSS_REF_2_SOURCES = 15   # Same story from 2+ sources
CROSS_REF_3_SOURCES = 25   # Same story from 3+ sources
TIER1_CONFIRMS_BONUS = 10  # A Tier 1 source confirms the story

# Social media bonuses (Telegram cross-refs are valuable)
SOCIAL_CROSS_REF_BONUS = 20   # Multiple Telegram channels report same thing
MAINSTREAM_CONFIRMS_BONUS = 25 # Mainstream confirms social media report

# Recency bonus
RECENCY_HOURS = 6
RECENCY_BONUS = 5

# Status thresholds (Option B: Confidence Levels)
# These map to user-friendly labels instead of showing raw scores
STATUS_VERIFIED = 85          # 🟢 "Verified" - high confidence, hide score
STATUS_LIKELY_VERIFIED = 70   # 🟢 "Likely Verified" - good confidence
STATUS_DEVELOPING = 50        # 🟡 "Developing Story" - moderate confidence
STATUS_UNVERIFIED = 30        # 🔴 "Unverified" - low confidence
# Below 30 = "Unconfirmed Report" - very low confidence

# AI Analysis - Newsroom Debate System
# Score < 46:  No analysis (too weak)
# Score 46-74: Full Newsroom Debate (Reporter → Fact-Checker → Editor)
# Score 75+:   Source Comparison (highlights differences between sources)
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def load_articles() -> List[Dict]:
    """Load articles from news_feed.json."""
    if not NEWS_FEED_PATH.exists():
        print(f"❌ News feed not found at {NEWS_FEED_PATH}")
        print("   Run: python agents/fetch_news.py")
        return []
    
    with open(NEWS_FEED_PATH, 'r') as f:
        data = json.load(f)
    
    return data.get("articles", [])


def load_telegram_posts() -> List[Dict]:
    """Load and normalize Telegram posts."""
    if not TELEGRAM_FEED_PATH.exists():
        print(f"⚠️  Telegram feed not found at {TELEGRAM_FEED_PATH}")
        print("   Run: python agents/fetch_telegram.py (optional)")
        return []
    
    with open(TELEGRAM_FEED_PATH, 'r') as f:
        data = json.load(f)
    
    posts = data.get("messages", [])
    
    # Normalize to article-like format
    normalized = []
    for post in posts:
        # Clean up text (remove markdown formatting)
        text = post.get("text", "")
        text_clean = text.replace("**", "").replace("*", "")
        
        # Extract first line as title, rest as summary
        lines = text_clean.strip().split("\n")
        title = lines[0][:150] if lines else "Telegram post"
        summary = " ".join(lines[1:])[:300] if len(lines) > 1 else ""
        
        normalized.append({
            "id": f"tg_{post.get('message_id', 'unknown')}",
            "source_id": post.get("source_id", "telegram"),
            "source_tier": 3,  # Social media = Tier 3
            "source_type": "Social Media",
            "title": title,
            "summary": summary,
            "full_text": text_clean,
            "link": f"https://t.me/{post.get('source_id', '').replace('telegram_', '')}/{post.get('message_id', '')}",
            "published_utc": post.get("date", ""),
            "priority": post.get("priority", "normal"),
            "content_type": "telegram"
        })
    
    return normalized


def load_all_content() -> List[Dict]:
    """
    Load and merge all content sources (news + Telegram).
    
    Returns unified list with consistent schema.
    """
    all_content = []
    
    # Load news articles
    articles = load_articles()
    for a in articles:
        a["content_type"] = "article"
    all_content.extend(articles)
    
    # Load Telegram posts
    posts = load_telegram_posts()
    all_content.extend(posts)
    
    return all_content


def get_status(score: int) -> str:
    """
    Convert numeric score to user-friendly confidence label.
    
    Option B: Confidence Levels
    - 85+:  Verified          (high confidence, no score shown)
    - 70-84: Likely Verified  (good confidence)
    - 50-69: Developing Story (moderate, still gathering info)
    - 30-49: Unverified       (low confidence)
    - <30:   Unconfirmed      (very low, treat with caution)
    """
    if score >= STATUS_VERIFIED:
        return "Verified"
    elif score >= STATUS_LIKELY_VERIFIED:
        return "Likely Verified"
    elif score >= STATUS_DEVELOPING:
        return "Developing"
    elif score >= STATUS_UNVERIFIED:
        return "Unverified"
    else:
        return "Unconfirmed"


def extract_location_keywords(text: str) -> List[str]:
    """
    Extract location keywords from article text.
    Simple keyword matching for now - can upgrade to NER later.
    """
    locations = [
        "gaza", "israel", "palestine", "west bank", "tel aviv", "jerusalem",
        "beirut", "lebanon", "hezbollah", "syria", "damascus", "aleppo",
        "iran", "tehran", "hamas", "rafah", "khan younis", "idf",
        "netanyahu", "houthi", "yemen", "jordan", "egypt", "cairo"
    ]
    
    text_lower = text.lower()
    found = []
    for loc in locations:
        if loc in text_lower:
            found.append(loc)
    return found


def calculate_recency_bonus(published_utc: str) -> int:
    """Add bonus points for recent articles."""
    try:
        # Parse the timestamp
        if published_utc.endswith('Z'):
            published_utc = published_utc[:-1] + '+00:00'
        pub_time = datetime.fromisoformat(published_utc)
        now = datetime.now(timezone.utc)
        
        age = now - pub_time
        if age < timedelta(hours=RECENCY_HOURS):
            return RECENCY_BONUS
    except:
        pass
    return 0


# ==============================================================================
# MAIN SCORING LOGIC
# ==============================================================================

def score_articles(articles: List[Dict]) -> List[Dict]:
    """
    Score all articles based on source tier and cross-references.
    
    Args:
        articles: List of article dicts from news_feed.json
        
    Returns:
        List of articles with added score fields
    """
    scored = []
    
    # Phase 1: Calculate base scores
    print(f"\n📊 Phase 1: Calculating base scores for {len(articles)} articles...")
    
    for article in articles:
        tier = article.get("source_tier", 3)
        base_score = TIER_SCORES.get(tier, 30)
        
        # Add recency bonus
        recency = calculate_recency_bonus(article.get("published_utc", ""))
        
        # Extract keywords for cross-referencing
        text = f"{article.get('title', '')} {article.get('summary', '')}"
        keywords = extract_location_keywords(text)
        
        scored.append({
            **article,
            "base_score": base_score,
            "recency_bonus": recency,
            "keywords": keywords,
            "cross_ref_bonus": 0,  # Will be filled in Phase 2
            "final_score": base_score + recency,
            "status": get_status(base_score + recency),
            "scoring_notes": [f"Base: {base_score} (Tier {tier})"]
        })
    
    # Phase 2: Cross-reference detection
    print(f"📊 Phase 2: Detecting cross-references...")
    
    # Group articles by primary keyword
    keyword_groups = defaultdict(list)
    for i, article in enumerate(scored):
        for kw in article["keywords"]:
            keyword_groups[kw].append(i)
    
    # Find articles that share keywords with others
    for i, article in enumerate(scored):
        related_articles = set()
        for kw in article["keywords"]:
            for related_idx in keyword_groups[kw]:
                if related_idx != i:
                    related_articles.add(related_idx)
        
        if len(related_articles) >= 1:
            # Check source diversity
            related_sources = {scored[j]["source_id"] for j in related_articles}
            own_source = article["source_id"]
            
            # Only count if actually different sources
            other_sources = related_sources - {own_source}
            
            if len(other_sources) >= 2:
                bonus = CROSS_REF_3_SOURCES
                note = f"Cross-ref: +{bonus} (3+ sources)"
            elif len(other_sources) >= 1:
                bonus = CROSS_REF_2_SOURCES
                note = f"Cross-ref: +{bonus} (2 sources)"
            else:
                bonus = 0
                note = None
            
            # Extra bonus if a Tier 1 source confirms
            if bonus > 0:
                tier1_confirms = any(
                    scored[j]["source_tier"] == 1 
                    for j in related_articles
                )
                if tier1_confirms and article["source_tier"] != 1:
                    bonus += TIER1_CONFIRMS_BONUS
                    note = f"{note} + Tier 1 confirms: +{TIER1_CONFIRMS_BONUS}"
                
                # Special: If this is a Telegram post and mainstream confirms it
                is_telegram = article.get("content_type") == "telegram"
                mainstream_confirms = any(
                    scored[j].get("content_type") == "article" and scored[j]["source_tier"] <= 2
                    for j in related_articles
                )
                if is_telegram and mainstream_confirms:
                    bonus += MAINSTREAM_CONFIRMS_BONUS
                    note = f"{note} + Mainstream confirms: +{MAINSTREAM_CONFIRMS_BONUS}"
            
            if bonus > 0:
                scored[i]["cross_ref_bonus"] = bonus
                scored[i]["cross_ref_count"] = len(other_sources)
                scored[i]["final_score"] = (
                    scored[i]["base_score"] + 
                    scored[i]["recency_bonus"] + 
                    bonus
                )
                scored[i]["status"] = get_status(scored[i]["final_score"])
                scored[i]["scoring_notes"].append(note)
                
                # Store which sources confirmed this story
                related_source_info = []
                for j in related_articles:
                    if scored[j]["source_id"] != own_source:
                        related_source_info.append({
                            "source_id": scored[j]["source_id"],
                            "source_tier": scored[j]["source_tier"],
                            "title": scored[j].get("title", "")[:80],
                            "content_type": scored[j].get("content_type", "article")
                        })
                scored[i]["related_sources"] = related_source_info
            else:
                scored[i]["cross_ref_count"] = 0
                scored[i]["related_sources"] = []
        else:
            scored[i]["cross_ref_count"] = 0
            scored[i]["related_sources"] = []
    
    return scored


# ==============================================================================
# AI ANALYSIS - NEWSROOM DEBATE SYSTEM
# ==============================================================================


def add_ai_reasoning(scored: List[Dict]) -> List[Dict]:
    """
    Add AI-generated analysis using the Newsroom Debate system.
    
    Analysis types by score:
    - Score < 46:   No analysis (too weak)
    - Score 46-74:  Full Newsroom Debate (Reporter → Fact-Checker → Editor)
    - Score 75+:    Source Comparison (highlights differences)
    """
    if not ANTHROPIC_API_KEY:
        print("\n⚠️  ANTHROPIC_API_KEY not set - skipping AI analysis")
        print("   Set it in .env to enable AI reasoning")
        return scored
    
    # Import the newsroom debate system
    try:
        from analysis.newsroom_debate import analyze_articles_batch
    except ImportError:
        print("\n⚠️  Newsroom debate module not found")
        return scored
    
    # Run the batch analysis
    scored = analyze_articles_batch(scored)
    
    return scored


def print_results(scored: List[Dict], limit: int = 30):
    """Print a summary table of scored articles."""
    print("\n" + "=" * 100)
    print("📰 ARTICLE CREDIBILITY SCORES")
    print("=" * 100)
    
    # Sort by score descending
    sorted_articles = sorted(scored, key=lambda x: x["final_score"], reverse=True)
    
    # Print header
    print(f"{'STATUS':<16} {'SOURCE':<20} {'TITLE':<58}")
    print("-" * 100)
    
    # Status icons for Option B confidence levels
    icons = {
        "Verified": "🟢",
        "Likely Verified": "🟢",
        "Developing": "🟡",
        "Unverified": "🔴",
        "Unconfirmed": "🔴"
    }
    
    for article in sorted_articles[:limit]:
        status = article["status"]
        icon = icons.get(status, "❓")
        # Add content type indicator
        type_icon = "📱" if article.get("content_type") == "telegram" else "📰"
        title = article.get("title", "No title")[:55]
        if len(article.get("title", "")) > 55:
            title += "..."
        source = article.get("source_id", "unknown")[:18]
        
        # For "Verified" status, don't show score (per Option B)
        if status == "Verified":
            status_display = f"{icon} {status}"
        else:
            status_display = f"{icon} {status}"
        
        print(f"{status_display:<16} {type_icon} {source:<18} {title}")
    
    # Summary stats with new labels
    print("\n" + "-" * 100)
    verified = sum(1 for a in scored if a["status"] == "Verified")
    likely = sum(1 for a in scored if a["status"] == "Likely Verified")
    developing = sum(1 for a in scored if a["status"] == "Developing")
    unverified = sum(1 for a in scored if a["status"] == "Unverified")
    unconfirmed = sum(1 for a in scored if a["status"] == "Unconfirmed")
    
    articles_count = sum(1 for a in scored if a.get("content_type") == "article")
    telegram_count = sum(1 for a in scored if a.get("content_type") == "telegram")
    
    print(f"📈 CONFIDENCE BREAKDOWN")
    print(f"   🟢 Verified:        {verified:>4} (score >= {STATUS_VERIFIED})")
    print(f"   🟢 Likely Verified: {likely:>4} (score {STATUS_LIKELY_VERIFIED}-{STATUS_VERIFIED-1})")
    print(f"   🟡 Developing:      {developing:>4} (score {STATUS_DEVELOPING}-{STATUS_LIKELY_VERIFIED-1})")
    print(f"   🔴 Unverified:      {unverified:>4} (score {STATUS_UNVERIFIED}-{STATUS_DEVELOPING-1})")
    print(f"   🔴 Unconfirmed:     {unconfirmed:>4} (score < {STATUS_UNVERIFIED})")
    print(f"   ─────────────────────────")
    print(f"   📰 Articles:  {articles_count:>4}")
    print(f"   📱 Telegram:  {telegram_count:>4}")
    print(f"   Total:        {len(scored):>4}")
    print("=" * 100)


def save_results(scored: List[Dict], map_markers: List[Dict] = None):
    """Save scored content to JSON."""
    output = {
        "metadata": {
            "scored_at": datetime.now(timezone.utc).isoformat(),
            "total_items": len(scored),
            "news_articles": sum(1 for a in scored if a.get("content_type") == "article"),
            "telegram_posts": sum(1 for a in scored if a.get("content_type") == "telegram"),
            "geolocated_items": sum(1 for a in scored if a.get("primary_location")),
            "confidence_breakdown": {
                "verified": sum(1 for a in scored if a["status"] == "Verified"),
                "likely_verified": sum(1 for a in scored if a["status"] == "Likely Verified"),
                "developing": sum(1 for a in scored if a["status"] == "Developing"),
                "unverified": sum(1 for a in scored if a["status"] == "Unverified"),
                "unconfirmed": sum(1 for a in scored if a["status"] == "Unconfirmed"),
            },
            "ai_analyzed": sum(1 for a in scored if a.get("ai_analyzed")),
        },
        "items": sorted(scored, key=lambda x: x["final_score"], reverse=True),
        "map_markers": map_markers or []
    }
    
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(output, f, indent=2)
    
    print(f"\n💾 Saved scored content to {OUTPUT_PATH}")


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    print("=" * 100)
    print("📰 CONTENT CREDIBILITY SCORING - Project Sentinel")
    print("   News Articles + Social Media (Telegram)")
    print("=" * 100)
    
    # Load all content (news + Telegram)
    print(f"\n📂 Loading content sources...")
    all_content = load_all_content()
    
    articles = [c for c in all_content if c.get("content_type") == "article"]
    telegram = [c for c in all_content if c.get("content_type") == "telegram"]
    
    print(f"   ✓ News articles: {len(articles)}")
    print(f"   ✓ Telegram posts: {len(telegram)}")
    print(f"   ✓ Total content:  {len(all_content)}")
    
    if not all_content:
        print("\n❌ No content to score. Run the data agents first.")
        return
    
    # Phase 1 & 2: Score all content
    scored = score_articles(all_content)
    
    # Phase 3: Add AI reasoning
    scored = add_ai_reasoning(scored)
    
    # Phase 4: Extract locations for map
    try:
        from analysis.geolocate import add_locations_to_articles, get_map_markers
        scored = add_locations_to_articles(scored)
        map_markers = get_map_markers(scored)
    except ImportError:
        print("\n⚠️  Geolocation module not found - skipping")
        map_markers = []
    
    # Print results
    print_results(scored)
    
    # Save results (with map markers)
    save_results(scored, map_markers)


if __name__ == "__main__":
    main()
