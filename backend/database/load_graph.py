#!/usr/bin/env python3
"""
==============================================================================
MASTER ETL: Neo4j Graph Database Loader for Project Sentinel
==============================================================================

Purpose:
    Loads all collected intelligence data (GDELT events, news articles,
    Telegram posts, flight data) into a Neo4j graph database with proper
    relationships and entity linking.

Graph Schema:
    -------------------------------------------------------------------------
    NODES:
    - (:Event)    - GDELT conflict events (kinetic/material conflicts)
    - (:Article)  - News articles from RSS feeds
    - (:Post)     - Telegram messages from channels
    - (:Flight)   - Aircraft state vectors from OpenSky
    - (:Source)   - News sources (Reuters, BBC, etc.)
    - (:Channel)  - Telegram channels
    - (:Location) - Geographic locations (countries, cities, regions)
    
    RELATIONSHIPS:
    - (:Source)-[:PUBLISHED]->(:Article)
    - (:Channel)-[:POSTED]->(:Post)
    - (:Flight)-[:PATROLLING]->(:Location)
    - (:Article)-[:MENTIONS]->(:Location)
    - (:Event)-[:OCCURRED_IN]->(:Location)
    -------------------------------------------------------------------------

Connection:
    bolt://localhost:7687
    Credentials loaded from .env or defaults to neo4j/sentinel_password

Author: Project Sentinel Team
Created: 2026
==============================================================================
"""

import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

from dotenv import load_dotenv

try:
    from neo4j import GraphDatabase
    from neo4j.exceptions import ServiceUnavailable, AuthError
except ImportError:
    print("❌ ERROR: neo4j driver not installed.")
    print("   Run: pip install neo4j")
    sys.exit(1)


# ==============================================================================
# CONFIGURATION
# ==============================================================================

load_dotenv()

# Neo4j connection settings
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "sentinel_password")

# Data file paths
DATA_DIR = Path(__file__).parent.parent.parent / "data"
GDELT_FILE = DATA_DIR / "kinetic_events.json"
NEWS_FILE = DATA_DIR / "news_feed.json"
TELEGRAM_FILE = DATA_DIR / "telegram_feed.json"
FLIGHTS_FILE = DATA_DIR / "flight_radar.json"

# Location keywords for entity linking
# Maps keywords to standardized location names
LOCATION_KEYWORDS = {
    # Countries
    "israel": "Israel",
    "israeli": "Israel",
    "palestine": "Palestine",
    "palestinian": "Palestine",
    "gaza": "Gaza",
    "lebanon": "Lebanon",
    "lebanese": "Lebanon",
    "hezbollah": "Lebanon",
    "syria": "Syria",
    "syrian": "Syria",
    "damascus": "Syria",
    "iran": "Iran",
    "iranian": "Iran",
    "tehran": "Iran",
    "iraq": "Iraq",
    "iraqi": "Iraq",
    "jordan": "Jordan",
    "egypt": "Egypt",
    "egyptian": "Egypt",
    "saudi": "Saudi Arabia",
    "yemen": "Yemen",
    "houthi": "Yemen",
    # Cities/Regions
    "tel aviv": "Tel Aviv",
    "jerusalem": "Jerusalem",
    "west bank": "West Bank",
    "beirut": "Beirut",
    "aleppo": "Aleppo",
    "idlib": "Idlib",
    "golan": "Golan Heights",
}

# Batch size for transactions
BATCH_SIZE = 500


# ==============================================================================
# DATABASE CONNECTION
# ==============================================================================

class Neo4jLoader:
    """
    Neo4j database loader with connection management and ETL functions.
    """
    
    def __init__(self, uri: str, user: str, password: str):
        """Initialize Neo4j driver connection."""
        self.driver = None
        try:
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
            # Verify connection
            with self.driver.session() as session:
                session.run("RETURN 1")
            print(f"   ✓ Connected to Neo4j at {uri}")
        except ServiceUnavailable:
            print(f"   ❌ Cannot connect to Neo4j at {uri}")
            print("      Is Docker running? Try: docker-compose up -d")
            sys.exit(1)
        except AuthError:
            print(f"   ❌ Authentication failed for user '{user}'")
            sys.exit(1)
    
    def close(self):
        """Close the driver connection."""
        if self.driver:
            self.driver.close()
    
    def run_query(self, query: str, parameters: Dict = None) -> Any:
        """Execute a single Cypher query."""
        with self.driver.session() as session:
            result = session.run(query, parameters or {})
            return result.consume()
    
    def run_batch(self, query: str, data: List[Dict], batch_size: int = BATCH_SIZE):
        """Execute a query in batches for large datasets."""
        total = len(data)
        processed = 0
        
        with self.driver.session() as session:
            for i in range(0, total, batch_size):
                batch = data[i:i + batch_size]
                session.run(query, {"batch": batch})
                processed += len(batch)
        
        return processed


# ==============================================================================
# SCHEMA SETUP
# ==============================================================================

def create_constraints(loader: Neo4jLoader):
    """
    Create uniqueness constraints to prevent duplicate nodes.
    
    These constraints also create indexes for faster lookups.
    """
    print("\n📋 Creating schema constraints...")
    
    constraints = [
        ("Event", "id"),
        ("Article", "id"),
        ("Post", "id"),
        ("Flight", "icao24"),
        ("Source", "name"),
        ("Channel", "name"),
        ("Location", "name"),
    ]
    
    for label, prop in constraints:
        try:
            query = f"""
            CREATE CONSTRAINT IF NOT EXISTS
            FOR (n:{label})
            REQUIRE n.{prop} IS UNIQUE
            """
            loader.run_query(query)
            print(f"   ✓ Constraint: {label}.{prop}")
        except Exception as e:
            print(f"   ⚠️  Constraint {label}.{prop}: {e}")


def wipe_database(loader: Neo4jLoader):
    """
    Delete all nodes and relationships (development reset).
    
    ⚠️  DESTRUCTIVE - Only use during development!
    """
    print("\n🗑️  Wiping database...")
    loader.run_query("MATCH (n) DETACH DELETE n")
    print("   ✓ All nodes and relationships deleted")


# ==============================================================================
# DATA LOADING FUNCTIONS
# ==============================================================================

def extract_locations(text: str) -> List[str]:
    """
    Extract location mentions from text using keyword matching.
    
    Args:
        text: Text to search for location keywords
        
    Returns:
        List of standardized location names found
    """
    if not text:
        return []
    
    text_lower = text.lower()
    found = set()
    
    for keyword, location in LOCATION_KEYWORDS.items():
        if keyword in text_lower:
            found.add(location)
    
    return list(found)


def load_gdelt(loader: Neo4jLoader):
    """
    Load GDELT conflict events into Neo4j.
    
    Creates:
    - (:Event) nodes with conflict data
    - (:Location) nodes based on coordinates
    - [:OCCURRED_IN] relationships
    """
    print("\n📥 Loading GDELT events...")
    
    if not GDELT_FILE.exists():
        print(f"   ⚠️  File not found: {GDELT_FILE}")
        return 0
    
    with open(GDELT_FILE, 'r') as f:
        events = json.load(f)
    
    if not events:
        print("   ⚠️  No events to load")
        return 0
    
    # Create Event nodes
    query = """
    UNWIND $batch AS event
    MERGE (e:Event {id: event.id})
    SET e.timestamp = event.timestamp,
        e.lat = event.lat,
        e.lon = event.lon,
        e.event_code = event.event_code,
        e.actor_1 = event.actor_1,
        e.actor_2 = event.actor_2,
        e.source_url = event.source_url,
        e.goldstein_scale = event.goldstein_scale
    
    WITH e, event
    
    // Create Location based on rounded coordinates
    MERGE (loc:Location {name: 'Grid_' + toString(round(event.lat, 0)) + '_' + toString(round(event.lon, 0))})
    SET loc.lat = round(event.lat, 0),
        loc.lon = round(event.lon, 0)
    
    MERGE (e)-[:OCCURRED_IN]->(loc)
    """
    
    # Add unique IDs if not present
    for i, event in enumerate(events):
        if 'id' not in event:
            event['id'] = f"gdelt_{i}_{event.get('timestamp', '')}"
    
    count = loader.run_batch(query, events)
    print(f"   ✓ Loaded {count} events")
    return count


def load_news(loader: Neo4jLoader):
    """
    Load news articles into Neo4j.
    
    Creates:
    - (:Source) nodes for news sources
    - (:Article) nodes for articles
    - (:Location) nodes for mentioned places
    - [:PUBLISHED] relationships
    - [:MENTIONS] relationships
    """
    print("\n📥 Loading news articles...")
    
    if not NEWS_FILE.exists():
        print(f"   ⚠️  File not found: {NEWS_FILE}")
        return 0
    
    with open(NEWS_FILE, 'r') as f:
        data = json.load(f)
    
    articles = data.get("articles", [])
    if not articles:
        print("   ⚠️  No articles to load")
        return 0
    
    # Create Source and Article nodes with relationship
    query = """
    UNWIND $batch AS article
    
    // Create Source node
    MERGE (s:Source {name: article.source_id})
    SET s.tier = article.source_tier,
        s.type = article.source_type
    
    // Create Article node
    MERGE (a:Article {id: article.id})
    SET a.title = article.title,
        a.summary = article.summary,
        a.link = article.link,
        a.published_utc = article.published_utc,
        a.priority = article.priority
    
    // Create PUBLISHED relationship
    MERGE (s)-[:PUBLISHED]->(a)
    """
    
    count = loader.run_batch(query, articles)
    print(f"   ✓ Loaded {count} articles")
    
    # Entity linking: Connect articles to locations mentioned in title
    print("   🔗 Linking articles to locations...")
    
    location_links = []
    for article in articles:
        title = article.get('title', '')
        summary = article.get('summary', '')
        locations = extract_locations(f"{title} {summary}")
        
        for loc in locations:
            location_links.append({
                "article_id": article['id'],
                "location": loc
            })
    
    if location_links:
        link_query = """
        UNWIND $batch AS link
        
        MATCH (a:Article {id: link.article_id})
        MERGE (loc:Location {name: link.location})
        MERGE (a)-[:MENTIONS]->(loc)
        """
        loader.run_batch(link_query, location_links)
        print(f"   ✓ Created {len(location_links)} location mentions")
    
    return count


def load_telegram(loader: Neo4jLoader):
    """
    Load Telegram posts into Neo4j.
    
    Creates:
    - (:Channel) nodes for Telegram channels
    - (:Post) nodes for messages
    - [:POSTED] relationships
    """
    print("\n📥 Loading Telegram posts...")
    
    if not TELEGRAM_FILE.exists():
        print(f"   ⚠️  File not found: {TELEGRAM_FILE}")
        return 0
    
    with open(TELEGRAM_FILE, 'r') as f:
        data = json.load(f)
    
    messages = data.get("messages", [])
    if not messages:
        print("   ⚠️  No messages to load")
        return 0
    
    # Create Channel and Post nodes with relationship
    query = """
    UNWIND $batch AS msg
    
    // Extract channel name from source_id (telegram_channelname -> channelname)
    WITH msg, replace(msg.source_id, 'telegram_', '') AS channel_name
    
    // Create Channel node
    MERGE (c:Channel {name: channel_name})
    
    // Create Post node
    MERGE (p:Post {id: toString(msg.message_id) + '_' + channel_name})
    SET p.text = msg.text,
        p.date = msg.date,
        p.message_id = msg.message_id,
        p.priority = msg.priority
    
    // Create POSTED relationship
    MERGE (c)-[:POSTED]->(p)
    """
    
    count = loader.run_batch(query, messages)
    print(f"   ✓ Loaded {count} posts")
    return count


def load_flights(loader: Neo4jLoader):
    """
    Load flight data into Neo4j.
    
    Creates:
    - (:Flight) nodes for aircraft
    - (:Location) nodes based on rounded coordinates
    - [:PATROLLING] relationships
    """
    print("\n📥 Loading flight data...")
    
    if not FLIGHTS_FILE.exists():
        print(f"   ⚠️  File not found: {FLIGHTS_FILE}")
        return 0
    
    with open(FLIGHTS_FILE, 'r') as f:
        data = json.load(f)
    
    aircraft = data.get("aircraft", [])
    if not aircraft:
        print("   ⚠️  No aircraft to load")
        return 0
    
    # Create Flight nodes with Location relationship
    query = """
    UNWIND $batch AS flight
    
    // Create Flight node
    MERGE (f:Flight {icao24: flight.icao24})
    SET f.callsign = flight.callsign,
        f.origin_country = flight.origin_country,
        f.latitude = flight.latitude,
        f.longitude = flight.longitude,
        f.geo_altitude = flight.geo_altitude,
        f.velocity = flight.velocity,
        f.tag = flight.tag,
        f.on_ground = flight.on_ground
    
    WITH f, flight
    
    // Create Location based on rounded coordinates (grid cell)
    MERGE (loc:Location {name: 'Grid_' + toString(round(flight.latitude, 0)) + '_' + toString(round(flight.longitude, 0))})
    SET loc.lat = round(flight.latitude, 0),
        loc.lon = round(flight.longitude, 0)
    
    // Create PATROLLING relationship
    MERGE (f)-[:PATROLLING]->(loc)
    """
    
    count = loader.run_batch(query, aircraft)
    print(f"   ✓ Loaded {count} aircraft")
    return count


# ==============================================================================
# MAIN ENTRY POINT
# ==============================================================================

def main(wipe: bool = False):
    """
    Main ETL entry point.
    
    Args:
        wipe: If True, delete all existing data before loading
    """
    print("=" * 70)
    print("🔷 NEO4J ETL: Master Graph Database Loader")
    print("   Project Sentinel - Intelligence Graph Construction")
    print("=" * 70)
    
    print(f"\n🔌 Connecting to Neo4j...")
    print(f"   URI: {NEO4J_URI}")
    
    loader = Neo4jLoader(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    
    try:
        # Optional: Wipe database for development reset
        if wipe:
            wipe_database(loader)
        
        # Create schema constraints
        create_constraints(loader)
        
        # Load all data sources
        totals = {
            "events": load_gdelt(loader),
            "articles": load_news(loader),
            "posts": load_telegram(loader),
            "flights": load_flights(loader),
        }
        
        # Summary
        print("\n" + "=" * 70)
        print("📊 ETL SUMMARY")
        print("=" * 70)
        print(f"   GDELT Events:      {totals['events']}")
        print(f"   News Articles:     {totals['articles']}")
        print(f"   Telegram Posts:    {totals['posts']}")
        print(f"   Flight Records:    {totals['flights']}")
        print("-" * 70)
        print(f"   Total Loaded:      {sum(totals.values())}")
        print("=" * 70)
        print("\n🎯 Graph ready! Access Neo4j Browser at http://localhost:7474")
        
    finally:
        loader.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Load intelligence data into Neo4j")
    parser.add_argument(
        "--wipe", 
        action="store_true", 
        help="Wipe database before loading (development only)"
    )
    args = parser.parse_args()
    
    main(wipe=args.wipe)
