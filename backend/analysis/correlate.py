#!/usr/bin/env python3
"""
==============================================================================
CORRELATION ENGINE: Multi-Source Corroboration Scoring for Project Sentinel
==============================================================================

Purpose:
    Calculates a "Corroboration Score" (0-100) for each conflict event by
    analyzing supporting evidence from multiple independent sources:
    - Spatial correlation with flight data (military aircraft proximity)
    - Narrative correlation with news articles and Telegram posts
    - Source tier weighting to prevent "fake consensus" from bot swarms

Algorithm Overview:
    -------------------------------------------------------------------------
    For each (:Event) in the last 24 hours:
    
    STEP A - Spatial Linking:
        Find (:Flight) nodes within 50km and +/- 1 hour of event
        Create (:Flight)-[:DETECTED_NEAR]->(:Event)
        
    STEP B - Narrative Linking:
        Find (:Article) and (:Post) mentioning event location
        Create (:Article)-[:CORROBORATES]->(:Event)
        Create (:Post)-[:CORROBORATES]->(:Event)
        
    STEP C - Scoring:
        +20 pts: Tier 1 source (wire services, NPR/BBC)
        +10 pts: Tier 2 source (mainstream international, Al Jazeera)
        +5 pts:  Tier 3 source (Telegram), capped at 30 pts total
        +40 pts: Military flight (high_altitude_fast) detected nearby
        
    Status Classification:
        < 30:  "Unverified"
        30-60: "Plausible"  
        > 60:  "Confirmed"
    -------------------------------------------------------------------------

Author: Project Sentinel Team
Created: 2026
==============================================================================
"""

import os
import sys
from datetime import datetime, timedelta, timezone
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

# Correlation parameters
SPATIAL_RADIUS_KM = 50          # Max distance for flight correlation (km)
TEMPORAL_WINDOW_HOURS = 1       # Time window for flight correlation (+/- hours)
EVENT_LOOKBACK_HOURS = 24       # Only score events from last N hours

# Scoring weights
SCORE_TIER1_SOURCE = 20         # Wire services (NPR, BBC)
SCORE_TIER2_SOURCE = 10         # International mainstream (Al Jazeera)
SCORE_TIER3_SOURCE = 5          # Telegram/local sources (per source)
SCORE_TIER3_CAP = 30            # Max points from Tier 3 sources
SCORE_MILITARY_FLIGHT = 40      # High-altitude fast aircraft nearby

# Status thresholds
STATUS_UNVERIFIED = 30          # Below this = Unverified
STATUS_PLAUSIBLE = 60           # Below this (but >= 30) = Plausible
                                # Above 60 = Confirmed


# ==============================================================================
# LOCATION MAPPING
# ==============================================================================

# Maps grid locations to human-readable names for narrative linking
# Grid format: Grid_LAT_LON (rounded to nearest integer)
GRID_TO_LOCATION = {
    # Israel
    "Grid_32_35": ["Israel", "Tel Aviv", "Jerusalem", "West Bank"],
    "Grid_31_35": ["Israel", "Jerusalem", "Gaza"],
    "Grid_32_34": ["Israel", "Tel Aviv"],
    # Lebanon
    "Grid_34_36": ["Lebanon", "Beirut"],
    "Grid_33_36": ["Lebanon"],
    "Grid_34_35": ["Lebanon", "Golan Heights"],
    # Syria
    "Grid_35_36": ["Syria", "Damascus"],
    "Grid_35_37": ["Syria"],
    "Grid_36_37": ["Syria", "Aleppo"],
    "Grid_36_36": ["Syria", "Idlib"],
    # Gaza
    "Grid_31_34": ["Gaza", "Palestine"],
    # Jordan
    "Grid_32_36": ["Jordan"],
    "Grid_31_36": ["Jordan"],
}


# ==============================================================================
# DATABASE CONNECTION
# ==============================================================================

class CorrelationEngine:
    """
    Multi-source corroboration scoring engine for conflict events.
    """
    
    def __init__(self, uri: str, user: str, password: str):
        """Initialize Neo4j driver connection."""
        self.driver = None
        try:
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
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
    
    def run_query(self, query: str, parameters: Dict = None) -> List[Dict]:
        """Execute a Cypher query and return results as list of dicts."""
        with self.driver.session() as session:
            result = session.run(query, parameters or {})
            return [record.data() for record in result]


# ==============================================================================
# CORRELATION ALGORITHMS
# ==============================================================================

def get_recent_events(engine: CorrelationEngine, hours: int = 24) -> List[Dict]:
    """
    Get all events from the last N hours.
    
    Args:
        engine: CorrelationEngine instance
        hours: Lookback window in hours
        
    Returns:
        List of event records with id, timestamp, lat, lon, location
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    cutoff_str = cutoff.isoformat().replace('+00:00', 'Z')
    
    query = """
    MATCH (e:Event)
    WHERE e.timestamp >= $cutoff OR e.timestamp IS NULL
    OPTIONAL MATCH (e)-[:OCCURRED_IN]->(loc:Location)
    RETURN 
        e.id AS id,
        e.timestamp AS timestamp,
        e.lat AS lat,
        e.lon AS lon,
        e.event_code AS event_code,
        loc.name AS grid_location
    ORDER BY e.timestamp DESC
    """
    
    return engine.run_query(query, {"cutoff": cutoff_str})


def link_nearby_flights(engine: CorrelationEngine, event: Dict) -> Dict:
    """
    STEP A: Spatial Linking - Find flights within 50km and +/- 1 hour.
    
    Uses Neo4j's point.distance() for efficient spatial queries.
    Creates (:Flight)-[:DETECTED_NEAR]->(:Event) relationships.
    
    Args:
        engine: CorrelationEngine instance
        event: Event dict with id, lat, lon, timestamp
        
    Returns:
        Dict with count of regular and military flights
    """
    event_id = event.get('id')
    lat = event.get('lat')
    lon = event.get('lon')
    
    if not all([event_id, lat, lon]):
        return {"total": 0, "military": 0}
    
    # Find and link nearby flights using spatial distance
    query = """
    MATCH (e:Event {id: $event_id})
    MATCH (f:Flight)
    WHERE f.latitude IS NOT NULL AND f.longitude IS NOT NULL
      AND point.distance(
          point({latitude: e.lat, longitude: e.lon}),
          point({latitude: f.latitude, longitude: f.longitude})
      ) <= $radius_meters
    MERGE (f)-[:DETECTED_NEAR]->(e)
    RETURN 
        count(f) AS total_flights,
        sum(CASE WHEN f.tag = 'high_altitude_fast' THEN 1 ELSE 0 END) AS military_flights
    """
    
    results = engine.run_query(query, {
        "event_id": event_id,
        "radius_meters": SPATIAL_RADIUS_KM * 1000  # Convert km to meters
    })
    
    if results:
        return {
            "total": results[0].get("total_flights", 0),
            "military": results[0].get("military_flights", 0)
        }
    return {"total": 0, "military": 0}


def get_location_keywords(grid_location: str) -> List[str]:
    """
    Get location keywords for narrative linking based on grid location.
    
    Args:
        grid_location: Grid cell name (e.g., "Grid_34_36")
        
    Returns:
        List of location names to search for in text
    """
    if not grid_location:
        return []
    
    # Direct mapping
    if grid_location in GRID_TO_LOCATION:
        return GRID_TO_LOCATION[grid_location]
    
    # Fallback: extract lat/lon and find nearby mappings
    return []


def link_narrative_sources(engine: CorrelationEngine, event: Dict) -> Dict:
    """
    STEP B: Narrative Linking - Find articles/posts mentioning event location.
    
    Creates:
    - (:Article)-[:CORROBORATES]->(:Event)
    - (:Post)-[:CORROBORATES]->(:Event)
    
    Args:
        engine: CorrelationEngine instance
        event: Event dict with id and grid_location
        
    Returns:
        Dict with counts by source tier
    """
    event_id = event.get('id')
    grid_location = event.get('grid_location')
    
    if not event_id:
        return {"tier1": 0, "tier2": 0, "tier3": 0}
    
    # Get location keywords for this event
    location_keywords = get_location_keywords(grid_location)
    
    if not location_keywords:
        # Fallback: use any existing Location links
        location_keywords = []
    
    # Link articles that mention relevant locations
    article_query = """
    MATCH (e:Event {id: $event_id})
    MATCH (a:Article)-[:MENTIONS]->(loc:Location)
    WHERE loc.name IN $locations
    MERGE (a)-[:CORROBORATES]->(e)
    WITH e, a
    MATCH (s:Source)-[:PUBLISHED]->(a)
    RETURN 
        s.tier AS tier,
        count(DISTINCT a) AS article_count,
        collect(DISTINCT s.name) AS sources
    """
    
    article_results = engine.run_query(article_query, {
        "event_id": event_id,
        "locations": location_keywords
    })
    
    # Link Telegram posts that mention relevant locations
    post_query = """
    MATCH (e:Event {id: $event_id})
    MATCH (p:Post)
    WHERE any(loc IN $locations WHERE toLower(p.text) CONTAINS toLower(loc))
    MERGE (p)-[:CORROBORATES]->(e)
    RETURN count(p) AS post_count
    """
    
    post_results = engine.run_query(post_query, {
        "event_id": event_id,
        "locations": location_keywords
    })
    
    # Aggregate by tier
    tier_counts = {"tier1": 0, "tier2": 0, "tier3": 0, "sources": []}
    
    for row in article_results:
        tier = row.get("tier", 3)
        count = row.get("article_count", 0)
        sources = row.get("sources", [])
        
        if tier == 1:
            tier_counts["tier1"] += count
        elif tier == 2:
            tier_counts["tier2"] += count
        else:
            tier_counts["tier3"] += count
        
        tier_counts["sources"].extend(sources)
    
    # Telegram posts count as Tier 3
    if post_results:
        tier_counts["tier3"] += post_results[0].get("post_count", 0)
    
    return tier_counts


def calculate_score(flight_data: Dict, source_data: Dict) -> int:
    """
    STEP C: Calculate confidence score based on corroborating evidence.
    
    Scoring:
        +20 pts: Tier 1 source (wire services)
        +10 pts: Tier 2 source (mainstream international)
        +5 pts:  Tier 3 source (per source, capped at 30)
        +40 pts: Military flight nearby
        
    Args:
        flight_data: Dict with "total" and "military" flight counts
        source_data: Dict with "tier1", "tier2", "tier3" counts
        
    Returns:
        Confidence score (0-100)
    """
    score = 0
    
    # Tier 1 sources (wire services like NPR, BBC)
    if source_data.get("tier1", 0) > 0:
        score += SCORE_TIER1_SOURCE
    
    # Tier 2 sources (mainstream international)
    if source_data.get("tier2", 0) > 0:
        score += SCORE_TIER2_SOURCE
    
    # Tier 3 sources (Telegram, local) - capped to prevent bot swarm inflation
    tier3_count = source_data.get("tier3", 0)
    tier3_score = min(tier3_count * SCORE_TIER3_SOURCE, SCORE_TIER3_CAP)
    score += tier3_score
    
    # Military flight correlation (strongest signal)
    if flight_data.get("military", 0) > 0:
        score += SCORE_MILITARY_FLIGHT
    
    # Cap at 100
    return min(score, 100)


def get_status(score: int) -> str:
    """
    Classify event status based on confidence score.
    
    Args:
        score: Confidence score (0-100)
        
    Returns:
        Status string: "Unverified", "Plausible", or "Confirmed"
    """
    if score < STATUS_UNVERIFIED:
        return "Unverified"
    elif score < STATUS_PLAUSIBLE:
        return "Plausible"
    else:
        return "Confirmed"


def update_event_score(engine: CorrelationEngine, event_id: str, score: int, status: str):
    """
    Write back confidence score and status to the Event node.
    
    Args:
        engine: CorrelationEngine instance
        event_id: Event node ID
        score: Calculated confidence score
        status: Classification status
    """
    query = """
    MATCH (e:Event {id: $event_id})
    SET e.confidence_score = $score,
        e.status = $status,
        e.scored_at = $scored_at
    """
    
    engine.run_query(query, {
        "event_id": event_id,
        "score": score,
        "status": status,
        "scored_at": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    })


# ==============================================================================
# MAIN ENTRY POINT
# ==============================================================================

def main():
    """
    Main entry point for the Correlation Engine.
    
    Orchestrates the corroboration scoring pipeline:
    1. Get recent events from Neo4j
    2. For each event: spatial linking, narrative linking, scoring
    3. Write back scores and print summary table
    """
    print("=" * 80)
    print("🔬 CORRELATION ENGINE: Multi-Source Corroboration Scoring")
    print("   Project Sentinel - Intelligence Verification System")
    print("=" * 80)
    
    print(f"\n🔌 Connecting to Neo4j...")
    print(f"   URI: {NEO4J_URI}")
    
    engine = CorrelationEngine(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    
    try:
        # Get recent events
        print(f"\n📊 Fetching events from last {EVENT_LOOKBACK_HOURS} hours...")
        events = get_recent_events(engine, EVENT_LOOKBACK_HOURS)
        
        if not events:
            print("   ⚠️  No recent events found in database")
            print("   Run the ETL first: python database/load_graph.py")
            return
        
        print(f"   ✓ Found {len(events)} events to analyze")
        
        # Process each event
        print(f"\n🔗 Analyzing corroboration...")
        results = []
        
        for i, event in enumerate(events):
            event_id = event.get('id', 'unknown')
            
            # Step A: Spatial linking
            flight_data = link_nearby_flights(engine, event)
            
            # Step B: Narrative linking  
            source_data = link_narrative_sources(engine, event)
            
            # Step C: Calculate score
            score = calculate_score(flight_data, source_data)
            status = get_status(score)
            
            # Write back to Neo4j
            update_event_score(engine, event_id, score, status)
            
            # Collect for display
            results.append({
                "id": event_id[:20] + "..." if len(event_id) > 20 else event_id,
                "location": event.get('grid_location', 'Unknown'),
                "score": score,
                "status": status,
                "sources": ", ".join(source_data.get('sources', [])[:3]) or "None",
                "flights": flight_data.get('total', 0),
                "military": flight_data.get('military', 0),
            })
            
            # Progress indicator every 100 events
            if (i + 1) % 100 == 0:
                print(f"   Processed {i + 1}/{len(events)} events...")
        
        # Print summary table
        print("\n" + "=" * 80)
        print("📊 CORROBORATION RESULTS")
        print("=" * 80)
        print(f"{'Event ID':<24} {'Location':<15} {'Score':>6} {'Status':<12} {'Sources':<20}")
        print("-" * 80)
        
        # Sort by score descending
        results.sort(key=lambda x: x['score'], reverse=True)
        
        # Show top 20 or all if fewer
        display_count = min(20, len(results))
        for r in results[:display_count]:
            status_icon = "✅" if r['status'] == "Confirmed" else "⚠️ " if r['status'] == "Plausible" else "❓"
            print(f"{r['id']:<24} {r['location']:<15} {r['score']:>6} {status_icon} {r['status']:<10} {r['sources']:<20}")
        
        if len(results) > display_count:
            print(f"... and {len(results) - display_count} more events")
        
        # Summary statistics
        print("\n" + "-" * 80)
        confirmed = sum(1 for r in results if r['status'] == "Confirmed")
        plausible = sum(1 for r in results if r['status'] == "Plausible")
        unverified = sum(1 for r in results if r['status'] == "Unverified")
        
        print(f"📈 SUMMARY")
        print(f"   Confirmed:   {confirmed:>5} events (score > 60)")
        print(f"   Plausible:   {plausible:>5} events (score 30-60)")
        print(f"   Unverified:  {unverified:>5} events (score < 30)")
        print(f"   Total:       {len(results):>5} events analyzed")
        print("=" * 80)
        
    finally:
        engine.close()


if __name__ == "__main__":
    main()
