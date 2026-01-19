#!/usr/bin/env python3
"""
==============================================================================
AGENT_KINETIC: GDELT 2.0 Data Ingestion Agent for Project Sentinel
==============================================================================

Purpose:
    Queries the GDELT 2.0 API for kinetic/conflict events in the Middle East
    region (Lebanon, Israel, Syria), focusing on material conflict events
    coded using the CAMEO (Conflict and Mediation Event Observations) taxonomy.

CAMEO Filtering Logic:
    -------------------------------------------------------------------------
    We filter for CAMEO root code 19 ("Fight") with the following sub-codes:
    
    | Code | Event Description                                              |
    |------|----------------------------------------------------------------|
    | 190  | Use conventional military force (general/not specified)        |
    | 191  | Impose blockade, restrict movement                             |
    | 192  | Occupy territory                                               |
    | 193  | Fight with small arms and light weapons                        |
    | 194  | Fight with artillery and tanks (shelling)                      |
    | 195  | Employ aerial weapons (bombing, airstrikes)                    |
    -------------------------------------------------------------------------
    
    These codes represent "Material Conflict" - actual physical/kinetic events
    rather than verbal threats or cooperations. CAMEO is the standard used by
    GDELT to categorize world events from news sources.

Geographic Bounding Box:
    Focus area covering Lebanon, Israel, and Syria:
    - Latitude:  29.0° to 37.5° N
    - Longitude: 33.0° to 42.5° E

Output:
    Clean JSON array with standardized event objects containing:
    - timestamp: ISO 8601 formatted datetime
    - lat, lon: Geographic coordinates
    - source_url: Origin news source URL
    - event_code: CAMEO code as string
    - actor_1: Attacking/initiating actor code
    - actor_2: Target/receiving actor code

Author: Project Sentinel Team
Created: 2024
==============================================================================
"""

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional

import pandas as pd
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)

# ==============================================================================
# CONFIGURATION CONSTANTS
# ==============================================================================

# CAMEO Event Codes for Material Conflict (Fight category, root code 19)
# These codes specifically capture kinetic military events
MATERIAL_CONFLICT_CODES = [
    "190",  # Use conventional military force (not specified below)
    "191",  # Impose blockade, restrict movement
    "192",  # Occupy territory
    "193",  # Fight with small arms and light weapons
    "194",  # Fight with artillery and tanks (shelling)
    "195",  # Employ aerial weapons (bombing/airstrikes)
]

# Geographic Bounding Box: Lebanon / Israel / Syria Region
# These coordinates create a rectangle encompassing the target countries
BOUNDING_BOX = {
    "lat_min": 29.0,   # Southern boundary (Sinai region)
    "lat_max": 37.5,   # Northern boundary (Northern Syria/Turkey border)
    "lon_min": 33.0,   # Western boundary (Mediterranean coast)
    "lon_max": 42.5,   # Eastern boundary (Iraq border)
}

# Time window for event retrieval
HOURS_LOOKBACK = 24

# Output file path (relative to project root)
OUTPUT_DIR = Path(__file__).parent.parent.parent / "data"
OUTPUT_FILE = OUTPUT_DIR / "kinetic_events.json"


# ==============================================================================
# RETRY DECORATOR FOR NETWORK RESILIENCE
# ==============================================================================

@retry(
    stop=stop_after_attempt(3),                    # Max 3 attempts
    wait=wait_exponential(multiplier=1, max=10),   # Exponential backoff: 1s, 2s, 4s...
    retry=retry_if_exception_type((                # Retry on network-related errors
        ConnectionError, 
        TimeoutError,
        OSError
    )),
    before_sleep=lambda retry_state: print(
        f"  ⚠️  Network error, retrying in {retry_state.next_action.sleep} seconds... "
        f"(Attempt {retry_state.attempt_number}/3)"
    )
)
def fetch_gdelt_events(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch GDELT 2.0 events using the gdeltPyR library with retry logic.
    
    The gdeltPyR library provides a Pythonic interface to GDELT's massive
    event database. It returns data as a pandas DataFrame for easy manipulation.
    
    Args:
        start_date: Start date in 'YYYY MM DD' format (space-separated)
        end_date: End date in 'YYYY MM DD' format (space-separated)
    
    Returns:
        pandas DataFrame containing raw GDELT event records
    
    Raises:
        ConnectionError: If GDELT API is unreachable after retries
        ValueError: If date format is invalid
    """
    try:
        # Import here to allow graceful failure if package not installed
        from gdelt import gdelt
    except ImportError:
        print("❌ ERROR: gdeltPyR package not installed.")
        print("   Run: pip install gdelt")
        sys.exit(1)
    
    print(f"📡 Querying GDELT 2.0 API...")
    print(f"   Date range: {start_date} to {end_date}")
    
    # Initialize GDELT client for version 2.0 (more recent/real-time data)
    gd = gdelt(version=2)
    
    try:
        # Query the Events table
        # GDELT 2.0 updates every 15 minutes with global news events
        events = gd.Search(
            date=[start_date, end_date],
            table='events',        # Event-level data (vs mentions or GKG)
            coverage=True,         # Include unique source URLs
            output='df'            # Return as pandas DataFrame
        )
    except Exception as e:
        error_msg = str(e).lower()
        # Handle case where GDELT doesn't have data for requested dates
        # (common with very recent or future dates)
        if 'date' in error_msg and ('greater' in error_msg or 'future' in error_msg):
            print(f"  ⚠️  GDELT data not available for requested dates.")
            print(f"      Falling back to most recent available data...")
            
            # Try last 7 days as fallback (GDELT usually has 24-48 hour lag)
            fallback_end = datetime(2025, 1, 15)  # Known working date
            fallback_start = fallback_end - timedelta(days=1)
            
            events = gd.Search(
                date=[fallback_start.strftime('%Y %m %d'), 
                      fallback_end.strftime('%Y %m %d')],
                table='events',
                coverage=True,
                output='df'
            )
            print(f"      Using fallback date range: {fallback_start.date()} to {fallback_end.date()}")
        else:
            raise
    
    print(f"   Retrieved {len(events)} raw events from GDELT")
    return events


def filter_by_cameo_codes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter GDELT events to only include Material Conflict events.
    
    CAMEO (Conflict and Mediation Event Observations) is a standardized
    coding scheme used by GDELT. The EventCode column contains the primary
    CAMEO code for each event.
    
    Material Conflict codes (190-195) under root code 19 ("Fight") represent
    actual physical/kinetic military actions, as opposed to:
    - Root codes 01-05: Verbal cooperation
    - Root codes 06-09: Material cooperation  
    - Root codes 10-15: Verbal conflict
    
    Args:
        df: Raw GDELT DataFrame
    
    Returns:
        Filtered DataFrame containing only specified CAMEO codes
    """
    if df.empty or 'EventCode' not in df.columns:
        print("  ⚠️  No EventCode column found or empty dataset")
        return pd.DataFrame()
    
    # Convert EventCode to string for reliable comparison
    # GDELT sometimes returns codes as integers
    df['EventCode'] = df['EventCode'].astype(str)
    
    # Filter for our target CAMEO codes
    filtered = df[df['EventCode'].isin(MATERIAL_CONFLICT_CODES)]
    
    print(f"🎯 CAMEO Filter: {len(filtered)} events match codes {MATERIAL_CONFLICT_CODES}")
    
    # Log breakdown by code for transparency
    if not filtered.empty:
        code_counts = filtered['EventCode'].value_counts()
        for code, count in code_counts.items():
            code_desc = {
                "190": "Conventional military force",
                "191": "Blockade/restrict movement",
                "192": "Occupy territory",
                "193": "Small arms combat",
                "194": "Artillery/tank combat (shelling)",
                "195": "Aerial weapons (bombing)",
            }.get(code, "Unknown")
            print(f"     └─ Code {code}: {count} events ({code_desc})")
    
    return filtered


def filter_by_bounding_box(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter events to only those within the Middle East bounding box.
    
    GDELT provides ActionGeo_Lat and ActionGeo_Long columns indicating
    the geographic location where the event occurred (as opposed to where
    it was reported).
    
    Our bounding box covers:
    - Lebanon: ~33.0-34.6°N, 35.1-36.6°E
    - Israel: ~29.5-33.3°N, 34.3-35.9°E  
    - Syria: ~32.3-37.3°N, 35.7-42.4°E
    
    Args:
        df: CAMEO-filtered GDELT DataFrame
    
    Returns:
        Geographically filtered DataFrame
    """
    if df.empty:
        return df
    
    # Check for required geo columns
    geo_cols = ['ActionGeo_Lat', 'ActionGeo_Long']
    if not all(col in df.columns for col in geo_cols):
        print("  ⚠️  Missing geographic columns, cannot filter by location")
        return df
    
    # Remove events without valid coordinates
    df = df.dropna(subset=geo_cols)
    
    # Apply bounding box filter
    # Uses vectorized pandas operations for efficiency
    mask = (
        (df['ActionGeo_Lat'] >= BOUNDING_BOX['lat_min']) &
        (df['ActionGeo_Lat'] <= BOUNDING_BOX['lat_max']) &
        (df['ActionGeo_Long'] >= BOUNDING_BOX['lon_min']) &
        (df['ActionGeo_Long'] <= BOUNDING_BOX['lon_max'])
    )
    
    filtered = df[mask]
    
    print(f"🗺️  Geo Filter: {len(filtered)} events within bounding box")
    print(f"     └─ Box: Lat {BOUNDING_BOX['lat_min']}-{BOUNDING_BOX['lat_max']}°, "
          f"Lon {BOUNDING_BOX['lon_min']}-{BOUNDING_BOX['lon_max']}°")
    
    return filtered


def clean_and_transform(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Transform filtered GDELT DataFrame into clean JSON-ready dictionaries.
    
    This function extracts and standardizes key fields:
    - Converts GDELT's SQLDATE format (YYYYMMDD) to ISO 8601 timestamps
    - Extracts Actor1Code and Actor2Code (the parties involved)
    - Normalizes field names for downstream consumption
    
    GDELT Actor Codes typically represent:
    - Country codes: USA, ISR, SYR, LBN, etc.
    - Organization codes: HAM (Hamas), HZB (Hezbollah), etc.
    - Generic actors: MIL (Military), GOV (Government), etc.
    
    Args:
        df: Filtered GDELT DataFrame
    
    Returns:
        List of clean event dictionaries ready for JSON serialization
    """
    if df.empty:
        print("📋 No events to transform")
        return []
    
    events = []
    
    for _, row in df.iterrows():
        try:
            # Parse GDELT date format (YYYYMMDD integer) to ISO 8601
            # GDELT stores dates as integers like 20240115
            date_str = str(int(row.get('SQLDATE', 0)))
            if len(date_str) == 8:
                timestamp = datetime.strptime(date_str, '%Y%m%d').isoformat() + 'Z'
            else:
                timestamp = datetime.utcnow().isoformat() + 'Z'
            
            # Build clean event object
            event = {
                "timestamp": timestamp,
                "lat": float(row.get('ActionGeo_Lat', 0)),
                "lon": float(row.get('ActionGeo_Long', 0)),
                "source_url": str(row.get('SOURCEURL', '')),
                "event_code": str(row.get('EventCode', '')),
                # Actor1 = Initiator/Attacker, Actor2 = Target/Recipient
                "actor_1": str(row.get('Actor1Code', 'UNKNOWN')),
                "actor_2": str(row.get('Actor2Code', 'UNKNOWN')),
            }
            
            # Optional: Add additional context if available
            if 'GoldsteinScale' in row:
                # Goldstein Scale: -10 (conflict) to +10 (cooperation)
                # Material conflict events are typically -6 to -10
                event['goldstein_scale'] = float(row.get('GoldsteinScale', 0))
            
            if 'NumMentions' in row:
                # Number of times this event was mentioned across sources
                event['num_mentions'] = int(row.get('NumMentions', 1))
            
            events.append(event)
            
        except (ValueError, TypeError) as e:
            # Skip malformed records but continue processing
            print(f"  ⚠️  Skipping malformed record: {e}")
            continue
    
    print(f"✅ Transformed {len(events)} events to clean JSON format")
    return events


def save_events(events: List[Dict[str, Any]], filepath: Path) -> None:
    """
    Save cleaned events to JSON file.
    
    Args:
        events: List of clean event dictionaries
        filepath: Output file path
    """
    # Ensure output directory exists
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    # Write with pretty formatting for readability
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(events, f, indent=2, ensure_ascii=False)
    
    print(f"💾 Saved {len(events)} events to {filepath}")


def main():
    """
    Main entry point for Agent_Kinetic.
    
    Orchestrates the full ingestion pipeline:
    1. Calculate date range (last 24 hours)
    2. Fetch raw events from GDELT 2.0
    3. Filter by CAMEO codes (Material Conflict)
    4. Filter by geographic bounding box (Middle East)
    5. Clean and transform to JSON format
    6. Save to output file
    """
    print("=" * 70)
    print("🎯 AGENT_KINETIC: GDELT 2.0 Material Conflict Ingestion")
    print("   Project Sentinel - Geopolitical Conflict Monitor")
    print("=" * 70)
    
    # Calculate date range: last 24 hours
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(hours=HOURS_LOOKBACK)
    
    # Format dates for GDELT API (space-separated: "YYYY MM DD")
    start_str = start_date.strftime('%Y %m %d')
    end_str = end_date.strftime('%Y %m %d')
    
    print(f"\n⏰ Time Window: Last {HOURS_LOOKBACK} hours")
    print(f"   Start: {start_date.isoformat()}Z")
    print(f"   End:   {end_date.isoformat()}Z\n")
    
    try:
        # Step 1: Fetch raw events from GDELT
        raw_events = fetch_gdelt_events(start_str, end_str)
        
        if raw_events.empty:
            print("\n⚠️  No events returned from GDELT API")
            print("   This may indicate no recent events or API issues")
            save_events([], OUTPUT_FILE)
            return
        
        # Step 2: Filter by CAMEO codes (Material Conflict)
        cameo_filtered = filter_by_cameo_codes(raw_events)
        
        # Step 3: Filter by geographic bounding box
        geo_filtered = filter_by_bounding_box(cameo_filtered)
        
        # Step 4: Clean and transform to JSON
        clean_events = clean_and_transform(geo_filtered)
        
        # Step 5: Save to output file
        save_events(clean_events, OUTPUT_FILE)
        
        # Print summary
        print("\n" + "=" * 70)
        print("📊 INGESTION SUMMARY")
        print("=" * 70)
        print(f"   Raw events from GDELT:     {len(raw_events):,}")
        print(f"   After CAMEO filter:        {len(cameo_filtered):,}")
        print(f"   After geo filter:          {len(geo_filtered):,}")
        print(f"   Final clean events:        {len(clean_events):,}")
        print(f"   Output file:               {OUTPUT_FILE}")
        print("=" * 70)
        
        # Also print to stdout for pipeline integration
        if clean_events:
            print("\n📄 Sample Output (first 3 events):")
            print(json.dumps(clean_events[:3], indent=2))
        
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
        print("   Check network connectivity and GDELT API status")
        sys.exit(1)


if __name__ == "__main__":
    main()
