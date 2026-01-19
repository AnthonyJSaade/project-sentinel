#!/usr/bin/env python3
"""
==============================================================================
AGENT_SKYWATCHER: OpenSky Network Flight Tracker for Project Sentinel
==============================================================================

Purpose:
    Queries the OpenSky Network API for real-time aircraft state vectors
    within the Middle East conflict zone (Israel/Lebanon/Syria border region).
    
    This agent provides aerial situational awareness to correlate with
    ground-level kinetic events from Agent_Kinetic (GDELT data).

API Reference:
    https://openskynetwork.github.io/opensky-api/rest.html
    
    State Vector Fields (from OpenSky):
    - icao24: ICAO 24-bit transponder address (hex string)
    - callsign: Aircraft callsign (8 chars, may be null)
    - origin_country: Country of registration
    - longitude/latitude: WGS-84 coordinates
    - geo_altitude: Geometric altitude in meters
    - velocity: Ground speed in m/s
    - true_track: Heading in degrees clockwise from north

Heuristic Filtering:
    Since we don't have a military ICAO database, we use proxy indicators:
    - High altitude (>10,000m) + High speed (>200 m/s) = Likely fast jet/military
    - These aircraft are tagged as "high_altitude_fast" for downstream analysis

Rate Limits:
    - Anonymous: 10 second resolution, 400 requests/day
    - Registered: 5 second resolution, unlimited requests
    
Author: Project Sentinel Team
Created: 2026
==============================================================================
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==============================================================================
# CONFIGURATION CONSTANTS
# ==============================================================================

# OpenSky Network API endpoint
OPENSKY_API_URL = "https://opensky-network.org/api/states/all"

# Geographic Bounding Box: Israel / Lebanon / Syria Border Region
# Slightly tighter than GDELT box to focus on the active conflict zone
BOUNDING_BOX = {
    "lamin": 29.0,   # Southern boundary (Southern Israel/Sinai)
    "lamax": 35.0,   # Northern boundary (Northern Lebanon/Syria)
    "lomin": 33.0,   # Western boundary (Mediterranean coast)
    "lomax": 39.0,   # Eastern boundary (Eastern Syria)
}

# Heuristic thresholds for "military proxy" tagging
# Aircraft meeting BOTH criteria are flagged as potentially significant
HIGH_ALTITUDE_THRESHOLD = 10000  # meters (~33,000 feet)
HIGH_VELOCITY_THRESHOLD = 200   # m/s (~720 km/h, ~390 knots)

# Output file path (relative to project root)
OUTPUT_DIR = Path(__file__).parent.parent.parent / "data"
OUTPUT_FILE = OUTPUT_DIR / "flight_radar.json"

# Request timeout in seconds
REQUEST_TIMEOUT = 30


# ==============================================================================
# STATE VECTOR FIELD INDICES
# ==============================================================================
# OpenSky returns state vectors as arrays, not objects
# These indices map to the array positions

class StateVectorIndex:
    """
    Indices for OpenSky state vector array.
    Reference: https://openskynetwork.github.io/opensky-api/rest.html#response
    """
    ICAO24 = 0          # Unique ICAO 24-bit address (hex)
    CALLSIGN = 1        # Callsign of the vehicle (8 chars)
    ORIGIN_COUNTRY = 2  # Country of registration
    TIME_POSITION = 3   # Unix timestamp of last position update
    LAST_CONTACT = 4    # Unix timestamp of last contact
    LONGITUDE = 5       # WGS-84 longitude
    LATITUDE = 6        # WGS-84 latitude
    BARO_ALTITUDE = 7   # Barometric altitude in meters
    ON_GROUND = 8       # Boolean, true if on ground
    VELOCITY = 9        # Ground speed in m/s
    TRUE_TRACK = 10     # Track angle in degrees (0=north)
    VERTICAL_RATE = 11  # Vertical rate in m/s
    SENSORS = 12        # IDs of sensors that contributed
    GEO_ALTITUDE = 13   # Geometric altitude in meters
    SQUAWK = 14         # Transponder code
    SPI = 15            # Special purpose indicator
    POSITION_SOURCE = 16  # Origin of position: 0=ADS-B, etc.


def create_retry_session() -> requests.Session:
    """
    Create a requests session with retry logic for network resilience.
    
    Implements exponential backoff for:
    - Connection errors
    - Timeout errors  
    - Server errors (500, 502, 503, 504)
    
    Returns:
        Configured requests.Session with retry adapter
    """
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=3,                    # Max 3 retry attempts
        backoff_factor=1,           # Wait 1s, 2s, 4s between retries
        status_forcelist=[429, 500, 502, 503, 504],  # Retry on these HTTP codes
        allowed_methods=["GET"],    # Only retry GET requests
    )
    
    # Mount adapter to session
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    return session


def fetch_flight_data() -> Optional[Dict[str, Any]]:
    """
    Query OpenSky Network API for aircraft in the bounding box.
    
    The API returns state vectors for all aircraft currently being
    tracked within the specified geographic area.
    
    Returns:
        JSON response dict containing 'time' and 'states' keys,
        or None if request failed
    
    Raises:
        requests.RequestException: On persistent network failures
    """
    print("=" * 70)
    print("✈️  AGENT_SKYWATCHER: OpenSky Network Flight Tracker")
    print("   Project Sentinel - Aerial Situational Awareness")
    print("=" * 70)
    
    # Build query parameters with bounding box
    params = {
        "lamin": BOUNDING_BOX["lamin"],
        "lamax": BOUNDING_BOX["lamax"],
        "lomin": BOUNDING_BOX["lomin"],
        "lomax": BOUNDING_BOX["lomax"],
    }
    
    print(f"\n📡 Querying OpenSky Network API...")
    print(f"   Bounding Box: Lat {params['lamin']}-{params['lamax']}°, "
          f"Lon {params['lomin']}-{params['lomax']}°")
    
    session = create_retry_session()
    
    try:
        response = session.get(
            OPENSKY_API_URL,
            params=params,
            timeout=REQUEST_TIMEOUT,
            headers={"Accept": "application/json"}
        )
        
        # Check for rate limiting
        if response.status_code == 429:
            print("  ⚠️  Rate limited by OpenSky API. Try again later.")
            return None
        
        response.raise_for_status()
        data = response.json()
        
        state_count = len(data.get("states", []) or [])
        print(f"   Retrieved {state_count} aircraft state vectors")
        
        return data
        
    except requests.exceptions.Timeout:
        print("  ❌ Request timed out. OpenSky may be experiencing high load.")
        return None
    except requests.exceptions.ConnectionError:
        print("  ❌ Connection error. Check network connectivity.")
        return None
    except requests.exceptions.HTTPError as e:
        print(f"  ❌ HTTP error: {e}")
        return None
    except json.JSONDecodeError:
        print("  ❌ Invalid JSON response from API")
        return None


def parse_state_vector(state: List) -> Optional[Dict[str, Any]]:
    """
    Parse a single OpenSky state vector array into a clean dictionary.
    
    Args:
        state: Array of values from OpenSky API response
        
    Returns:
        Parsed aircraft dict, or None if essential data is missing
    """
    try:
        # Skip if no position data (aircraft not transmitting location)
        if state[StateVectorIndex.LONGITUDE] is None or \
           state[StateVectorIndex.LATITUDE] is None:
            return None
        
        # Extract core fields
        aircraft = {
            "icao24": state[StateVectorIndex.ICAO24],
            "callsign": (state[StateVectorIndex.CALLSIGN] or "").strip(),
            "origin_country": state[StateVectorIndex.ORIGIN_COUNTRY],
            "longitude": float(state[StateVectorIndex.LONGITUDE]),
            "latitude": float(state[StateVectorIndex.LATITUDE]),
            "geo_altitude": state[StateVectorIndex.GEO_ALTITUDE],
            "velocity": state[StateVectorIndex.VELOCITY],
            "on_ground": state[StateVectorIndex.ON_GROUND],
            "true_track": state[StateVectorIndex.TRUE_TRACK],
            "vertical_rate": state[StateVectorIndex.VERTICAL_RATE],
        }
        
        # Apply heuristic filter: High altitude + Fast = Potentially significant
        # This is a proxy for military/fast jet detection without ICAO database
        altitude = aircraft["geo_altitude"] or 0
        velocity = aircraft["velocity"] or 0
        
        is_high_altitude = altitude > HIGH_ALTITUDE_THRESHOLD
        is_high_velocity = velocity > HIGH_VELOCITY_THRESHOLD
        
        if is_high_altitude and is_high_velocity:
            aircraft["tag"] = "high_altitude_fast"
            aircraft["tag_reason"] = (
                f"Alt: {altitude:.0f}m (>{HIGH_ALTITUDE_THRESHOLD}m), "
                f"Speed: {velocity:.0f}m/s (>{HIGH_VELOCITY_THRESHOLD}m/s)"
            )
        else:
            aircraft["tag"] = None
            aircraft["tag_reason"] = None
        
        return aircraft
        
    except (IndexError, TypeError, ValueError) as e:
        # Malformed state vector, skip it
        return None


def process_flight_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process raw OpenSky response into clean, structured output.
    
    Args:
        raw_data: Raw JSON response from OpenSky API
        
    Returns:
        Structured dict with metadata and parsed aircraft list
    """
    states = raw_data.get("states") or []
    api_time = raw_data.get("time", 0)
    
    # Parse all state vectors
    aircraft_list = []
    tagged_count = 0
    
    for state in states:
        aircraft = parse_state_vector(state)
        if aircraft:
            aircraft_list.append(aircraft)
            if aircraft.get("tag") == "high_altitude_fast":
                tagged_count += 1
    
    # Build output structure
    output = {
        "metadata": {
            "source": "OpenSky Network",
            "api_timestamp": api_time,
            "query_timestamp": datetime.now(timezone.utc).isoformat(),
            "bounding_box": BOUNDING_BOX,
            "total_aircraft": len(aircraft_list),
            "tagged_high_altitude_fast": tagged_count,
        },
        "aircraft": aircraft_list,
    }
    
    # Print summary
    print(f"\n🔍 Processing Results:")
    print(f"   Total aircraft with position: {len(aircraft_list)}")
    print(f"   Tagged as 'high_altitude_fast': {tagged_count}")
    
    # List tagged aircraft for visibility
    if tagged_count > 0:
        print(f"\n   ⚠️  High Altitude + Fast Aircraft:")
        for ac in aircraft_list:
            if ac.get("tag") == "high_altitude_fast":
                print(f"      └─ {ac['callsign'] or ac['icao24']}: "
                      f"{ac['origin_country']}, "
                      f"Alt: {ac['geo_altitude']:.0f}m, "
                      f"Speed: {ac['velocity']:.0f}m/s")
    
    return output


def save_flight_data(data: Dict[str, Any], filepath: Path) -> None:
    """
    Save processed flight data to JSON file.
    
    Args:
        data: Processed flight data dict
        filepath: Output file path
    """
    # Ensure output directory exists
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    # Write with pretty formatting
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Saved to {filepath}")


def main():
    """
    Main entry point for Agent_Skywatcher.
    
    Orchestrates the flight tracking pipeline:
    1. Query OpenSky Network API for aircraft in bounding box
    2. Parse state vectors into clean format
    3. Apply heuristic tagging for potentially significant aircraft
    4. Save to output file
    """
    # Fetch raw data from OpenSky
    raw_data = fetch_flight_data()
    
    if raw_data is None:
        print("\n❌ Failed to fetch flight data. Exiting.")
        sys.exit(1)
    
    if not raw_data.get("states"):
        print("\n📭 No aircraft found in the specified bounding box.")
        # Save empty result for consistency
        empty_result = {
            "metadata": {
                "source": "OpenSky Network",
                "query_timestamp": datetime.now(timezone.utc).isoformat(),
                "bounding_box": BOUNDING_BOX,
                "total_aircraft": 0,
                "tagged_high_altitude_fast": 0,
            },
            "aircraft": [],
        }
        save_flight_data(empty_result, OUTPUT_FILE)
        print(f"\n🎯 Found 0 aircraft in the danger zone.")
        return
    
    # Process and structure the data
    processed_data = process_flight_data(raw_data)
    
    # Save to output file
    save_flight_data(processed_data, OUTPUT_FILE)
    
    # Final summary
    total = processed_data["metadata"]["total_aircraft"]
    tagged = processed_data["metadata"]["tagged_high_altitude_fast"]
    
    print("\n" + "=" * 70)
    print(f"🎯 Found {total} aircraft in the danger zone.")
    if tagged > 0:
        print(f"   ⚠️  {tagged} aircraft tagged as high_altitude_fast")
    print("=" * 70)


if __name__ == "__main__":
    main()
