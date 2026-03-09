#!/usr/bin/env python3
"""
==============================================================================
GEOLOCATION EXTRACTOR
==============================================================================

Extracts location data from articles for map display.

Features:
1. Keyword-based location detection (fast, no API)
2. Geocoding to lat/lng coordinates
3. Location confidence scoring

Usage:
    from analysis.geolocate import extract_locations, add_locations_to_articles
    
    # Single article
    locations = extract_locations(article)
    
    # Batch processing
    articles = add_locations_to_articles(scored_articles)

==============================================================================
"""

import re
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

# ==============================================================================
# LOCATION DATABASE - Middle East Focus
# ==============================================================================

# Major cities and regions with coordinates
# Format: name -> (lat, lng, country, location_type)
LOCATION_DB: Dict[str, Tuple[float, float, str, str]] = {
    # Israel
    "tel aviv": (32.0853, 34.7818, "Israel", "city"),
    "jerusalem": (31.7683, 35.2137, "Israel", "city"),
    "haifa": (32.7940, 34.9896, "Israel", "city"),
    "beer sheva": (31.2518, 34.7913, "Israel", "city"),
    "beersheba": (31.2518, 34.7913, "Israel", "city"),
    "eilat": (29.5577, 34.9519, "Israel", "city"),
    "netanya": (32.3215, 34.8532, "Israel", "city"),
    "ashkelon": (31.6688, 34.5743, "Israel", "city"),
    "ashdod": (31.8044, 34.6553, "Israel", "city"),
    
    # Palestine/Gaza
    "gaza": (31.5017, 34.4668, "Palestine", "region"),
    "gaza city": (31.5017, 34.4668, "Palestine", "city"),
    "gaza strip": (31.3547, 34.3088, "Palestine", "region"),
    "rafah": (31.2969, 34.2455, "Palestine", "city"),
    "khan yunis": (31.3462, 34.3060, "Palestine", "city"),
    "khan younis": (31.3462, 34.3060, "Palestine", "city"),
    "jabalia": (31.5314, 34.4831, "Palestine", "city"),
    "beit hanoun": (31.5390, 34.5356, "Palestine", "city"),
    "deir al-balah": (31.4167, 34.3500, "Palestine", "city"),
    "nuseirat": (31.4500, 34.3900, "Palestine", "city"),
    "west bank": (31.9522, 35.2332, "Palestine", "region"),
    "ramallah": (31.9038, 35.2034, "Palestine", "city"),
    "nablus": (32.2211, 35.2544, "Palestine", "city"),
    "hebron": (31.5326, 35.0998, "Palestine", "city"),
    "bethlehem": (31.7054, 35.2024, "Palestine", "city"),
    "jenin": (32.4607, 35.3008, "Palestine", "city"),
    
    # Lebanon
    "beirut": (33.8938, 35.5018, "Lebanon", "city"),
    "lebanon": (33.8547, 35.8623, "Lebanon", "country"),
    "tripoli": (34.4333, 35.8500, "Lebanon", "city"),
    "sidon": (33.5631, 35.3697, "Lebanon", "city"),
    "tyre": (33.2705, 35.2038, "Lebanon", "city"),
    "baalbek": (34.0047, 36.2110, "Lebanon", "city"),
    "nabatieh": (33.3772, 35.4836, "Lebanon", "city"),
    "dahiyeh": (33.8500, 35.4800, "Lebanon", "city"),
    
    # Syria
    "syria": (34.8021, 38.9968, "Syria", "country"),
    "damascus": (33.5138, 36.2765, "Syria", "city"),
    "aleppo": (36.2021, 37.1343, "Syria", "city"),
    "homs": (34.7324, 36.7137, "Syria", "city"),
    "latakia": (35.5317, 35.7919, "Syria", "city"),
    "deir ez-zor": (35.3359, 40.1408, "Syria", "city"),
    "idlib": (35.9306, 36.6347, "Syria", "city"),
    "raqqa": (35.9594, 39.0100, "Syria", "city"),
    "daraa": (32.6189, 36.1021, "Syria", "city"),
    "golan": (33.0000, 35.7500, "Syria", "region"),
    "golan heights": (33.0000, 35.7500, "Syria", "region"),
    "hasakah": (36.5000, 40.7500, "Syria", "city"),
    "qamishli": (37.0506, 41.2261, "Syria", "city"),
    
    # Iran
    "iran": (32.4279, 53.6880, "Iran", "country"),
    "tehran": (35.6892, 51.3890, "Iran", "city"),
    "isfahan": (32.6546, 51.6680, "Iran", "city"),
    "shiraz": (29.5918, 52.5836, "Iran", "city"),
    "tabriz": (38.0800, 46.2919, "Iran", "city"),
    "mashhad": (36.2605, 59.6168, "Iran", "city"),
    "qom": (34.6416, 50.8746, "Iran", "city"),
    "natanz": (33.5125, 51.9164, "Iran", "city"),
    "bushehr": (28.9684, 50.8385, "Iran", "city"),
    "bandar abbas": (27.1832, 56.2666, "Iran", "city"),
    "ahvaz": (31.3183, 48.6706, "Iran", "city"),
    "kermanshah": (34.3142, 47.0650, "Iran", "city"),
    "arak": (34.0917, 49.6892, "Iran", "city"),
    "fordo": (34.8889, 51.0167, "Iran", "facility"),
    
    # Iraq
    "iraq": (33.2232, 43.6793, "Iraq", "country"),
    "baghdad": (33.3152, 44.3661, "Iraq", "city"),
    "basra": (30.5085, 47.7804, "Iraq", "city"),
    "mosul": (36.3350, 43.1189, "Iraq", "city"),
    "erbil": (36.1911, 44.0094, "Iraq", "city"),
    "kirkuk": (35.4681, 44.3922, "Iraq", "city"),
    "najaf": (32.0000, 44.3360, "Iraq", "city"),
    "karbala": (32.6160, 44.0249, "Iraq", "city"),
    "sulaymaniyah": (35.5613, 45.4306, "Iraq", "city"),
    
    # Jordan
    "jordan": (30.5852, 36.2384, "Jordan", "country"),
    "amman": (31.9454, 35.9284, "Jordan", "city"),
    "aqaba": (29.5267, 35.0078, "Jordan", "city"),
    "zarqa": (32.0728, 36.0880, "Jordan", "city"),
    "irbid": (32.5556, 35.8500, "Jordan", "city"),
    
    # Egypt
    "egypt": (26.8206, 30.8025, "Egypt", "country"),
    "cairo": (30.0444, 31.2357, "Egypt", "city"),
    "alexandria": (31.2001, 29.9187, "Egypt", "city"),
    "sinai": (29.5000, 34.0000, "Egypt", "region"),
    "suez": (29.9668, 32.5498, "Egypt", "city"),
    "suez canal": (30.4550, 32.3500, "Egypt", "landmark"),
    "port said": (31.2653, 32.3019, "Egypt", "city"),
    "sharm el-sheikh": (27.9158, 34.3300, "Egypt", "city"),
    
    # Saudi Arabia
    "saudi arabia": (23.8859, 45.0792, "Saudi Arabia", "country"),
    "riyadh": (24.7136, 46.6753, "Saudi Arabia", "city"),
    "jeddah": (21.4858, 39.1925, "Saudi Arabia", "city"),
    "mecca": (21.3891, 39.8579, "Saudi Arabia", "city"),
    "medina": (24.5247, 39.5692, "Saudi Arabia", "city"),
    "dammam": (26.4207, 50.0888, "Saudi Arabia", "city"),
    
    # Yemen
    "yemen": (15.5527, 48.5164, "Yemen", "country"),
    "sanaa": (15.3694, 44.1910, "Yemen", "city"),
    "aden": (12.7797, 45.0095, "Yemen", "city"),
    "hodeidah": (14.7980, 42.9540, "Yemen", "city"),
    "marib": (15.4542, 45.3261, "Yemen", "city"),
    "taiz": (13.5789, 44.0219, "Yemen", "city"),
    
    # Turkey
    "turkey": (38.9637, 35.2433, "Turkey", "country"),
    "ankara": (39.9334, 32.8597, "Turkey", "city"),
    "istanbul": (41.0082, 28.9784, "Turkey", "city"),
    "incirlik": (37.0017, 35.4259, "Turkey", "military"),
    "diyarbakir": (37.9144, 40.2306, "Turkey", "city"),
    "gaziantep": (37.0662, 37.3833, "Turkey", "city"),
    
    # UAE
    "uae": (23.4241, 53.8478, "UAE", "country"),
    "united arab emirates": (23.4241, 53.8478, "UAE", "country"),
    "dubai": (25.2048, 55.2708, "UAE", "city"),
    "abu dhabi": (24.4539, 54.3773, "UAE", "city"),
    
    # Qatar
    "qatar": (25.3548, 51.1839, "Qatar", "country"),
    "doha": (25.2854, 51.5310, "Qatar", "city"),
    
    # Kuwait
    "kuwait": (29.3759, 47.9774, "Kuwait", "country"),
    "kuwait city": (29.3759, 47.9774, "Kuwait", "city"),
    
    # Bahrain
    "bahrain": (26.0667, 50.5577, "Bahrain", "country"),
    "manama": (26.2285, 50.5860, "Bahrain", "city"),
    
    # Key waterways/regions
    "red sea": (20.0000, 38.0000, "International", "water"),
    "persian gulf": (26.0000, 52.0000, "International", "water"),
    "strait of hormuz": (26.5667, 56.2500, "International", "water"),
    "bab el-mandeb": (12.5833, 43.3333, "International", "water"),
    "mediterranean": (35.0000, 18.0000, "International", "water"),
    
    # Military/Strategic
    "dimona": (31.0667, 35.0333, "Israel", "facility"),
    "negev": (30.8500, 34.7500, "Israel", "region"),
}

# Aliases for common variations
LOCATION_ALIASES = {
    "the gaza strip": "gaza strip",
    "tel-aviv": "tel aviv",
    "tel-aviv-yafo": "tel aviv",
    "teheran": "tehran",
    "sana'a": "sanaa",
    "idf": None,  # Not a location
    "hamas": None,
    "hezbollah": None,
    "houthi": None,
    "houthis": None,
}


@dataclass
class Location:
    """Extracted location with metadata."""
    name: str
    lat: float
    lng: float
    country: str
    location_type: str
    confidence: float  # 0-1
    mention_count: int = 1


def extract_locations(article: Dict) -> List[Location]:
    """
    Extract locations from an article's title and summary.
    
    Args:
        article: Article dict with 'title' and 'summary' fields
        
    Returns:
        List of Location objects sorted by confidence
    """
    # Combine text sources
    text = " ".join([
        article.get("title", ""),
        article.get("summary", ""),
    ]).lower()
    
    found_locations: Dict[str, Location] = {}
    
    # Search for each known location
    for loc_name, (lat, lng, country, loc_type) in LOCATION_DB.items():
        # Create regex pattern for whole word matching
        pattern = r'\b' + re.escape(loc_name) + r'\b'
        matches = re.findall(pattern, text, re.IGNORECASE)
        
        if matches:
            # Calculate confidence based on mention count and location type
            mention_count = len(matches)
            base_confidence = 0.5
            
            # Boost for cities (more specific)
            if loc_type == "city":
                base_confidence += 0.2
            elif loc_type == "facility":
                base_confidence += 0.3
            
            # Boost for title mentions
            if loc_name in article.get("title", "").lower():
                base_confidence += 0.2
            
            # Boost for multiple mentions
            if mention_count > 1:
                base_confidence += min(0.1 * mention_count, 0.2)
            
            confidence = min(base_confidence, 1.0)
            
            # Use canonical name (capitalized)
            canonical_name = loc_name.title()
            
            if canonical_name not in found_locations:
                found_locations[canonical_name] = Location(
                    name=canonical_name,
                    lat=lat,
                    lng=lng,
                    country=country,
                    location_type=loc_type,
                    confidence=confidence,
                    mention_count=mention_count
                )
            else:
                # Update existing location
                existing = found_locations[canonical_name]
                existing.mention_count += mention_count
                existing.confidence = min(existing.confidence + 0.1, 1.0)
    
    # Sort by confidence
    locations = sorted(
        found_locations.values(),
        key=lambda x: (x.confidence, x.mention_count),
        reverse=True
    )
    
    return locations


def add_locations_to_articles(articles: List[Dict]) -> List[Dict]:
    """
    Add location data to a list of articles.
    
    Adds:
    - locations: List of location dicts with lat/lng
    - primary_location: The highest-confidence location
    - location_count: Number of locations found
    
    Args:
        articles: List of article dicts
        
    Returns:
        Articles with location data added
    """
    print(f"\n🗺️  Extracting locations from {len(articles)} articles...")
    
    located_count = 0
    
    for article in articles:
        locations = extract_locations(article)
        
        if locations:
            located_count += 1
            
            # Convert to dicts for JSON serialization
            article["locations"] = [
                {
                    "name": loc.name,
                    "lat": loc.lat,
                    "lng": loc.lng,
                    "country": loc.country,
                    "type": loc.location_type,
                    "confidence": round(loc.confidence, 2),
                    "mentions": loc.mention_count
                }
                for loc in locations[:5]  # Top 5 locations
            ]
            
            # Primary location (highest confidence)
            primary = locations[0]
            article["primary_location"] = {
                "name": primary.name,
                "lat": primary.lat,
                "lng": primary.lng,
                "country": primary.country
            }
            article["location_count"] = len(locations)
        else:
            article["locations"] = []
            article["primary_location"] = None
            article["location_count"] = 0
    
    print(f"   ✓ Found locations in {located_count}/{len(articles)} articles")
    
    return articles


def get_map_markers(articles: List[Dict]) -> List[Dict]:
    """
    Generate map marker data from articles.
    
    Returns a list of markers suitable for frontend map display.
    Each marker aggregates articles at the same location.
    """
    # Group articles by primary location
    location_groups: Dict[str, List[Dict]] = {}
    
    for article in articles:
        primary = article.get("primary_location")
        if not primary:
            continue
            
        key = f"{primary['lat']},{primary['lng']}"
        if key not in location_groups:
            location_groups[key] = {
                "location": primary,
                "articles": []
            }
        location_groups[key]["articles"].append({
            "id": article.get("id"),
            "title": article.get("title"),
            "status": article.get("status"),
            "final_score": article.get("final_score"),
            "content_type": article.get("content_type")
        })
    
    # Convert to marker list
    markers = []
    for key, data in location_groups.items():
        loc = data["location"]
        articles_at_loc = data["articles"]
        
        # Determine marker priority based on article statuses
        has_verified = any(a["status"] == "Verified" for a in articles_at_loc)
        has_developing = any(a["status"] == "Developing" for a in articles_at_loc)
        
        if has_verified:
            priority = "high"
            color = "green"
        elif has_developing:
            priority = "medium"
            color = "yellow"
        else:
            priority = "low"
            color = "red"
        
        markers.append({
            "id": key,
            "lat": loc["lat"],
            "lng": loc["lng"],
            "name": loc["name"],
            "country": loc["country"],
            "article_count": len(articles_at_loc),
            "priority": priority,
            "color": color,
            "articles": articles_at_loc[:10]  # Top 10 articles
        })
    
    # Sort by article count (most active locations first)
    markers.sort(key=lambda x: x["article_count"], reverse=True)
    
    return markers


# ==============================================================================
# MAIN - Test the module
# ==============================================================================

if __name__ == "__main__":
    import json
    from pathlib import Path
    
    print("=" * 60)
    print("🗺️  GEOLOCATION EXTRACTOR - TEST")
    print("=" * 60)
    
    # Load scored articles
    data_path = Path(__file__).parent.parent.parent / "data" / "scored_articles.json"
    if not data_path.exists():
        print("❌ No scored_articles.json found. Run score_articles.py first.")
        exit(1)
    
    with open(data_path) as f:
        data = json.load(f)
    
    articles = data.get("items", [])
    
    # Add locations
    articles = add_locations_to_articles(articles)
    
    # Generate markers
    markers = get_map_markers(articles)
    
    print(f"\n📍 Generated {len(markers)} map markers:")
    for marker in markers[:10]:
        print(f"   {marker['name']}, {marker['country']}: "
              f"{marker['article_count']} articles ({marker['priority']})")
    
    # Save updated data
    data["items"] = articles
    data["map_markers"] = markers
    
    output_path = Path(__file__).parent.parent.parent / "data" / "scored_articles.json"
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"\n✅ Saved updated data with locations to {output_path}")
