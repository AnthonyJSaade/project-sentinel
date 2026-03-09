#!/usr/bin/env python3
"""
==============================================================================
PROJECT SENTINEL - API SERVER
==============================================================================

FastAPI backend serving scored news articles and Telegram posts.

Endpoints:
    GET /                   - API info and status
    GET /items              - List scored content (with filters)
    GET /items/{id}         - Get single item with full details
    GET /stats              - Summary statistics
    POST /refresh           - Trigger re-scoring of content

Run with:
    uvicorn api.main:app --reload --port 8000

==============================================================================
"""

import json
from pathlib import Path
from typing import Optional, List
from datetime import datetime

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ==============================================================================
# CONFIGURATION
# ==============================================================================

PROJECT_ROOT = Path(__file__).parent.parent.parent  # OverWatch/
DATA_DIR = PROJECT_ROOT / "data"
SCORED_DATA_PATH = DATA_DIR / "scored_articles.json"

# ==============================================================================
# PYDANTIC MODELS
# ==============================================================================

class ConfidenceBreakdown(BaseModel):
    verified: int
    likely_verified: int
    developing: int
    unverified: int
    unconfirmed: int


class Metadata(BaseModel):
    scored_at: str
    total_items: int
    news_articles: int
    telegram_posts: int
    confidence_breakdown: ConfidenceBreakdown
    ai_analyzed: int


class RelatedSource(BaseModel):
    """A source that reports on the same story."""
    source_id: str
    source_tier: int
    title: str
    content_type: str


class NewsroomDebateAnalysis(BaseModel):
    """Full newsroom debate output (for scores 46-74)."""
    analysis_type: str = "newsroom_debate"
    reporter: Optional[dict] = None
    fact_checker: Optional[dict] = None
    editor: Optional[dict] = None
    summary: Optional[str] = None
    recommendation: Optional[str] = None  # trust, caution, wait
    confidence: Optional[str] = None


class SourceComparisonAnalysis(BaseModel):
    """Source comparison output (for scores 75+)."""
    analysis_type: str = "source_comparison"
    comparison: Optional[dict] = None
    summary: Optional[str] = None
    core_agreement: Optional[str] = None
    potential_differences: Optional[List[str]] = None


class ContentItem(BaseModel):
    id: str
    source_id: str
    source_tier: int
    source_type: Optional[str] = None
    content_type: str  # "article" or "telegram"
    title: str
    summary: Optional[str] = None
    link: Optional[str] = None
    published_utc: Optional[str] = None
    
    # Scoring
    final_score: int
    status: str  # Verified, Likely Verified, Developing, Unverified, Unconfirmed
    base_score: int
    recency_bonus: int
    cross_ref_bonus: int
    cross_ref_count: int
    scoring_notes: List[str]
    
    # Related sources (which sources confirmed this story)
    related_sources: Optional[List[RelatedSource]] = None
    
    # AI Analysis - either newsroom debate or source comparison
    ai_analyzed: Optional[bool] = False
    ai_reasoning: Optional[str] = None  # Simple summary for backward compatibility
    ai_analysis: Optional[dict] = None  # Full debate/comparison output
    
    # Extra fields
    keywords: Optional[List[str]] = None
    priority: Optional[str] = None


class ContentListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: List[ContentItem]


class StatsResponse(BaseModel):
    metadata: Metadata
    status_counts: dict
    source_counts: dict


# ==============================================================================
# DATA LOADING
# ==============================================================================

def load_scored_data() -> dict:
    """Load scored content from JSON file."""
    if not SCORED_DATA_PATH.exists():
        raise HTTPException(
            status_code=503,
            detail="Scored data not available. Run: python analysis/score_articles.py"
        )
    
    with open(SCORED_DATA_PATH, 'r') as f:
        return json.load(f)


# ==============================================================================
# FASTAPI APP
# ==============================================================================

app = FastAPI(
    title="Project Sentinel API",
    description="Verified news from the Middle East with AI-powered credibility scoring",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure properly in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==============================================================================
# ENDPOINTS
# ==============================================================================

@app.get("/")
async def root():
    """API status and info."""
    try:
        data = load_scored_data()
        metadata = data.get("metadata", {})
        return {
            "name": "Project Sentinel API",
            "version": "1.0.0",
            "status": "operational",
            "last_scored": metadata.get("scored_at"),
            "total_items": metadata.get("total_items", 0),
            "endpoints": {
                "items": "/items",
                "item_detail": "/items/{id}",
                "stats": "/stats",
                "docs": "/docs"
            }
        }
    except HTTPException:
        return {
            "name": "Project Sentinel API",
            "version": "1.0.0",
            "status": "no_data",
            "message": "Run scoring pipeline first"
        }


@app.get("/items", response_model=ContentListResponse)
async def list_items(
    # Pagination
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    
    # Filters
    status: Optional[str] = Query(None, description="Filter by status: Verified, Likely Verified, Developing, Unverified, Unconfirmed"),
    content_type: Optional[str] = Query(None, description="Filter by type: article, telegram"),
    source: Optional[str] = Query(None, description="Filter by source_id (partial match)"),
    min_score: Optional[int] = Query(None, ge=0, le=100, description="Minimum score"),
    
    # Search
    q: Optional[str] = Query(None, description="Search in title/summary"),
    
    # Sorting
    sort_by: str = Query("score", description="Sort by: score, date, source"),
    order: str = Query("desc", description="Order: asc, desc")
):
    """
    List scored content with filtering and pagination.
    
    Examples:
    - GET /items?status=Verified - Only verified content
    - GET /items?content_type=telegram - Only Telegram posts
    - GET /items?q=iran&min_score=50 - Search for "iran" with score >= 50
    """
    data = load_scored_data()
    items = data.get("items", [])
    
    # Apply filters
    if status:
        items = [i for i in items if i.get("status", "").lower() == status.lower()]
    
    if content_type:
        items = [i for i in items if i.get("content_type") == content_type]
    
    if source:
        items = [i for i in items if source.lower() in i.get("source_id", "").lower()]
    
    if min_score is not None:
        items = [i for i in items if i.get("final_score", 0) >= min_score]
    
    if q:
        q_lower = q.lower()
        items = [i for i in items if 
                 q_lower in i.get("title", "").lower() or 
                 q_lower in i.get("summary", "").lower()]
    
    # Apply sorting
    if sort_by == "score":
        items = sorted(items, key=lambda x: x.get("final_score", 0), reverse=(order == "desc"))
    elif sort_by == "date":
        items = sorted(items, key=lambda x: x.get("published_utc", ""), reverse=(order == "desc"))
    elif sort_by == "source":
        items = sorted(items, key=lambda x: x.get("source_id", ""), reverse=(order == "desc"))
    
    # Pagination
    total = len(items)
    start = (page - 1) * page_size
    end = start + page_size
    paginated_items = items[start:end]
    
    return ContentListResponse(
        total=total,
        page=page,
        page_size=page_size,
        items=paginated_items
    )


@app.get("/items/{item_id}")
async def get_item(item_id: str):
    """
    Get a single item by ID with full details including AI reasoning.
    """
    data = load_scored_data()
    items = data.get("items", [])
    
    # Find item by ID
    for item in items:
        if item.get("id") == item_id:
            return item
    
    raise HTTPException(status_code=404, detail=f"Item not found: {item_id}")


@app.get("/stats", response_model=StatsResponse)
async def get_stats():
    """
    Get summary statistics about scored content.
    """
    data = load_scored_data()
    metadata = data.get("metadata", {})
    items = data.get("items", [])
    
    # Count by status
    status_counts = {}
    for item in items:
        status = item.get("status", "Unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
    
    # Count by source
    source_counts = {}
    for item in items:
        source = item.get("source_id", "unknown")
        source_counts[source] = source_counts.get(source, 0) + 1
    
    return StatsResponse(
        metadata=Metadata(**metadata),
        status_counts=status_counts,
        source_counts=source_counts
    )


@app.get("/verified")
async def get_verified_only(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100)
):
    """
    Shortcut to get only Verified and Likely Verified content.
    This is what most users will want to see.
    """
    data = load_scored_data()
    items = data.get("items", [])
    
    # Filter for high-confidence items
    verified = [i for i in items if i.get("status") in ["Verified", "Likely Verified"]]
    
    # Pagination
    total = len(verified)
    start = (page - 1) * page_size
    end = start + page_size
    
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": verified[start:end]
    }


@app.get("/breaking")
async def get_breaking():
    """
    Get recent Telegram posts that may be breaking news.
    Returns unverified social media content for users who want to see
    news as it breaks, before mainstream confirmation.
    """
    data = load_scored_data()
    items = data.get("items", [])
    
    # Get Telegram posts sorted by date
    telegram_posts = [i for i in items if i.get("content_type") == "telegram"]
    telegram_posts = sorted(
        telegram_posts, 
        key=lambda x: x.get("published_utc", ""), 
        reverse=True
    )
    
    return {
        "total": len(telegram_posts),
        "items": telegram_posts[:20],
        "disclaimer": "Breaking news from social media. May be unverified - check status labels."
    }


@app.get("/map/markers")
async def get_map_markers():
    """
    Get map marker data for the geospatial view.
    
    Returns markers with:
    - lat/lng coordinates
    - location name and country
    - article count at that location
    - priority level (high/medium/low)
    - list of articles at that location
    """
    data = load_scored_data()
    
    # Return pre-computed markers if available
    markers = data.get("map_markers", [])
    
    if markers:
        return {
            "total": len(markers),
            "markers": markers,
            "center": {"lat": 31.5, "lng": 35.5},  # Middle East center
            "zoom": 6
        }
    
    # If no markers, return empty with instructions
    return {
        "total": 0,
        "markers": [],
        "message": "No location data. Run: python analysis/geolocate.py"
    }


@app.get("/map/events")
async def get_map_events(
    location: Optional[str] = Query(None, description="Filter by location name"),
    min_score: Optional[int] = Query(None, ge=0, le=100)
):
    """
    Get articles with location data for map display.
    
    Returns articles that have been geolocated, suitable for
    plotting on a map or filtering by region.
    """
    data = load_scored_data()
    items = data.get("items", [])
    
    # Filter for items with location data
    geolocated = [i for i in items if i.get("primary_location")]
    
    if location:
        location_lower = location.lower()
        geolocated = [
            i for i in geolocated 
            if location_lower in i.get("primary_location", {}).get("name", "").lower()
            or location_lower in i.get("primary_location", {}).get("country", "").lower()
        ]
    
    if min_score is not None:
        geolocated = [i for i in geolocated if i.get("final_score", 0) >= min_score]
    
    # Sort by score
    geolocated = sorted(geolocated, key=lambda x: x.get("final_score", 0), reverse=True)
    
    return {
        "total": len(geolocated),
        "items": geolocated[:100]  # Limit for performance
    }


# ==============================================================================
# MAIN
# ==============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)

