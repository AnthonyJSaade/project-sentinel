/**
 * API utilities for fetching data from the backend
 */

const API_BASE = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

export interface ContentItem {
    id: string;
    source_id: string;
    source_tier: number;
    source_type?: string;
    content_type: 'article' | 'telegram';
    title: string;
    summary?: string;
    link?: string;
    published_utc?: string;
    final_score: number;
    status: 'Verified' | 'Likely Verified' | 'Developing' | 'Unverified' | 'Unconfirmed';
    base_score: number;
    recency_bonus: number;
    cross_ref_bonus: number;
    cross_ref_count: number;
    scoring_notes: string[];
    related_sources?: RelatedSource[];
    ai_analyzed?: boolean;
    ai_reasoning?: string;
    ai_analysis?: AIAnalysis;
    primary_location?: Location;
    locations?: Location[];
    keywords?: string[];
}

export interface RelatedSource {
    source_id: string;
    source_tier: number;
    title: string;
    content_type: string;
}

export interface Location {
    name: string;
    lat: number;
    lng: number;
    country: string;
    type?: string;
    confidence?: number;
    mentions?: number;
}

export interface AIAnalysis {
    analysis_type: 'newsroom_debate' | 'source_comparison';
    reporter?: Record<string, unknown>;
    fact_checker?: Record<string, unknown>;
    editor?: Record<string, unknown>;
    comparison?: Record<string, unknown>;
    summary?: string;
    recommendation?: 'trust' | 'caution' | 'wait';
}

export interface MapMarker {
    id: string;
    lat: number;
    lng: number;
    name: string;
    country: string;
    article_count: number;
    priority: 'high' | 'medium' | 'low';
    color: 'green' | 'yellow' | 'red';
    articles: {
        id: string;
        title: string;
        status: string;
        final_score: number;
        content_type: string;
    }[];
}

export interface ContentListResponse {
    total: number;
    page: number;
    page_size: number;
    items: ContentItem[];
}

export interface MapMarkersResponse {
    total: number;
    markers: MapMarker[];
    center: { lat: number; lng: number };
    zoom: number;
}

export interface StatsResponse {
    metadata: {
        scored_at: string;
        total_items: number;
        news_articles: number;
        telegram_posts: number;
        geolocated_items?: number;
        confidence_breakdown: {
            verified: number;
            likely_verified: number;
            developing: number;
            unverified: number;
            unconfirmed: number;
        };
        ai_analyzed: number;
    };
    status_counts: Record<string, number>;
    source_counts: Record<string, number>;
}

/**
 * Fetch items from the API with optional filters
 */
export async function fetchItems(params?: {
    page?: number;
    page_size?: number;
    status?: string;
    content_type?: string;
    source?: string;
    min_score?: number;
    q?: string;
    sort_by?: 'score' | 'date' | 'source';
    order?: 'asc' | 'desc';
}): Promise<ContentListResponse> {
    const searchParams = new URLSearchParams();

    if (params) {
        Object.entries(params).forEach(([key, value]) => {
            if (value !== undefined && value !== null) {
                searchParams.set(key, String(value));
            }
        });
    }

    const url = `${API_BASE}/items?${searchParams.toString()}`;
    const res = await fetch(url, { next: { revalidate: 60 } });

    if (!res.ok) {
        throw new Error(`API error: ${res.status}`);
    }

    return res.json();
}

/**
 * Fetch a single item by ID
 */
export async function fetchItem(id: string): Promise<ContentItem> {
    const res = await fetch(`${API_BASE}/items/${id}`, { next: { revalidate: 60 } });

    if (!res.ok) {
        throw new Error(`API error: ${res.status}`);
    }

    return res.json();
}

/**
 * Fetch map markers
 */
export async function fetchMapMarkers(): Promise<MapMarkersResponse> {
    const res = await fetch(`${API_BASE}/map/markers`, { next: { revalidate: 60 } });

    if (!res.ok) {
        throw new Error(`API error: ${res.status}`);
    }

    return res.json();
}

/**
 * Fetch stats
 */
export async function fetchStats(): Promise<StatsResponse> {
    const res = await fetch(`${API_BASE}/stats`, { next: { revalidate: 60 } });

    if (!res.ok) {
        throw new Error(`API error: ${res.status}`);
    }

    return res.json();
}

/**
 * Format relative time (e.g., "2m ago", "1h ago")
 */
export function formatRelativeTime(dateString?: string): string {
    if (!dateString) return '';

    const date = new Date(dateString);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMins / 60);
    const diffDays = Math.floor(diffHours / 24);

    if (diffMins < 1) return 'just now';
    if (diffMins < 60) return `${diffMins}m ago`;
    if (diffHours < 24) return `${diffHours}h ago`;
    if (diffDays < 7) return `${diffDays}d ago`;

    return new Intl.DateTimeFormat('en-US', {
        month: 'short',
        day: 'numeric'
    }).format(date);
}

/**
 * Get status color class
 */
export function getStatusColor(status: string): string {
    switch (status) {
        case 'Verified':
        case 'Likely Verified':
            return 'accent-green';
        case 'Developing':
            return 'accent-amber';
        case 'Unverified':
        case 'Unconfirmed':
            return 'accent-red';
        default:
            return 'text-muted';
    }
}

/**
 * Get priority from score
 */
export function getPriority(score: number): 'critical' | 'high' | 'medium' | 'low' {
    if (score >= 85) return 'critical';
    if (score >= 70) return 'high';
    if (score >= 50) return 'medium';
    return 'low';
}
