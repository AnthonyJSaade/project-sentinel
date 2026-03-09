'use client';

import { useState, useEffect } from 'react';
import { ContentItem, MapMarker, fetchItems, fetchMapMarkers } from '@/lib/api';
import NewsPanel from '@/components/news/NewsPanel';
import MapWrapper from '@/components/map/MapWrapper';

/**
 * Dashboard - Main client component
 * 
 * Fetches data and manages state for the news feed and map
 */
export function Dashboard() {
    const [items, setItems] = useState<ContentItem[]>([]);
    const [markers, setMarkers] = useState<MapMarker[]>([]);
    const [selectedItem, setSelectedItem] = useState<ContentItem | null>(null);
    const [selectedMarkerId, setSelectedMarkerId] = useState<string | undefined>();
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    // Fetch data on mount
    useEffect(() => {
        async function loadData() {
            try {
                setLoading(true);
                setError(null);

                // Fetch in parallel (Vercel best practice)
                const [itemsRes, markersRes] = await Promise.all([
                    fetchItems({ page_size: 100 }),
                    fetchMapMarkers(),
                ]);

                setItems(itemsRes.items);
                setMarkers(markersRes.markers);
            } catch (err) {
                console.error('Failed to load data:', err);
                setError(err instanceof Error ? err.message : 'Failed to load data');
            } finally {
                setLoading(false);
            }
        }

        loadData();
    }, []);

    // Handle item selection
    const handleSelectItem = (item: ContentItem) => {
        setSelectedItem(item);

        // If item has a location, highlight the marker
        if (item.primary_location) {
            const markerId = `${item.primary_location.lat},${item.primary_location.lng}`;
            setSelectedMarkerId(markerId);
        }
    };

    // Handle marker click
    const handleMarkerClick = (marker: MapMarker) => {
        setSelectedMarkerId(marker.id);

        // Select the first article at this location
        if (marker.articles.length > 0) {
            const firstArticle = items.find(i => i.id === marker.articles[0].id);
            if (firstArticle) {
                setSelectedItem(firstArticle);
            }
        }
    };

    if (loading) {
        return (
            <div className="h-screen w-screen flex items-center justify-center bg-bg-primary">
                <div className="text-center">
                    <div className="w-12 h-12 border-2 border-accent-red border-t-transparent rounded-full animate-spin mx-auto mb-4" />
                    <p className="text-text-secondary">Loading intelligence feed…</p>
                </div>
            </div>
        );
    }

    if (error) {
        return (
            <div className="h-screen w-screen flex items-center justify-center bg-bg-primary">
                <div className="text-center max-w-md px-4">
                    <div className="text-4xl mb-4">⚠️</div>
                    <h1 className="text-xl font-semibold text-text-primary mb-2">Connection Error</h1>
                    <p className="text-text-secondary mb-4">{error}</p>
                    <p className="text-sm text-text-muted">
                        Make sure the backend API is running at http://localhost:8000
                    </p>
                    <button
                        onClick={() => window.location.reload()}
                        className="mt-4 px-4 py-2 bg-accent-red text-white rounded-lg hover:bg-accent-red/90"
                    >
                        Retry
                    </button>
                </div>
            </div>
        );
    }

    return (
        <div className="h-screen w-screen flex flex-col bg-bg-primary overflow-hidden">
            {/* Header */}
            <header className="h-14 px-4 flex items-center justify-between border-b border-bg-border bg-bg-card shrink-0">
                <div className="flex items-center gap-3">
                    <span className="text-2xl">🔴</span>
                    <h1 className="text-xl font-bold text-gradient">OVERWATCH</h1>
                </div>

                <div className="flex items-center gap-4">
                    <div className="text-xs text-text-muted">
                        <span className="tabular-nums">{items.length}</span> stories tracked
                    </div>
                    <div className="flex items-center gap-2 text-xs">
                        <span className="w-2 h-2 bg-accent-green rounded-full animate-pulse-slow" />
                        <span className="text-accent-green">LIVE</span>
                    </div>
                </div>
            </header>

            {/* Main content */}
            <div className="flex-1 flex overflow-hidden">
                {/* News Panel - 30% width */}
                <div className="w-[30%] min-w-[320px] max-w-[420px] shrink-0">
                    <NewsPanel
                        items={items}
                        onSelectItem={handleSelectItem}
                        selectedItemId={selectedItem?.id}
                    />
                </div>

                {/* Map Panel - 70% width */}
                <div className="flex-1">
                    <MapWrapper
                        markers={markers}
                        onMarkerClick={handleMarkerClick}
                        selectedMarkerId={selectedMarkerId}
                    />
                </div>
            </div>
        </div>
    );
}

export default Dashboard;
