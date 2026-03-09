'use client';

import dynamic from 'next/dynamic';
import { MapMarker } from '@/lib/api';

// Dynamically import MapPanel with no SSR (Leaflet requires window)
const MapPanel = dynamic(
    () => import('./MapPanel'),
    {
        ssr: false,
        loading: () => (
            <div className="w-full h-full flex items-center justify-center bg-bg-card">
                <div className="text-center">
                    <div className="w-8 h-8 border-2 border-accent-red border-t-transparent rounded-full animate-spin mx-auto mb-3" />
                    <p className="text-sm text-text-muted">Loading map…</p>
                </div>
            </div>
        )
    }
);

interface MapWrapperProps {
    markers: MapMarker[];
    center?: { lat: number; lng: number };
    zoom?: number;
    onMarkerClick?: (marker: MapMarker) => void;
    selectedMarkerId?: string;
}

/**
 * MapWrapper - Client-only wrapper for MapPanel
 * 
 * Uses Next.js dynamic import to prevent SSR issues with Leaflet
 */
export function MapWrapper(props: MapWrapperProps) {
    return <MapPanel {...props} />;
}

export default MapWrapper;
