'use client';

import { useEffect, useRef } from 'react';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';
import { MapMarker } from '@/lib/api';

interface MapPanelProps {
    markers: MapMarker[];
    center?: { lat: number; lng: number };
    zoom?: number;
    onMarkerClick?: (marker: MapMarker) => void;
    selectedMarkerId?: string;
}

// CartoDB dark tile layer (free, no API key needed)
const TILE_URL = 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png';
const TILE_ATTRIBUTION = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>';

/**
 * MapPanel - Right panel with interactive map
 * 
 * Uses Leaflet with CartoDB dark tiles for the tactical theme.
 * Markers are color-coded by priority.
 */
export function MapPanel({
    markers,
    center = { lat: 31.5, lng: 35.5 },
    zoom = 6,
    onMarkerClick,
    selectedMarkerId
}: MapPanelProps) {
    const mapRef = useRef<L.Map | null>(null);
    const mapContainerRef = useRef<HTMLDivElement>(null);
    const markersRef = useRef<L.CircleMarker[]>([]);

    // Initialize map
    useEffect(() => {
        if (!mapContainerRef.current || mapRef.current) return;

        const map = L.map(mapContainerRef.current, {
            center: [center.lat, center.lng],
            zoom: zoom,
            zoomControl: true,
            attributionControl: true,
        });

        L.tileLayer(TILE_URL, {
            attribution: TILE_ATTRIBUTION,
            maxZoom: 18,
        }).addTo(map);

        // Style the zoom control
        const zoomControl = document.querySelector('.leaflet-control-zoom');
        if (zoomControl) {
            zoomControl.classList.add('!bg-bg-card', '!border-bg-border');
        }

        mapRef.current = map;

        return () => {
            map.remove();
            mapRef.current = null;
        };
    }, [center.lat, center.lng, zoom]);

    // Update markers
    useEffect(() => {
        if (!mapRef.current) return;

        // Clear existing markers
        markersRef.current.forEach(m => m.remove());
        markersRef.current = [];

        // Add new markers
        markers.forEach(marker => {
            const color = getMarkerColor(marker.priority);
            const isSelected = marker.id === selectedMarkerId;
            const radius = Math.min(8 + marker.article_count, 20);

            const circleMarker = L.circleMarker([marker.lat, marker.lng], {
                radius: isSelected ? radius + 4 : radius,
                fillColor: color,
                color: isSelected ? '#ffffff' : color,
                weight: isSelected ? 3 : 2,
                opacity: 1,
                fillOpacity: 0.7,
            });

            // Popup content
            const popupContent = `
        <div style="min-width: 200px; color: #e5e7eb; font-family: system-ui;">
          <strong style="font-size: 14px;">${marker.name}</strong>
          <span style="color: #9ca3af; font-size: 12px;">${marker.country}</span>
          <div style="margin-top: 8px; font-size: 12px; color: #9ca3af;">
            ${marker.article_count} stories
          </div>
          <div style="margin-top: 8px; max-height: 120px; overflow-y: auto;">
            ${marker.articles.slice(0, 3).map(a => `
              <div style="padding: 4px 0; border-top: 1px solid #333; font-size: 11px;">
                ${getStatusDot(a.status)} ${truncate(a.title, 50)}
              </div>
            `).join('')}
            ${marker.articles.length > 3 ? `
              <div style="padding: 4px 0; font-size: 11px; color: #6b7280;">
                +${marker.articles.length - 3} more…
              </div>
            ` : ''}
          </div>
        </div>
      `;

            circleMarker.bindPopup(popupContent, {
                className: 'dark-popup',
                closeButton: true,
            });

            circleMarker.on('click', () => {
                onMarkerClick?.(marker);
            });

            circleMarker.addTo(mapRef.current!);
            markersRef.current.push(circleMarker);
        });
    }, [markers, selectedMarkerId, onMarkerClick]);

    return (
        <div className="relative w-full h-full">
            {/* Map container */}
            <div
                ref={mapContainerRef}
                className="w-full h-full bg-bg-card"
                style={{ minHeight: '100%' }}
            />

            {/* Overlay: Stats bar */}
            <div className="absolute top-4 left-4 right-4 flex justify-between items-center pointer-events-none">
                <div className="bg-bg-card/90 backdrop-blur-sm px-4 py-2 rounded-lg border border-bg-border pointer-events-auto">
                    <span className="text-xs text-text-muted uppercase tracking-wide">
                        OVERWATCH
                    </span>
                    <span className="text-sm font-semibold text-accent-red ml-2">
                        ACTIVE
                    </span>
                </div>

                <div className="flex gap-2">
                    <StatBadge label="Events" value={markers.length} />
                    <StatBadge
                        label="Critical"
                        value={markers.filter(m => m.priority === 'high').length}
                        color="red"
                    />
                </div>
            </div>

            {/* Overlay: Legend */}
            <div className="absolute bottom-4 left-4 bg-bg-card/90 backdrop-blur-sm px-3 py-2 rounded-lg border border-bg-border">
                <div className="text-xs text-text-muted mb-2">Priority</div>
                <div className="flex gap-3 text-xs">
                    <span className="flex items-center gap-1">
                        <span className="w-2 h-2 rounded-full bg-accent-green" /> High
                    </span>
                    <span className="flex items-center gap-1">
                        <span className="w-2 h-2 rounded-full bg-accent-amber" /> Med
                    </span>
                    <span className="flex items-center gap-1">
                        <span className="w-2 h-2 rounded-full bg-accent-red" /> Low
                    </span>
                </div>
            </div>

            {/* Dark popup styles */}
            <style jsx global>{`
        .dark-popup .leaflet-popup-content-wrapper {
          background: #1a1a1a;
          border: 1px solid #333;
          border-radius: 8px;
          box-shadow: 0 4px 12px rgba(0, 0, 0, 0.5);
        }
        .dark-popup .leaflet-popup-tip {
          background: #1a1a1a;
          border-color: #333;
        }
        .dark-popup .leaflet-popup-close-button {
          color: #9ca3af !important;
        }
        .leaflet-control-zoom a {
          background: #1a1a1a !important;
          color: #e5e7eb !important;
          border-color: #333 !important;
        }
        .leaflet-control-zoom a:hover {
          background: #252525 !important;
        }
        .leaflet-control-attribution {
          background: rgba(26, 26, 26, 0.8) !important;
          color: #6b7280 !important;
        }
        .leaflet-control-attribution a {
          color: #9ca3af !important;
        }
      `}</style>
        </div>
    );
}

// Helper components

function StatBadge({ label, value, color }: { label: string; value: number; color?: string }) {
    const colorClass = color === 'red' ? 'text-accent-red' : 'text-text-primary';
    return (
        <div className="bg-bg-card/90 backdrop-blur-sm px-3 py-2 rounded-lg border border-bg-border pointer-events-auto">
            <span className="text-xs text-text-muted">{label}:</span>
            <span className={`ml-1 font-semibold tabular-nums ${colorClass}`}>{value}</span>
        </div>
    );
}

// Helper functions

function getMarkerColor(priority: string): string {
    switch (priority) {
        case 'high': return '#22c55e';   // Green for verified/high confidence
        case 'medium': return '#f59e0b'; // Amber for developing
        case 'low': return '#dc2626';    // Red for unverified
        default: return '#6b7280';
    }
}

function getStatusDot(status: string): string {
    switch (status) {
        case 'Verified':
        case 'Likely Verified':
            return '🟢';
        case 'Developing':
            return '🟡';
        default:
            return '🔴';
    }
}

function truncate(str: string, max: number): string {
    return str.length > max ? str.slice(0, max) + '…' : str;
}

export default MapPanel;
