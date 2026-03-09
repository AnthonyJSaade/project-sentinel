'use client';

import { ContentItem, formatRelativeTime } from '@/lib/api';

interface NewsCardProps {
    item: ContentItem;
    onClick?: () => void;
    isSelected?: boolean;
}

/**
 * NewsCard - Displays a single news item in the feed
 * 
 * Compound component structure for flexibility
 */
export function NewsCard({ item, onClick, isSelected }: NewsCardProps) {
    const statusIcon = getStatusIcon(item.status);
    const statusClass = getStatusClass(item.status);
    const contentIcon = item.content_type === 'telegram' ? '📱' : '📰';
    const priority = getPriorityLabel(item.final_score);

    return (
        <article
            className={`
        p-4 border-b border-bg-border cursor-pointer card-hover
        ${isSelected ? 'bg-bg-hover border-l-2 border-l-accent-red' : ''}
      `}
            onClick={onClick}
            role="button"
            tabIndex={0}
            onKeyDown={(e) => e.key === 'Enter' && onClick?.()}
        >
            {/* Header: Priority + Status */}
            <div className="flex items-center justify-between mb-2">
                <div className="flex items-center gap-2">
                    <span className={`text-xs font-semibold uppercase ${priority.class}`}>
                        {priority.label}
                    </span>
                    <span className={`px-2 py-0.5 text-xs font-medium rounded ${statusClass}`}>
                        {statusIcon} {item.status}
                    </span>
                </div>
                <span className="text-xs text-text-muted tabular-nums">
                    {item.cross_ref_count > 0 && (
                        <span className="mr-2">{item.cross_ref_count} SRC</span>
                    )}
                    {item.final_score}%
                </span>
            </div>

            {/* Title */}
            <h3 className="text-sm font-medium text-text-primary line-clamp-2 mb-2">
                {item.title}
            </h3>

            {/* Footer: Source + Location + Time */}
            <div className="flex items-center justify-between text-xs text-text-secondary">
                <div className="flex items-center gap-2 min-w-0">
                    <span>{contentIcon}</span>
                    <span className="truncate">{formatSourceName(item.source_id)}</span>
                    {item.primary_location && (
                        <>
                            <span className="text-text-muted">•</span>
                            <span className="truncate">
                                📍 {item.primary_location.name}
                            </span>
                        </>
                    )}
                </div>
                <span className="text-text-muted whitespace-nowrap ml-2">
                    {formatRelativeTime(item.published_utc)}
                </span>
            </div>
        </article>
    );
}

// Helper functions

function getStatusIcon(status: string): string {
    switch (status) {
        case 'Verified':
        case 'Likely Verified':
            return '🟢';
        case 'Developing':
            return '🟡';
        case 'Unverified':
        case 'Unconfirmed':
            return '🔴';
        default:
            return '⚪';
    }
}

function getStatusClass(status: string): string {
    switch (status) {
        case 'Verified':
        case 'Likely Verified':
            return 'bg-accent-green/20 text-accent-green';
        case 'Developing':
            return 'bg-accent-amber/20 text-accent-amber';
        case 'Unverified':
        case 'Unconfirmed':
            return 'bg-accent-red/20 text-accent-red';
        default:
            return 'bg-bg-hover text-text-muted';
    }
}

function getPriorityLabel(score: number): { label: string; class: string } {
    if (score >= 85) {
        return { label: 'HIGH CONFIDENCE', class: 'text-accent-green' };
    }
    if (score >= 70) {
        return { label: 'GOOD CONFIDENCE', class: 'text-accent-green' };
    }
    if (score >= 50) {
        return { label: 'DEVELOPING', class: 'text-accent-amber' };
    }
    if (score >= 30) {
        return { label: 'UNVERIFIED', class: 'text-accent-red' };
    }
    return { label: 'UNCONFIRMED', class: 'text-accent-red' };
}

function formatSourceName(sourceId: string): string {
    // Convert source_id like "aljazeera_english" to "Al Jazeera"
    return sourceId
        .replace(/_/g, ' ')
        .replace(/\b\w/g, c => c.toUpperCase())
        .replace(/English$/i, '')
        .replace(/World$/i, '')
        .trim();
}

export default NewsCard;
