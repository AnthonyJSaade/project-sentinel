'use client';

import { useState, useMemo } from 'react';
import { ContentItem } from '@/lib/api';
import NewsCard from './NewsCard';

interface NewsPanelProps {
    items: ContentItem[];
    onSelectItem?: (item: ContentItem) => void;
    selectedItemId?: string;
}

type FilterType = 'all' | 'verified' | 'developing' | 'telegram';

const FILTER_OPTIONS: { value: FilterType; label: string; icon: string }[] = [
    { value: 'all', label: 'All', icon: '📋' },
    { value: 'verified', label: 'Verified', icon: '🟢' },
    { value: 'developing', label: 'Developing', icon: '🟡' },
    { value: 'telegram', label: 'Telegram', icon: '📱' },
];

/**
 * NewsPanel - Left sidebar with news feed
 */
export function NewsPanel({ items, onSelectItem, selectedItemId }: NewsPanelProps) {
    const [filter, setFilter] = useState<FilterType>('all');
    const [searchQuery, setSearchQuery] = useState('');

    // Filter items based on current filter and search
    const filteredItems = useMemo(() => {
        let result = items;

        // Apply filter
        switch (filter) {
            case 'verified':
                result = result.filter(i =>
                    i.status === 'Verified' || i.status === 'Likely Verified'
                );
                break;
            case 'developing':
                result = result.filter(i => i.status === 'Developing');
                break;
            case 'telegram':
                result = result.filter(i => i.content_type === 'telegram');
                break;
        }

        // Apply search
        if (searchQuery.trim()) {
            const q = searchQuery.toLowerCase();
            result = result.filter(i =>
                i.title.toLowerCase().includes(q) ||
                i.summary?.toLowerCase().includes(q) ||
                i.source_id.toLowerCase().includes(q)
            );
        }

        return result;
    }, [items, filter, searchQuery]);

    return (
        <div className="flex flex-col h-full bg-bg-card border-r border-bg-border">
            {/* Header */}
            <div className="p-4 border-b border-bg-border">
                <div className="flex items-center justify-between mb-3">
                    <h2 className="text-lg font-semibold text-text-primary">
                        Intelligence Feed
                    </h2>
                    <div className="flex items-center gap-2">
                        <span className="w-2 h-2 bg-accent-green rounded-full animate-pulse-slow" />
                        <span className="text-xs text-text-secondary">Live</span>
                    </div>
                </div>

                {/* Search */}
                <div className="relative mb-3">
                    <input
                        type="text"
                        placeholder="Search stories…"
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        className="w-full px-3 py-2 pl-9 text-sm bg-bg-primary border border-bg-border rounded-lg 
                       text-text-primary placeholder:text-text-muted
                       focus:outline-none focus:ring-2 focus:ring-accent-red/50 focus:border-accent-red"
                        spellCheck={false}
                    />
                    <span className="absolute left-3 top-1/2 -translate-y-1/2 text-text-muted">
                        🔍
                    </span>
                </div>

                {/* Filter chips */}
                <div className="flex gap-2 overflow-x-auto pb-1">
                    {FILTER_OPTIONS.map(opt => (
                        <button
                            key={opt.value}
                            onClick={() => setFilter(opt.value)}
                            className={`
                px-3 py-1 text-xs font-medium rounded-full whitespace-nowrap
                transition-colors duration-150
                ${filter === opt.value
                                    ? 'bg-accent-red text-white'
                                    : 'bg-bg-primary text-text-secondary hover:bg-bg-hover'}
              `}
                        >
                            {opt.icon} {opt.label}
                        </button>
                    ))}
                </div>
            </div>

            {/* Stats bar */}
            <div className="px-4 py-2 text-xs text-text-muted border-b border-bg-border bg-bg-primary/50">
                Showing {filteredItems.length} of {items.length} stories
            </div>

            {/* News list */}
            <div className="flex-1 overflow-y-auto">
                {filteredItems.length === 0 ? (
                    <div className="p-8 text-center text-text-muted">
                        <p>No stories match your filters</p>
                    </div>
                ) : (
                    filteredItems.map(item => (
                        <NewsCard
                            key={item.id}
                            item={item}
                            isSelected={item.id === selectedItemId}
                            onClick={() => onSelectItem?.(item)}
                        />
                    ))
                )}
            </div>
        </div>
    );
}

export default NewsPanel;
