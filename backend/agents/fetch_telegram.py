#!/usr/bin/env python3
"""
==============================================================================
AGENT_TELEGRAM: Telethon MTProto Channel Scraper for Project Sentinel
==============================================================================

Purpose:
    Scrapes public Telegram channels for OSINT intelligence using the Telethon
    library (MTProto protocol). This is a USER CLIENT, not a Bot API client,
    which allows scraping public channels without admin privileges.

Authentication:
    Uses Telegram API credentials (api_id, api_hash) obtained from:
    https://my.telegram.org/apps
    
    Credentials are loaded from .env file:
    - TELEGRAM_API_ID
    - TELEGRAM_API_HASH
    - TELEGRAM_PHONE (optional, for first-time auth)

Session Persistence:
    The script generates an 'anon.session' file on first login. Subsequent
    runs reuse this session, so the user only authenticates once.

Target Channels:
    Curated list of geopolitical intelligence channels focused on Middle East
    conflict coverage. Channels are public and do not require membership.

Smart Filtering:
    - Skips media-only messages (no text content)
    - Skips short messages (<50 chars) to filter noise
    - Adds priority: "high" for BREAKING/URGENT/CONFIRMED keywords

Rate Limiting:
    Random 2-5 second delays between channel requests to avoid FloodWait bans.

Author: Project Sentinel Team
Created: 2026
==============================================================================
"""

import asyncio
import json
import os
import random
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional

from dotenv import load_dotenv

# Telethon imports
try:
    from telethon import TelegramClient
    from telethon.errors import (
        ChannelPrivateError,
        FloodWaitError,
        UsernameNotOccupiedError,
        UsernameInvalidError,
    )
    from telethon.tl.types import Message
except ImportError:
    print("❌ ERROR: Telethon not installed.")
    print("   Run: pip install telethon")
    sys.exit(1)


# ==============================================================================
# CONFIGURATION
# ==============================================================================

# Load environment variables from .env file
load_dotenv()

# Telegram API credentials (from https://my.telegram.org/apps)
API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")

# Session file path (persists authentication)
SESSION_DIR = Path(__file__).parent.parent
SESSION_NAME = SESSION_DIR / "anon"

# Target channels for intelligence gathering
# These are public geopolitical/OSINT channels focused on Middle East
TARGET_CHANNELS = [
    "geopolitics_live",        # General geopolitical updates
    "Middle_East_Spectator",   # Middle East focused coverage
    "Bellincat",               # OSINT investigations (Bellingcat)
    "Gaza_Now_News",           # Gaza/Palestinian coverage
]

# Scraping configuration
MESSAGES_PER_CHANNEL = 30      # Number of recent messages to fetch
MIN_MESSAGE_LENGTH = 50        # Skip messages shorter than this
DELAY_MIN = 2                  # Minimum delay between channels (seconds)
DELAY_MAX = 5                  # Maximum delay between channels (seconds)

# Priority keywords (case-insensitive)
PRIORITY_KEYWORDS = ["BREAKING", "URGENT", "CONFIRMED", "DEVELOPING"]

# Output configuration
OUTPUT_DIR = Path(__file__).parent.parent.parent / "data"
OUTPUT_FILE = OUTPUT_DIR / "telegram_feed.json"


# ==============================================================================
# UTILITY FUNCTIONS
# ==============================================================================

def check_priority(text: str) -> str:
    """
    Check if message contains priority keywords.
    
    Args:
        text: Message text content
        
    Returns:
        "high" if priority keywords found, "normal" otherwise
    """
    text_upper = text.upper()
    for keyword in PRIORITY_KEYWORDS:
        if keyword in text_upper:
            return "high"
    return "normal"


def format_message(message: Message, channel_username: str) -> Optional[Dict[str, Any]]:
    """
    Format a Telethon message object into a clean dictionary.
    
    Applies smart filtering:
    - Skips messages without text (media-only)
    - Skips messages shorter than MIN_MESSAGE_LENGTH
    
    Args:
        message: Telethon Message object
        channel_username: Source channel username
        
    Returns:
        Formatted dict or None if message should be skipped
    """
    # Skip media-only messages (no text)
    if not message.text or not message.text.strip():
        return None
    
    text = message.text.strip()
    
    # Skip short messages (noise filter)
    if len(text) < MIN_MESSAGE_LENGTH:
        return None
    
    # Format timestamp to ISO 8601 UTC
    if message.date:
        date_str = message.date.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
    else:
        date_str = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    
    # Check for priority keywords
    priority = check_priority(text)
    
    return {
        "source_id": f"telegram_{channel_username}",
        "text": text,
        "date": date_str,
        "message_id": message.id,
        "priority": priority,
    }


# ==============================================================================
# MAIN SCRAPING LOGIC
# ==============================================================================

async def fetch_channel_messages(
    client: TelegramClient, 
    channel_username: str
) -> List[Dict[str, Any]]:
    """
    Fetch recent messages from a single Telegram channel.
    
    Args:
        client: Authenticated TelegramClient
        channel_username: Channel username (without @)
        
    Returns:
        List of formatted message dicts
    """
    messages = []
    
    try:
        # Get channel entity
        entity = await client.get_entity(channel_username)
        
        # Fetch recent messages
        async for message in client.iter_messages(entity, limit=MESSAGES_PER_CHANNEL):
            formatted = format_message(message, channel_username)
            if formatted:
                messages.append(formatted)
        
        # Count priority messages
        high_priority = sum(1 for m in messages if m.get("priority") == "high")
        print(f"      ✓ Fetched {len(messages)} messages ({high_priority} high priority)")
        
    except ChannelPrivateError:
        print(f"      ⚠️  Channel is private or restricted")
    except UsernameNotOccupiedError:
        print(f"      ⚠️  Channel username not found")
    except UsernameInvalidError:
        print(f"      ⚠️  Invalid channel username")
    except FloodWaitError as e:
        print(f"      ❌ FloodWait: Must wait {e.seconds} seconds")
        # Wait the required time if hit by rate limit
        await asyncio.sleep(e.seconds)
    except Exception as e:
        print(f"      ❌ Error: {type(e).__name__}: {e}")
    
    return messages


async def main():
    """
    Main entry point for Agent_Telegram.
    
    Orchestrates the Telegram scraping pipeline:
    1. Validate credentials
    2. Connect and authenticate (first time requires phone verification)
    3. Iterate through target channels
    4. Fetch and filter messages
    5. Save to output file
    """
    print("=" * 70)
    print("📱 AGENT_TELEGRAM: MTProto Channel Scraper")
    print("   Project Sentinel - Telegram Intelligence Gathering")
    print("=" * 70)
    
    # Validate credentials
    if not API_ID or not API_HASH:
        print("\n❌ ERROR: Missing Telegram API credentials.")
        print("   Please set in .env file:")
        print("   - TELEGRAM_API_ID")
        print("   - TELEGRAM_API_HASH")
        print("\n   Get credentials at: https://my.telegram.org/apps")
        sys.exit(1)
    
    print(f"\n📋 Target Channels: {len(TARGET_CHANNELS)}")
    for channel in TARGET_CHANNELS:
        print(f"   └─ @{channel}")
    
    # Initialize client with session persistence
    print(f"\n🔐 Session file: {SESSION_NAME}.session")
    client = TelegramClient(str(SESSION_NAME), int(API_ID), API_HASH)
    
    try:
        # Connect and authenticate
        print("📡 Connecting to Telegram...")
        await client.start()
        
        if not await client.is_user_authorized():
            print("\n⚠️  First-time authentication required.")
            print("   You will receive a code on your Telegram app.")
            # The client.start() will handle the interactive login
        
        me = await client.get_me()
        print(f"   ✓ Authenticated as: {me.first_name} (@{me.username})")
        
        # Fetch from all channels
        print(f"\n🔄 Scraping channels...")
        all_messages = []
        
        for i, channel in enumerate(TARGET_CHANNELS):
            print(f"   📢 @{channel}...")
            
            messages = await fetch_channel_messages(client, channel)
            all_messages.extend(messages)
            
            # Random delay between channels (except for last one)
            if i < len(TARGET_CHANNELS) - 1:
                delay = random.uniform(DELAY_MIN, DELAY_MAX)
                print(f"      ⏳ Waiting {delay:.1f}s before next channel...")
                await asyncio.sleep(delay)
        
        # Sort by date (newest first)
        all_messages.sort(key=lambda x: x.get('date', ''), reverse=True)
        
        # Save results
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        
        output = {
            "metadata": {
                "source": "Telegram MTProto",
                "scraped_at": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                "channels": TARGET_CHANNELS,
                "total_messages": len(all_messages),
                "high_priority_count": sum(1 for m in all_messages if m.get("priority") == "high"),
            },
            "messages": all_messages,
        }
        
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"\n💾 Saved to {OUTPUT_FILE}")
        
        # Summary
        print("\n" + "=" * 70)
        print("📊 SCRAPING SUMMARY")
        print("=" * 70)
        print(f"   Channels scraped:      {len(TARGET_CHANNELS)}")
        print(f"   Total messages:        {len(all_messages)}")
        print(f"   High priority:         {output['metadata']['high_priority_count']}")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {type(e).__name__}: {e}")
        sys.exit(1)
    
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
