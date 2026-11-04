#!/usr/bin/env python3
"""
Element (Matrix) Chat Exporter
==============================
Connects to an Element chat client and exports all chat rooms,
messages, and media files.

Usage:
    python main.py [--config config.ini] [--list-rooms] [--export-all]
                   [--room ROOM_ID] [--limit N] [--formats markdown json]
                   [--media] [--verbose]

Requires: matrix-nio package installed (pip install matrix-nio)
"""

import argparse
import asyncio
import configparser
import hashlib
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, TypedDict

import aiohttp


# ── Configuration ──────────────────────────────────────────────────────────

DEFAULT_CONFIG = "config.ini"
EXPORT_ROOT = Path("export")
DEFAULT_HOMESERVER = "https://matrix.org"
CHECKPOINT_DB = "checkpoints.sqlite"


# ── Checkpoint Database ───────────────────────────────────────────────────

class CheckpointDB:
    """SQLite-based checkpoint tracker for resuming interrupted downloads."""

    def __init__(self, db_path: str = CHECKPOINT_DB):
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._create_tables()

    def _create_tables(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS rooms (
                room_id TEXT PRIMARY KEY,
                room_name TEXT,
                state TEXT DEFAULT 'queued',
                total_messages INTEGER DEFAULT 0,
                total_media INTEGER DEFAULT 0,
                messages_done INTEGER DEFAULT 0,
                media_done INTEGER DEFAULT 0,
                total_size INTEGER DEFAULT 0,
                started_at TEXT,
                finished_at TEXT,
                error TEXT
            );

            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                room_id TEXT NOT NULL,
                event_id TEXT NOT NULL UNIQUE,
                msg_type TEXT DEFAULT 'unknown',
                subtype TEXT DEFAULT 'unknown',
                state TEXT DEFAULT 'exported',
                FOREIGN KEY (room_id) REFERENCES rooms(room_id)
            );

            CREATE TABLE IF NOT EXISTS media_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                room_id TEXT NOT NULL,
                media_url TEXT NOT NULL UNIQUE,
                file_name TEXT,
                file_path TEXT,
                media_type TEXT,
                size_bytes INTEGER DEFAULT 0,
                state TEXT DEFAULT 'downloaded',
                download_time REAL DEFAULT 0,
                error TEXT,
                FOREIGN KEY (room_id) REFERENCES rooms(room_id)
            );

            CREATE INDEX IF NOT EXISTS idx_messages_room ON messages(room_id, state);
            CREATE INDEX IF NOT EXISTS idx_media_room ON media_files(room_id, state);
            CREATE INDEX IF NOT EXISTS idx_media_url ON media_files(media_url);
        """)

    def save_room(self, room_id: str, room_name: str, state: str,
                  total_messages: int = 0, total_media: int = 0,
                  messages_done: int = 0, media_done: int = 0,
                  total_size: int = 0) -> None:
        self.conn.execute(
            """INSERT INTO rooms (room_id, room_name, state, total_messages,
              total_media, messages_done, media_done, total_size, started_at,
              finished_at, error)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(room_id) DO UPDATE SET
                room_name=excluded.room_name,
                state=excluded.state,
                total_messages=excluded.total_messages,
                total_media=excluded.total_media,
                messages_done=excluded.messages_done,
                media_done=excluded.media_done,
                total_size=excluded.total_size,
                finished_at=excluded.finished_at,
                error=excluded.error""",
            (room_id, room_name, state, total_messages, total_media,
             messages_done, media_done, total_size,
             datetime.now(timezone.utc).isoformat() if state in ("running", "finished") else None,
             datetime.now(timezone.utc).isoformat() if state == "finished" else None,
             None)
        )
        self.conn.commit()

    def mark_room_running(self, room_id: str, room_name: str,
                          total_messages: int, total_media: int,
                          total_size: int) -> None:
        self.save_room(room_id, room_name, "running",
                       total_messages, total_media, 0, 0, total_size)

    def update_progress(self, room_id: str, messages_done: int,
                        media_done: int) -> None:
        self.conn.execute(
            "UPDATE rooms SET messages_done=?, media_done=? WHERE room_id=?",
            (messages_done, media_done, room_id)
        )
        self.conn.commit()

    def complete_room(self, room_id: str, messages_done: int,
                      media_done: int, total_size: int) -> None:
        self.save_room(room_id, "", "finished", 0, 0,
                       messages_done, media_done, total_size)

    def skip_room(self, room_id: str, reason: str) -> None:
        self.conn.execute(
            "UPDATE rooms SET state='skipped', error=? WHERE room_id=?",
            (reason, room_id)
        )
        self.conn.commit()

    def mark_message_done(self, room_id: str, event_id: str,
                          msg_type: str, subtype: str) -> None:
        self.conn.execute(
            """INSERT INTO messages (room_id, event_id, msg_type, subtype, state)
               VALUES (?, ?, ?, ?, 'exported')
               ON CONFLICT(event_id) DO UPDATE SET state='exported'""",
            (room_id, event_id, msg_type, subtype)
        )
        self.conn.commit()

    def is_message_done(self, event_id: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM messages WHERE event_id=? AND state='exported'",
            (event_id,)
        ).fetchone()
        return row is not None

    def mark_media_done(self, room_id: str, media_url: str, file_name: str,
                        file_path: str, media_type: str, size_bytes: int,
                        download_time: float) -> None:
        self.conn.execute(
            """INSERT OR REPLACE INTO media_files (room_id, media_url, file_name, file_path,
              media_type, size_bytes, state, download_time)
               VALUES (?, ?, ?, ?, ?, ?, 'downloaded', ?)""",
            (room_id, media_url, file_name, file_path, media_type,
             size_bytes, download_time)
        )
        self.conn.commit()

    def is_media_done(self, media_url: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM media_files WHERE media_url=? AND state='downloaded'",
            (media_url,)
        ).fetchone()
        return row is not None

    def mark_media_error(self, room_id: str, media_url: str, error: str) -> None:
        self.conn.execute(
            """INSERT INTO media_files (room_id, media_url, state, error)
               VALUES (?, ?, 'failed', ?)
               ON CONFLICT(media_url) DO UPDATE SET state='failed', error=excluded.error""",
            (room_id, media_url, error)
        )
        self.conn.commit()

    def get_room_status(self, room_id: str) -> Optional[dict]:
        row = self.conn.execute(
            "SELECT * FROM rooms WHERE room_id=?", (room_id,)
        ).fetchone()
        if not row:
            return None
        cols = [d[1] for d in self.conn.execute(  # d[1] is the column name
            "PRAGMA table_info(rooms)"
        ).fetchall()]
        return dict(zip(cols, row))

    def get_all_rooms(self) -> list[dict]:
        rows = self.conn.execute("SELECT * FROM rooms ORDER BY started_at").fetchall()
        cols = [d[1] for d in self.conn.execute(  # d[1] is the column name
            "PRAGMA table_info(rooms)"
        ).fetchall()]
        return [dict(zip(cols, row)) for row in rows]

    def get_checkpoint_summary(self) -> dict:
        summary_rows = self.conn.execute("""
            SELECT
                state,
                COUNT(*) as count,
                SUM(total_size) as total_bytes
            FROM rooms
            GROUP BY state
        """).fetchall()
        summary_dict = {row[0]: {"count": row[1], "bytes": row[2]} for row in summary_rows}
        total = self.conn.execute("SELECT COUNT(*) FROM rooms").fetchone()[0]
        rooms_done = self.conn.execute("SELECT COUNT(*) FROM rooms WHERE state='finished'").fetchone()[0]
        rooms_running = self.conn.execute("SELECT COUNT(*) FROM rooms WHERE state='running'").fetchone()[0]
        media_downloaded = self.conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(size_bytes), 0) FROM media_files WHERE state='downloaded'"
        ).fetchone()
        total_messages = self.conn.execute(
            "SELECT COUNT(*) FROM messages WHERE state='exported'"
        ).fetchone()[0]

        return {
            "total_rooms": total,
            "rooms_finished": rooms_done,
            "rooms_running": rooms_running,
            "rooms_skipped": self.conn.execute(
                "SELECT COUNT(*) FROM rooms WHERE state='skipped'"
            ).fetchone()[0],
            "rooms_queued": self.conn.execute(
                "SELECT COUNT(*) FROM rooms WHERE state='queued'"
            ).fetchone()[0],
            "media_files_downloaded": media_downloaded[0],
            "total_size_bytes": media_downloaded[1],
            "total_messages": total_messages,
            "state_breakdown": summary_dict,
        }

    def reset_room(self, room_id: str) -> None:
        self.conn.execute("UPDATE rooms SET state='queued', messages_done=0, media_done=0, error=NULL WHERE room_id=?", (room_id,))
        self.conn.commit()

    def close(self):
        self.conn.close()


# ── Configuration ──────────────────────────────────────────────────────────


def load_config(config_path: str) -> tuple[str, str, str]:
    """Load homeserver URL, user_id, and access_token from config file."""
    config = configparser.ConfigParser()
    if not Path(config_path).exists():
        print(f"Config file '{config_path}' not found.")
        print(f"Create it from the template: cp config.ini.tmpl {config_path}")
        sys.exit(1)

    config.read(config_path)

    url = config.get("HOMESERVER", "url", fallback=DEFAULT_HOMESERVER).rstrip("/")
    user_id = config.get("AUTH", "user_id", fallback="")
    access_token = config.get("AUTH", "access_token", fallback="")

    if not user_id or not access_token:
        print("ERROR: user_id and access_token must be set in config.ini")
        print("Get these from Element: Settings → Help & about → Technical details")
        sys.exit(1)

    return url, user_id, access_token


# ── Logging ────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("element_exporter")


async def auth_check(homeserver: str, access_token: str) -> bool:
    """Verify credentials by making a simple API call."""
    async with aiohttp.ClientSession() as session:
        headers = {"Authorization": f"Bearer {access_token}"}
        # Use joined_rooms as the auth check - if we get a 200 with data, it works
        url = f"{homeserver}/_matrix/client/v3/joined_rooms"
        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    count = len(data.get("joined_rooms", []))
                    logger.info(f"✓ Authenticated successfully! ({count} joined rooms)")
                    return True
                else:
                    text = await resp.text()
                    logger.error(f"Auth failed: {resp.status} - {text[:200]}")
                    return False
        except Exception as e:
            logger.error(f"Auth check error: {e}")
            return False


# ── Room Listing ───────────────────────────────────────────────────────────

async def list_rooms(homeserver: str, access_token: str, verbose: bool = False) -> list[dict]:
    """List all joined rooms from the Matrix account using direct API calls."""
    rooms = []
    
    async with aiohttp.ClientSession() as session:
        headers = {"Authorization": f"Bearer {access_token}"}
        
        # Get joined rooms
        url = f"{homeserver}/_matrix/client/v3/joined_rooms"
        async with session.get(url, headers=headers) as resp:
            if resp.status != 200:
                logger.error(f"Failed to get joined rooms: {resp.status}")
                return []
            data = await resp.json()
            room_ids = data.get("joined_rooms", [])
        
        if verbose:
            logger.info(f"Found {len(room_ids)} joined rooms")
        
        # Get room info for each room in parallel (batches of 50)
        for i in range(0, len(room_ids), 50):
            batch = room_ids[i:i+50]
            tasks = [get_room_info(session, headers, homeserver, rid) for rid in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, Exception):
                    logger.warning(f"Error fetching room info: {r}")
                elif r:
                    rooms.append(r)
        
        return rooms


async def get_room_info(session: aiohttp.ClientSession, headers: dict, homeserver: str, room_id: str) -> Optional[dict]:
    """Get room info in parallel."""
    room_data = {
        "room_id": room_id,
        "name": room_id,
        "topic": "",
        "member_count": 0,
    }
    
    try:
        url = f"{homeserver}/_matrix/client/v3/rooms/{room_id}/state/m.room.name"
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                name_data = await resp.json()
                name = name_data.get("name", "")
                if name:
                    room_data["name"] = name
    except Exception:
        pass
    
    try:
        url = f"{homeserver}/_matrix/client/v3/rooms/{room_id}/state/m.room.topic"
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                topic_data = await resp.json()
                topic = topic_data.get("body", "")
                if topic:
                    room_data["topic"] = topic
    except Exception:
        pass
    
    try:
        url = f"{homeserver}/_matrix/client/v3/joined_members/{room_id}"
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                members_data = await resp.json()
                room_data["member_count"] = len(members_data.get("joined_members", []))
    except Exception:
        pass
    
    return room_data


async def print_room_list(rooms: list[dict]):
    """Pretty-print the list of rooms."""
    print(f"\n{'='*70}")
    print(f"  Element Chat Rooms ({len(rooms)} rooms)")
    print(f"{'='*70}\n")

    for i, room in enumerate(rooms, 1):
        name = room.get("name", room["room_id"])[:40]
        topic = room.get("topic", "")
        members = room.get("member_count", "?")
        print(f"  {i:>3}. {name}")
        if topic:
            print(f"       Topic: {topic[:80]}")
        print(f"       Room: {room['room_id']}")
        print(f"       Members: {members}")
        print()


# ── Message Export ─────────────────────────────────────────────────────────

async def export_room_messages(
    cp: CheckpointDB,
    homeserver: str,
    access_token: str,
    room_id: str,
    room_name: str,
    fmt: str = "markdown",
    limit: int = 0,  # 0 = no limit
    media: bool = False,
    verbose: bool = False,
) -> Path:
    """
    Export all messages from a room using direct API calls with checkpoint/resume.
    Downloads media files if media=True, tracking progress and skipping already-downloaded files.
    Returns the path to the export file.
    """
    export_dir = EXPORT_ROOT / sanitize_filename(room_name)
    export_dir.mkdir(parents=True, exist_ok=True)

    messages = []
    media_files_info = []
    start_key: Optional[str] = None
    total_downloaded = 0
    total_size_downloaded = 0

    # Check if room was previously running - resume from checkpoint if possible
    cp_status = cp.get_room_status(room_id)
    if cp_status and cp_status.get("state") in ("running", "skipped"):
        logger.info(f"  Resuming room from checkpoint (previously {cp_status.get('state')})")

    iteration = 0
    async with aiohttp.ClientSession() as session:
        headers = {"Authorization": f"Bearer {access_token}"}

        while True:
            iteration += 1
            logger.info(f"Fetching messages for {room_name} (page {iteration})...")

            try:
                params = {
                    "from": start_key if start_key else "",
                    "limit": "100",
                    "dir": "b",
                }
                url = f"{homeserver}/_matrix/client/v3/rooms/{room_id}/messages"
                async with session.get(url, headers=headers, params=params) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        logger.error(f"Error fetching messages: {resp.status} - {text[:200]}")
                        break
                    data = await resp.json()
            except Exception as e:
                logger.error(f"Error fetching messages: {e}")
                break

            chunk = data.get("chunk", [])
            end_key = data.get("end")
            start_key = data.get("end")

            if not chunk:
                if verbose:
                    logger.info("No more messages.")
                break

            if not end_key:
                if verbose:
                    logger.info("Reached earliest message.")
                break

            # Count messages in this batch for progress
            batch_size = len(chunk)
            for event in chunk:
                msg_info = await extract_message_info(
                    event, room_name, media, export_dir,
                    homeserver, access_token, session, cp
                )
                if msg_info:
                    messages.append(msg_info)
                    if msg_info.get("file_path"):
                        media_files_info.append(msg_info)

            msg_count = len(messages)
            if limit and msg_count >= limit:
                messages = messages[:limit]
                media_files_info = [m for m in media_files_info if m["event_id"] in {m2["event_id"] for m2 in messages}]
                logger.info(f"Reached limit of {limit} messages.")
                break

            if verbose:
                logger.info(f"  Fetched {msg_count} messages so far...")

            # Update checkpoint progress periodically
            media_so_far = sum(1 for m in media_files_info if m.get("_downloaded"))
            cp.update_progress(room_id, msg_count, media_so_far)

        # Sort by timestamp (oldest first)
        messages.sort(key=lambda m: m.get("timestamp", 0))

    # Compute total media size from exported info
    total_size = sum(m.get("file_info", {}).get("size", 0) for m in media_files_info)

    # Write export file
    safe_name = sanitize_filename(room_name)
    if fmt == "markdown":
        export_path = export_dir / f"{safe_name}.md"
        write_markdown(export_path, room_name, messages)
    elif fmt == "json":
        export_path = export_dir / f"{safe_name}.json"
        write_json(export_path, room_name, messages)
    else:
        md_path = export_dir / f"{safe_name}.md"
        json_path = export_dir / f"{safe_name}.json"
        write_markdown(md_path, room_name, messages)
        write_json(json_path, room_name, messages)
        export_path = md_path

    total_downloaded = len(media_files_info)
    logger.info(
        f"Exported {len(messages)} messages with "
        f"{total_downloaded} media files ({format_size(total_size)}) → {export_dir}"
    )

    # Mark room as finished in checkpoint
    cp.complete_room(room_id, len(messages), total_downloaded, total_size)

    return export_path


async def extract_message_info(
    event, room_name: str, media: bool,
    export_dir: Path, homeserver: str, access_token: str,
    session: aiohttp.ClientSession, cp: CheckpointDB
) -> Optional[dict]:
    """Extract message info from a Matrix event dict. Downloads media if needed."""
    msg_type = event.get("type", "")
    content = event.get("content", {})
    msg_subtype = content.get("msgtype", "unknown") if content else "unknown"
    body = content.get("body", "") if content else ""

    # Get sender display name
    sender = event.get("sender", "unknown")
    membership = content.get("m.relates_to", {}).get("m.room_membership", {})
    if isinstance(membership, dict) and "displayname" in membership:
        sender = membership["displayname"]

    # Get timestamp
    ts = event.get("origin_server_ts", 0)
    ts_str = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    ) if ts else "unknown"

    msg_info = {
        "type": "message" if msg_type == "m.room.message" else "state",
        "subtype": msg_subtype,
        "sender": sender,
        "sender_id": event.get("sender", ""),
        "timestamp": ts,
        "ts_str": ts_str,
        "event_id": event.get("event_id", ""),
        "raw": {},
    }

    # Always record message in checkpoint
    event_id = event.get("event_id", "")
    if event_id:
        cp.mark_message_done(event.get("room_id", ""), event_id, msg_type, msg_subtype)

    if msg_subtype == "m.text":
        msg_info["body"] = body
        formatted = content.get("format")
        if formatted:
            msg_info["formatted_body"] = content.get("formatted_body", "")
            msg_info["format"] = formatted

    elif msg_subtype in ("m.image", "m.video", "m.audio", "m.file", "m.document"):
        info = content.get("info", {}) or {}
        file_info = {
            "body": body,
            "url": content.get("url", ""),
            "mimetype": info.get("mimetype", ""),
            "size": info.get("size", 0),
        }
        msg_info["file_info"] = file_info

        # Download media if requested and not already done
        if media and content.get("url"):
            url = content["url"]
            downloaded_path = await download_media(
                url, body, info,
                export_dir, msg_subtype, homeserver, access_token, cp, event_id
            )
            if downloaded_path:
                msg_info["file_path"] = str(downloaded_path)
                msg_info["_downloaded"] = True

    return msg_info


async def download_media(
    url: str,
    filename: str,
    info: dict,
    export_dir: Path,
    media_type: str,
    homeserver: str,
    access_token: str,
    cp: CheckpointDB,
    event_id: str,
) -> Optional[Path]:
    """
    Download a media file from a Matrix media server with checkpoint tracking.
    Skips if already downloaded.
    """
    # Skip if already downloaded
    if cp.is_media_done(url):
        return None

    try:
        async with aiohttp.ClientSession() as session:
            headers = {"Authorization": f"Bearer {access_token}"}
            start_time = time.time()

            # Convert mxc:// URL to HTTP download URL
            download_url = url
            if url.startswith("mxc://"):
                # Extract host and file ID from mxc://host/file
                parts = url[6:].split("/", 1)
                if len(parts) == 2:
                    mxc_host, mxc_file = parts
                    download_url = f"{homeserver}/_matrix/media/r0/download/{mxc_host}/{mxc_file}"

            async with session.get(download_url, headers=headers) as resp:
                if resp.status != 200:
                    cp.mark_media_error(event_id or "", url, f"status {resp.status}")
                    logger.warning(
                        f"Media unavailable: {url[:60]}... "
                        f"(status {resp.status}, may be private room)"
                    )
                    return None

                content = await resp.read()
                elapsed = time.time() - start_time

                # Determine file extension from info or content-type
                ext = ""
                if info and "mimetype" in info:
                    mime = info["mimetype"]
                    ext_map = {
                        "image/png": ".png", "image/jpeg": ".jpg", "image/gif": ".gif",
                        "image/webp": ".webp", "video/mp4": ".mp4", "video/webm": ".webm",
                        "audio/mp3": ".mp3", "audio/ogg": ".ogg", "audio/wav": ".wav",
                        "audio/mpeg": ".mp3", "application/pdf": ".pdf", "application/zip": ".zip",
                    }
                    ext = ext_map.get(mime, "")

                if not ext:
                    for e in [".png", ".jpg", ".jpeg", ".gif", ".webp", ".mp4", ".webm", ".mp3", ".ogg", ".pdf", ".zip"]:
                        if e in filename.lower():
                            ext = e
                            break

                safe_name = sanitize_filename(filename or f"media_{int(time.time())}{ext}")
                media_path = export_dir / safe_name

                media_path.write_bytes(content)

                # Record in checkpoint
                cp.mark_media_done(
                    event_id, url, safe_name, str(media_path),
                    media_type, len(content), elapsed
                )

                logger.info(
                    f"  Downloaded {media_type}: {media_path.name} "
                    f"({len(content):,} bytes, {elapsed:.1f}s)"
                )
                return media_path

    except Exception as e:
        logger.error(f"Failed to download media {url}: {e}")
        cp.mark_media_error("", url, str(e))
        return None


def sanitize_filename(name: str) -> str:
    """Sanitize a filename for cross-platform compatibility."""
    import re
    # Remove problematic characters
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', name)
    # Trim whitespace
    name = name.strip()
    # Limit length
    if len(name) > 200:
        name = name[:200]
    return name or "unnamed"


# ── Export Formats ─────────────────────────────────────────────────────────

def write_markdown(path: Path, room_name: str, messages: list[dict]):
    """Write messages as Markdown."""
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"# {room_name}\n\n")
        f.write(f"Exported: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
        f.write(f"Total messages: {len(messages)}\n\n")
        f.write("---\n\n")

        for msg in messages:
            ts = msg.get("ts_str", "unknown")
            sender = msg.get("sender", "unknown")
            subtype = msg.get("subtype", "unknown")

            f.write(f"### [{ts}] **{sender}**\n\n")

            if subtype == "m.text":
                body = msg.get("body", "")
                formatted = msg.get("formatted_body")
                if formatted and msg.get("format") == "org.matrix.custom.html":
                    f.write(f"{formatted}\n\n")
                else:
                    f.write(f"{body}\n\n")

            elif subtype in ("m.image", "m.video", "m.audio", "m.file", "m.document"):
                file_info = msg.get("file_info", {})
                body = file_info.get("body", msg.get("body", ""))
                size = file_info.get("size", 0)

                f.write(f"**[{subtype.upper()}]** {body}\n\n")
                f.write(f"  - Size: {format_size(size) if size else 'unknown'}\n")

                if msg.get("_downloaded"):
                    f.write(f"  - Local: `{msg.get('file_path', '')}`\n")

                f.write("\n")

            else:
                f.write(f"*{subtype} event*\n\n")

            f.write("---\n\n")


def write_json(path: Path, room_name: str, messages: list[dict]):
    """Write messages as JSON."""
    export_data = {
        "room_name": room_name,
        "exported": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "total_messages": len(messages),
        "messages": messages,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(export_data, f, indent=2, ensure_ascii=False)


def format_size(size_bytes: int) -> str:
    """Format bytes as human-readable size."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


# ── Progress Tracking ──────────────────────────────────────────────────────

def print_progress(rooms_completed: int, total_rooms: int, media_count: int) -> None:
    """Print current progress bar and stats."""
    if total_rooms <= 0:
        return

    pct = (rooms_completed / total_rooms) * 100
    bar_len = 40
    filled = int(bar_len * rooms_completed / total_rooms)
    bar = "█" * filled + "░" * (bar_len - filled)

    print(
        f"\r  [{bar}] {pct:5.1f}% | "
        f"{rooms_completed}/{total_rooms} rooms | "
        f"{media_count} media files downloaded",
        end="", flush=True
    )


def print_final_summary(summary: dict, rooms_completed: int, total_rooms: int) -> None:
    """Print final export summary."""
    print()  # newline after progress bar

    elapsed = time.time()
    print(f"\n{'='*70}")
    print(f"  Export Complete - Summary")
    print(f"{'='*70}")
    print(f"  Total rooms processed:  {summary['total_rooms']}")
    print(f"  Rooms finished:         {summary['rooms_finished']}")
    print(f"  Rooms skipped:          {summary['rooms_skipped']}")
    print(f"  Media files downloaded: {summary['media_files_downloaded']}")
    print(f"  Total size downloaded:  {format_size(summary['total_size_bytes'])}")
    print(f"  Total messages exported: {summary.get('total_messages', 'N/A')}")
    print(f"{'='*70}\n")


# ── Main ───────────────────────────────────────────────────────────────────

async def main(args):
    """Main entry point with checkpoint support."""
    homeserver_url, user_id, access_token = load_config(args.config)

    # Initialize checkpoint database
    cp = CheckpointDB()

    # Handle --status flag
    if args.status:
        print(f"\n{'='*70}")
        print(f"  Checkpoint Status")
        print(f"{'='*70}\n")
        summary = cp.get_checkpoint_summary()
        print(f"  Total rooms:        {summary['total_rooms']}")
        print(f"  Finished:           {summary['rooms_finished']}")
        print(f"  Running:            {summary['rooms_running']}")
        print(f"  Skipped:            {summary['rooms_skipped']}")
        print(f"  Media files:        {summary['media_files_downloaded']}")
        print(f"  Total size:         {format_size(summary['total_size_bytes'])}")
        print(f"  Total messages:     {summary['total_messages']}")
        print()

        # Show detailed room list
        rooms = cp.get_all_rooms()
        if rooms:
            print(f"  {'Room':<40} {'State':<10} {'Msgs':>6} {'Media':>6}")
            print(f"  {'-'*40} {'-'*10} {'-'*6} {'-'*6}")
            for r in rooms[-30:]:  # Last 30 rooms
                name = (r.get('room_name') or r.get('room_id') or '')[:38]
                print(f"  {name:<40} {r.get('state', '?'):<10} "
                      f"{r.get('messages_done', 0):>6} {r.get('media_done', 0):>6}")
            if len(rooms) > 30:
                print(f"\n  ... and {len(rooms) - 30} more rooms")
        print(f"\n{'='*70}\n")
        cp.close()
        return

    # Handle --reset flag
    if args.reset:
        import sqlite3
        conn = sqlite3.connect(CHECKPOINT_DB)
        conn.executescript("DELETE FROM rooms; DELETE FROM messages; DELETE FROM media_files;")
        conn.commit()
        conn.close()
        print("✓ Checkpoint database reset. All rooms will be re-exported.\n")
        return

    # Initialize checkpoint summary for progress tracking
    summary = cp.get_checkpoint_summary()
    if summary["total_rooms"] > 0:
        print(f"\n{'='*70}")
        print(f"  Previous Export Run - Checkpoint Summary")
        print(f"{'='*70}")
        print(f"  Rooms finished:    {summary['rooms_finished']}")
        print(f"  Rooms skipped:     {summary['rooms_skipped']}")
        print(f"  Media files:       {summary['media_files_downloaded']}")
        print(f"  Total size:        {format_size(summary['total_size_bytes'])}")
        print(f"{'='*70}\n")

    # List rooms (this will fail with auth error if credentials are wrong)
    logger.info("Fetching joined rooms...")
    rooms = await list_rooms(homeserver_url, access_token, verbose=args.verbose)

    if not rooms:
        logger.warning("No joined rooms found.")
        return

    # Print room list if --list-rooms
    if args.list_rooms:
        await print_room_list(rooms)
        if not args.export_all:
            return

    # Determine target rooms
    if args.room:
        target_rooms = [r for r in rooms
                       if args.room.lower() in r["room_id"].lower()
                       or args.room.lower() in r.get("name", "").lower()]
        if not target_rooms:
            target_rooms = [{"room_id": args.room, "name": args.room, "topic": "", "member_count": 0}]
    else:
        target_rooms = rooms

    # Export each room with checkpoint tracking and progress
    total_rooms = len(target_rooms)
    # Track only current-run progress for the progress bar
    current_run_progress = 0

    for idx, room in enumerate(target_rooms, 1):
        room_id = room["room_id"]
        room_name = room.get("name", room_id[:30])

        # Check if room was previously finished
        cp_status = cp.get_room_status(room_id)

        logger.info(f"{'='*70}")
        logger.info(f"Room {idx}/{total_rooms} | {room_name} ({room_id})")
        logger.info(f"{'='*70}")

        if cp_status and cp_status.get("state") == "finished":
            logger.info(f"✓ Skipped (already exported)")
            logger.info(f"  {cp_status.get('messages_done', 0)} messages, "
                       f"{cp_status.get('media_done', 0)} media files\n")
            continue

        if cp_status and cp_status.get("state") == "skipped":
            logger.warning(f"Room was previously skipped: {cp_status.get('error', 'unknown')}")
            if not args.force:
                logger.info("Use --force to retry")
                continue

        try:
            path = await export_room_messages(
                cp,
                homeserver_url, access_token,
                room_id, room_name,
                fmt=args.formats, limit=args.limit,
                media=args.media, verbose=args.verbose,
            )
            current_run_progress += 1

            # Print progress (get live count from checkpoint DB)
            live_summary = cp.get_checkpoint_summary()
            print_progress(current_run_progress, total_rooms, live_summary["media_files_downloaded"])

        except Exception as e:
            logger.error(f"✗ Failed to export {room_name}: {e}")
            cp.skip_room(room_id, str(e)[:200])

    # Final summary
    final_summary = cp.get_checkpoint_summary()
    print_final_summary(final_summary, current_run_progress, total_rooms)

    cp.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Element (Matrix) Chat Exporter - export rooms, messages, and media",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all joined rooms
  python main.py --list-rooms

  # Export all rooms (messages only)
  python main.py --export-all

  # Export a specific room with media
  python main.py --export-all --media --room general

  # Export all rooms in both markdown and JSON
  python main.py --export-all --formats markdown,json --media

  # Limit to 100 messages per room
  python main.py --export-all --limit 100

  # Check checkpoint status
  python main.py --status

  # Reset checkpoint for all rooms (full re-export)
  python main.py --reset

  # Force re-export a skipped room
  python main.py --export-all --room "Room Name" --force
        """,
    )
    parser.add_argument(
        "--config", default=DEFAULT_CONFIG,
        help=f"Path to config file (default: {DEFAULT_CONFIG})",
    )
    parser.add_argument(
        "--list-rooms", action="store_true",
        help="List all joined rooms without exporting",
    )
    parser.add_argument(
        "--export-all", action="store_true",
        help="Export all joined rooms",
    )
    parser.add_argument(
        "--room", default=None,
        help="Export a specific room (by ID or name substring)",
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Max messages per room (0 = unlimited)",
    )
    parser.add_argument(
        "--formats", default="markdown",
        help="Export format(s): markdown, json, or markdown,json (default: markdown)",
    )
    parser.add_argument(
        "--media", action="store_true",
        help="Download media files (images, videos, files) along with messages",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Verbose output",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Force re-export of skipped/failed rooms",
    )
    parser.add_argument(
        "--status", action="store_true",
        help="Show checkpoint status and exit",
    )
    parser.add_argument(
        "--reset", action="store_true",
        help="Reset checkpoint database (start fresh)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    fmt_list = [f.strip() for f in args.formats.split(",")]

    asyncio.run(main(args))
