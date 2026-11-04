# Element (Matrix) Chat Exporter

Automatically connect to your Element/Matrix chat client and export all chat rooms,
messages, and media files using the Matrix API.

## Quick Start

### 1. Install dependencies

```bash
pip install aiohttp
```

### 2. Configure credentials

The config file is already set up with your credentials:

- **User:** `@bigtexxx:matrix.org`
- **Homeserver:** `matrix.org`

Edit `config.ini` if you need to change the homeserver URL.

### 3. Run

```bat
# Windows:
run.bat --list-rooms           # List all rooms
run.bat --export-all            # Export all rooms (messages only)
run.bat --export-all --media    # Export all rooms with media downloads

# Linux/Mac:
python3 main.py --list-rooms    # List all rooms
python3 main.py --export-all    # Export all rooms (messages only)
python3 main.py --export-all --media  # Export all rooms with media downloads
```

## Usage

```
python main.py [options]

Options:
  --config CONFIG       Path to config file (default: config.ini)
  --list-rooms          List all joined rooms without exporting
  --export-all          Export all joined rooms
  --room ROOM           Export a specific room (by ID or name substring)
  --limit N             Max messages per room (0 = unlimited)
  --formats FMTS        Export format(s): markdown, json, or markdown,json
  --media               Download media files (images, videos, files)
  --verbose, -v         Verbose output
```

## Examples

```bash
# Just see what rooms you're in
python main.py --list-rooms --verbose

# Export everything (messages only, fastest)
python main.py --export-all

# Export a specific room
python main.py --export-all --room "#general:example.com"

# Export with media downloads
python main.py --export-all --media

# Limit to 50 messages per room, both formats
python main.py --export-all --limit 50 --formats markdown,json

# Self-hosted server
# (Set the URL in config.ini, then run as normal)
python main.py --config config.ini --export-all
```

## Output

Exports are saved to the `export/` directory:

```
export/
├── #general/
│   ├── general.md          # Markdown export
│   └── general.json        # JSON export
│   └── [media files]       # Downloaded images/videos/files
├── #random/
│   ├── random.md
│   └── random.json
└── ...
```

## Supported Message Types

- Text messages
- Images (downloaded as local files)
- Videos
- Audio files
- File attachments
- State events (room name changes, topics, joins, etc.)

## Checkpoint Database

The tool maintains `checkpoints.sqlite` to track progress and resume interrupted runs.

| Table | Tracks |
|-------|--------|
| `rooms` | Room state, message count, media count, timestamps |
| `messages` | Each message event_id (prevents re-processing) |
| `media_files` | Each media URL, file path, size, download time |

### Checkpoint Commands

```bash
# Show checkpoint status
python3 main.py --status

# Reset checkpoint (start fresh)
python3 main.py --reset

# Force re-export a skipped room
python3 main.py --export-all --room "Room Name" --force
```

### How Checkpoints Work

1. **First run**: Exports all messages and media, records everything in the SQLite DB
2. **Re-run**: Checks DB before each room — skips finished rooms instantly
3. **Interrupt**: Partial state saved mid-export, next run continues from there
4. **Progress**: Live progress bar: `█░░ 50% | 5/10 rooms | 23 media files downloaded`

## Notes

- **Pagination**: The Matrix API returns messages in batches (~100 at a time). The script automatically paginates through all history.
- **Media downloads**: Server-dependent — some Matrix instances don't expose media publicly. The script handles 404s gracefully and logs unavailable media.
- **Large rooms**: With 328 rooms, consider using `--room "keyword"` to target specific rooms first.
