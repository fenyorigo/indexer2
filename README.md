# Photo Indexer

Cross-platform (macOS + Fedora) photo/video indexer that scans a directory tree and stores metadata in a single SQLite database. The GUI is built with PyQt6.

## Requirements
- Python 3.11+
- ExifTool (external binary)

### macOS
- Install ExifTool: `brew install exiftool`

### Fedora
- Install ExifTool: `sudo dnf install exiftool`

## Setup
```bash
python3 -m pip install -r requirements.txt
```

## Run
```bash
python3 -m app
```

## CLI
Run headless scans with `--cli`:
```bash
source .venv/bin/activate
python3 -m app --cli --db /path/to/photos.db --media-root /path/to/photos
```

Important:
- `--include-videos` defaults to `yes`.
- `--include-docs` defaults to `no`.
- `--include-audio` defaults to `no`.
- `--video-tags` defaults to `no` (videos are indexed, but no tag rows are created from video metadata unless enabled).
- `--images-only` is kept as a legacy compatibility switch:
  - `--images-only yes` => `include_videos=no`, `include_docs=no`, `include_audio=no`
  - `--images-only no` => `include_videos=yes`, `include_docs=yes`, `include_audio=yes`
- `--media-root` is where indexer reads media files from.
- `--db-media-path` controls the base path written into SQLite `roots/directories/files.path` values.
- Non-image classification uses: `video` (`.mp4/.mov/.m4v/.avi`), `doc` (`.pdf/.txt/.doc/.docx/.xls/.xlsx/.ppt/.pptx`), `audio` (`.mp3/.m4a/.flac`).

Common options:
```bash
python3 -m app --cli --db /path/to/photos.db --media-root /path/to/photos --dry-run
python3 -m app --cli --db /path/to/photos.db --media-root /path/to/photos --changed-only
python3 -m app --cli --db /path/to/photos.db --media-root /path/to/photos --include-videos yes --include-docs no --include-audio no
python3 -m app --cli --db /path/to/photos.db --media-root /path/to/photos --video-tags no
python3 -m app --cli --db /path/to/photos.db --media-root /path/to/photos --video-tags yes --video-tag-blacklist /path/to/video-tag-blacklist.txt
python3 -m app --cli --db /path/to/photos.db --media-root /path/to/photos --images-only yes
python3 -m app --cli --db /path/to/photos.db --media-root /path/to/photos --include-root-files
python3 -m app --cli --db /path/to/photos.db --media-root /Volumes/SanDisk --db-media-path /data/photos
python3 -m app --cli --db /path/to/photos.db --media-root /path/to/photos --json --report scan_report.json
python3 -m app --cli --db /path/to/photos.db --media-root /path/to/photos --errors-log /path/to/errors.jsonl
```

## Maintenance CLI
`indexer2` also includes maintenance commands that operate on media files but stay separate from normal scanning.

### Retag
`retag` rewrites media metadata from a two-column CSV map and refreshes SQLite after each successful file update.
It was added to `indexer2` as a maintenance CLI tool because `retag` and `indexer2` are operationally interdependent around the same SQLite database: retag changes file metadata, and `indexer2` is responsible for bringing the SQLite index back into sync. The necessary supporting changes for that workflow are now part of `indexer2`.

Dry-run example:
```bash
python3 -m app --cli \
  --db /path/to/photos.db \
  --media-root /data/photos \
  --retag-map /path/to/tag-map.csv \
  --retag-report /tmp/retag-report.csv \
  --retag-log /tmp/retag.log
```

Apply example:
```bash
python3 -m app --cli \
  --db /path/to/photos.db \
  --media-root /data/photos \
  --db-media-path /data/photos \
  --retag-map /path/to/tag-map.csv \
  --retag-report /tmp/retag-report.csv \
  --retag-log /tmp/retag.log \
  --retag-apply
```

Notes:
- Retag updates `XMP:Subject`, `IPTC:Keywords`, and `XMP-lr:HierarchicalSubject`.
- Hierarchical replacements only rewrite the leaf token, for example `People|Veronika` -> `People|Baján Veronika (Veronika)`.
- Retag defaults to dry-run unless `--retag-apply` is passed.
- Use `--retag-no-reindex` only if you explicitly want metadata changed without refreshing SQLite.
- MariaDB is not touched by this command.

Fedora example:
```bash
python3 -m app --cli --db /home/user/photos.db --media-root /home/user/Pictures
```

Fedora wrapper example:
```bash
./webalbum-indexer \
  --version 2 \
  --db /var/lib/webalbum/index2/build/images2_$(date +%Y%m%d_%H%M%S).db \
  --root /data/photos \
  --errors-log /var/log/webalbum/indexer2-errors.jsonl \
  --publish
```

The wrapper script [webalbum-indexer](/Users/bajanp/Projects/indexer2/webalbum-indexer) supports `--version 1|2` and switches the install base, live DB path, build directory, and backup directory together.

macOS example:
```bash
python3 -m app --cli --db /Users/you/photos.db --media-root /Users/you/Pictures
```

## Migration
To migrate a v1 DB to v2:
```bash
python3 scripts/migrate_v1_to_v2.py /path/to/db.sqlite
```

## Config
Copy `config.sample.yaml` to `config.yaml` and edit as needed.
Key options:
- `hash_mode`: `none`, `quick`, `sha256`
- `mime_mode`: `ext`, `magic`, `filecmd`

## UI Tips
- `Only changed files` skips unchanged files (mtime/size check; also fills missing hashes if enabled).
- `Images only` is checked by default. Uncheck it to include videos, documents, audio, and other non-image files in scans.
- `Scan Report` exports the last scan summary (JSON/CSV).
- Use the status filter to view directory scan states.

## Schema
The current SQLite schema (v1) is in `schema.sql`.

## Notes
- SQLite DB schema is versioned via `PRAGMA user_version`.
- ExifTool integration is implemented in `app/core/exiftool.py`.
