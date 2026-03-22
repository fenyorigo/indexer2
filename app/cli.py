from __future__ import annotations

import argparse
import json
import signal
import sys
from pathlib import Path
from typing import Callable

from app import __version__
from app.maintenance.retag import run_retag
from app.core.config import load_config
from app.core.db import Database
from app.core.exiftool import find_exiftool
from app.core.models import DirectorySelection, ScanResult
from app.core.scanner import refresh_file, scan

TAKEN_SRC_ORDER = [
    "SubSecDateTimeOriginal",
    "DateTimeOriginal",
    "CreateDate",
    "XMP_CreateDate",
    "XMP_DateCreated",
    "mtime_fallback",
    "unknown",
]


def _parse_yes_no(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"yes", "y", "true", "1"}:
        return True
    if normalized in {"no", "n", "false", "0"}:
        return False
    raise argparse.ArgumentTypeError("Expected yes or no")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Photo Indexer CLI")
    parser.add_argument("--cli", action="store_true", help="Run in CLI mode")
    parser.add_argument("--db", type=Path, help="SQLite DB file path")
    parser.add_argument(
        "--media-root",
        "--root",
        dest="media_root",
        type=Path,
        help="Root directory to scan (source media path)",
    )
    parser.add_argument(
        "--db-media-path",
        type=Path,
        help="Media base path written into DB paths (defaults to --media-root)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Do not write to DB")
    parser.add_argument("--changed-only", action="store_true", help="Only scan changed files")
    parser.add_argument(
        "--images-only",
        type=_parse_yes_no,
        default=None,
        metavar="yes|no",
        help="Legacy compatibility switch. yes => disable videos/docs/audio; no => enable videos/docs/audio.",
    )
    parser.add_argument(
        "--include-videos",
        type=_parse_yes_no,
        default=None,
        metavar="yes|no",
        help="Include video files in SQLite (default: yes)",
    )
    parser.add_argument(
        "--include-docs",
        type=_parse_yes_no,
        default=None,
        metavar="yes|no",
        help="Include document files in SQLite (default: no)",
    )
    parser.add_argument(
        "--include-audio",
        type=_parse_yes_no,
        default=None,
        metavar="yes|no",
        help="Include audio files in SQLite (default: no)",
    )
    parser.add_argument(
        "--video-tags",
        type=_parse_yes_no,
        default=False,
        metavar="yes|no",
        help="Extract and store tags for videos (default: no)",
    )
    parser.add_argument(
        "--video-tag-blacklist",
        type=Path,
        help="Optional newline-separated UTF-8 blacklist applied only when --video-tags yes",
    )
    parser.add_argument(
        "--include-root-files",
        action="store_true",
        default=True,
        help="Include files in root (default: true)",
    )
    parser.add_argument("--json", action="store_true", help="Print report as JSON")
    parser.add_argument("--report", type=Path, help="Write report to file")
    parser.add_argument("--errors-log", type=Path, help="Errors JSONL log path")
    parser.add_argument("--config", type=Path, help="Path to config.yaml (default: ./config.yaml)")
    parser.add_argument("--refresh-file", type=Path, help="Refresh exactly one file relative to --media-root")
    parser.add_argument("--retag-map", type=Path, help="Run maintenance retag mode using a two-column CSV map")
    parser.add_argument("--retag-report", type=Path, default=Path("retag_report.csv"), help="Retag CSV report path")
    parser.add_argument("--retag-log", type=Path, default=Path("retag.log"), help="Retag log file path")
    parser.add_argument("--retag-apply", action="store_true", help="Actually write retag changes; otherwise retag defaults to dry-run")
    parser.add_argument("--retag-allow-sidecar", action="store_true", help="Allow ExifTool sidecar creation in retag mode")
    parser.add_argument("--retag-case-insensitive", action="store_true", help="Match old tags case-insensitively in retag mode")
    parser.add_argument("--retag-no-video", action="store_true", help="Exclude video extensions in retag mode")
    parser.add_argument("--retag-no-reindex", action="store_true", help="Skip SQLite refresh after successful retag writes")
    parser.add_argument("--retag-ext", action="append", default=[], help="Extra extension to include in retag mode")
    parser.add_argument("--no-progress", action="store_true", help="Disable progress output")
    parser.add_argument(
        "--progress-every",
        type=int,
        default=200,
        help="Print progress every N files",
    )
    return parser


def _prompt_path(prompt: str) -> str:
    value = input(prompt).strip()
    return value


def _prompt_yes_no(prompt: str, default: bool = False) -> bool:
    suffix = "Y/n" if default else "y/N"
    value = input(f"{prompt} [{suffix}]: ").strip().lower()
    if not value:
        return default
    return value in {"y", "yes"}


def _resolve_errors_log_path(args: argparse.Namespace, config, db_path: Path) -> Path:
    if args.errors_log:
        return args.errors_log
    if config.errors_log_path:
        return Path(config.errors_log_path)
    return db_path.with_suffix("").with_suffix(".errors.jsonl")


def _validate_db_path(path: Path) -> Path:
    parent = path.parent
    if not parent.exists():
        parent.mkdir(parents=True, exist_ok=True)
    return path


def _validate_root(path: Path) -> Path:
    if not path.exists() or not path.is_dir():
        raise ValueError(f"Root must be an existing directory: {path}")
    return path


def _format_taken_src(dist: dict[str, int]) -> list[str]:
    lines: list[str] = []
    width = max(len(k) for k in TAKEN_SRC_ORDER)
    lines.append("taken_src distribution:")
    for key in TAKEN_SRC_ORDER:
        count = dist.get(key, 0)
        lines.append(f"  {key.ljust(width)}: {count}")
    return lines


def _build_report(
    result: ScanResult,
    taken_src_dist: dict[str, int],
    media_root: Path,
    db_media_path: Path,
    args: argparse.Namespace,
) -> dict:
    payload = {
        "version": __version__,
        "root": str(media_root),
        "media_root": str(media_root),
        "db_media_path": str(db_media_path),
        "errors_log_path": "",
        "dry_run": args.dry_run,
        "changed_only": args.changed_only,
        "images_only": args.images_only,
        "include_videos": args.include_videos,
        "include_docs": args.include_docs,
        "include_audio": args.include_audio,
        "video_tags": args.video_tags,
        "video_tag_blacklist": str(args.video_tag_blacklist) if args.video_tag_blacklist else "",
        "include_root_files": args.include_root_files,
        "directories": result.stats.directories,
        "images": result.stats.images,
        "videos": result.stats.videos,
        "warnings": result.stats.warnings,
        "errors": result.stats.errors,
        "tags_added": result.stats.tags_added,
        "file_tag_links_added": result.stats.file_tag_links_added,
        "category_tags_added": result.stats.category_tags_added,
        "value_tags_added": result.stats.value_tags_added,
        "cancelled": result.cancelled,
        "taken_src_distribution": {
            key: taken_src_dist.get(key, 0) for key in TAKEN_SRC_ORDER
        },
    }
    return payload


def _write_report_text(payload: dict) -> str:
    if payload.get("mode") == "refresh_file":
        lines = [
            f"Version: {payload.get('version', '')}",
            f"Mode: refresh_file",
            f"Media root: {payload.get('media_root', '')}",
            f"DB media path: {payload.get('db_media_path', '')}",
            f"Config: {payload.get('config_path', '')}",
            f"File: {payload.get('refresh_file', '')}",
            f"Warnings: {payload.get('warnings', 0)}",
            f"Errors: {payload.get('errors', 0)}",
            f"Indexed images: {payload.get('images', 0)}",
            f"Indexed videos: {payload.get('videos', 0)}",
            f"Tags added: {payload.get('tags_added', 0)}",
            f"Tag links added: {payload.get('file_tag_links_added', 0)}",
            f"Category tags added: {payload.get('category_tags_added', 0)}",
            f"Value tags added: {payload.get('value_tags_added', 0)}",
        ]
        warning = payload.get("warning")
        if warning:
            lines.append(f"ExifTool warning: {warning}")
        if payload.get("errors") and payload.get("errors_log_path"):
            lines.append(f"See errors log: {payload.get('errors_log_path')}")
        lines.append(f"Cancelled: {payload.get('cancelled', False)}")
        return "\n".join(lines)

    if payload.get("mode") == "retag":
        lines = [
            f"Version: {payload.get('version', '')}",
            "Mode: retag",
            f"Media root: {payload.get('media_root', '')}",
            f"DB media path: {payload.get('db_media_path', '')}",
            f"SQLite DB: {payload.get('db', '')}",
            f"Mapping CSV: {payload.get('map_path', '')}",
            f"Dry run: {payload.get('dry_run', False)}",
            f"Reindex enabled: {payload.get('reindex_enabled', False)}",
            f"Files matched: {payload.get('files', 0)}",
            f"Changed files: {payload.get('changed_files', 0)}",
            f"Planned files: {payload.get('planned_files', 0)}",
            f"Unchanged files: {payload.get('unchanged_files', 0)}",
            f"Field updates: {payload.get('field_updates', 0)}",
            f"Errors: {payload.get('errors', 0)}",
            f"Detailed report: {payload.get('report_path', '')}",
            f"Detailed log: {payload.get('log_path', '')}",
        ]
        return "\n".join(lines)

    lines = [
        f"Version: {payload.get('version', '')}",
        f"Media root: {payload.get('media_root', '')}",
        f"DB media path: {payload.get('db_media_path', '')}",
        f"Config: {payload.get('config_path', '')}",
        f"Images only: {payload.get('images_only', '')}",
        f"Include videos: {payload.get('include_videos', '')}",
        f"Include docs: {payload.get('include_docs', '')}",
        f"Include audio: {payload.get('include_audio', '')}",
        f"Video tags: {payload.get('video_tags', '')}",
        f"Scanned {payload['directories']} directories",
        f"Indexed {payload['images']} images",
        f"Indexed {payload['videos']} videos",
        f"Warnings: {payload['warnings']}",
        f"Errors: {payload['errors']}",
        f"Tags added: {payload['tags_added']}",
        f"Tag links added: {payload['file_tag_links_added']}",
        f"Category tags added: {payload['category_tags_added']}",
        f"Value tags added: {payload['value_tags_added']}",
    ]
    dist = payload.get("taken_src_distribution", {})
    lines += _format_taken_src(dist)
    if payload.get("errors") and payload.get("errors_log_path"):
        lines.append(f"See errors log: {payload.get('errors_log_path')}")
    lines.append(f"Cancelled: {payload['cancelled']}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.cli:
        print("Use --cli to run in headless mode.")
        return 1

    if not args.db:
        value = _prompt_path("DB path: ")
        if value:
            args.db = Path(value)
    if not args.media_root:
        value = _prompt_path("Scan root directory: ")
        if value:
            args.media_root = Path(value)

    if not args.db or not args.media_root:
        print("Missing required --db or --media-root")
        return 1

    try:
        db_path = _validate_db_path(args.db)
        media_root_path = _validate_root(args.media_root)
    except ValueError as exc:
        print(str(exc))
        return 1
    db_media_path = args.db_media_path if args.db_media_path else media_root_path
    include_videos = True if args.include_videos is None else bool(args.include_videos)
    include_docs = False if args.include_docs is None else bool(args.include_docs)
    include_audio = False if args.include_audio is None else bool(args.include_audio)
    if args.images_only is True:
        include_videos = False
        include_docs = False
        include_audio = False
    elif args.images_only is False:
        include_videos = True
        include_docs = True
        include_audio = True
    args.include_videos = include_videos
    args.include_docs = include_docs
    args.include_audio = include_audio

    config_path = args.config if args.config else Path("config.yaml")
    config = load_config(config_path)
    if not find_exiftool(config.exiftool_path):
        print("ExifTool not found. Install it or set exiftool_path in config.yaml")
        return 1

    cancelled = False

    def handle_sigint(_signum, _frame):
        nonlocal cancelled
        cancelled = True

    signal.signal(signal.SIGINT, handle_sigint)

    file_counter = 0

    def file_progress(path: str):
        nonlocal file_counter
        file_counter += 1
        if args.no_progress:
            return
        if args.progress_every > 0 and file_counter % args.progress_every == 0:
            print(f"Processed {file_counter} files... ({path})")

    def warning_log(message: str) -> None:
        if args.no_progress:
            return
        print(f"ExifTool warning: {message}", file=sys.stderr)

    try:
        errors_log_path = _resolve_errors_log_path(args, config, db_path)
        db = Database(Path(":memory:")) if args.dry_run else Database(db_path)
        if args.retag_map and args.refresh_file:
            raise ValueError("--retag-map cannot be combined with --refresh-file")

        if args.retag_map:
            retag_dry_run = args.dry_run or not args.retag_apply
            if not args.retag_map.is_file():
                raise ValueError(f"Retag map file not found: {args.retag_map}")
            if retag_dry_run and not args.dry_run:
                db.close()
                db = Database(Path(":memory:"))
            result_payload = run_retag(
                db,
                config,
                db_path=db_path,
                media_root=media_root_path,
                db_media_path=db_media_path,
                map_path=args.retag_map,
                report_path=args.retag_report,
                log_path=args.retag_log,
                dry_run=retag_dry_run,
                case_insensitive=args.retag_case_insensitive,
                allow_sidecar=args.retag_allow_sidecar,
                no_video=args.retag_no_video,
                extra_exts=args.retag_ext,
                no_reindex=args.retag_no_reindex,
                errors_log_path=errors_log_path,
            )
            db.close()
            payload = {
                "version": __version__,
                **result_payload,
            }
        elif args.refresh_file:
            refresh_target = args.refresh_file
            if not refresh_target.is_absolute():
                refresh_target = media_root_path / refresh_target
            result_payload = refresh_file(
                db,
                config,
                media_root_path,
                refresh_target,
                db_media_root=db_media_path,
                dry_run=args.dry_run,
                video_tags=True,
                errors_log_path=errors_log_path,
                db_path=db_path,
            )
            db.close()
            payload = {
                "version": __version__,
                "mode": "refresh_file",
                "root": str(media_root_path),
                "media_root": str(media_root_path),
                "db_media_path": str(db_media_path),
                "config_path": str(config_path),
                "errors_log_path": str(errors_log_path) if errors_log_path else "",
                "refresh_file": str(refresh_target),
                **result_payload,
            }
        else:
            selections = [
                DirectorySelection(
                    path=media_root_path,
                    recursive=True,
                    include_root_files=args.include_root_files,
                )
            ]
            result = scan(
                db,
                config,
                media_root_path,
                selections=selections,
                db_media_root=db_media_path,
                dry_run=args.dry_run,
                changed_only=args.changed_only,
                include_videos=include_videos,
                include_docs=include_docs,
                include_audio=include_audio,
                images_only=args.images_only,
                video_tags=args.video_tags,
                video_tag_blacklist_path=args.video_tag_blacklist,
                cancel_check=lambda: cancelled,
                progress_cb=None,
                file_progress_cb=lambda p: file_progress(p),
                warning_cb=warning_log,
                errors_log_path=errors_log_path,
                db_path=db_path,
            )
            db.close()
            taken_src_dist = {}
            if not args.dry_run:
                db = Database(db_path)
                taken_src_dist = db.taken_src_distribution(str(db_media_path))
                db.close()
            payload = _build_report(result, taken_src_dist, media_root_path, db_media_path, args)
            payload["errors_log_path"] = str(errors_log_path) if errors_log_path else ""
            payload["config_path"] = str(config_path)
    except KeyboardInterrupt:
        cancelled = True
        payload = {
            "warnings": 0,
            "directories": 0,
            "images": 0,
            "videos": 0,
            "errors": 0,
            "tags_added": 0,
            "file_tag_links_added": 0,
            "category_tags_added": 0,
            "value_tags_added": 0,
            "cancelled": True,
            "taken_src_distribution": {},
        }

    output = json.dumps(payload, indent=2) if args.json else _write_report_text(payload)
    if args.report:
        args.report.write_text(output)
    print(output)

    if payload.get("cancelled"):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
