from __future__ import annotations

import csv
import json
import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from app.core.config import AppConfig
from app.core.db import Database
from app.core.exiftool import find_exiftool
from app.core.scanner import refresh_file

TAG_FIELDS = [
    ("XMP:Subject", "subject"),
    ("IPTC:Keywords", "keywords"),
    ("XMP-lr:HierarchicalSubject", "hierarchical"),
]

DEFAULT_EXTS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".tif",
    ".tiff",
    ".heic",
    ".cr2",
    ".cr3",
    ".nef",
    ".arw",
    ".rw2",
    ".orf",
    ".raf",
    ".dng",
    ".mp4",
    ".mov",
    ".m4v",
}

SIDECAR_PRONE_EXTS = {
    ".cr2",
    ".cr3",
    ".nef",
    ".arw",
    ".rw2",
    ".orf",
    ".raf",
    ".dng",
}


@dataclass
class Change:
    path: str
    field: str
    before: list[str]
    after: list[str]


@dataclass
class FileResult:
    path: str
    rel_path: str
    status: str
    changed_fields: int
    reindexed: bool
    message: str


def setup_logger(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("indexer2.retag")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def load_map(csv_path: Path, case_insensitive: bool) -> dict[str, str]:
    mapping: dict[str, str] = {}
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if not header:
            raise ValueError("Mapping CSV is empty")
        has_header = any(h.lower() in ("old_tag", "old", "from") for h in header)
        if not has_header:
            if len(header) < 2:
                raise ValueError("Mapping CSV must have two columns: old_tag,new_tag")
            old, new = header[0].strip(), header[1].strip()
            if old and new:
                mapping[old.lower() if case_insensitive else old] = new
        for row in reader:
            if not row or len(row) < 2:
                continue
            old, new = row[0].strip(), row[1].strip()
            if not old or not new:
                continue
            mapping[old.lower() if case_insensitive else old] = new
    return mapping


def list_media(root: Path, exts: set[str]) -> list[Path]:
    matches: list[Path] = []
    for path in root.rglob("*"):
        if path.is_file() and path.suffix.lower() in exts:
            matches.append(path)
    return matches


def dedupe_preserve(seq: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in seq:
        if value not in seen:
            seen.add(value)
            out.append(value)
    return out


def rewrite_tags(
    tags: list[str],
    mapping: dict[str, str],
    case_insensitive: bool,
    *,
    hierarchical: bool = False,
) -> tuple[list[str], bool]:
    changed = False
    new_tags: list[str] = []

    for tag in tags:
        if not hierarchical:
            key = tag.lower() if case_insensitive else tag
            if key in mapping:
                new_tags.append(mapping[key])
                changed = True
            else:
                new_tags.append(tag)
            continue

        parts = tag.split("|")
        leaf = parts[-1]
        key = leaf.lower() if case_insensitive else leaf
        if key in mapping:
            parts[-1] = mapping[key]
            rewritten = "|".join(parts)
            new_tags.append(rewritten)
            if rewritten != tag:
                changed = True
        else:
            new_tags.append(tag)

    new_tags = dedupe_preserve(new_tags)
    if new_tags == tags:
        changed = False
    return new_tags, changed


def _run_exiftool(cmd: list[str]) -> tuple[int, str, str]:
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    return proc.returncode, proc.stdout, proc.stderr


def exiftool_read_tags(exiftool_path: str, file_path: Path) -> dict[str, list[str]]:
    cmd = [exiftool_path, "-json", "-charset", "filename=utf8", "-charset", "iptc=utf8"]
    for field, _ in TAG_FIELDS:
        cmd.append(f"-{field}")
    cmd.append(str(file_path))

    rc, out, err = _run_exiftool(cmd)
    if rc != 0:
        raise RuntimeError(f"exiftool read failed: {err.strip()}")

    payload = json.loads(out)
    if not payload:
        return {}

    record = payload[0]
    result: dict[str, list[str]] = {}
    for field, key in TAG_FIELDS:
        value = record.get(field.split(":", 1)[-1])
        if value is None:
            value = record.get(field)
        if value is None:
            continue
        if isinstance(value, list):
            result[key] = [str(item) for item in value]
        else:
            result[key] = [str(value)]
    return result


def exiftool_write_file(
    exiftool_path: str,
    file_path: Path,
    updates: dict[str, list[str]],
    *,
    allow_sidecar: bool,
) -> None:
    cmd = [exiftool_path, "-charset", "filename=utf8", "-charset", "iptc=utf8", "-overwrite_original", "-P"]
    for field, values in updates.items():
        cmd.append(f"-{field}=")
        for value in values:
            cmd.append(f"-{field}={value}")
    cmd.append(str(file_path))

    rc, out, err = _run_exiftool(cmd)
    if rc != 0:
        raise RuntimeError(f"exiftool write failed: {err.strip()}")

    output_lower = (out + "\n" + err).lower()
    if (not allow_sidecar) and "created xmp sidecar file" in output_lower:
        raise RuntimeError("Refusing sidecar creation (use --retag-allow-sidecar to opt in).")


def write_report(report_path: Path, results: list[FileResult], changes: list[Change]) -> None:
    with report_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "path",
                "rel_path",
                "status",
                "changed_fields",
                "reindexed",
                "message",
                "field",
                "before_json",
                "after_json",
            ]
        )
        changes_by_path: dict[str, list[Change]] = {}
        for change in changes:
            changes_by_path.setdefault(change.path, []).append(change)
        for result in results:
            file_changes = changes_by_path.get(result.path, [])
            if not file_changes:
                writer.writerow(
                    [
                        result.path,
                        result.rel_path,
                        result.status,
                        result.changed_fields,
                        1 if result.reindexed else 0,
                        result.message,
                        "",
                        "",
                        "",
                    ]
                )
                continue
            for idx, change in enumerate(file_changes):
                writer.writerow(
                    [
                        result.path,
                        result.rel_path,
                        result.status if idx == 0 else "",
                        result.changed_fields if idx == 0 else "",
                        (1 if result.reindexed else 0) if idx == 0 else "",
                        result.message if idx == 0 else "",
                        change.field,
                        json.dumps(change.before, ensure_ascii=False),
                        json.dumps(change.after, ensure_ascii=False),
                    ]
                )


def _resolve_exts(config: AppConfig, extra_exts: list[str], no_video: bool) -> set[str]:
    exts = set(DEFAULT_EXTS)
    exts.update(config.image_extensions)
    exts.update(config.video_extensions)
    for ext in extra_exts:
        exts.add(ext.lower() if ext.startswith(".") else f".{ext.lower()}")
    if no_video:
        exts.difference_update(config.video_extensions)
        exts.difference_update({".mp4", ".mov", ".m4v"})
    return exts


def run_retag(
    db: Database,
    config: AppConfig,
    *,
    db_path: Path,
    media_root: Path,
    db_media_path: Path,
    map_path: Path,
    report_path: Path,
    log_path: Path,
    dry_run: bool,
    case_insensitive: bool,
    allow_sidecar: bool,
    no_video: bool,
    extra_exts: list[str],
    no_reindex: bool,
    errors_log_path: Optional[Path],
) -> dict:
    exiftool_path = find_exiftool(config.exiftool_path)
    if not exiftool_path:
        raise RuntimeError("ExifTool not found. Install it or set exiftool_path in config.yaml")

    logger = setup_logger(log_path)
    mapping = load_map(map_path, case_insensitive)
    exts = _resolve_exts(config, extra_exts, no_video)
    files = list_media(media_root, exts)

    logger.info(
        "Start retag root=%s map=%s mode=%s reindex=%s",
        media_root,
        map_path,
        "dry-run" if dry_run else "apply",
        not no_reindex,
    )

    changes: list[Change] = []
    results: list[FileResult] = []
    errors: list[tuple[str, str]] = []

    for file_path in files:
        try:
            if (not allow_sidecar) and file_path.suffix.lower() in SIDECAR_PRONE_EXTS and not dry_run:
                raise RuntimeError("Sidecar-prone format blocked by default (use --retag-allow-sidecar to opt in).")

            tags_by_field = exiftool_read_tags(exiftool_path, file_path)
            updates: dict[str, list[str]] = {}
            pending_changes: list[Change] = []

            for field, key in TAG_FIELDS:
                before = tags_by_field.get(key, [])
                if not before:
                    continue

                after, changed = rewrite_tags(
                    before,
                    mapping,
                    case_insensitive,
                    hierarchical=field.endswith("HierarchicalSubject"),
                )
                if not changed:
                    continue

                pending_changes.append(Change(str(file_path), field, before, after))
                updates[field] = after

            if not updates:
                results.append(
                    FileResult(
                        path=str(file_path),
                        rel_path="",
                        status="unchanged",
                        changed_fields=0,
                        reindexed=False,
                        message="No matching tags",
                    )
                )
                continue

            changes.extend(pending_changes)
            if dry_run:
                results.append(
                    FileResult(
                        path=str(file_path),
                        rel_path="",
                        status="planned",
                        changed_fields=len(pending_changes),
                        reindexed=False,
                        message="Would update metadata",
                    )
                )
                logger.info("planned path=%s fields=%d", file_path, len(pending_changes))
                continue

            exiftool_write_file(
                exiftool_path,
                file_path,
                updates,
                allow_sidecar=allow_sidecar,
            )

            rel_path = ""
            reindexed = False
            status = "changed"
            message = "Updated metadata"
            if not no_reindex:
                try:
                    refresh_result = refresh_file(
                        db,
                        config,
                        media_root,
                        file_path,
                        db_media_root=db_media_path,
                        dry_run=False,
                        video_tags=True,
                        errors_log_path=errors_log_path,
                        db_path=db_path,
                    )
                    rel_path = str(refresh_result.get("rel_path", ""))
                    reindexed = True
                    message = "Updated metadata and refreshed SQLite index"
                except Exception as exc:
                    errors.append((str(file_path), str(exc)))
                    status = "changed_reindex_failed"
                    message = f"Metadata updated but reindex failed: {exc}"
                    logger.error("reindex_failed path=%s message=%s", file_path, exc)

            results.append(
                FileResult(
                    path=str(file_path),
                    rel_path=rel_path,
                    status=status,
                    changed_fields=len(pending_changes),
                    reindexed=reindexed,
                    message=message,
                )
            )
            logger.info(
                "changed path=%s fields=%d reindexed=%s rel_path=%s",
                file_path,
                len(pending_changes),
                reindexed,
                rel_path or "-",
            )
        except Exception as exc:
            errors.append((str(file_path), str(exc)))
            results.append(
                FileResult(
                    path=str(file_path),
                    rel_path="",
                    status="error",
                    changed_fields=0,
                    reindexed=False,
                    message=str(exc),
                )
            )
            logger.error("error path=%s message=%s", file_path, exc)

    write_report(report_path, results, changes)

    changed_files = sum(1 for item in results if item.status in {"changed", "changed_reindex_failed"})
    planned_files = sum(1 for item in results if item.status == "planned")
    unchanged_files = sum(1 for item in results if item.status == "unchanged")
    summary = {
        "version": "",
        "mode": "retag",
        "media_root": str(media_root),
        "db_media_path": str(db_media_path),
        "db": str(db_path),
        "map_path": str(map_path),
        "report_path": str(report_path),
        "log_path": str(log_path),
        "dry_run": dry_run,
        "reindex_enabled": not no_reindex and not dry_run,
        "files": len(results),
        "changed_files": changed_files,
        "planned_files": planned_files,
        "unchanged_files": unchanged_files,
        "field_updates": len(changes),
        "errors": len(errors),
    }
    logger.info(
        "Done files=%d changed_files=%d planned_files=%d unchanged_files=%d field_updates=%d errors=%d",
        summary["files"],
        changed_files,
        planned_files,
        unchanged_files,
        len(changes),
        len(errors),
    )
    return summary
