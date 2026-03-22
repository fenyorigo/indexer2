from __future__ import annotations

from pathlib import Path

import pytest

from app.core.config import default_config
from app.core.db import Database
from app.core.scanner import move_file, scan
from app.core.models import DirectorySelection
from app.cli import build_parser


def _fake_run_factory(tags_by_name: dict[str, list[str]] | None = None):
    tags_by_name = tags_by_name or {}

    def fake_run(_path, files):
        records = []
        for file_path in files:
            records.append(
                {
                    "SourceFile": str(file_path),
                    "Subject": tags_by_name.get(Path(file_path).name, []),
                }
            )
        return records, None

    return fake_run


def test_move_file_renames_across_directories_and_prunes_empty_source(tmp_path: Path, monkeypatch) -> None:
    media_root = tmp_path / "media"
    source = media_root / "a"
    target = media_root / "b"
    source.mkdir(parents=True)
    target.mkdir(parents=True)
    original = source / "photo.jpg"
    original.write_bytes(b"img")

    monkeypatch.setattr("app.core.scanner.run_exiftool", _fake_run_factory({"photo.jpg": ["tag-a"]}))

    db_path = tmp_path / "test.db"
    db = Database(db_path)
    scan(
        db,
        default_config(),
        media_root,
        selections=[DirectorySelection(path=media_root, recursive=True, include_root_files=True)],
        include_videos=False,
        include_docs=False,
        include_audio=False,
        video_tags=False,
        errors_log_path=tmp_path / "errors.jsonl",
        db_path=db_path,
    )

    moved = target / "photo.jpg"
    original.rename(moved)

    result = move_file(
        db,
        default_config(),
        media_root,
        Path("a/photo.jpg"),
        Path("b/photo.jpg"),
        errors_log_path=tmp_path / "errors.jsonl",
        db_path=db_path,
    )

    file_rows = db.conn.execute("SELECT path, rel_path FROM files ORDER BY path").fetchall()
    dir_rows = db.conn.execute("SELECT path, rel_path FROM directories ORDER BY path").fetchall()
    tag_rows = db.conn.execute(
        "SELECT t.tag FROM tags t JOIN file_tags ft ON ft.tag_id = t.id ORDER BY t.tag"
    ).fetchall()
    db.close()

    assert result["old_path_removed"] is True
    assert result["old_path_missing"] is False
    assert result["pruned_directories"] == [str(media_root / "a")]
    assert [(row["path"], row["rel_path"]) for row in file_rows] == [
        (str(media_root / "b" / "photo.jpg"), "b/photo.jpg")
    ]
    assert [row["rel_path"] for row in dir_rows] == ["", "b"]
    assert [row["tag"] for row in tag_rows] == ["tag-a"]


def test_move_file_keeps_source_directory_when_other_files_remain(tmp_path: Path, monkeypatch) -> None:
    media_root = tmp_path / "media"
    source = media_root / "a"
    target = media_root / "b"
    source.mkdir(parents=True)
    target.mkdir(parents=True)
    original = source / "photo.jpg"
    sibling = source / "keep.jpg"
    original.write_bytes(b"img")
    sibling.write_bytes(b"img2")

    monkeypatch.setattr("app.core.scanner.run_exiftool", _fake_run_factory())

    db_path = tmp_path / "test.db"
    db = Database(db_path)
    scan(
        db,
        default_config(),
        media_root,
        selections=[DirectorySelection(path=media_root, recursive=True, include_root_files=True)],
        include_videos=False,
        include_docs=False,
        include_audio=False,
        video_tags=False,
        errors_log_path=tmp_path / "errors.jsonl",
        db_path=db_path,
    )

    original.rename(target / "photo.jpg")
    result = move_file(
        db,
        default_config(),
        media_root,
        Path("a/photo.jpg"),
        Path("b/photo.jpg"),
        errors_log_path=tmp_path / "errors.jsonl",
        db_path=db_path,
    )

    dir_rows = db.conn.execute("SELECT rel_path FROM directories ORDER BY rel_path").fetchall()
    db.close()

    assert result["pruned_directories"] == []
    assert [row["rel_path"] for row in dir_rows] == ["", "a", "b"]


def test_move_file_refreshes_when_old_path_missing(tmp_path: Path, monkeypatch) -> None:
    media_root = tmp_path / "media"
    media_root.mkdir()
    moved = media_root / "new.jpg"
    moved.write_bytes(b"img")

    monkeypatch.setattr("app.core.scanner.run_exiftool", _fake_run_factory({"new.jpg": ["tag-a"]}))

    db_path = tmp_path / "test.db"
    db = Database(db_path)
    result = move_file(
        db,
        default_config(),
        media_root,
        Path("old.jpg"),
        Path("new.jpg"),
        errors_log_path=tmp_path / "errors.jsonl",
        db_path=db_path,
    )
    file_rows = db.conn.execute("SELECT rel_path FROM files").fetchall()
    db.close()

    assert result["old_path_removed"] is False
    assert result["old_path_missing"] is True
    assert [row["rel_path"] for row in file_rows] == ["new.jpg"]


def test_move_file_fails_when_new_path_missing(tmp_path: Path, monkeypatch) -> None:
    media_root = tmp_path / "media"
    media_root.mkdir()

    monkeypatch.setattr("app.core.scanner.run_exiftool", _fake_run_factory())

    db_path = tmp_path / "test.db"
    db = Database(db_path)
    with pytest.raises(ValueError, match="New file path must exist"):
        move_file(
            db,
            default_config(),
            media_root,
            Path("old.jpg"),
            Path("missing.jpg"),
            errors_log_path=tmp_path / "errors.jsonl",
            db_path=db_path,
        )
    db.close()


def test_move_file_preserves_old_row_when_new_path_missing(tmp_path: Path, monkeypatch) -> None:
    media_root = tmp_path / "media"
    source = media_root / "a"
    source.mkdir(parents=True)
    original = source / "photo.jpg"
    original.write_bytes(b"img")

    monkeypatch.setattr("app.core.scanner.run_exiftool", _fake_run_factory({"photo.jpg": ["tag-a"]}))

    db_path = tmp_path / "test.db"
    db = Database(db_path)
    scan(
        db,
        default_config(),
        media_root,
        selections=[DirectorySelection(path=media_root, recursive=True, include_root_files=True)],
        include_videos=False,
        include_docs=False,
        include_audio=False,
        video_tags=False,
        errors_log_path=tmp_path / "errors.jsonl",
        db_path=db_path,
    )

    with pytest.raises(ValueError, match="New file path must exist"):
        move_file(
            db,
            default_config(),
            media_root,
            Path("a/photo.jpg"),
            Path("missing/photo.jpg"),
            errors_log_path=tmp_path / "errors.jsonl",
            db_path=db_path,
        )

    file_rows = db.conn.execute("SELECT rel_path FROM files ORDER BY rel_path").fetchall()
    dir_rows = db.conn.execute("SELECT rel_path FROM directories ORDER BY rel_path").fetchall()
    tag_rows = db.conn.execute(
        "SELECT t.tag FROM tags t JOIN file_tags ft ON ft.tag_id = t.id ORDER BY t.tag"
    ).fetchall()
    db.close()

    assert [row["rel_path"] for row in file_rows] == ["a/photo.jpg"]
    assert [row["rel_path"] for row in dir_rows] == ["", "a"]
    assert [row["tag"] for row in tag_rows] == ["tag-a"]


def test_move_file_cli_parses_move_arguments() -> None:
    args = build_parser().parse_args(
        [
            "--cli",
            "--db",
            "/tmp/photos.db",
            "--media-root",
            "/tmp/photos",
            "--move-file",
            "old.jpg",
            "--to",
            "new.jpg",
        ]
    )
    assert str(args.move_file) == "old.jpg"
    assert str(args.to) == "new.jpg"
