from pathlib import Path
import sqlite3

from app.core.db import Database, SCHEMA_VERSION


def test_db_init_and_version(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    db = Database(db_path)
    db.close()

    conn = sqlite3.connect(db_path)
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version == SCHEMA_VERSION

    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    assert "roots" in tables
    assert "directories" in tables
    assert "files" in tables
    assert "tags" in tables
    assert "file_tags" in tables
    assert "errors" in tables
    meta_cols = {
        row[1]
        for row in conn.execute("PRAGMA table_info(meta)").fetchall()
    }
    assert "indexer_version" in meta_cols
    assert "include_videos" in meta_cols
    assert "include_docs" in meta_cols
    assert "include_audio" in meta_cols
    assert "video_tags" in meta_cols
    assert "video_tag_blacklist_sha256" in meta_cols
    file_cols = {
        row[1]
        for row in conn.execute("PRAGMA table_info(files)").fetchall()
    }
    assert "sha256" in file_cols
    conn.close()
