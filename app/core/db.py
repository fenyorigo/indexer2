from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from app.core.models import RootRecord

SCHEMA_VERSION = 5


@dataclass
class DbPaths:
    path: Path


class Database:
    def __init__(self, path: Path):
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.migrate()

    def close(self) -> None:
        self.conn.close()

    def migrate(self) -> None:
        cur = self.conn.cursor()
        cur.execute("PRAGMA user_version")
        version = cur.fetchone()[0]
        if version == 0:
            try:
                self._create_schema()
                cur.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
                self.conn.commit()
            except sqlite3.Error:
                self.conn.rollback()
                self._drop_schema()
                self._create_schema()
                cur.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
                self.conn.commit()
        elif version == 1:
            self._migrate_v1_to_v2()
            self._migrate_v2_to_v3()
            self._migrate_v3_to_v4()
            self._migrate_v4_to_v5()
            cur.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            self.conn.commit()
        elif version == 2:
            self._migrate_v2_to_v3()
            self._migrate_v3_to_v4()
            self._migrate_v4_to_v5()
            cur.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            self.conn.commit()
        elif version == 3:
            self._migrate_v3_to_v4()
            self._migrate_v4_to_v5()
            cur.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            self.conn.commit()
        elif version == 4:
            self._migrate_v4_to_v5()
            cur.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            self.conn.commit()
        elif version > SCHEMA_VERSION:
            raise RuntimeError(f"Unsupported schema version: {version}")

    def _create_schema(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE meta (
                db_version INTEGER NOT NULL,
                indexer_version TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                include_videos INTEGER NOT NULL DEFAULT 1,
                include_docs INTEGER NOT NULL DEFAULT 0,
                include_audio INTEGER NOT NULL DEFAULT 0,
                video_tags INTEGER NOT NULL DEFAULT 0,
                video_tag_blacklist_sha256 TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE roots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                path TEXT UNIQUE NOT NULL,
                added_at TEXT NOT NULL,
                last_scan_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE directories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                root_id INTEGER NOT NULL,
                parent_id INTEGER,
                path TEXT UNIQUE NOT NULL,
                rel_path TEXT NOT NULL,
                depth INTEGER NOT NULL,
                added_at TEXT NOT NULL,
                last_scan_at TEXT,
                scan_status TEXT NOT NULL DEFAULT 'pending',
                FOREIGN KEY (root_id) REFERENCES roots(id) ON DELETE CASCADE,
                FOREIGN KEY (parent_id) REFERENCES directories(id) ON DELETE CASCADE
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                directory_id INTEGER NOT NULL,
                path TEXT UNIQUE NOT NULL,
                rel_path TEXT NOT NULL,
                name TEXT NOT NULL,
                ext TEXT NOT NULL,
                size INTEGER NOT NULL,
                mtime INTEGER NOT NULL,
                ctime INTEGER NOT NULL,
                taken_ts INTEGER,
                taken_src TEXT NOT NULL,
                type TEXT NOT NULL,
                width INTEGER,
                height INTEGER,
                lat REAL,
                lon REAL,
                make TEXT,
                model TEXT,
                hash TEXT,
                sha256 TEXT,
                mime TEXT,
                exiftool_json TEXT,
                indexed_at TEXT NOT NULL,
                FOREIGN KEY (directory_id) REFERENCES directories(id) ON DELETE CASCADE
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tag TEXT NOT NULL,
                kind TEXT NOT NULL,
                source TEXT NOT NULL,
                UNIQUE(tag, kind, source)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE file_tags (
                file_id INTEGER NOT NULL,
                tag_id INTEGER NOT NULL,
                PRIMARY KEY (file_id, tag_id),
                FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE,
                FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                scope TEXT NOT NULL,
                message TEXT NOT NULL,
                details TEXT
            )
            """
        )
        cur.execute(
            "INSERT INTO meta (db_version, indexer_version, created_at, updated_at, include_videos, include_docs, include_audio, video_tags, video_tag_blacklist_sha256)\n"
            "VALUES (?, NULL, datetime('now'), datetime('now'), 1, 0, 0, 0, NULL)",
            (SCHEMA_VERSION,),
        )
        cur.execute("CREATE INDEX idx_files_path ON files(path)")
        cur.execute("CREATE INDEX idx_files_type ON files(type)")
        cur.execute("CREATE INDEX idx_files_mtime ON files(mtime)")
        cur.execute("CREATE INDEX idx_files_taken_ts ON files(taken_ts)")
        cur.execute("CREATE INDEX idx_files_sha256 ON files(sha256)")
        cur.execute("CREATE INDEX idx_tags_tag ON tags(tag)")
        cur.execute("CREATE INDEX idx_file_tags_tag_file ON file_tags(tag_id, file_id)")
        cur.execute("CREATE INDEX idx_dirs_root ON directories(root_id)")
        cur.execute("CREATE INDEX idx_dirs_parent ON directories(parent_id)")
        cur.execute("CREATE INDEX idx_files_directory ON files(directory_id)")

    def _drop_schema(self) -> None:
        cur = self.conn.cursor()
        for name in [
            "file_tags",
            "tags",
            "files",
            "directories",
            "roots",
            "meta",
            "errors",
        ]:
            cur.execute(f"DROP TABLE IF EXISTS {name}")

    def list_roots(self) -> list[RootRecord]:
        cur = self.conn.execute("SELECT id, path, added_at, last_scan_at FROM roots ORDER BY path")
        return [RootRecord(**dict(row)) for row in cur.fetchall()]

    def list_root_children(self, root_id: int) -> list[str]:
        cur = self.conn.execute(
            "SELECT path FROM directories WHERE root_id = ? AND depth = 1 ORDER BY path",
            (root_id,),
        )
        return [row[0] for row in cur.fetchall()]

    def list_root_children_with_status(self, root_id: int) -> list[tuple[str, str]]:
        cur = self.conn.execute(
            "SELECT path, scan_status FROM directories WHERE root_id = ? AND depth = 1 ORDER BY path",
            (root_id,),
        )
        return [(row[0], row[1]) for row in cur.fetchall()]

    def get_root_id(self, path: str) -> Optional[int]:
        cur = self.conn.execute("SELECT id FROM roots WHERE path = ?", (path,))
        row = cur.fetchone()
        return int(row[0]) if row else None

    def ensure_root(self, path: str) -> int:
        cur = self.conn.execute("SELECT id FROM roots WHERE path = ?", (path,))
        row = cur.fetchone()
        if row:
            return int(row[0])
        cur = self.conn.execute(
            "INSERT INTO roots (path, added_at) VALUES (?, datetime('now'))",
            (path,),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def update_root_scan_time(self, root_id: int) -> None:
        self.conn.execute(
            "UPDATE roots SET last_scan_at = datetime('now') WHERE id = ?",
            (root_id,),
        )
        self.conn.commit()

    def directory_exists(self, path: str) -> bool:
        cur = self.conn.execute("SELECT 1 FROM directories WHERE path = ?", (path,))
        return cur.fetchone() is not None

    def delete_directory_subtree(self, path_prefix: str) -> None:
        normalized = path_prefix.rstrip("/")
        like_pattern = f"{normalized}/%"
        self.conn.execute(
            "DELETE FROM files WHERE path = ? OR path LIKE ?",
            (normalized, like_pattern),
        )
        self.conn.execute(
            "DELETE FROM directories WHERE path = ? OR path LIKE ?",
            (normalized, like_pattern),
        )
        self.conn.commit()

    def ensure_directory(
        self,
        root_id: int,
        parent_id: Optional[int],
        path: str,
        rel_path: str,
        depth: int,
    ) -> int:
        cur = self.conn.execute("SELECT id FROM directories WHERE path = ?", (path,))
        row = cur.fetchone()
        if row:
            return int(row[0])
        cur = self.conn.execute(
            """
            INSERT INTO directories (root_id, parent_id, path, rel_path, depth, added_at, scan_status)
            VALUES (?, ?, ?, ?, ?, datetime('now'), 'pending')
            """,
            (root_id, parent_id, path, rel_path, depth),
        )
        return int(cur.lastrowid)

    def update_directory_scan_time(self, directory_id: int) -> None:
        self.conn.execute(
            "UPDATE directories SET last_scan_at = datetime('now') WHERE id = ?",
            (directory_id,),
        )

    def update_directory_status(self, directory_id: int, status: str) -> None:
        self.conn.execute(
            "UPDATE directories SET scan_status = ? WHERE id = ?",
            (status, directory_id),
        )

    def insert_file(
        self,
        directory_id: int,
        path: str,
        rel_path: str,
        name: str,
        ext: str,
        size: int,
        mtime: int,
        ctime: int,
        taken_ts: Optional[int],
        taken_src: str,
        file_type: str,
        width: Optional[int] = None,
        height: Optional[int] = None,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
        make: Optional[str] = None,
        model: Optional[str] = None,
        hash_value: Optional[str] = None,
        sha256_value: Optional[str] = None,
        mime: Optional[str] = None,
        exiftool_json: Optional[str] = None,
    ) -> int:
        existing = self.conn.execute("SELECT id FROM files WHERE path = ?", (path,)).fetchone()
        params = (
            directory_id,
            path,
            rel_path,
            name,
            ext,
            size,
            mtime,
            ctime,
            taken_ts,
            taken_src,
            file_type,
            width,
            height,
            lat,
            lon,
            make,
            model,
            hash_value,
            sha256_value,
            mime,
            exiftool_json,
        )
        if existing:
            file_id = int(existing[0])
            self.conn.execute(
                """
                UPDATE files
                SET directory_id = ?, path = ?, rel_path = ?, name = ?, ext = ?, size = ?, mtime = ?, ctime = ?,
                    taken_ts = ?, taken_src = ?, type = ?, width = ?, height = ?, lat = ?, lon = ?, make = ?,
                    model = ?, hash = ?, sha256 = ?, mime = ?, exiftool_json = ?, indexed_at = datetime('now')
                WHERE id = ?
                """,
                params + (file_id,),
            )
            return file_id

        cur = self.conn.execute(
            """
            INSERT INTO files
            (directory_id, path, rel_path, name, ext, size, mtime, ctime, taken_ts, taken_src, type,
             width, height, lat, lon, make, model, hash, sha256, mime, exiftool_json, indexed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            params,
        )
        return int(cur.lastrowid)

    def ensure_tag(self, tag: str, kind: str, source: str) -> tuple[int, bool]:
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO tags (tag, kind, source) VALUES (?, ?, ?)",
            (tag, kind, source),
        )
        created = cur.rowcount == 1
        cur = self.conn.execute(
            "SELECT id FROM tags WHERE tag = ? AND kind = ? AND source = ?",
            (tag, kind, source),
        )
        row = cur.fetchone()
        return int(row[0]), created

    def link_file_tag(self, file_id: int, tag_id: int) -> bool:
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO file_tags (file_id, tag_id) VALUES (?, ?)",
            (file_id, tag_id),
        )
        return cur.rowcount == 1

    def clear_file_tags(self, file_id: int) -> None:
        self.conn.execute("DELETE FROM file_tags WHERE file_id = ?", (file_id,))

    def prune_orphan_tags(self) -> int:
        cur = self.conn.execute(
            """
            DELETE FROM tags
            WHERE NOT EXISTS (
                SELECT 1
                FROM file_tags ft
                WHERE ft.tag_id = tags.id
            )
            """
        )
        self.conn.commit()
        return int(cur.rowcount)

    def begin(self) -> None:
        if self.conn.in_transaction:
            return
        self.conn.execute("BEGIN")

    def rollback(self) -> None:
        self.conn.rollback()

    def _migrate_v1_to_v2(self) -> None:
        cur = self.conn.cursor()
        cur.execute("ALTER TABLE directories ADD COLUMN scan_status TEXT NOT NULL DEFAULT 'pending'")

        cur.execute(
            """
            CREATE TABLE files_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                directory_id INTEGER NOT NULL,
                path TEXT UNIQUE NOT NULL,
                rel_path TEXT NOT NULL,
                name TEXT NOT NULL,
                ext TEXT NOT NULL,
                size INTEGER NOT NULL,
                mtime INTEGER NOT NULL,
                ctime INTEGER NOT NULL,
                taken_ts INTEGER,
                taken_src TEXT NOT NULL,
                type TEXT NOT NULL,
                width INTEGER,
                height INTEGER,
                lat REAL,
                lon REAL,
                make TEXT,
                model TEXT,
                hash TEXT,
                mime TEXT,
                exiftool_json TEXT,
                indexed_at TEXT NOT NULL,
                FOREIGN KEY (directory_id) REFERENCES directories(id) ON DELETE CASCADE
            )
            """
        )
        cur.execute(
            """
            INSERT INTO files_new
            (id, directory_id, path, rel_path, name, ext, size, mtime, ctime, taken_ts, taken_src, type,
             width, height, lat, lon, make, model, hash, mime, exiftool_json, indexed_at)
            SELECT id, directory_id, path, rel_path, name, ext, size,
                   CAST(mtime AS INTEGER), CAST(ctime AS INTEGER), CAST(mtime AS INTEGER),
                   'mtime_fallback',
                   type, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, exiftool_json, indexed_at
            FROM files
            """
        )
        cur.execute("DROP TABLE files")
        cur.execute("ALTER TABLE files_new RENAME TO files")

        cur.execute(
            """
            CREATE TABLE tags_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tag TEXT NOT NULL,
                kind TEXT NOT NULL,
                source TEXT NOT NULL,
                UNIQUE(tag, kind, source)
            )
            """
        )
        cur.execute(
            "INSERT INTO tags_new (id, tag, kind, source) SELECT id, tag, kind, source FROM tags"
        )
        cur.execute("DROP TABLE tags")
        cur.execute("ALTER TABLE tags_new RENAME TO tags")

        cur.execute("DROP INDEX IF EXISTS idx_file_tags_tag")
        cur.execute("DROP INDEX IF EXISTS idx_file_tags_file")
        cur.execute("DROP INDEX IF EXISTS idx_files_path")
        cur.execute("DROP INDEX IF EXISTS idx_files_type")
        cur.execute("DROP INDEX IF EXISTS idx_files_mtime")
        cur.execute("DROP INDEX IF EXISTS idx_files_taken_ts")
        cur.execute("DROP INDEX IF EXISTS idx_tags_tag")
        cur.execute("DROP INDEX IF EXISTS idx_file_tags_tag_file")
        cur.execute("DROP INDEX IF EXISTS idx_dirs_root")
        cur.execute("DROP INDEX IF EXISTS idx_dirs_parent")
        cur.execute("DROP INDEX IF EXISTS idx_files_directory")

        cur.execute("CREATE INDEX idx_files_path ON files(path)")
        cur.execute("CREATE INDEX idx_files_type ON files(type)")
        cur.execute("CREATE INDEX idx_files_mtime ON files(mtime)")
        cur.execute("CREATE INDEX idx_files_taken_ts ON files(taken_ts)")
        cur.execute("CREATE INDEX idx_tags_tag ON tags(tag)")
        cur.execute("CREATE INDEX idx_file_tags_tag_file ON file_tags(tag_id, file_id)")
        cur.execute("CREATE INDEX idx_dirs_root ON directories(root_id)")
        cur.execute("CREATE INDEX idx_dirs_parent ON directories(parent_id)")
        cur.execute("CREATE INDEX idx_files_directory ON files(directory_id)")

    def _migrate_v2_to_v3(self) -> None:
        cur = self.conn.cursor()
        cur.execute("ALTER TABLE files ADD COLUMN taken_src TEXT NOT NULL DEFAULT 'mtime_fallback'")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_files_taken_ts ON files(taken_ts)")

    def _migrate_v3_to_v4(self) -> None:
        cur = self.conn.cursor()
        cur.execute("ALTER TABLE meta ADD COLUMN indexer_version TEXT")
        cur.execute("ALTER TABLE meta ADD COLUMN include_videos INTEGER NOT NULL DEFAULT 1")
        cur.execute("ALTER TABLE meta ADD COLUMN include_docs INTEGER NOT NULL DEFAULT 0")
        cur.execute("ALTER TABLE meta ADD COLUMN include_audio INTEGER NOT NULL DEFAULT 0")
        cur.execute("ALTER TABLE meta ADD COLUMN video_tags INTEGER NOT NULL DEFAULT 0")
        cur.execute("ALTER TABLE meta ADD COLUMN video_tag_blacklist_sha256 TEXT")

    def _migrate_v4_to_v5(self) -> None:
        cur = self.conn.cursor()
        cur.execute("ALTER TABLE files ADD COLUMN sha256 TEXT")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_files_sha256 ON files(sha256)")
        cur.execute("UPDATE files SET sha256 = lower(hash) WHERE sha256 IS NULL AND hash IS NOT NULL AND length(hash) = 64")

    def update_scan_meta(
        self,
        *,
        indexer_version: str,
        include_videos: bool,
        include_docs: bool,
        include_audio: bool,
        video_tags: bool,
        video_tag_blacklist_sha256: Optional[str],
    ) -> None:
        self.conn.execute(
            "UPDATE meta\n"
            "SET db_version = ?, indexer_version = ?, updated_at = datetime('now'),\n"
            "    include_videos = ?, include_docs = ?, include_audio = ?, video_tags = ?, video_tag_blacklist_sha256 = ?",
            (
                SCHEMA_VERSION,
                indexer_version,
                1 if include_videos else 0,
                1 if include_docs else 0,
                1 if include_audio else 0,
                1 if video_tags else 0,
                video_tag_blacklist_sha256,
            ),
        )

    def log_error(self, scope: str, message: str, details: Optional[str] = None) -> None:
        self.conn.execute(
            "INSERT INTO errors (created_at, scope, message, details) VALUES (datetime('now'), ?, ?, ?)",
            (scope, message, details),
        )
        self.conn.commit()

    def list_errors(self, limit: int = 50) -> list[sqlite3.Row]:
        cur = self.conn.execute(
            "SELECT created_at, scope, message, details FROM errors ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        return list(cur.fetchall())

    def taken_src_distribution(self, root_path: str) -> dict[str, int]:
        normalized = root_path.rstrip("/")
        like_pattern = f"{normalized}/%"
        cur = self.conn.execute(
            """
            SELECT COALESCE(f.taken_src, 'unknown') AS taken_src, COUNT(*) AS count
            FROM files f
            JOIN directories d ON f.directory_id = d.id
            WHERE (d.path = ? OR d.path LIKE ?)
              AND f.type = 'image'
            GROUP BY f.taken_src
            ORDER BY count DESC
            """,
            (normalized, like_pattern),
        )
        return {row["taken_src"]: int(row["count"]) for row in cur.fetchall()}

    def get_files_by_paths(self, paths: list[str]) -> dict[str, sqlite3.Row]:
        if not paths:
            return {}
        result: dict[str, sqlite3.Row] = {}
        chunk_size = 900
        for i in range(0, len(paths), chunk_size):
            chunk = paths[i : i + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            cur = self.conn.execute(
                f"SELECT path, size, mtime, hash, sha256 FROM files WHERE path IN ({placeholders})",
                chunk,
            )
            for row in cur.fetchall():
                result[row["path"]] = row
        return result

    def commit(self) -> None:
        self.conn.commit()
