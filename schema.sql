BEGIN TRANSACTION;

CREATE TABLE meta (
    db_version INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE roots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    path TEXT UNIQUE NOT NULL,
    added_at TEXT NOT NULL,
    last_scan_at TEXT
);

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
);

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
);

CREATE TABLE tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tag TEXT NOT NULL,
    kind TEXT NOT NULL,
    source TEXT NOT NULL,
    UNIQUE(tag, kind, source)
);

CREATE TABLE file_tags (
    file_id INTEGER NOT NULL,
    tag_id INTEGER NOT NULL,
    PRIMARY KEY (file_id, tag_id),
    FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE,
    FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
);

CREATE TABLE errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    scope TEXT NOT NULL,
    message TEXT NOT NULL,
    details TEXT
);

CREATE INDEX idx_files_path ON files(path);
CREATE INDEX idx_files_type ON files(type);
CREATE INDEX idx_files_mtime ON files(mtime);
CREATE INDEX idx_files_taken_ts ON files(taken_ts);
CREATE INDEX idx_files_sha256 ON files(sha256);
CREATE INDEX idx_tags_tag ON tags(tag);
CREATE INDEX idx_file_tags_tag_file ON file_tags(tag_id, file_id);
CREATE INDEX idx_dirs_root ON directories(root_id);
CREATE INDEX idx_dirs_parent ON directories(parent_id);
CREATE INDEX idx_files_directory ON files(directory_id);

COMMIT;
