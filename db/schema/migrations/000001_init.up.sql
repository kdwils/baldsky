CREATE TABLE IF NOT EXISTS post (
    uri        TEXT PRIMARY KEY,
    cid        TEXT NOT NULL,
    indexed_at TEXT NOT NULL,
    feed_name  TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_post_indexed_at ON post (indexed_at DESC, cid DESC);
CREATE INDEX IF NOT EXISTS idx_post_feed_name ON post (feed_name);

CREATE TABLE IF NOT EXISTS sub_state (
    service TEXT PRIMARY KEY,
    cursor  INTEGER NOT NULL DEFAULT 0
);
