CREATE TABLE IF NOT EXISTS post (
    uri        TEXT PRIMARY KEY,
    cid        TEXT NOT NULL,
    indexed_at TEXT NOT NULL,
    feed_name  TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_post_feed_cursor ON post (feed_name, indexed_at DESC, cid DESC);

CREATE TABLE IF NOT EXISTS sub_state (
    service TEXT PRIMARY KEY,
    cursor  BIGINT NOT NULL DEFAULT 0
);
