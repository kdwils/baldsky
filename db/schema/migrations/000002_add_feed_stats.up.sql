CREATE TABLE IF NOT EXISTS feed_stats (
    feed_name      TEXT PRIMARY KEY,
    total_views    BIGINT NOT NULL DEFAULT 0,
    last_viewed_at TEXT
);