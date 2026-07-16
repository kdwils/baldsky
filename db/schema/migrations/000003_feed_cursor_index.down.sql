DROP INDEX IF EXISTS idx_post_feed_cursor;
CREATE INDEX IF NOT EXISTS idx_post_indexed_at ON post (indexed_at DESC, cid DESC);
CREATE INDEX IF NOT EXISTS idx_post_feed_name ON post (feed_name);
