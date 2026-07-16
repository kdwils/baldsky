DROP INDEX IF EXISTS idx_post_indexed_at;
DROP INDEX IF EXISTS idx_post_feed_name;
CREATE INDEX IF NOT EXISTS idx_post_feed_cursor ON post (feed_name, indexed_at DESC, cid DESC);
