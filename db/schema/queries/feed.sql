-- name: InsertPost :exec
INSERT INTO post (uri, cid, indexed_at, feed_name)
VALUES (sqlc.arg('uri'), sqlc.arg('cid'), sqlc.arg('indexed_at'), sqlc.arg('feed_name'))
ON CONFLICT DO NOTHING;

-- name: DeletePost :exec
DELETE FROM post WHERE uri = sqlc.arg('uri');

-- name: DeletePosts :exec
DELETE FROM post WHERE uri = ANY(sqlc.arg('uris')::text[]);

-- name: GetFeedPage :many
SELECT uri, cid, indexed_at FROM post
WHERE feed_name = sqlc.arg('feed_name')
AND (
    sqlc.narg('cursor_indexed_at')::text IS NULL
    OR indexed_at < sqlc.narg('cursor_indexed_at')
    OR (indexed_at = sqlc.narg('cursor_indexed_at') AND cid < sqlc.narg('cursor_cid'))
)
ORDER BY indexed_at DESC, cid DESC
LIMIT sqlc.arg('limit');

-- name: GetCursor :one
SELECT cursor FROM sub_state WHERE service = sqlc.arg('service');

-- name: UpsertCursor :exec
INSERT INTO sub_state (service, cursor)
VALUES (sqlc.arg('service'), sqlc.arg('cursor'))
ON CONFLICT (service) DO UPDATE SET cursor = excluded.cursor;

-- name: RecordView :exec
INSERT INTO feed_stats (feed_name, total_views, last_viewed_at)
VALUES ($1, 1, $2)
ON CONFLICT (feed_name) DO UPDATE SET
    total_views = feed_stats.total_views + 1,
    last_viewed_at = excluded.last_viewed_at;

-- name: GetFeedStats :one
SELECT feed_name, total_views, last_viewed_at
FROM feed_stats
WHERE feed_name = $1;