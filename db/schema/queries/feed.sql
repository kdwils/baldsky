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
