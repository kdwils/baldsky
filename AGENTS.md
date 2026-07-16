# Unit Testing Conventions

## Structure
- One test function per method/function being tested (e.g., `TestGetFeedPage`)
- Individual `t.Run` subtests within for each test case
- No table-driven tests

## Assertions
- Always assert the full returned response struct (`got` vs `want`), never partial field checks
- For multi-return functions, capture and assert ALL return values, including zero values on error paths
- Use `require` for preconditions that must pass before assertions (`require.NoError`, `require.ErrorIs`)
- Use `assert` for the actual value comparisons (`assert.Equal`, `assert.ElementsMatch`)

## Mocks
- Use `gomock` with generated mocks from `go.uber.org/mock`
- Each `t.Run` subtest creates its own `gomock.Controller` and service instance
- For methods with runtime-generated fields (e.g., timestamps), capture the arg via `gomock.Do` and assert the captured struct after the call
- Prefer explicit `GetFeedPageParams` matchers over `gomock.Any()` when verifying DB query params

---

# AT Protocol & Firehose Architecture

## What the Firehose Is

The firehose is a real-time WebSocket stream exposed by the BGS (Big Graph Server / Relay) at the XRPC endpoint `com.atproto.sync.subscribeRepos`. It broadcasts every repository mutation that occurs across the entire network — posts, likes, follows, deletes, profile updates, etc. — as a continuous, ordered sequence of events.

This project connects to the firehose to populate custom Bluesky feed generator databases. The subscription package (`subscription/subscription.go`) owns this responsibility.

## Wire Protocol

Each message on the stream is a **DAG-CBOR** encoded frame consisting of two parts sent as a single WebSocket binary message:

1. **Header** — a CBOR map with an `op` field:
   - `op: 1` — a regular event message (contains a `t` field for the event type, e.g. `#commit`)
   - `op: -1` — an error frame (contains `error` and `message` string fields)
2. **Body** — a CBOR-encoded payload whose shape depends on the event type declared in the header

Consumers must decode the header first to determine how to decode the body. The indigo library's `events.HandleRepoStream` handles this framing transparently.

## Event Types

The stream emits a union of the following message types:

### `#commit`
The most common and important event. Fires whenever a user's repository is updated (a record is created, updated, or deleted). Fields:

| Field | Type | Description |
|---|---|---|
| `seq` | int64 | Monotonically increasing stream sequence number — the **cursor** |
| `repo` | string | The DID of the account whose repo changed |
| `commit` | CID | The CID of the new commit object |
| `rev` | string | TID-format revision string; acts as a logical clock for the repo |
| `since` | string | The previous revision (`rev`) this commit is based on |
| `blocks` | bytes | A **CAR-encoded** byte slice containing all IPLD blocks needed to read the changed records |
| `ops` | []RepoOp | The list of mutations performed in this commit (see below) |
| `blobs` | []CID | CIDs of any new blobs referenced by the commit |
| `time` | string | RFC3339 timestamp of the commit |
| `tooBig` | bool | When `true`, the commit exceeded size limits; `blocks` and `ops` may be incomplete — consumer must call `com.atproto.sync.getRepo` separately |

### `RepoOp` (element of `ops`)
Each entry in the `ops` array describes a single record mutation:

| Field | Type | Description |
|---|---|---|
| `action` | string | One of `"create"`, `"update"`, or `"delete"` |
| `path` | string | `collection/rkey` identifying the record (e.g. `app.bsky.feed.post/3jqkl...`) |
| `cid` | CID? | CID of the new record value; `null` for `delete` ops |

The `path` format is `<NSID collection>/<record key>`. Record keys for posts are TIDs (Timestamp IDs). The collection NSID tells you what type of record it is (`app.bsky.feed.post`, `app.bsky.feed.like`, `app.bsky.graph.follow`, etc.).

### `#sync`
A more efficient synchronization event introduced alongside the newer sync protocol. Contains a `blocks` CAR and repo metadata but omits the granular `ops` list. Consumers that need per-record deltas should still use `#commit`.

### `#identity`
Fired when an account's DID document or handle changes. Contains `seq`, `did`, `handle`, and `time`. Useful for keeping handle caches fresh.

### `#account`
Fired when an account's hosting status changes (e.g., account deactivated, taken down, reactivated). Contains `seq`, `did`, `active` (bool), `status`, and `time`.

### `#info`
Informational messages from the relay, typically sent at connection time. The most common value is `OutdatedCursor` — the relay could not find the requested cursor and is streaming from its current head instead.

## The Cursor

The cursor is the integer `seq` value attached to every event. It uniquely identifies a position in the relay's event log and is the mechanism for **resumable consumption**.

### How This Project Uses It

1. On connection (`subscribe`), `CursorStore.GetCursor` loads the last persisted `seq` from the database.
2. If `seq > 0`, it is appended to the WebSocket URL as `?cursor=<seq>`, instructing the relay to replay all events after that point.
3. After successfully processing each `#commit` event, `CursorStore.UpsertCursor` saves the event's `seq` to the database.
4. On reconnect (the outer `Listen` loop retries on error), the saved cursor is loaded again, enabling the consumer to catch up from where it left off without missing events.

### Cursor Edge Cases

- **`cursor = 0`** (no stored cursor): Connect without a cursor parameter — the relay streams from its live head. The consumer accepts that it has missed all historical events.
- **`FutureCursor`**: The requested `seq` is ahead of what the relay knows about. The relay returns an error frame; this project will log the error and reconnect.
- **`OutdatedCursor`**: The relay has pruned its log past the requested `seq`. It sends a `#info` message with `name: "OutdatedCursor"` and then continues streaming from its current head. Events between the last saved cursor and the relay's current head are permanently lost for this consumer.
- **`ConsumerTooSlow`**: The relay may drop the connection if the consumer cannot keep up with the live head. The outer `Listen` loop handles this by reconnecting with the last saved cursor.

## CAR Blocks and Record Extraction

The `blocks` field in a `#commit` event is a **Content Addressed Archive (CAR)** — a binary serialization of a set of IPLD nodes. It contains exactly the nodes that changed in this commit: the new or modified records and the MST (Merkle Search Tree) nodes along the path to each changed record.

To read a record from a `#commit` event:

1. Call `repo.ReadRepoFromCar(ctx, bytes.NewReader(evt.Blocks))` (indigo library) to parse the CAR into an in-memory repo view.
2. Iterate `evt.Ops` to find operations of interest (filtered by `op.Path` prefix, e.g. `app.bsky.feed.post/`).
3. For `create` and `update` ops, call `rr.GetRecord(ctx, op.Path)` to decode the CBOR record bytes into a Go struct (e.g. `*bsky.FeedPost`).
4. For `delete` ops, no record data is present — only the AT-URI is needed to remove the record from the local database.

## Event Processing Pipeline

```
WebSocket frame (DAG-CBOR)
  └─► events.HandleRepoStream        (indigo — decodes frames, routes by event type)
        └─► parallel.Scheduler        (concurrency-limited goroutine pool)
              └─► RepoStreamCallbacks.RepoCommit
                    └─► handleCommit
                          ├─► Skip if evt.TooBig
                          ├─► repo.ReadRepoFromCar  (parse CAR blocks into repo view)
                          └─► for each op in evt.Ops:
                                ├─► Skip if path prefix != "app.bsky.feed.post/"
                                ├─► "delete" → handleDelete → PipelineStore.DeletePosts
                                └─► "create" → handleCreate
                                      ├─► rr.GetRecord → *bsky.FeedPost
                                      └─► for each Pipeline:
                                            ├─► shouldInsert (DID block, media gate, language, keyword match)
                                            └─► PipelineStore.InsertPost
                          └─► CursorStore.UpsertCursor (persist seq after all ops processed)
```

## Pipeline Matching

Each `Pipeline` encapsulates a named feed's matching logic. A post is inserted into a pipeline's feed table when `shouldInsert` returns `true`:

1. **DID block**: if the author's DID is in `blockedDIDs`, reject immediately.
2. **Media gate**: if `requireMedia` is set, reject posts with no embed (no attached image/video).
3. **Language filter**: if the pipeline declares languages, the post must declare at least one matching language tag.
4. **Keyword match** (`matches`): evaluated against the post text (lowercased):
   - **Primary keywords**: a regex word-boundary match on any keyword causes an immediate pass (unless an exclude keyword also matches).
   - **Context keywords**: a keyword that only qualifies if at least one **context word** also appears in the text. Provides topic disambiguation (e.g. `"python"` alone is ambiguous; `"python"` + `"programming"` is specific).
   - **Exclude keywords**: if matched, the post is rejected regardless of which keyword triggered the match.

## Feed Cursor (Client-Facing)

The feed pagination cursor used by Bluesky clients is **entirely separate** from the firehose sequence cursor. They serve different purposes:

- **Firehose cursor** (`seq` integer): tracks the consumer's position in the relay's event log. Stored in the `cursors` DB table keyed by service URL.
- **Feed cursor** (opaque string `indexedAt::cid`): tracks a client's position within a feed's result set for keyset pagination. Encoded as `<RFC3339 timestamp>::<CID string>` and reconstructed from the last row of each page. Never stored in the database.

## Key Identifiers in AT Protocol

| Identifier | Format | Example |
|---|---|---|
| DID | `did:plc:<base32>` or `did:web:<domain>` | `did:plc:abc123...` |
| AT-URI | `at://<DID>/<collection>/<rkey>` | `at://did:plc:abc/app.bsky.feed.post/3jq...` |
| CID | Content-addressed hash (DAG-CBOR) | `bafyreia...` |
| TID | Base32-sortable timestamp ID used as record key | `3jqkl2abc7k2a` |
| NSID | Reverse-DNS namespaced identifier for a collection | `app.bsky.feed.post` |

## Reconnection Strategy

`Listen` runs an infinite retry loop around `subscribe`. If `subscribe` returns any error (network drop, relay restart, `ConsumerTooSlow`, etc.):

1. Log the error with the reconnect delay.
2. Wait `reconnectDelay` (or return immediately if the context is cancelled).
3. Call `subscribe` again, which reloads the last saved cursor and re-dials the WebSocket.

The `connected` atomic bool is set to `true` inside `subscribe` and reset to `false` via `defer` when it exits, providing a health-check signal via `Subscription.Connected()`.
