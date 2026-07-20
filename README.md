# baldsky

A Bluesky custom feed generator that watches the firehose and filters posts into feeds based on keyword matching.

Right now it runs one feed -- `Bald Things`.

Posts are filtered through a pipeline: DID blocklist, optional media/language requirements, then a two-tier keyword system where some terms match on their own and others (like "bald") need a context word (like "shave" or "scalp") to also be present. Exclude keywords filter out false positives like "bald eagle" or "lawn".

## Built with

- [indigo](https://github.com/bluesky-social/indigo)
- [gorilla/mux](https://github.com/gorilla/mux)
- [gorilla/websocket](https://github.com/gorilla/websocket)
- [pgx](https://github.com/jackc/pgx)
- [sqlc](https://github.com/sqlc-dev/sqlc)
- [golang-migrate](https://github.com/golang-migrate/migrate)
- [cobra](https://github.com/spf13/cobra)
- [viper](https://github.com/spf13/viper)
- [gomock](https://go.uber.org/mock)
- [testify](https://github.com/stretchr/testify)
- Docker

## Live in Kubernetes
- Deployed from https://github.com/kdwils/homelab/blob/main/apps/baldsky/environments/homelab/homelab.yaml
- Available at `baldsky.kyledev.co`
- `at://did:plc:4rlgcneeu5n4jdnlfwljdf2j/app.bsky.feed.generator/bald`