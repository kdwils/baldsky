package mocks

//go:generate go run -tags tools go.uber.org/mock/mockgen -destination=mock_feed_store.go -package=mocks github.com/kdwils/baldsky/feed Store
//go:generate go run -tags tools go.uber.org/mock/mockgen -destination=mock_querier.go -package=mocks github.com/kdwils/baldsky/db/gen Querier
