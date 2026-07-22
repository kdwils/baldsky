package mocks

//go:generate go run -tags tools go.uber.org/mock/mockgen -destination=mock_cursor_store.go -package=mocks github.com/kdwils/baldsky/firehose CursorStore
//go:generate go run -tags tools go.uber.org/mock/mockgen -destination=mock_dialer.go -package=mocks github.com/kdwils/baldsky/firehose Dialer
