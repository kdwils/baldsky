package mocks

//go:generate go run -tags tools go.uber.org/mock/mockgen -destination=mock_feed_service.go -package=mocks github.com/kdwils/baldsky/server FeedService
//go:generate go run -tags tools go.uber.org/mock/mockgen -destination=mock_worker_checker.go -package=mocks github.com/kdwils/baldsky/server WorkerChecker
