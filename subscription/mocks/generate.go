package mocks

//go:generate go run -tags tools go.uber.org/mock/mockgen -destination=mock_pipeline_store.go -package=mocks github.com/kdwils/baldsky/subscription PipelineStore
