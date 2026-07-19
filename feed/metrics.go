package feed

import (
	"context"
	"database/sql"
	"time"

	"github.com/kdwils/baldsky/db/gen"
	"github.com/kdwils/baldsky/logger"
)

type FeedStatsResponse struct {
	FeedName     string  `json:"feed_name"`
	TotalViews   int64   `json:"total_views"`
	LastViewedAt *string `json:"last_viewed_at,omitempty"`
}

type ViewEvent struct {
	FeedName string
}

type MetricsService struct {
	ch    chan ViewEvent
	store gen.Querier
	now   func() string
}

func NewMetricsService(store gen.Querier, bufferSize int) *MetricsService {
	ms := &MetricsService{
		ch:    make(chan ViewEvent, bufferSize),
		store: store,
		now:   now,
	}
	return ms
}

func (ms *MetricsService) RecordView(ctx context.Context, feedName string) {
	select {
	case ms.ch <- ViewEvent{FeedName: feedName}:
	default:
		logger.FromContext(ctx).Warn("dropping event from channel")
	}
}

func (ms *MetricsService) Run(ctx context.Context) {
	logger := logger.FromContext(ctx)
	for event := range ms.ch {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		if err := ms.store.RecordView(ctx, gen.RecordViewParams{
			FeedName: event.FeedName,
			LastViewedAt: sql.NullString{
				String: ms.now(),
				Valid:  true,
			},
		}); err != nil {
			logger.Warn("failed to record view", "feed", event.FeedName, "err", err)
		}
		cancel()
	}
}

func (ms *MetricsService) Close() {
	close(ms.ch)
}

func (ms *MetricsService) GetFeedStats(ctx context.Context, feedName string) (FeedStatsResponse, error) {
	stats, err := ms.store.GetFeedStats(ctx, feedName)
	if err != nil {
		return FeedStatsResponse{}, err
	}

	return FeedStatsResponse{
		FeedName:     stats.FeedName,
		TotalViews:   stats.TotalViews,
		LastViewedAt: &stats.LastViewedAt.String,
	}, nil
}
