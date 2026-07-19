package feed_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/kdwils/baldsky/db/gen"
	"github.com/kdwils/baldsky/feed"
	"github.com/kdwils/baldsky/feed/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestRecordView(t *testing.T) {
	t.Run("sends event to channel and worker processes it", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		q := mocks.NewMockQuerier(ctrl)
		ms := feed.NewMetricsService(q, 10)

		ts := time.Now().UTC().Format(time.RFC3339)
		done := make(chan struct{})
		q.EXPECT().RecordView(gomock.Any(), gen.RecordViewParams{
			FeedName: "test-feed",
			LastViewedAt: sql.NullString{
				String: ts,
				Valid:  true,
			},
		}).Do(func(_ context.Context, _ gen.RecordViewParams) {
			close(done)
		}).Return(nil)

		go ms.Run(t.Context())
		ms.RecordView(t.Context(), "test-feed")

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for RecordView to be called")
		}
		ms.Close()
	})

	t.Run("drops event when channel is full", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		q := mocks.NewMockQuerier(ctrl)
		ms := feed.NewMetricsService(q, 1)

		ms.RecordView(t.Context(), "feed-1")
		ms.RecordView(t.Context(), "feed-2")

		ts := time.Now().UTC().Format(time.RFC3339)
		done := make(chan struct{})
		q.EXPECT().RecordView(gomock.Any(), gen.RecordViewParams{
			FeedName: "feed-1",
			LastViewedAt: sql.NullString{
				String: ts,
				Valid:  true,
			},
		}).Do(func(_ context.Context, _ gen.RecordViewParams) {
			close(done)
		}).Return(nil)

		go ms.Run(t.Context())

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for RecordView to be called")
		}
		ms.Close()
	})
}

func TestGetFeedStats(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		q := mocks.NewMockQuerier(ctrl)
		ms := feed.NewMetricsService(q, 10)
		ctx := t.Context()

		now := time.Now().UTC().Format(time.RFC3339)
		q.EXPECT().GetFeedStats(ctx, "test-feed").Return(gen.FeedStat{
			FeedName:   "test-feed",
			TotalViews: 42,
			LastViewedAt: sql.NullString{
				String: now,
				Valid:  true,
			},
		}, nil)

		got, err := ms.GetFeedStats(ctx, "test-feed")
		require.NoError(t, err)
		want := feed.FeedStatsResponse{
			FeedName:     "test-feed",
			TotalViews:   42,
			LastViewedAt: &now,
		}
		assert.Equal(t, want, got)
	})

	t.Run("error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		q := mocks.NewMockQuerier(ctrl)
		ms := feed.NewMetricsService(q, 10)
		ctx := t.Context()

		q.EXPECT().GetFeedStats(ctx, "nonexistent").Return(gen.FeedStat{}, sql.ErrNoRows)

		got, err := ms.GetFeedStats(ctx, "nonexistent")
		require.Error(t, err)
		assert.Equal(t, feed.FeedStatsResponse{}, got)
	})
}
