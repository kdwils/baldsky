//go:build conformance

package conformance

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/kdwils/baldsky/db"
	"github.com/kdwils/baldsky/db/gen"
	"github.com/kdwils/baldsky/feed"
	"github.com/kdwils/baldsky/server"
	"github.com/kdwils/baldsky/server/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/mock/gomock"
)

var testDSN string

func TestMain(m *testing.M) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_DB":       "baldsky_test",
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
		},
		WaitingFor: wait.ForListeningPort("5432/tcp").WithStartupTimeout(30 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		slog.Error("failed to start postgres container", "err", err)
		panic(err)
	}
	defer container.Terminate(ctx)

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "5432")
	testDSN = fmt.Sprintf("postgres://test:test@%s:%s/baldsky_test?sslmode=disable", host, port.Port())

	m.Run()
}

func newDB(t *testing.T) *sql.DB {
	t.Helper()
	sqldb, err := sql.Open("pgx", testDSN)
	require.NoError(t, err)
	t.Cleanup(func() { sqldb.Close() })

	pg := &db.Postgres{DB: sqldb}
	require.NoError(t, pg.Migrate())

	_, err = sqldb.Exec("TRUNCATE TABLE post")
	require.NoError(t, err)

	_, err = sqldb.Exec("TRUNCATE TABLE feed_stats")
	require.NoError(t, err)

	return sqldb
}

func waitForServer(t *testing.T, port int) {
	t.Helper()
	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%d/xrpc/_health", port))
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 5*time.Second, 100*time.Millisecond, "server did not become ready")
}

func TestConformance(t *testing.T) {
	publisherDID := "did:plc:conformance_tester"
	serviceDID := "did:web:test.example.com"
	hostname := "test.example.com"

	t.Run("post appears in feed", func(t *testing.T) {
		ctx := t.Context()
		ctrl := gomock.NewController(t)
		fh := mocks.NewMockFirehoseChecker(ctrl)
		fh.EXPECT().Connected().Return(true).AnyTimes()
		rl := server.NewRateLimiter(10.0, 20, 3*time.Minute)

		sqldb := newDB(t)
		queries := gen.New(sqldb)

		feedEntries := []feed.FeedEntry{
			{ShortName: "bald", DisplayName: "Bald", Description: "Bald feed", CollectionName: "app.bsky.feed.generator"},
		}
		feedSvc := feed.New(queries, serviceDID, hostname, publisherDID, "https://www.w3.org/ns/did/v1", "#bsky_fg", feedEntries)
		feedURI := "at://" + publisherDID + "/app.bsky.feed.generator/bald"

		ms := feed.NewMetricsService(queries, 10)
		go ms.Run(ctx)
		defer ms.Close()

		srv := server.New(18081, slog.New(slog.NewTextHandler(io.Discard, nil)), feedSvc, sqldb, "test-admin-token", rl, server.WithFirehose(fh), server.WithMetrics(ms))
		go srv.Run(ctx)
		waitForServer(t, 18081)

		postURI := feedURI + "/1"
		postCID := "cid-1"
		require.NoError(t, feedSvc.InsertPost(ctx, "bald", postURI, postCID))

		resp, err := http.Get("http://localhost:18081/xrpc/app.bsky.feed.getFeedSkeleton?feed=" + feedURI)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var got feed.FeedResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
		want := feed.FeedResponse{
			Feed: []feed.FeedItem{
				{Post: postURI},
			},
		}
		assert.Equal(t, want, got)

		assert.Eventually(t, func() bool {
			stats, err := queries.GetFeedStats(ctx, "bald")
			return err == nil && stats.TotalViews == 1
		}, 5*time.Second, 10*time.Millisecond, "expected TotalViews to be 1")
	})

	t.Run("non matching feed returns empty", func(t *testing.T) {
		ctx := t.Context()
		ctrl := gomock.NewController(t)
		fh := mocks.NewMockFirehoseChecker(ctrl)
		fh.EXPECT().Connected().Return(true).AnyTimes()
		rl := server.NewRateLimiter(10.0, 20, 3*time.Minute)

		sqldb := newDB(t)
		queries := gen.New(sqldb)

		feedEntries := []feed.FeedEntry{
			{ShortName: "bald", DisplayName: "Bald", Description: "Bald feed", CollectionName: "app.bsky.feed.generator"},
		}
		feedSvc := feed.New(queries, serviceDID, hostname, publisherDID, "https://www.w3.org/ns/did/v1", "#bsky_fg", feedEntries)

		ms := feed.NewMetricsService(queries, 10)
		go ms.Run(ctx)
		defer ms.Close()

		srv := server.New(18082, slog.New(slog.NewTextHandler(io.Discard, nil)), feedSvc, sqldb, "test-admin-token", rl, server.WithFirehose(fh), server.WithMetrics(ms))
		go srv.Run(ctx)
		waitForServer(t, 18082)

		require.NoError(t, feedSvc.InsertPost(ctx, "bald", "at://"+publisherDID+"/app.bsky.feed.post/1", "cid-1"))

		otherFeedURI := "at://" + publisherDID + "/app.bsky.feed.generator/nonexistent"
		resp, err := http.Get("http://localhost:18082/xrpc/app.bsky.feed.getFeedSkeleton?feed=" + otherFeedURI)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		var got map[string]string
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
		want := map[string]string{"error": "UnknownFeed"}
		assert.Equal(t, want, got)
	})

	t.Run("posts returned in descending order", func(t *testing.T) {
		ctx := t.Context()
		ctrl := gomock.NewController(t)
		fh := mocks.NewMockFirehoseChecker(ctrl)
		fh.EXPECT().Connected().Return(true).AnyTimes()
		rl := server.NewRateLimiter(10.0, 20, 3*time.Minute)

		sqldb := newDB(t)
		queries := gen.New(sqldb)

		feedEntries := []feed.FeedEntry{
			{ShortName: "bald", DisplayName: "Bald", Description: "Bald feed", CollectionName: "app.bsky.feed.generator"},
		}
		feedSvc := feed.New(queries, serviceDID, hostname, publisherDID, "https://www.w3.org/ns/did/v1", "#bsky_fg", feedEntries)
		feedURI := "at://" + publisherDID + "/app.bsky.feed.generator/bald"

		ms := feed.NewMetricsService(queries, 10)
		go ms.Run(ctx)
		defer ms.Close()

		srv := server.New(18083, slog.New(slog.NewTextHandler(io.Discard, nil)), feedSvc, sqldb, "test-admin-token", rl, server.WithFirehose(fh), server.WithMetrics(ms))
		go srv.Run(ctx)
		waitForServer(t, 18083)

		require.NoError(t, feedSvc.InsertPost(ctx, "bald", feedURI+"/1", "cid-1"))
		require.NoError(t, feedSvc.InsertPost(ctx, "bald", feedURI+"/2", "cid-2"))
		require.NoError(t, feedSvc.InsertPost(ctx, "bald", feedURI+"/3", "cid-3"))

		resp, err := http.Get("http://localhost:18083/xrpc/app.bsky.feed.getFeedSkeleton?feed=" + feedURI)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var got feed.FeedResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
		want := feed.FeedResponse{
			Feed: []feed.FeedItem{
				{Post: feedURI + "/3"},
				{Post: feedURI + "/2"},
				{Post: feedURI + "/1"},
			},
		}
		assert.Equal(t, want, got)

		assert.Eventually(t, func() bool {
			stats, err := queries.GetFeedStats(ctx, "bald")
			return err == nil && stats.TotalViews == 1
		}, 5*time.Second, 10*time.Millisecond, "expected TotalViews to be 1")
	})

	t.Run("feed pagination with cursor", func(t *testing.T) {
		ctx := t.Context()
		ctrl := gomock.NewController(t)
		fh := mocks.NewMockFirehoseChecker(ctrl)
		fh.EXPECT().Connected().Return(true).AnyTimes()
		rl := server.NewRateLimiter(10.0, 20, 3*time.Minute)

		sqldb := newDB(t)
		queries := gen.New(sqldb)

		feedEntries := []feed.FeedEntry{
			{ShortName: "bald", DisplayName: "Bald", Description: "Bald feed", CollectionName: "app.bsky.feed.generator"},
		}
		feedSvc := feed.New(queries, serviceDID, hostname, publisherDID, "https://www.w3.org/ns/did/v1", "#bsky_fg", feedEntries)
		feedURI := "at://" + publisherDID + "/app.bsky.feed.generator/bald"

		ms := feed.NewMetricsService(queries, 10)
		go ms.Run(ctx)
		defer ms.Close()

		srv := server.New(18084, slog.New(slog.NewTextHandler(io.Discard, nil)), feedSvc, sqldb, "test-admin-token", rl, server.WithFirehose(fh), server.WithMetrics(ms))
		go srv.Run(ctx)
		waitForServer(t, 18084)

		for i := 1; i <= 5; i++ {
			require.NoError(t, feedSvc.InsertPost(ctx, "bald", fmt.Sprintf("%s/%d", feedURI, i), fmt.Sprintf("cid-%d", i)))
			time.Sleep(10 * time.Millisecond)
		}

		resp, err := http.Get("http://localhost:18084/xrpc/app.bsky.feed.getFeedSkeleton?feed=" + feedURI + "&limit=3")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var page1 feed.FeedResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&page1))
		require.NotNil(t, page1.Cursor)

		resp2, err := http.Get("http://localhost:18084/xrpc/app.bsky.feed.getFeedSkeleton?feed=" + feedURI + "&limit=3&cursor=" + *page1.Cursor)
		require.NoError(t, err)
		defer resp2.Body.Close()

		assert.Equal(t, http.StatusOK, resp2.StatusCode)

		var page2 feed.FeedResponse
		require.NoError(t, json.NewDecoder(resp2.Body).Decode(&page2))

		wantPage1 := feed.FeedResponse{
			Feed: []feed.FeedItem{
				{Post: feedURI + "/5"},
				{Post: feedURI + "/4"},
				{Post: feedURI + "/3"},
			},
			Cursor: page1.Cursor,
		}
		wantPage2 := feed.FeedResponse{
			Feed: []feed.FeedItem{
				{Post: feedURI + "/2"},
				{Post: feedURI + "/1"},
			},
		}
		assert.Equal(t, wantPage1, page1)
		assert.Equal(t, wantPage2, page2)

		assert.Eventually(t, func() bool {
			stats, err := queries.GetFeedStats(ctx, "bald")
			return err == nil && stats.TotalViews == 2
		}, 5*time.Second, 10*time.Millisecond, "expected TotalViews to be 2")
	})

	t.Run("limit capped at 100", func(t *testing.T) {
		ctx := t.Context()
		ctrl := gomock.NewController(t)
		fh := mocks.NewMockFirehoseChecker(ctrl)
		fh.EXPECT().Connected().Return(true).AnyTimes()
		rl := server.NewRateLimiter(10.0, 20, 3*time.Minute)

		sqldb := newDB(t)
		queries := gen.New(sqldb)

		feedEntries := []feed.FeedEntry{
			{ShortName: "bald", DisplayName: "Bald", Description: "Bald feed", CollectionName: "app.bsky.feed.generator"},
		}
		feedSvc := feed.New(queries, serviceDID, hostname, publisherDID, "https://www.w3.org/ns/did/v1", "#bsky_fg", feedEntries)
		feedURI := "at://" + publisherDID + "/app.bsky.feed.generator/bald"

		ms := feed.NewMetricsService(queries, 10)
		go ms.Run(ctx)
		defer ms.Close()

		srv := server.New(18085, slog.New(slog.NewTextHandler(io.Discard, nil)), feedSvc, sqldb, "test-admin-token", rl, server.WithFirehose(fh), server.WithMetrics(ms))
		go srv.Run(ctx)
		waitForServer(t, 18085)

		for i := 1; i <= 110; i++ {
			require.NoError(t, feedSvc.InsertPost(ctx, "bald", fmt.Sprintf("%s/%d", feedURI, i), fmt.Sprintf("cid-%d", i)))
		}

		resp, err := http.Get("http://localhost:18085/xrpc/app.bsky.feed.getFeedSkeleton?feed=" + feedURI + "&limit=200")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var got feed.FeedResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))

		type post struct {
			uri string
			cid string
		}
		allPosts := make([]post, 110)
		for i := 1; i <= 110; i++ {
			allPosts[i-1] = post{uri: fmt.Sprintf("%s/%d", feedURI, i), cid: fmt.Sprintf("cid-%d", i)}
		}
		sort.Slice(allPosts, func(i, j int) bool {
			return allPosts[i].cid > allPosts[j].cid
		})
		wantFeed := make([]feed.FeedItem, 100)
		for i := 0; i < 100; i++ {
			wantFeed[i] = feed.FeedItem{Post: allPosts[i].uri}
		}
		want := feed.FeedResponse{
			Feed:   wantFeed,
			Cursor: got.Cursor,
		}
		assert.Equal(t, want, got)

		assert.Eventually(t, func() bool {
			stats, err := queries.GetFeedStats(ctx, "bald")
			return err == nil && stats.TotalViews == 1
		}, 5*time.Second, 10*time.Millisecond, "expected TotalViews to be 1")
	})

	t.Run("deleted post no longer appears", func(t *testing.T) {
		ctx := t.Context()
		ctrl := gomock.NewController(t)
		fh := mocks.NewMockFirehoseChecker(ctrl)
		fh.EXPECT().Connected().Return(true).AnyTimes()
		rl := server.NewRateLimiter(10.0, 20, 3*time.Minute)

		sqldb := newDB(t)
		queries := gen.New(sqldb)

		feedEntries := []feed.FeedEntry{
			{ShortName: "bald", DisplayName: "Bald", Description: "Bald feed", CollectionName: "app.bsky.feed.generator"},
		}
		feedSvc := feed.New(queries, serviceDID, hostname, publisherDID, "https://www.w3.org/ns/did/v1", "#bsky_fg", feedEntries)
		feedURI := "at://" + publisherDID + "/app.bsky.feed.generator/bald"

		ms := feed.NewMetricsService(queries, 10)
		go ms.Run(ctx)
		defer ms.Close()

		srv := server.New(18086, slog.New(slog.NewTextHandler(io.Discard, nil)), feedSvc, sqldb, "test-admin-token", rl, server.WithFirehose(fh), server.WithMetrics(ms))
		go srv.Run(ctx)
		waitForServer(t, 18086)

		postURI := feedURI + "/1"
		require.NoError(t, feedSvc.InsertPost(ctx, "bald", postURI, "cid-1"))

		resp, err := http.Get("http://localhost:18086/xrpc/app.bsky.feed.getFeedSkeleton?feed=" + feedURI)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var beforeDelete feed.FeedResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&beforeDelete))
		wantBefore := feed.FeedResponse{
			Feed: []feed.FeedItem{
				{Post: postURI},
			},
		}
		assert.Equal(t, wantBefore, beforeDelete)

		require.NoError(t, feedSvc.DeletePost(ctx, postURI))

		resp2, err := http.Get("http://localhost:18086/xrpc/app.bsky.feed.getFeedSkeleton?feed=" + feedURI)
		require.NoError(t, err)
		defer resp2.Body.Close()

		assert.Equal(t, http.StatusOK, resp2.StatusCode)

		var afterDelete feed.FeedResponse
		require.NoError(t, json.NewDecoder(resp2.Body).Decode(&afterDelete))
		wantAfter := feed.FeedResponse{
			Feed: []feed.FeedItem{},
		}
		assert.Equal(t, wantAfter, afterDelete)

		assert.Eventually(t, func() bool {
			stats, err := queries.GetFeedStats(ctx, "bald")
			return err == nil && stats.TotalViews == 2
		}, 5*time.Second, 10*time.Millisecond, "expected TotalViews to be 2")
	})

	t.Run("multiple pipelines are isolated", func(t *testing.T) {
		ctx := t.Context()
		ctrl := gomock.NewController(t)
		fh := mocks.NewMockFirehoseChecker(ctrl)
		fh.EXPECT().Connected().Return(true).AnyTimes()
		rl := server.NewRateLimiter(10.0, 20, 3*time.Minute)

		sqldb := newDB(t)
		queries := gen.New(sqldb)

		feedEntries := []feed.FeedEntry{
			{ShortName: "bald", DisplayName: "Bald", Description: "Bald feed", CollectionName: "app.bsky.feed.generator"},
			{ShortName: "hair", DisplayName: "Hair", Description: "Hair feed", CollectionName: "app.bsky.feed.generator"},
		}
		feedSvc := feed.New(queries, serviceDID, hostname, publisherDID, "https://www.w3.org/ns/did/v1", "#bsky_fg", feedEntries)
		baldFeedURI := "at://" + publisherDID + "/app.bsky.feed.generator/bald"
		hairFeedURI := "at://" + publisherDID + "/app.bsky.feed.generator/hair"

		ms := feed.NewMetricsService(queries, 10)
		go ms.Run(ctx)
		defer ms.Close()

		srv := server.New(18087, slog.New(slog.NewTextHandler(io.Discard, nil)), feedSvc, sqldb, "test-admin-token", rl, server.WithFirehose(fh), server.WithMetrics(ms))
		go srv.Run(ctx)
		waitForServer(t, 18087)

		baldPostURI := baldFeedURI + "/1"
		hairPostURI := hairFeedURI + "/1"
		require.NoError(t, feedSvc.InsertPost(ctx, "bald", baldPostURI, "cid-1"))
		require.NoError(t, feedSvc.InsertPost(ctx, "hair", hairPostURI, "cid-1"))

		resp1, err := http.Get("http://localhost:18087/xrpc/app.bsky.feed.getFeedSkeleton?feed=" + baldFeedURI)
		require.NoError(t, err)
		defer resp1.Body.Close()

		assert.Equal(t, http.StatusOK, resp1.StatusCode)

		var gotBald feed.FeedResponse
		require.NoError(t, json.NewDecoder(resp1.Body).Decode(&gotBald))
		wantBald := feed.FeedResponse{
			Feed: []feed.FeedItem{
				{Post: baldPostURI},
			},
		}
		assert.Equal(t, wantBald, gotBald)

		resp2, err := http.Get("http://localhost:18087/xrpc/app.bsky.feed.getFeedSkeleton?feed=" + hairFeedURI)
		require.NoError(t, err)
		defer resp2.Body.Close()

		assert.Equal(t, http.StatusOK, resp2.StatusCode)

		var gotHair feed.FeedResponse
		require.NoError(t, json.NewDecoder(resp2.Body).Decode(&gotHair))
		wantHair := feed.FeedResponse{
			Feed: []feed.FeedItem{
				{Post: hairPostURI},
			},
		}
		assert.Equal(t, wantHair, gotHair)

		assert.Eventually(t, func() bool {
			baldStats, err := queries.GetFeedStats(ctx, "bald")
			return err == nil && baldStats.TotalViews == 1
		}, 5*time.Second, 10*time.Millisecond, "expected bald TotalViews to be 1")

		assert.Eventually(t, func() bool {
			hairStats, err := queries.GetFeedStats(ctx, "hair")
			return err == nil && hairStats.TotalViews == 1
		}, 5*time.Second, 10*time.Millisecond, "expected hair TotalViews to be 1")
	})

	t.Run("health endpoints", func(t *testing.T) {
		ctx := t.Context()
		ctrl := gomock.NewController(t)
		fh := mocks.NewMockFirehoseChecker(ctrl)
		fh.EXPECT().Connected().Return(true).AnyTimes()
		rl := server.NewRateLimiter(10.0, 20, 3*time.Minute)

		sqldb := newDB(t)
		queries := gen.New(sqldb)

		feedEntries := []feed.FeedEntry{
			{ShortName: "bald", DisplayName: "Bald", Description: "Bald feed", CollectionName: "app.bsky.feed.generator"},
		}
		feedSvc := feed.New(queries, serviceDID, hostname, publisherDID, "https://www.w3.org/ns/did/v1", "#bsky_fg", feedEntries)

		srv := server.New(18088, slog.New(slog.NewTextHandler(io.Discard, nil)), feedSvc, sqldb, "test-admin-token", rl, server.WithFirehose(fh))
		go srv.Run(ctx)
		waitForServer(t, 18088)

		resp, err := http.Get("http://localhost:18088/xrpc/_health")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var gotHealth map[string]bool
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&gotHealth))
		wantHealth := map[string]bool{"database": true, "firehose": true, "worker": false}
		assert.Equal(t, wantHealth, gotHealth)
	})

	t.Run("did document", func(t *testing.T) {
		ctx := t.Context()
		ctrl := gomock.NewController(t)
		fh := mocks.NewMockFirehoseChecker(ctrl)
		fh.EXPECT().Connected().Return(true).AnyTimes()
		rl := server.NewRateLimiter(10.0, 20, 3*time.Minute)

		sqldb := newDB(t)
		queries := gen.New(sqldb)

		feedEntries := []feed.FeedEntry{
			{ShortName: "bald", DisplayName: "Bald", Description: "Bald feed", CollectionName: "app.bsky.feed.generator"},
		}
		feedSvc := feed.New(queries, serviceDID, hostname, publisherDID, "https://www.w3.org/ns/did/v1", "#bsky_fg", feedEntries)

		srv := server.New(18089, slog.New(slog.NewTextHandler(io.Discard, nil)), feedSvc, sqldb, "test-admin-token", rl, server.WithFirehose(fh))
		go srv.Run(ctx)
		waitForServer(t, 18089)

		resp, err := http.Get("http://localhost:18089/.well-known/did.json")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var got feed.DIDDocument
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
		want := feed.DIDDocument{
			Context: []string{"https://www.w3.org/ns/did/v1"},
			ID:      serviceDID,
			Service: []feed.DIDServiceEntry{
				{
					ID:              "#bsky_fg",
					Type:            "BskyFeedGenerator",
					ServiceEndpoint: "https://" + hostname,
				},
			},
		}
		assert.Equal(t, want, got)
	})

	t.Run("describeFeedGenerator", func(t *testing.T) {
		ctx := t.Context()
		ctrl := gomock.NewController(t)
		fh := mocks.NewMockFirehoseChecker(ctrl)
		fh.EXPECT().Connected().Return(true).AnyTimes()
		rl := server.NewRateLimiter(10.0, 20, 3*time.Minute)

		sqldb := newDB(t)
		queries := gen.New(sqldb)

		feedEntries := []feed.FeedEntry{
			{ShortName: "bald", DisplayName: "Bald", Description: "Bald feed", CollectionName: "app.bsky.feed.generator"},
			{ShortName: "hair", DisplayName: "Hair", Description: "Hair feed", CollectionName: "app.bsky.feed.generator"},
		}
		feedSvc := feed.New(queries, serviceDID, hostname, publisherDID, "https://www.w3.org/ns/did/v1", "#bsky_fg", feedEntries)

		srv := server.New(18090, slog.New(slog.NewTextHandler(io.Discard, nil)), feedSvc, sqldb, "test-admin-token", rl, server.WithFirehose(fh))
		go srv.Run(ctx)
		waitForServer(t, 18090)

		resp, err := http.Get("http://localhost:18090/xrpc/app.bsky.feed.describeFeedGenerator")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var got feed.FeedDescription
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))

		sort.Slice(got.Feeds, func(i, j int) bool {
			return got.Feeds[i].URI < got.Feeds[j].URI
		})
		want := feed.FeedDescription{
			DID: serviceDID,
			Feeds: []feed.FeedDescriptionEntry{
				{
					URI:         "at://" + publisherDID + "/app.bsky.feed.generator/bald",
					DisplayName: "Bald",
					Description: "Bald feed",
				},
				{
					URI:         "at://" + publisherDID + "/app.bsky.feed.generator/hair",
					DisplayName: "Hair",
					Description: "Hair feed",
				},
			},
		}
		assert.Equal(t, want, got)
	})

	t.Run("getFeedSkeleton missing feed returns 400", func(t *testing.T) {
		ctx := t.Context()
		ctrl := gomock.NewController(t)
		fh := mocks.NewMockFirehoseChecker(ctrl)
		fh.EXPECT().Connected().Return(true).AnyTimes()
		rl := server.NewRateLimiter(10.0, 20, 3*time.Minute)

		sqldb := newDB(t)
		queries := gen.New(sqldb)

		feedEntries := []feed.FeedEntry{
			{ShortName: "bald", DisplayName: "Bald", Description: "Bald feed", CollectionName: "app.bsky.feed.generator"},
		}
		feedSvc := feed.New(queries, serviceDID, hostname, publisherDID, "https://www.w3.org/ns/did/v1", "#bsky_fg", feedEntries)

		srv := server.New(18091, slog.New(slog.NewTextHandler(io.Discard, nil)), feedSvc, sqldb, "test-admin-token", rl, server.WithFirehose(fh))
		go srv.Run(ctx)
		waitForServer(t, 18091)

		resp, err := http.Get("http://localhost:18091/xrpc/app.bsky.feed.getFeedSkeleton")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		var got map[string]string
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
		want := map[string]string{"error": "missing feed parameter"}
		assert.Equal(t, want, got)
	})
}
