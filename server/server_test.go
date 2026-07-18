package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/kdwils/baldsky/feed"
	"github.com/kdwils/baldsky/logger"
	"github.com/kdwils/baldsky/server/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

const (
	testHostname = "https://test.example.com"
	testDID      = "did:web:test.example.com"
)

type mockPingDB struct {
	err error
}

func (m *mockPingDB) PingContext(ctx context.Context) error {
	return m.err
}

type mockFirehose struct {
	connected bool
}

func (m *mockFirehose) Connected() bool {
	return m.connected
}

func newTestServer(ctrl *gomock.Controller) (*Server, *mocks.MockFeedService) {
	svc := mocks.NewMockFeedService(ctrl)
	srv := New(8080, slog.Default(), svc, &mockPingDB{}, &mockFirehose{connected: true}, "test-token", NewRateLimiter(100, 200))
	return srv, svc
}

func TestNew(t *testing.T) {
	t.Run("creates server with all fields", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc := mocks.NewMockFeedService(ctrl)
		log := slog.Default()
		db := &mockPingDB{}
		fh := &mockFirehose{connected: true}

		got := New(9090, log, svc, db, fh, "secret", NewRateLimiter(10.0, 20))
		want := &Server{
			port:       9090,
			logger:     log,
			svc:        svc,
			db:         db,
			firehose:   fh,
			adminToken: "secret",
			rl:         got.rl,
		}
		assert.Equal(t, want, got)
	})
}

func TestHandleHealth(t *testing.T) {
	t.Run("returns ok", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		srv, _ := newTestServer(ctrl)
		req := httptest.NewRequest(http.MethodGet, "/xrpc/_health", nil)
		w := httptest.NewRecorder()

		srv.healthz()(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

		var body map[string]bool
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.True(t, body["database"])
		assert.True(t, body["firehose"])
	})

	t.Run("database disconnected returns 503", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc := mocks.NewMockFeedService(ctrl)
		srv := New(8080, slog.Default(), svc, &mockPingDB{err: fmt.Errorf("connection refused")}, &mockFirehose{connected: true}, "", NewRateLimiter(100, 200))

		req := httptest.NewRequest(http.MethodGet, "/xrpc/_health", nil)
		w := httptest.NewRecorder()

		srv.healthz()(w, req)

		assert.Equal(t, http.StatusServiceUnavailable, w.Code)

		var body map[string]bool
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.False(t, body["database"])
		assert.True(t, body["firehose"])
	})

	t.Run("firehose disconnected returns 503", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc := mocks.NewMockFeedService(ctrl)
		srv := New(8080, slog.Default(), svc, &mockPingDB{}, &mockFirehose{connected: false}, "", NewRateLimiter(100, 200))

		req := httptest.NewRequest(http.MethodGet, "/xrpc/_health", nil)
		w := httptest.NewRecorder()

		srv.healthz()(w, req)

		assert.Equal(t, http.StatusServiceUnavailable, w.Code)

		var body map[string]bool
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.True(t, body["database"])
		assert.False(t, body["firehose"])
	})

	t.Run("both disconnected returns 503", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc := mocks.NewMockFeedService(ctrl)
		srv := New(8080, slog.Default(), svc, &mockPingDB{err: fmt.Errorf("connection refused")}, &mockFirehose{connected: false}, "", NewRateLimiter(100, 200))

		req := httptest.NewRequest(http.MethodGet, "/xrpc/_health", nil)
		w := httptest.NewRecorder()

		srv.healthz()(w, req)

		assert.Equal(t, http.StatusServiceUnavailable, w.Code)

		var body map[string]bool
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.False(t, body["database"])
		assert.False(t, body["firehose"])
	})
}

func TestHandleDIDDocument(t *testing.T) {
	t.Run("valid hostname returns document", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		srv, svc := newTestServer(ctrl)
		svc.EXPECT().GetDIDDocument().Return(feed.DIDDocument{
			Context: []string{"https://www.w3.org/ns/did/v1"},
			ID:      "did:web:test.example.com",
			Service: []feed.DIDServiceEntry{
				{
					ID:              "#bsky_fg",
					Type:            "BskyFeedGenerator",
					ServiceEndpoint: "test.example.com",
				},
			},
		})
		svc.EXPECT().Hostname().Return("test.example.com")

		req := httptest.NewRequest(http.MethodGet, "/.well-known/did.json", nil)
		w := httptest.NewRecorder()

		srv.handleDIDDocument()(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var got feed.DIDDocument
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
		want := feed.DIDDocument{
			Context: []string{"https://www.w3.org/ns/did/v1"},
			ID:      "did:web:test.example.com",
			Service: []feed.DIDServiceEntry{
				{
					ID:              "#bsky_fg",
					Type:            "BskyFeedGenerator",
					ServiceEndpoint: "test.example.com",
				},
			},
		}
		assert.Equal(t, want, got)
	})

	t.Run("mismatched hostname returns 404", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		srv, svc := newTestServer(ctrl)
		svc.EXPECT().GetDIDDocument().Return(feed.DIDDocument{
			ID: "did:web:wrong.com",
		})
		svc.EXPECT().Hostname().Return("test.example.com")

		req := httptest.NewRequest(http.MethodGet, "/.well-known/did.json", nil)
		w := httptest.NewRecorder()

		srv.handleDIDDocument()(w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)
	})
}

func TestHandleDescribeFeedGenerator(t *testing.T) {
	t.Run("returns feed description", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		srv, svc := newTestServer(ctrl)
		svc.EXPECT().GetFeedDescription().Return(feed.FeedDescription{
			DID: testDID,
			Feeds: []feed.FeedDescriptionEntry{
				{
					URI:         "at://did:web:test.example.com/app.bsky.feed.generator/test-feed",
					DisplayName: "Test Feed",
					Description: "A test feed",
				},
			},
		})

		req := httptest.NewRequest(http.MethodGet, "/xrpc/app.bsky.feed.describeFeedGenerator", nil)
		w := httptest.NewRecorder()

		srv.handleDescribeFeedGenerator()(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var got feed.FeedDescription
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
		want := feed.FeedDescription{
			DID: testDID,
			Feeds: []feed.FeedDescriptionEntry{
				{
					URI:         "at://did:web:test.example.com/app.bsky.feed.generator/test-feed",
					DisplayName: "Test Feed",
					Description: "A test feed",
				},
			},
		}
		assert.Equal(t, want, got)
	})

	t.Run("empty feeds", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		srv, svc := newTestServer(ctrl)
		svc.EXPECT().GetFeedDescription().Return(feed.FeedDescription{
			DID:   testDID,
			Feeds: []feed.FeedDescriptionEntry{},
		})

		req := httptest.NewRequest(http.MethodGet, "/xrpc/app.bsky.feed.describeFeedGenerator", nil)
		w := httptest.NewRecorder()

		srv.handleDescribeFeedGenerator()(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var got feed.FeedDescription
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
		assert.Equal(t, testDID, got.DID)
		assert.Empty(t, got.Feeds)
	})
}

func TestHandleGetFeedSkeleton(t *testing.T) {
	t.Run("missing feed parameter returns 400", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		srv, _ := newTestServer(ctrl)
		req := httptest.NewRequest(http.MethodGet, "/xrpc/app.bsky.feed.getFeedSkeleton", nil)
		w := httptest.NewRecorder()

		srv.handleGetFeedSkeleton()(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)

		var body map[string]string
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.Equal(t, "missing feed parameter", body["error"])
	})

	t.Run("unknown feed returns 400", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		srv, svc := newTestServer(ctrl)
		svc.EXPECT().GetFeedPage(gomock.Any(), "at://did:web:wrong.com/app.bsky.feed.generator/nonexistent", "", "").
			Return(feed.FeedResponse{}, feed.ErrUnknownFeed)

		req := httptest.NewRequest(http.MethodGet, "/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:wrong.com/app.bsky.feed.generator/nonexistent", nil)
		w := httptest.NewRecorder()

		srv.handleGetFeedSkeleton()(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)

		var body map[string]string
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.Equal(t, "UnknownFeed", body["error"])
	})

	t.Run("invalid cursor returns 400", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		srv, svc := newTestServer(ctrl)
		svc.EXPECT().GetFeedPage(gomock.Any(), "at://did:web:test.example.com/app.bsky.feed.generator/test-feed", "", "bad-cursor").
			Return(feed.FeedResponse{}, feed.ErrInvalidCursor)

		req := httptest.NewRequest(http.MethodGet, "/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:test.example.com/app.bsky.feed.generator/test-feed&cursor=bad-cursor", nil)
		w := httptest.NewRecorder()

		srv.handleGetFeedSkeleton()(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)

		var body map[string]string
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.Equal(t, "invalid cursor format", body["error"])
	})

	t.Run("internal error returns 500", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		srv, svc := newTestServer(ctrl)
		svc.EXPECT().GetFeedPage(gomock.Any(), "at://did:web:test.example.com/app.bsky.feed.generator/test-feed", "", "").
			Return(feed.FeedResponse{}, errors.New("db connection lost"))

		req := httptest.NewRequest(http.MethodGet, "/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:test.example.com/app.bsky.feed.generator/test-feed", nil)
		w := httptest.NewRecorder()

		srv.handleGetFeedSkeleton()(w, req)

		assert.Equal(t, http.StatusInternalServerError, w.Code)

		var body map[string]string
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.Equal(t, "internal server error", body["error"])
	})

	t.Run("successful response with posts", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		srv, svc := newTestServer(ctrl)
		cursor := "2024-01-01T00:00:00Z::bafycid"
		resp := feed.FeedResponse{
			Feed: []feed.FeedItem{
				{Post: "at://did:plc:actor1/app.bsky.feed.post/abc123"},
				{Post: "at://did:plc:actor2/app.bsky.feed.post/def456"},
			},
			Cursor: &cursor,
		}
		svc.EXPECT().GetFeedPage(gomock.Any(), "at://did:web:test.example.com/app.bsky.feed.generator/test-feed", "", "").
			Return(resp, nil)

		req := httptest.NewRequest(http.MethodGet, "/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:test.example.com/app.bsky.feed.generator/test-feed", nil)
		w := httptest.NewRecorder()

		srv.handleGetFeedSkeleton()(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

		var got feed.FeedResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
		assert.Equal(t, resp, got)
	})

	t.Run("passes limit and cursor to service", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		srv, svc := newTestServer(ctrl)
		svc.EXPECT().GetFeedPage(gomock.Any(), "at://did:web:test.example.com/app.bsky.feed.generator/test-feed", "25", "2024-01-01T00:00:00Z::bafycid").
			Return(feed.FeedResponse{Feed: []feed.FeedItem{}}, nil)

		req := httptest.NewRequest(http.MethodGet, "/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:test.example.com/app.bsky.feed.generator/test-feed&limit=25&cursor=2024-01-01T00:00:00Z::bafycid", nil)
		w := httptest.NewRecorder()

		srv.handleGetFeedSkeleton()(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("empty feed response", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		srv, svc := newTestServer(ctrl)
		svc.EXPECT().GetFeedPage(gomock.Any(), "at://did:web:test.example.com/app.bsky.feed.generator/test-feed", "", "").
			Return(feed.FeedResponse{Feed: []feed.FeedItem{}}, nil)

		req := httptest.NewRequest(http.MethodGet, "/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:test.example.com/app.bsky.feed.generator/test-feed", nil)
		w := httptest.NewRecorder()

		srv.handleGetFeedSkeleton()(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var got feed.FeedResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
		assert.Equal(t, feed.FeedResponse{Feed: []feed.FeedItem{}}, got)
	})
}

func TestWithLogger(t *testing.T) {
	t.Run("adds logger to context", func(t *testing.T) {
		log := slog.Default()
		inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got := logger.FromContext(r.Context())
			assert.NotNil(t, got)
			w.WriteHeader(http.StatusOK)
		})

		handler := withLogger(log)(inner)
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})
}

func TestWriteJSON(t *testing.T) {
	t.Run("writes json response", func(t *testing.T) {
		w := httptest.NewRecorder()
		writeJSON(w, http.StatusOK, map[string]string{"key": "value"})

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

		var body map[string]string
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.Equal(t, "value", body["key"])
	})

	t.Run("writes error status", func(t *testing.T) {
		w := httptest.NewRecorder()
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})

		assert.Equal(t, http.StatusNotFound, w.Code)

		var body map[string]string
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.Equal(t, "not found", body["error"])
	})
}

func TestWithRateLimit(t *testing.T) {
	t.Run("allows request within rate limit", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		srv, _ := newTestServer(ctrl)
		inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		handler := srv.withRateLimit()(inner)
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.RemoteAddr = "1.2.3.4:1234"
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("rejects request exceeding rate limit", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc := mocks.NewMockFeedService(ctrl)
		srv := New(8080, slog.Default(), svc, &mockPingDB{}, &mockFirehose{connected: true}, "", NewRateLimiter(0.001, 1))
		inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		handler := srv.withRateLimit()(inner)

		req1 := httptest.NewRequest(http.MethodGet, "/test", nil)
		req1.RemoteAddr = "1.2.3.4:1234"
		w1 := httptest.NewRecorder()
		handler.ServeHTTP(w1, req1)

		req2 := httptest.NewRequest(http.MethodGet, "/test", nil)
		req2.RemoteAddr = "1.2.3.4:1234"
		w2 := httptest.NewRecorder()
		handler.ServeHTTP(w2, req2)

		assert.Equal(t, http.StatusTooManyRequests, w2.Code)
	})

	t.Run("different ips have independent limits", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc := mocks.NewMockFeedService(ctrl)
		srv := New(8080, slog.Default(), svc, &mockPingDB{}, &mockFirehose{connected: true}, "", NewRateLimiter(0.001, 1))
		inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		handler := srv.withRateLimit()(inner)

		req1 := httptest.NewRequest(http.MethodGet, "/test", nil)
		req1.RemoteAddr = "1.1.1.1:1234"
		w1 := httptest.NewRecorder()
		handler.ServeHTTP(w1, req1)

		req2 := httptest.NewRequest(http.MethodGet, "/test", nil)
		req2.RemoteAddr = "1.1.1.1:1234"
		w2 := httptest.NewRecorder()
		handler.ServeHTTP(w2, req2)

		assert.Equal(t, http.StatusTooManyRequests, w2.Code)

		req3 := httptest.NewRequest(http.MethodGet, "/test", nil)
		req3.RemoteAddr = "2.2.2.2:1234"
		w3 := httptest.NewRecorder()
		handler.ServeHTTP(w3, req3)

		assert.Equal(t, http.StatusOK, w3.Code)
	})
}

func TestHandleDeletePost(t *testing.T) {
	t.Run("missing auth returns 401", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		srv, _ := newTestServer(ctrl)
		handler := srv.withBearerAuth()(srv.handleDeletePost())
		req := httptest.NewRequest(http.MethodDelete, "/admin/posts/some-uri", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		var got map[string]string
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
		assert.Equal(t, http.StatusUnauthorized, w.Code)
		assert.Equal(t, map[string]string{"error": "unauthorized"}, got)
	})

	t.Run("wrong token returns 401", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		srv, _ := newTestServer(ctrl)
		handler := srv.withBearerAuth()(srv.handleDeletePost())
		req := httptest.NewRequest(http.MethodDelete, "/admin/posts/some-uri", nil)
		req.Header.Set("Authorization", "Bearer wrong-token")
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		var got map[string]string
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
		assert.Equal(t, http.StatusUnauthorized, w.Code)
		assert.Equal(t, map[string]string{"error": "unauthorized"}, got)
	})

	t.Run("valid token deletes post and returns 204", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		srv, svc := newTestServer(ctrl)
		uri := "at://did:plc:actor1/app.bsky.feed.post/abc123"
		svc.EXPECT().DeletePost(gomock.Any(), uri).Return(nil)

		handler := srv.withBearerAuth()(srv.handleDeletePost())
		req := httptest.NewRequest(http.MethodDelete, "/admin/posts/"+uri, nil)
		req.Header.Set("Authorization", "Bearer test-token")
		req = mux.SetURLVars(req, map[string]string{"uri": uri})
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		assert.Equal(t, http.StatusNoContent, w.Code)
	})

	t.Run("valid token service error returns 500", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		srv, svc := newTestServer(ctrl)
		uri := "at://did:plc:actor1/app.bsky.feed.post/abc123"
		svc.EXPECT().DeletePost(gomock.Any(), uri).Return(errors.New("db error"))

		handler := srv.withBearerAuth()(srv.handleDeletePost())
		req := httptest.NewRequest(http.MethodDelete, "/admin/posts/"+uri, nil)
		req.Header.Set("Authorization", "Bearer test-token")
		req = mux.SetURLVars(req, map[string]string{"uri": uri})
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		var got map[string]string
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
		assert.Equal(t, http.StatusInternalServerError, w.Code)
		assert.Equal(t, map[string]string{"error": "internal server error"}, got)
	})
}
