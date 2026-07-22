package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/kdwils/baldsky/feed"
	"github.com/kdwils/baldsky/logger"
)

type FeedService interface {
	GetDIDDocument() feed.DIDDocument
	GetFeedDescription() feed.FeedDescription
	GetFeedPage(ctx context.Context, feedURI, limit, cursor string) (feed.FeedResponse, error)
	DeletePost(ctx context.Context, uri string) error
	Hostname() string
}

type FirehoseChecker interface {
	Connected() bool
}

type WorkerChecker interface {
	Connected() bool
}

type PingDB interface {
	PingContext(ctx context.Context) error
}

type Server struct {
	port       int
	logger     *slog.Logger
	svc        FeedService
	db         PingDB
	firehose   FirehoseChecker
	workers    []WorkerChecker
	adminToken string
	rl         *RateLimiter
	metrics    *feed.MetricsService
}

type Option func(*Server)

func WithFirehose(fh FirehoseChecker) Option {
	return func(s *Server) { s.firehose = fh }
}

func WithWorker(w WorkerChecker) Option {
	return func(s *Server) { s.workers = append(s.workers, w) }
}

func WithMetrics(m *feed.MetricsService) Option {
	return func(s *Server) { s.metrics = m }
}

func New(port int, logger *slog.Logger, svc FeedService, db PingDB, adminToken string, rl *RateLimiter, opts ...Option) *Server {
	s := &Server{
		port:       port,
		logger:     logger,
		svc:        svc,
		db:         db,
		adminToken: adminToken,
		rl:         rl,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Server) Run(ctx context.Context) error {
	s.rl.StartCleanup(ctx)

	r := mux.NewRouter()
	r.Use(withLogger(s.logger))
	r.Use(s.withRateLimit())

	r.HandleFunc("/.well-known/did.json", s.handleDIDDocument()).Methods(http.MethodGet)
	r.HandleFunc("/xrpc/_health", s.healthz()).Methods(http.MethodGet)
	r.HandleFunc("/xrpc/app.bsky.feed.describeFeedGenerator", s.handleDescribeFeedGenerator()).Methods(http.MethodGet)
	r.HandleFunc("/xrpc/app.bsky.feed.getFeedSkeleton", s.handleGetFeedSkeleton()).Methods(http.MethodGet)

	admin := r.PathPrefix("/admin").Subrouter()
	admin.Use(s.withBearerAuth())
	admin.HandleFunc("/posts/{uri}", s.handleDeletePost()).Methods(http.MethodDelete)
	admin.HandleFunc("/metrics/{feed}", s.handleGetFeedMetrics()).Methods(http.MethodGet)

	corsHandler := handlers.CORS(
		handlers.AllowedOrigins([]string{"*"}),
		handlers.AllowedMethods([]string{http.MethodGet, http.MethodOptions}),
		handlers.AllowedHeaders([]string{"*"}),
		handlers.MaxAge(3600),
	)(r)

	srv := &http.Server{
		Addr:    ":" + strconv.Itoa(s.port),
		Handler: corsHandler,
	}

	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("server starting", "port", s.port)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		s.logger.Info("shutting down server")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}

func withLogger(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqLog := log.With("path", r.URL.Path)
			ctx := logger.WithContext(r.Context(), reqLog)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func (s *Server) withBearerAuth() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			auth := r.Header.Get("Authorization")
			token, ok := strings.CutPrefix(auth, "Bearer ")
			if !ok || token != s.adminToken {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("failed to encode json response", "err", err)
	}
}

func (s *Server) handleDIDDocument() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		doc := s.svc.GetDIDDocument()
		if !strings.HasSuffix(doc.ID, s.svc.Hostname()) {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, http.StatusOK, doc)
	}
}

func (s *Server) healthz() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		dbOK := s.db.PingContext(r.Context()) == nil
		fhOK := s.firehose != nil && s.firehose.Connected()
		wOK := len(s.workers) > 0
		for _, wr := range s.workers {
			if !wr.Connected() {
				wOK = false
				break
			}
		}

		status := http.StatusOK
		if !dbOK || (!fhOK && !wOK) {
			status = http.StatusServiceUnavailable
		}

		writeJSON(w, status, map[string]bool{
			"database": dbOK,
			"firehose": fhOK,
			"worker":   wOK,
		})
	}
}

func (s *Server) handleDescribeFeedGenerator() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, s.svc.GetFeedDescription())
	}
}

func (s *Server) handleGetFeedSkeleton() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log := logger.FromContext(r.Context())

		feedParam := r.URL.Query().Get("feed")
		if feedParam == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing feed parameter"})
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		resp, err := s.svc.GetFeedPage(
			ctx,
			feedParam,
			r.URL.Query().Get("limit"),
			r.URL.Query().Get("cursor"),
		)

		if err != nil {
			log.Error("get feed page", "error", err)
			switch {
			case errors.Is(err, feed.ErrUnknownFeed):
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			case errors.Is(err, feed.ErrInvalidCursor):
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			default:
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
			}
			return
		}

		writeJSON(w, http.StatusOK, resp)

		if s.metrics != nil {
			if feedName := extractFeedName(feedParam); feedName != "" {
				s.metrics.RecordView(r.Context(), feedName)
			}
		}
	}
}

func (s *Server) handleDeletePost() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		uri := mux.Vars(r)["uri"]
		if uri == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing uri"})
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		if err := s.svc.DeletePost(ctx, uri); err != nil {
			log := logger.FromContext(r.Context())
			log.Error("delete post", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

func (s *Server) withRateLimit() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip, _, err := net.SplitHostPort(r.RemoteAddr)
			if err != nil {
				ip = r.RemoteAddr
			}
			if !s.rl.Allow(ip) {
				writeJSON(w, http.StatusTooManyRequests, nil)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func extractFeedName(feedURI string) string {
	parts := strings.Split(strings.TrimPrefix(feedURI, "at://"), "/")
	if len(parts) == 3 {
		return parts[2]
	}
	return ""
}

func (s *Server) handleGetFeedMetrics() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		feedName := mux.Vars(r)["feed"]
		if feedName == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing feed"})
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		stats, err := s.metrics.GetFeedStats(ctx, feedName)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
			return
		}

		writeJSON(w, http.StatusOK, stats)
	}
}
