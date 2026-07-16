package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/kdwils/baldsky/feed"
	"github.com/kdwils/baldsky/logger"
)

//go:generate go run go.uber.org/mock/mockgen -destination=mocks/mock_feed_service.go -package=mocks github.com/kdwils/baldsky/server FeedService

type FeedService interface {
	GetDIDDocument() feed.DIDDocument
	GetFeedDescription() feed.FeedDescription
	GetFeedPage(ctx context.Context, feedURI, limit, cursor string) (feed.FeedResponse, error)
	Hostname() string
}

type FirehoseChecker interface {
	Connected() bool
}

type PingDB interface {
	PingContext(ctx context.Context) error
}

type Server struct {
	port     int
	logger   *slog.Logger
	svc      FeedService
	db       PingDB
	firehose FirehoseChecker
}

func New(port int, logger *slog.Logger, svc FeedService, db PingDB, firehose FirehoseChecker) *Server {
	return &Server{
		port:     port,
		logger:   logger,
		svc:      svc,
		db:       db,
		firehose: firehose,
	}
}

func (s *Server) Run(ctx context.Context) error {
	r := mux.NewRouter()
	r.Use(withLogger(s.logger))

	r.HandleFunc("/.well-known/did.json", s.handleDIDDocument()).Methods(http.MethodGet)
	r.HandleFunc("/xrpc/_health", s.handleHealth()).Methods(http.MethodGet)
	r.HandleFunc("/healthz", s.healthz()).Methods(http.MethodGet)
	r.HandleFunc("/xrpc/app.bsky.feed.describeFeedGenerator", s.handleDescribeFeedGenerator()).Methods(http.MethodGet)
	r.HandleFunc("/xrpc/app.bsky.feed.getFeedSkeleton", s.handleGetFeedSkeleton()).Methods(http.MethodGet)

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

func (s *Server) handleHealth() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

func (s *Server) healthz() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		dbOK := s.db.PingContext(r.Context()) == nil
		fhOK := s.firehose != nil && s.firehose.Connected()

		status := http.StatusOK
		if !dbOK || !fhOK {
			status = http.StatusServiceUnavailable
		}

		writeJSON(w, status, map[string]bool{
			"database": dbOK,
			"firehose": fhOK,
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
	}
}
