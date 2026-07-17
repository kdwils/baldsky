package feed

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kdwils/baldsky/db/gen"
)

//go:generate go run go.uber.org/mock/mockgen -destination=mocks/mock_feed_store.go -package=mocks github.com/kdwils/baldsky/feed Store
//go:generate go run go.uber.org/mock/mockgen -destination=mocks/mock_querier.go -package=mocks github.com/kdwils/baldsky/db/gen Querier

var (
	ErrUnknownFeed   = errors.New("UnknownFeed")
	ErrInvalidCursor = errors.New("invalid cursor format")
)

type DIDDocument struct {
	Context []string          `json:"@context"`
	ID      string            `json:"id"`
	Service []DIDServiceEntry `json:"service"`
}

type DIDServiceEntry struct {
	ID              string `json:"id"`
	Type            string `json:"type"`
	ServiceEndpoint string `json:"serviceEndpoint"`
}

type FeedDescription struct {
	DID   string                `json:"did"`
	Feeds []FeedDescriptionEntry `json:"feeds"`
}

type FeedDescriptionEntry struct {
	URI         string `json:"uri"`
	DisplayName string `json:"displayName"`
	Description string `json:"description"`
}

type FeedItem struct {
	Post string `json:"post"`
}

type FeedResponse struct {
	Feed   []FeedItem `json:"feed"`
	Cursor *string    `json:"cursor,omitempty"`
}

type Store interface {
	InsertPost(ctx context.Context, feedName, uri, cid string) error
	DeletePosts(ctx context.Context, uris []string) error
	GetFeedPage(ctx context.Context, feedName string, limit int, cursor *string) (FeedPage, error)
	GetCursor(ctx context.Context, service string) (int64, error)
	UpsertCursor(ctx context.Context, service string, cursor int64) error
}

type FeedPage struct {
	Posts      []FeedPost
	NextCursor *string
}

type FeedPost struct {
	URI       string
	CID       string
	IndexedAt string
}

type FeedEntry struct {
	ShortName      string
	DisplayName    string
	Description    string
	CollectionName string
}

type Service struct {
	q            gen.Querier
	serviceDID   string
	hostname     string
	publisherDID string
	didContext   string
	serviceID    string
	feeds        map[string]FeedEntry
}

func New(q gen.Querier, serviceDID, hostname, publisherDID, didContext, serviceID string, entries []FeedEntry) *Service {
	feeds := make(map[string]FeedEntry, len(entries))
	for _, e := range entries {
		feeds[e.ShortName] = e
	}
	return &Service{
		q:            q,
		serviceDID:   serviceDID,
		hostname:     hostname,
		publisherDID: publisherDID,
		didContext:   didContext,
		serviceID:    serviceID,
		feeds:        feeds,
	}
}

func (s *Service) Hostname() string {
	return s.hostname
}

func (s *Service) GetDIDDocument() DIDDocument {
	return DIDDocument{
		Context: []string{s.didContext},
		ID:      s.serviceDID,
		Service: []DIDServiceEntry{
			{
				ID:              s.serviceID,
				Type:            "BskyFeedGenerator",
				ServiceEndpoint: s.hostname,
			},
		},
	}
}

func (s *Service) GetFeedDescription() FeedDescription {
	entries := make([]FeedDescriptionEntry, 0, len(s.feeds))
	for _, f := range s.feeds {
		entries = append(entries, FeedDescriptionEntry{
			URI:         feedGeneratorURI(s.publisherDID, f.ShortName),
			DisplayName: f.DisplayName,
			Description: f.Description,
		})
	}
	return FeedDescription{DID: s.serviceDID, Feeds: entries}
}

func feedGeneratorURI(publisherDID, shortName string) string {
	return "at://" + publisherDID + "/app.bsky.feed.generator/" + shortName
}

func (s *Service) InsertPost(ctx context.Context, feedName, uri, cid string) error {
	return s.q.InsertPost(ctx, gen.InsertPostParams{
		FeedName:  feedName,
		Uri:       uri,
		Cid:       cid,
		IndexedAt: time.Now().UTC().Format(time.RFC3339),
	})
}

func (s *Service) DeletePost(ctx context.Context, uri string) error {
	return s.q.DeletePost(ctx, uri)
}

func (s *Service) DeletePosts(ctx context.Context, uris []string) error {
	for _, uri := range uris {
		if err := s.q.DeletePost(ctx, uri); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) GetFeedPage(ctx context.Context, feedURI, limitStr string, cursorStr string) (FeedResponse, error) {
	if feedURI == "" {
		return FeedResponse{}, ErrUnknownFeed
	}

	parts := strings.Split(strings.TrimPrefix(feedURI, "at://"), "/")
	if len(parts) != 3 {
		return FeedResponse{}, ErrUnknownFeed
	}

	did, collection, rkey := parts[0], parts[1], parts[2]

	if did != s.publisherDID {
		return FeedResponse{}, ErrUnknownFeed
	}

	entry, ok := s.feeds[rkey]
	if !ok {
		return FeedResponse{}, ErrUnknownFeed
	}
	if collection != entry.CollectionName {
		return FeedResponse{}, ErrUnknownFeed
	}

	limit := 50
	if limitStr != "" {
		n, err := strconv.Atoi(limitStr)
		if err == nil && n > 0 {
			limit = min(n, 100)
		}
	}

	var cursor *string
	if cursorStr != "" {
		cursor = &cursorStr
	}

	params := gen.GetFeedPageParams{
		FeedName:        rkey,
		CursorIndexedAt: sql.NullString{},
		CursorCid:       sql.NullString{},
		Limit:           int32(limit),
	}

	if cursor != nil {
		indexedAt, cid, err := parseCursor(*cursor)
		if err != nil {
			return FeedResponse{}, ErrInvalidCursor
		}
		params.CursorIndexedAt = sql.NullString{String: indexedAt, Valid: true}
		params.CursorCid = sql.NullString{String: cid, Valid: true}
	}

	rows, err := s.q.GetFeedPage(ctx, params)
	if err != nil {
		return FeedResponse{}, fmt.Errorf("get feed page: %w", err)
	}

	feedItems := make([]FeedItem, len(rows))
	var nextCursor *string
	for i, row := range rows {
		feedItems[i] = FeedItem{Post: row.Uri}
		if i == len(rows)-1 {
			c := buildCursor(row.IndexedAt, row.Cid)
			nextCursor = &c
		}
	}

	if len(rows) < limit {
		nextCursor = nil
	}

	return FeedResponse{
		Feed:   feedItems,
		Cursor: nextCursor,
	}, nil
}

func (s *Service) GetCursor(ctx context.Context, service string) (int64, error) {
	cursor, err := s.q.GetCursor(ctx, service)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return cursor, err
}

func (s *Service) UpsertCursor(ctx context.Context, service string, cursor int64) error {
	return s.q.UpsertCursor(ctx, gen.UpsertCursorParams{
		Service: service,
		Cursor:  cursor,
	})
}

func parseCursor(cursor string) (string, string, error) {
	parts := strings.SplitN(cursor, "::", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid cursor format: %s", cursor)
	}
	return parts[0], parts[1], nil
}

func buildCursor(indexedAt, cid string) string {
	return fmt.Sprintf("%s::%s", indexedAt, cid)
}
