package subscription

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	bsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/parallel"
	"github.com/bluesky-social/indigo/repo"
	"github.com/gorilla/websocket"

	"github.com/kdwils/baldsky/logger"
	"github.com/kdwils/baldsky/version"
)

func BuildUserAgent(name, url string) string {
	return name + "/" + version.Version + " (+" + url + ")"
}

//go:generate go run go.uber.org/mock/mockgen -destination=mocks/mock_dialer.go -package=mocks github.com/kdwils/baldsky/subscription Dialer

type Dialer interface {
	DialContext(ctx context.Context, urlStr string, requestHeader http.Header) (*websocket.Conn, *http.Response, error)
}

type PipelineStore interface {
	InsertPost(ctx context.Context, feedName, uri, cid string) error
	DeletePosts(ctx context.Context, uris []string) error
	PostExists(ctx context.Context, feedName, uri string) (bool, error)
}

type CursorStore interface {
	GetCursor(ctx context.Context, service string) (int32, error)
	UpsertCursor(ctx context.Context, service string, cursor int32) error
}

type Pipeline struct {
	Name            string
	keywords        []string
	excludeKeywords []string
	blockedDIDs     map[string]struct{}
	requireMedia    bool
	Store           PipelineStore
}

func NewPipeline(name string, keywords, excludeKeywords, blockDIDs []string, requireMedia bool, store PipelineStore) Pipeline {
	blocked := make(map[string]struct{}, len(blockDIDs))
	for _, did := range blockDIDs {
		blocked[did] = struct{}{}
	}
	lowerKW := make([]string, len(keywords))
	for i, kw := range keywords {
		lowerKW[i] = strings.ToLower(kw)
	}
	lowerExclude := make([]string, len(excludeKeywords))
	for i, kw := range excludeKeywords {
		lowerExclude[i] = strings.ToLower(kw)
	}
	return Pipeline{
		Name:            name,
		keywords:        lowerKW,
		excludeKeywords: lowerExclude,
		blockedDIDs:     blocked,
		requireMedia:    requireMedia,
		Store:           store,
	}
}

func (p *Pipeline) matches(text string) bool {
	lower := strings.ToLower(text)
	matched := false
	for _, kw := range p.keywords {
		if strings.Contains(lower, kw) {
			matched = true
			break
		}
	}
	if !matched {
		return false
	}
	for _, kw := range p.excludeKeywords {
		if strings.Contains(lower, kw) {
			return false
		}
	}
	return true
}

type Subscription struct {
	pipelines      []Pipeline
	dialer         Dialer
	service        string
	cursorStore    CursorStore
	reconnectDelay time.Duration
	userAgent      string
}

func New(pipelines []Pipeline, cursorStore CursorStore, dialer Dialer, service string, reconnectDelay time.Duration, userAgent string) *Subscription {
	return &Subscription{
		pipelines:      pipelines,
		dialer:         dialer,
		service:        service,
		cursorStore:    cursorStore,
		reconnectDelay: reconnectDelay,
		userAgent:      userAgent,
	}
}

func (s *Subscription) Listen(ctx context.Context) {
	log := logger.FromContext(ctx)
	for {
		if err := s.subscribe(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("subscription error, reconnecting", "err", err, "delay", s.reconnectDelay)
			select {
			case <-time.After(s.reconnectDelay):
			case <-ctx.Done():
				return
			}
		}
	}
}

func (s *Subscription) subscribe(ctx context.Context) error {
	log := logger.FromContext(ctx)

	cursor, err := s.cursorStore.GetCursor(ctx, s.service)
	if err != nil {
		return fmt.Errorf("getting cursor: %w", err)
	}

	u, err := url.Parse(s.service)
	if err != nil {
		return fmt.Errorf("parsing service URL: %w", err)
	}
	u.Path = "xrpc/com.atproto.sync.subscribeRepos"

	if cursor > 0 {
		q := u.Query()
		q.Set("cursor", fmt.Sprintf("%d", cursor))
		u.RawQuery = q.Encode()
	}

	names := make([]string, len(s.pipelines))
	for i, p := range s.pipelines {
		names[i] = p.Name
	}

	log.Info("connecting to firehose", "url", u.String(), "pipelines", names)

	firehoseConn, _, err := s.dialer.DialContext(ctx, u.String(), http.Header{
		"User-Agent": []string{s.userAgent},
	})
	if err != nil {
		return fmt.Errorf("dialing firehose: %w", err)
	}
	defer firehoseConn.Close()

	rsc := &events.RepoStreamCallbacks{
		RepoCommit: func(evt *comatproto.SyncSubscribeRepos_Commit) error {
			return s.handleCommit(ctx, evt)
		},
	}

	sched := parallel.NewScheduler(
		4,
		1000,
		s.service,
		rsc.EventHandler,
	)

	log.Info("firehose connected, consuming events")
	return events.HandleRepoStream(ctx, firehoseConn, sched, log)
}

func (s *Subscription) handleCommit(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Commit) error {
	log := logger.FromContext(ctx)

	if evt == nil {
		log.Warn("received nil event")
		return nil
	}

	log.Debug("received event", "repo", evt.Repo, "seq", evt.Seq, "time", evt.Time)

	if evt.TooBig {
		log.Debug("skipping oversized event", "repo", evt.Repo, "seq", evt.Seq)
		return nil
	}

	rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(evt.Blocks))
	if err != nil {
		log.Warn("failed to read repo from car", "repo", evt.Repo, "seq", evt.Seq, "err", err)
		return nil
	}

	for _, op := range evt.Ops {
		if !strings.HasPrefix(op.Path, "app.bsky.feed.post/") {
			log.Debug("skipping non-post operation", "path", op.Path, "action", op.Action)
			continue
		}

		uri := buildURI(evt.Repo, op.Path)

		switch op.Action {
		case "delete":
			s.handleDelete(ctx, uri)
		case "create":
			s.handleCreate(ctx, rr, evt.Repo, uri, op)
		default:
			log.Debug("skipping unsupported action", "action", op.Action, "path", op.Path)
		}
	}

	if err := s.cursorStore.UpsertCursor(ctx, s.service, int32(evt.Seq)); err != nil {
		return fmt.Errorf("upsert cursor: %w", err)
	}

	log.Debug("cursor updated", "seq", evt.Seq)

	return nil
}

func (s *Subscription) handleDelete(ctx context.Context, uri string) {
	log := logger.FromContext(ctx)
	log.Debug("deleting post", "uri", uri)
	for i := range s.pipelines {
		p := &s.pipelines[i]
		if err := p.Store.DeletePosts(ctx, []string{uri}); err != nil {
			log.Warn("failed to delete post", "pipeline", p.Name, "uri", uri, "err", err)
		}
	}
}

func (s *Subscription) handleCreate(ctx context.Context, rr *repo.Repo, actor, uri string, op *comatproto.SyncSubscribeRepos_RepoOp) {
	log := logger.FromContext(ctx)

	_, rec, err := rr.GetRecord(ctx, op.Path)
	if err != nil {
		log.Warn("failed to get record", "path", op.Path, "err", err)
		return
	}

	post, ok := rec.(*bsky.FeedPost)
	if !ok {
		log.Debug("record is not a feed post", "path", op.Path)
		return
	}

	for i := range s.pipelines {
		p := &s.pipelines[i]

		if !p.shouldInsert(ctx, actor, post) {
			continue
		}

		log.Info("inserting post", "pipeline", p.Name, "uri", uri)
		if err := p.Store.InsertPost(ctx, p.Name, uri, op.Cid.String()); err != nil {
			log.Warn("failed to insert post", "pipeline", p.Name, "uri", uri, "err", err)
		}
	}
}

func (p *Pipeline) shouldInsert(ctx context.Context, actor string, post *bsky.FeedPost) bool {
	if _, blocked := p.blockedDIDs[actor]; blocked {
		return false
	}

	if p.requireMedia && post.Embed == nil {
		return false
	}

	if !p.matches(post.Text) {
		if post.Reply == nil || post.Reply.Parent == nil {
			return false
		}

		ok, err := p.Store.PostExists(ctx, p.Name, post.Reply.Parent.Uri)
		if err != nil || !ok {
			return false
		}
	}

	return true
}

func buildURI(repo, path string) string {
	url := url.URL{
		Scheme: "at",
		Host:   repo,
		Path:   path,
	}

	return url.String()
}
