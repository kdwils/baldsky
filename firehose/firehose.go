package firehose

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/parallel"
	"github.com/gorilla/websocket"

	"github.com/kdwils/baldsky/atproto"
	"github.com/kdwils/baldsky/logger"
)

type Dialer interface {
	DialContext(ctx context.Context, urlStr string, requestHeader http.Header) (*websocket.Conn, *http.Response, error)
}

type CursorStore interface {
	GetCursor(ctx context.Context, service string) (int64, error)
	UpsertCursor(ctx context.Context, service string, cursor int64) error
}

type Processor interface {
	ProcessEvent(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Commit) error
}

// FirehoseConn manages a single firehose websocket connection and event stream.
type FirehoseConn struct {
	dialer      Dialer
	service     string
	userAgent   string
	concurrency int
	queueSize   int
	connected   atomic.Bool
}

func NewFirehoseConn(dialer Dialer, service, userAgent string, concurrency, queueSize int) *FirehoseConn {
	return &FirehoseConn{
		dialer:      dialer,
		service:     service,
		userAgent:   userAgent,
		concurrency: concurrency,
		queueSize:   queueSize,
	}
}

func (c *FirehoseConn) Connected() bool {
	return c.connected.Load()
}

func (c *FirehoseConn) Service() string {
	return c.service
}

// Run connects to the firehose at the given cursor position, streams events
// through handleCommit, and blocks until ctx is cancelled.
func (c *FirehoseConn) Run(ctx context.Context, cursor int64, handleCommit func(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Commit) error) error {
	log := logger.FromContext(ctx)

	firehoseURL, err := atproto.FirehoseURL(c.service, cursor)
	if err != nil {
		return fmt.Errorf("building firehose URL: %w", err)
	}

	log.Info("connecting to firehose", "url", firehoseURL)

	firehoseConn, _, err := c.dialer.DialContext(ctx, firehoseURL, http.Header{
		"User-Agent": []string{c.userAgent},
	})
	if err != nil {
		return fmt.Errorf("dialing firehose: %w", err)
	}
	defer firehoseConn.Close()

	rsc := &events.RepoStreamCallbacks{
		RepoCommit: func(evt *comatproto.SyncSubscribeRepos_Commit) error {
			return handleCommit(ctx, evt)
		},
	}

	sched := parallel.NewScheduler(
		c.concurrency,
		c.queueSize,
		c.service,
		rsc.EventHandler,
	)

	c.connected.Store(true)
	defer c.connected.Store(false)

	log.Info("firehose connected, consuming events")
	return events.HandleRepoStream(ctx, firehoseConn, sched, log)
}
