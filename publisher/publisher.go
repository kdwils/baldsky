package publisher

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/parallel"
	"github.com/nats-io/nats.go"

	"github.com/kdwils/baldsky/atproto"
	"github.com/kdwils/baldsky/config"
	"github.com/kdwils/baldsky/logger"
	"github.com/kdwils/baldsky/subscription"
)

type Publisher struct {
	dialer         subscription.Dialer
	cursorStore    subscription.CursorStore
	nc             *nats.Conn
	subject        string
	service        string
	concurrency    int
	queueSize      int
	reconnectDelay time.Duration
	userAgent      string
	connected      atomic.Bool
}

func New(
	cursorStore subscription.CursorStore,
	dialer subscription.Dialer,
	service string,
	natsCfg config.NATSConfig,
	concurrency, queueSize int,
	reconnectDelay time.Duration,
	userAgent string,
) (*Publisher, error) {
	nc, err := nats.Connect(natsCfg.URL, nats.Name("baldsky-publisher"))
	if err != nil {
		return nil, fmt.Errorf("connect to NATS: %w", err)
	}
	return &Publisher{
		dialer:         dialer,
		cursorStore:    cursorStore,
		nc:             nc,
		subject:        natsCfg.Subject,
		service:        service,
		concurrency:    concurrency,
		queueSize:      queueSize,
		reconnectDelay: reconnectDelay,
		userAgent:      userAgent,
	}, nil
}

func (p *Publisher) Connected() bool {
	return p.connected.Load()
}

func (p *Publisher) Listen(ctx context.Context) {
	log := logger.FromContext(ctx)
	defer p.nc.Close()
	for {
		if err := p.subscribe(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("publisher error, reconnecting", "err", err, "delay", p.reconnectDelay)
			select {
			case <-time.After(p.reconnectDelay):
			case <-ctx.Done():
				return
			}
		}
	}
}

func (p *Publisher) subscribe(ctx context.Context) error {
	log := logger.FromContext(ctx)

	cursor, err := p.cursorStore.GetCursor(ctx, p.service)
	if err != nil {
		return fmt.Errorf("getting cursor: %w", err)
	}

	firehoseURL, err := atproto.FirehoseURL(p.service, cursor)
	if err != nil {
		return fmt.Errorf("building firehose URL: %w", err)
	}

	log.Info("publisher connecting to firehose", "url", firehoseURL)

	firehoseConn, _, err := p.dialer.DialContext(ctx, firehoseURL, http.Header{
		"User-Agent": []string{p.userAgent},
	})
	if err != nil {
		return fmt.Errorf("dialing firehose: %w", err)
	}
	defer firehoseConn.Close()

	rsc := &events.RepoStreamCallbacks{
		RepoCommit: func(evt *comatproto.SyncSubscribeRepos_Commit) error {
			return p.handleCommit(ctx, evt)
		},
	}

	sched := parallel.NewScheduler(
		p.concurrency,
		p.queueSize,
		p.service,
		rsc.EventHandler,
	)

	p.connected.Store(true)
	defer p.connected.Store(false)

	log.Info("publisher connected, forwarding events to NATS")
	return events.HandleRepoStream(ctx, firehoseConn, sched, log)
}

func (p *Publisher) handleCommit(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Commit) error {
	log := logger.FromContext(ctx)

	if evt == nil {
		return nil
	}

	var buf bytes.Buffer
	if err := evt.MarshalCBOR(&buf); err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	if err := p.nc.Publish(p.subject, buf.Bytes()); err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	if err := p.cursorStore.UpsertCursor(ctx, p.service, evt.Seq); err != nil {
		return fmt.Errorf("upsert cursor: %w", err)
	}

	log.Debug("event published", "seq", evt.Seq, "repo", evt.Repo)
	return nil
}
