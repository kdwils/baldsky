package publisher

import (
	"bytes"
	"context"
	"fmt"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/nats-io/nats.go"

	"github.com/kdwils/baldsky/config"
	"github.com/kdwils/baldsky/logger"
	fh "github.com/kdwils/baldsky/firehose"
)

type Publisher struct {
	cursorStore    fh.CursorStore
	firehose       *fh.FirehoseConn
	nc             *nats.Conn
	subject        string
	reconnectDelay time.Duration
	flushTimeout   time.Duration
}

func New(
	cursorStore fh.CursorStore,
	firehose *fh.FirehoseConn,
	natsCfg config.NATSConfig,
	reconnectDelay time.Duration,
	flushTimeout time.Duration,
) (*Publisher, error) {
	nc, err := nats.Connect(natsCfg.URL,
		nats.Name("baldsky-publisher"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to NATS: %w", err)
	}
	return &Publisher{
		cursorStore:    cursorStore,
		firehose:       firehose,
		nc:             nc,
		subject:        natsCfg.Subject,
		reconnectDelay: reconnectDelay,
		flushTimeout:   flushTimeout,
	}, nil
}

func (p *Publisher) Connected() bool {
	return p.firehose.Connected()
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
	cursor, err := p.cursorStore.GetCursor(ctx, p.firehose.Service())
	if err != nil {
		return fmt.Errorf("getting cursor: %w", err)
	}
	return p.firehose.Run(ctx, cursor, p.handleCommit)
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

	if err := p.nc.FlushTimeout(p.flushTimeout); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	if err := p.cursorStore.UpsertCursor(ctx, p.firehose.Service(), evt.Seq); err != nil {
		return fmt.Errorf("upsert cursor: %w", err)
	}

	log.Debug("event published", "seq", evt.Seq, "repo", evt.Repo)
	return nil
}
