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
	name string,
) (*Publisher, error) {
	nc, err := nats.Connect(natsCfg.URL,
		nats.Name(name),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(natsCfg.ReconnectWait),
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
	log := logger.FromContext(ctx).With("subject", p.subject)
	defer p.nc.Close()
	log.Info("publisher listening")
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
	log := logger.FromContext(ctx).With("subject", p.subject)
	cursor, err := p.cursorStore.GetCursor(ctx, p.firehose.Service())
	if err != nil {
		log.Error("failed to get cursor", "err", err)
		return fmt.Errorf("getting cursor: %w", err)
	}
	log.Info("publisher connecting to firehose", "cursor", cursor)
	return p.firehose.Run(ctx, cursor, p.handleCommit)
}

func (p *Publisher) handleCommit(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Commit) error {
	log := logger.FromContext(ctx)

	if evt == nil {
		return nil
	}

	if evt.TooBig {
		log.Warn("skipping too-big event", "seq", evt.Seq, "repo", evt.Repo)
		if err := p.cursorStore.UpsertCursor(ctx, p.firehose.Service(), evt.Seq); err != nil {
			return fmt.Errorf("upsert cursor: %w", err)
		}
		return nil
	}

	var buf bytes.Buffer
	if err := evt.MarshalCBOR(&buf); err != nil {
		log.Error("failed to marshal event", "seq", evt.Seq, "repo", evt.Repo, "err", err)
		return fmt.Errorf("marshal event: %w", err)
	}

	if err := p.nc.Publish(p.subject, buf.Bytes()); err != nil {
		log.Error("failed to publish event to NATS", "seq", evt.Seq, "repo", evt.Repo, "subject", p.subject, "err", err)
		return fmt.Errorf("publish: %w", err)
	}

	if err := p.nc.FlushTimeout(p.flushTimeout); err != nil {
		log.Error("failed to flush NATS", "seq", evt.Seq, "repo", evt.Repo, "err", err)
		return fmt.Errorf("flush: %w", err)
	}

	if err := p.cursorStore.UpsertCursor(ctx, p.firehose.Service(), evt.Seq); err != nil {
		log.Error("failed to upsert cursor", "seq", evt.Seq, "repo", evt.Repo, "err", err)
		return fmt.Errorf("upsert cursor: %w", err)
	}

	log.Debug("event published", "seq", evt.Seq, "repo", evt.Repo)
	return nil
}
