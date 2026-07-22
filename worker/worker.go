package worker

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/nats-io/nats.go"

	"github.com/kdwils/baldsky/config"
	"github.com/kdwils/baldsky/logger"
)

// Processor abstracts event processing so consumers don't need
// to depend on the full Subscription type.
type Processor interface {
	ProcessEvent(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Commit) error
}

// CursorStore persists the last processed cursor position.
type CursorStore interface {
	UpsertCursor(ctx context.Context, service string, cursor int64) error
}

type Worker struct {
	processor   Processor
	cursorStore CursorStore
	nc          *nats.Conn
	subject     string
	queueGroup  string
	service     string
	connected   atomic.Bool
}

func New(processor Processor, natsCfg config.NATSConfig, cursorStore CursorStore) (*Worker, error) {
	nc, err := nats.Connect(natsCfg.URL, nats.Name("baldsky-worker"))
	if err != nil {
		return nil, fmt.Errorf("connect to NATS: %w", err)
	}
	return &Worker{
		processor:   processor,
		cursorStore: cursorStore,
		nc:          nc,
		subject:     natsCfg.Subject,
		queueGroup:  natsCfg.QueueGroup,
		service:     natsCfg.Subject,
	}, nil
}

func (w *Worker) Connected() bool {
	return w.connected.Load()
}

func (w *Worker) Run(ctx context.Context) {
	log := logger.FromContext(ctx)
	defer w.nc.Close()

	sub, err := w.nc.QueueSubscribe(w.subject, w.queueGroup, func(msg *nats.Msg) {
		var evt comatproto.SyncSubscribeRepos_Commit
		if err := evt.UnmarshalCBOR(bytes.NewReader(msg.Data)); err != nil {
			log.Error("failed to unmarshal event", "err", err)
			return
		}
		if err := w.processor.ProcessEvent(ctx, &evt); err != nil {
			log.Error("failed to process event", "err", err)
			return
		}
		if w.cursorStore != nil {
			if err := w.cursorStore.UpsertCursor(ctx, w.service, evt.Seq); err != nil {
				log.Warn("failed to update cursor", "seq", evt.Seq, "err", err)
			}
		}
	})
	if err != nil {
		log.Error("failed to subscribe", "err", err)
		return
	}

	w.connected.Store(true)
	defer w.connected.Store(false)

	<-ctx.Done()
	if err := sub.Drain(); err != nil {
		log.Error("failed to drain subscription", "err", err)
	}
}
