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

type Worker struct {
	processor  Processor
	nc         *nats.Conn
	subject    string
	queueGroup string
	connected  atomic.Bool
}

func New(processor Processor, natsCfg config.NATSConfig) (*Worker, error) {
	nc, err := nats.Connect(natsCfg.URL, nats.Name("baldsky-worker"))
	if err != nil {
		return nil, fmt.Errorf("connect to NATS: %w", err)
	}
	return &Worker{
		processor:  processor,
		nc:         nc,
		subject:    natsCfg.Subject,
		queueGroup: natsCfg.QueueGroup,
	}, nil
}

func (w *Worker) Connected() bool {
	return w.connected.Load()
}

func (w *Worker) Run(ctx context.Context) {
	log := logger.FromContext(ctx)

	sub, err := w.nc.QueueSubscribe(w.subject, w.queueGroup, func(msg *nats.Msg) {
		var evt comatproto.SyncSubscribeRepos_Commit
		if err := evt.UnmarshalCBOR(bytes.NewReader(msg.Data)); err != nil {
			log.Error("failed to unmarshal event", "err", err)
			return
		}
		if err := w.processor.ProcessEvent(ctx, &evt); err != nil {
			log.Error("failed to process event", "err", err)
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
