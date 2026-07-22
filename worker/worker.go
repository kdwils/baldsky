package worker

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/nats-io/nats.go"

	"github.com/kdwils/baldsky/config"
	fh "github.com/kdwils/baldsky/firehose"
	"github.com/kdwils/baldsky/logger"
)

type Worker struct {
	processor   fh.Processor
	cursorStore fh.CursorStore
	nc          *nats.Conn
	name        string
	subject     string
	queueGroup  string
	service     string
	connected   atomic.Bool
}

func New(processor fh.Processor, service string, natsCfg config.NATSConfig, cursorStore fh.CursorStore, name string) (*Worker, error) {
	nc, err := nats.Connect(natsCfg.URL,
		nats.Name(name),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(natsCfg.ReconnectWait),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to NATS: %w", err)
	}
	return &Worker{
		processor:   processor,
		cursorStore: cursorStore,
		nc:          nc,
		name:        name,
		subject:     natsCfg.Subject,
		queueGroup:  natsCfg.QueueGroup,
		service:     service,
	}, nil
}

func (w *Worker) Connected() bool {
	return w.connected.Load()
}

func (w *Worker) Run(ctx context.Context) {
	log := logger.FromContext(ctx).With("name", w.name, "subject", w.subject, "queue_group", w.queueGroup)
	defer w.nc.Close()

	log.Info("worker starting")

	procCtx := context.WithoutCancel(ctx)

	sub, err := w.nc.QueueSubscribe(w.subject, w.queueGroup, func(msg *nats.Msg) {
		var evt comatproto.SyncSubscribeRepos_Commit
		if err := evt.UnmarshalCBOR(bytes.NewReader(msg.Data)); err != nil {
			log.Error("failed to unmarshal event", "err", err)
			return
		}
		if err := w.processor.ProcessEvent(procCtx, &evt); err != nil {
			log.Error("failed to process event", "seq", evt.Seq, "repo", evt.Repo, "err", err)
			return
		}
		if w.cursorStore != nil {
			if err := w.cursorStore.UpsertCursor(procCtx, w.service, evt.Seq); err != nil {
				log.Warn("failed to update cursor", "seq", evt.Seq, "service", w.service, "err", err)
			}
		}

		log.Info("processed event")
	})
	if err != nil {
		log.Error("failed to subscribe to NATS", "err", err)
		return
	}

	w.connected.Store(true)
	defer w.connected.Store(false)

	log.Info("worker subscribed")

	<-ctx.Done()
	if err := sub.Drain(); err != nil {
		log.Error("failed to drain subscription", "err", err)
	}
	log.Info("drained subscription")
}
