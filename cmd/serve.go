package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/gorilla/websocket"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/kdwils/baldsky/config"
	"github.com/kdwils/baldsky/db"
	"github.com/kdwils/baldsky/db/gen"
	"github.com/kdwils/baldsky/feed"
	"github.com/kdwils/baldsky/logger"
	fh "github.com/kdwils/baldsky/firehose"
	"github.com/kdwils/baldsky/publisher"
	"github.com/kdwils/baldsky/server"
	"github.com/kdwils/baldsky/subscription"
	"github.com/kdwils/baldsky/worker"
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the feed generator server",
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.New(viper.GetViper())
		if err != nil {
			return err
		}

		if !cfg.Server.Enabled && !cfg.Subscription.Enabled && !cfg.Publisher.Enabled && !cfg.Worker.Enabled {
			return fmt.Errorf("no roles enabled; nothing to do")
		}

		if (cfg.Publisher.Enabled || cfg.Worker.Enabled) && cfg.NATS.URL == "" {
			return fmt.Errorf("nats.url is required when publisher or worker is enabled")
		}

		log := logger.New(cfg.Server.LogLevel)

		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		ctx = logger.WithContext(ctx, log)

		postgres, err := db.New(ctx, cfg.Database.DSN, cfg.Database.ReconnectDelay)
		if err != nil {
			return err
		}
		defer postgres.Close()

		if err := postgres.Migrate(); err != nil {
			return err
		}

		querier := gen.New(postgres.DB)

		feedEntries := make([]feed.FeedEntry, 0, len(cfg.Pipelines))

		for _, p := range cfg.Pipelines {
			if !p.Enabled {
				continue
			}
			feedEntries = append(feedEntries, feed.FeedEntry{
				ShortName:      p.ShortName,
				DisplayName:    p.DisplayName,
				Description:    p.Description,
				CollectionName: p.CollectionName,
				LinkLabel:      p.LinkLabel,
				LinkURL:        p.LinkURL,
			})
		}

		feedSvc := feed.New(
			querier,
			cfg.Server.ServiceDID,
			cfg.Server.Hostname,
			cfg.Server.PublisherDID,
			cfg.Server.DIDContext,
			cfg.Server.ServiceID,
			feedEntries,
			feed.WithPublisher(
				cfg.Auth.PDS,
				cfg.Auth.Identifier,
				cfg.Auth.Password,
				subscription.BuildUserAgent(cfg.Server.UserAgent, cfg.Server.UserAgentURL),
			),
		)

		if cfg.Server.UserAgent == "" {
			return fmt.Errorf("user_agent is required")
		}

		metricsSvc := feed.NewMetricsService(querier, 1000)
		go metricsSvc.Run(ctx)
		defer metricsSvc.Close()

		ua := subscription.BuildUserAgent(cfg.Server.UserAgent, cfg.Server.UserAgentURL)

		var sub *subscription.Subscription
		var pub *publisher.Publisher
		var w *worker.Worker

		if cfg.Publisher.Enabled {
			firehose := fh.NewFirehoseConn(
				websocket.DefaultDialer,
				cfg.Subscription.Endpoint,
				ua,
				cfg.Subscription.Concurrency,
				cfg.Subscription.QueueSize,
			)

			pub, err = publisher.New(
				feedSvc,
				firehose,
				cfg.NATS,
				cfg.Subscription.ReconnectDelay,
			)
			if err != nil {
				return err
			}
			go pub.Listen(ctx)
		}

		if cfg.Worker.Enabled {
			pipelines, err := buildPipelines(cfg, feedSvc)
			if err != nil {
				return err
			}

			proc := subscription.NewProcessor(pipelines)

			w, err = worker.New(proc, cfg.Subscription.Endpoint, cfg.NATS, feedSvc)
			if err != nil {
				return err
			}
			go w.Run(ctx)
		}

		if cfg.Subscription.Enabled && !cfg.Publisher.Enabled {
			pipelines, err := buildPipelines(cfg, feedSvc)
			if err != nil {
				return err
			}

			firehose := fh.NewFirehoseConn(
				websocket.DefaultDialer,
				cfg.Subscription.Endpoint,
				ua,
				cfg.Subscription.Concurrency,
				cfg.Subscription.QueueSize,
			)

			sub = subscription.New(
				pipelines,
				feedSvc,
				firehose,
				cfg.Subscription.ReconnectDelay,
			)

			go sub.Listen(ctx)
		}

		if !cfg.Server.Enabled {
			<-ctx.Done()
			return nil
		}

		var serverOpts []server.Option
		if sub != nil {
			serverOpts = append(serverOpts, server.WithFirehose(sub))
		}
		if pub != nil {
			serverOpts = append(serverOpts, server.WithFirehose(pub))
		}
		if w != nil {
			serverOpts = append(serverOpts, server.WithWorker(w))
		}
		serverOpts = append(serverOpts, server.WithMetrics(metricsSvc))

		srv := server.New(cfg.Server.Port, log, feedSvc, postgres.DB, cfg.Server.AdminToken, server.NewRateLimiter(cfg.Server.Rate, cfg.Server.Limit, cfg.Server.RateMaxAge), serverOpts...)
		return srv.Run(ctx)
	},
}

func init() {
	rootCmd.AddCommand(serveCmd)
}

func buildPipelines(cfg config.Config, feedSvc *feed.Service) ([]subscription.Pipeline, error) {
	pipelines := make([]subscription.Pipeline, 0, len(cfg.Pipelines))
	for _, p := range cfg.Pipelines {
		if !p.Enabled {
			continue
		}
		pipeline, err := subscription.NewPipeline(
			p.ShortName,
			p.Keywords,
			p.ExcludeKeywords,
			p.ContextKeywords,
			p.ContextWords,
			p.BlockDIDs,
			p.Languages,
			p.RequireMedia,
			feedSvc,
		)
		if err != nil {
			return nil, err
		}
		pipelines = append(pipelines, pipeline)
	}
	return pipelines, nil
}
