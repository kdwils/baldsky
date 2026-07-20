package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/gorilla/websocket"

	"github.com/kdwils/baldsky/config"
	"github.com/kdwils/baldsky/db"
	"github.com/kdwils/baldsky/db/gen"
	"github.com/kdwils/baldsky/feed"
	"github.com/kdwils/baldsky/logger"
	"github.com/kdwils/baldsky/server"
	"github.com/kdwils/baldsky/subscription"
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the feed generator server",
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.New(viper.GetViper())
		if err != nil {
			return err
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
		pipelines := make([]subscription.Pipeline, 0, len(cfg.Pipelines))

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
				return err
			}
			pipelines = append(pipelines, pipeline)
		}

		if cfg.Server.UserAgent == "" {
			return fmt.Errorf("user_agent is required")
		}

		sub := subscription.New(
			pipelines,
			feedSvc,
			websocket.DefaultDialer,
			cfg.Subscription.Endpoint,
			cfg.Subscription.Concurrency,
			cfg.Subscription.QueueSize,
			cfg.Subscription.ReconnectDelay,
			subscription.BuildUserAgent(cfg.Server.UserAgent, cfg.Server.UserAgentURL),
		)

		go sub.Listen(ctx)

		metricsSvc := feed.NewMetricsService(querier, 1000)
		go metricsSvc.Run(ctx)
		defer metricsSvc.Close()

		srv := server.New(cfg.Server.Port, log, feedSvc, postgres.DB, sub, cfg.Server.AdminToken, server.NewRateLimiter(cfg.Server.Rate, cfg.Server.Limit, cfg.Server.RateMaxAge), metricsSvc)
		return srv.Run(ctx)
	},
}

func init() {
	rootCmd.AddCommand(serveCmd)
}
