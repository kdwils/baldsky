package cmd

import (
	"context"
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

		postgres, err := db.New(cfg.Database.DSN)
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
		)

		for _, p := range cfg.Pipelines {
			if !p.Enabled {
				continue
			}
			pipelines = append(pipelines, subscription.NewPipeline(
				p.ShortName,
				p.Keywords,
				p.ExcludeKeywords,
				p.BlockDIDs,
				p.RequireMedia,
				feedSvc,
			))
		}

		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		ctx = logger.WithContext(ctx, log)

		go subscription.New(
			pipelines,
			feedSvc,
			websocket.DefaultDialer,
			cfg.Subscription.Endpoint,
			cfg.Subscription.ReconnectDelay,
		).Listen(ctx)

		srv := server.New(cfg.Server.Port, log, feedSvc)
		return srv.Run(ctx)
	},
}

func init() {
	rootCmd.AddCommand(serveCmd)
}
