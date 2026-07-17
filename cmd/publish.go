package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/kdwils/baldsky/config"
	"github.com/kdwils/baldsky/db"
	"github.com/kdwils/baldsky/db/gen"
	"github.com/kdwils/baldsky/feed"
	"github.com/kdwils/baldsky/subscription"
)

var publishCmd = &cobra.Command{
	Use:   "publish",
	Short: "Publish feed generator records to the publisher's PDS",
	Long: `Publish creates (or updates) an app.bsky.feed.generator record on the
publisher's PDS for every enabled pipeline defined in the config. This makes
each feed discoverable and subscribable by Bluesky clients.

Authentication uses the auth.identifier (handle or DID) and auth.password
(app password) fields against the auth.pds endpoint.`,
	RunE: runPublish,
}

func init() {
	rootCmd.AddCommand(publishCmd)
}

func runPublish(cmd *cobra.Command, args []string) error {
	cfg, err := config.New(viper.GetViper())
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	published, err := feedSvc.Publish(ctx)
	if err != nil {
		return err
	}

	if len(published) == 0 {
		fmt.Println("no enabled pipelines to publish")
		return nil
	}

	for shortName, uri := range published {
		fmt.Printf("[%s] published feed generator: %s\n", shortName, uri)
	}
	return nil
}
