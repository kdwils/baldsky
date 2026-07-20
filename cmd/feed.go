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
	"github.com/kdwils/baldsky/feed"
	"github.com/kdwils/baldsky/subscription"
)

var publishFeedCmd = &cobra.Command{
	Use:   "feed",
	Short: "publish a feed",
	RunE:  runPublishFeed,
}

func init() {
	publishCmd.AddCommand(publishFeedCmd)
}

func runPublishFeed(cmd *cobra.Command, args []string) error {
	cfg, err := config.New(viper.GetViper())
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
		nil,
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

var updateFeedCmd = &cobra.Command{
	Use:   "feed",
	Short: "update a feed",
	RunE:  runUpdateFeed,
}

func init() {
	updateCmd.AddCommand(updateFeedCmd)
}

func runUpdateFeed(cmd *cobra.Command, args []string) error {
	cfg, err := config.New(viper.GetViper())
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
		nil,
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

	updated, err := feedSvc.Update(ctx)
	if err != nil {
		return err
	}

	if len(updated) == 0 {
		fmt.Println("no enabled pipelines to update")
		return nil
	}

	for shortName, uri := range updated {
		fmt.Printf("[%s] updated feed generator: %s\n", shortName, uri)
	}
	return nil
}

var deleteFeedCmd = &cobra.Command{
	Use:   "feed",
	Short: "delete a feed",
	RunE:  runDeleteFeed,
}

func init() {
	deleteCmd.AddCommand(deleteFeedCmd)
}

func runDeleteFeed(cmd *cobra.Command, args []string) error {
	cfg, err := config.New(viper.GetViper())
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
		nil,
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

	deleted, err := feedSvc.Delete(ctx)
	if err != nil {
		return err
	}

	if len(deleted) == 0 {
		fmt.Println("no enabled pipelines to delete")
		return nil
	}

	for shortName, uri := range deleted {
		fmt.Printf("[%s] deleted feed generator: %s\n", shortName, uri)
	}
	return nil
}
