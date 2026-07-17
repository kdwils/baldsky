package cmd

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	bsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/kdwils/baldsky/config"
	"github.com/kdwils/baldsky/db"
	"github.com/kdwils/baldsky/db/gen"
	"github.com/kdwils/baldsky/subscription"
)

var purgeCmd = &cobra.Command{
	Use:   "purge",
	Short: "Purge posts from the database that no longer match the current config",
	RunE:  runPurge,
}

var dryRun bool

type stalePost struct {
	URI  string
	Text string
}

func init() {
	rootCmd.AddCommand(purgeCmd)
	purgeCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Print posts that would be deleted without deleting them")
}

func runPurge(cmd *cobra.Command, args []string) error {
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

	querier := gen.New(postgres.DB)

	xrpcClient := &xrpc.Client{
		Host: "https://public.api.bsky.app",
	}

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
			nil,
		)
		if err != nil {
			return fmt.Errorf("building pipeline %s: %w", p.ShortName, err)
		}

		toDelete, err := findStalePosts(ctx, querier, xrpcClient, pipeline, p.ShortName)
		if err != nil {
			return fmt.Errorf("pipeline %s: %w", p.ShortName, err)
		}

		if len(toDelete) == 0 {
			fmt.Printf("[%s] no stale posts found\n", p.ShortName)
			continue
		}

		if dryRun {
			fmt.Printf("[%s] dry-run: would delete %d posts:\n", p.ShortName, len(toDelete))
			for _, sp := range toDelete {
				fmt.Printf("  uri:  %s\n  text: %s\n\n", sp.URI, sp.Text)
			}
			continue
		}

		deleted := 0
		for _, sp := range toDelete {
			if err := deletePost(ctx, cfg.Server.Hostname, cfg.Server.AdminToken, sp.URI); err != nil {
				fmt.Fprintf(os.Stderr, "[%s] failed to delete %s: %v\n", p.ShortName, sp.URI, err)
				continue
			}
			fmt.Printf("[%s] deleted %s\n", p.ShortName, sp.URI)
			deleted++
		}
		fmt.Printf("[%s] deleted %d/%d stale posts\n", p.ShortName, deleted, len(toDelete))
	}

	return nil
}

func findStalePosts(ctx context.Context, q gen.Querier, c *xrpc.Client, pipeline subscription.Pipeline, feedName string) ([]stalePost, error) {
	const (
		pageSize  = 100
		batchSize = 25
	)

	var stale []stalePost
	params := gen.GetFeedPageParams{
		FeedName: feedName,
		Limit:    pageSize,
	}

	for {
		rows, err := q.GetFeedPage(ctx, params)
		if err != nil {
			return nil, fmt.Errorf("querying posts: %w", err)
		}

		for i := 0; i < len(rows); i += batchSize {
			end := i + batchSize
			if end > len(rows) {
				end = len(rows)
			}
			batch := rows[i:end]

			uris := make([]string, len(batch))
			for j, r := range batch {
				uris[j] = r.Uri
			}

			out, err := bsky.FeedGetPosts(ctx, c, uris)
			if err != nil {
				return nil, fmt.Errorf("fetching post content from bluesky: %w", err)
			}

			fetched := make(map[string]*bsky.FeedDefs_PostView, len(out.Posts))
			for _, pv := range out.Posts {
				fetched[pv.Uri] = pv
			}

			for _, row := range batch {
				pv, ok := fetched[row.Uri]
				if !ok {
					stale = append(stale, stalePost{URI: row.Uri, Text: "[deleted from Bluesky]"})
					continue
				}

				post, ok := pv.Record.Val.(*bsky.FeedPost)
				if !ok {
					stale = append(stale, stalePost{URI: row.Uri, Text: "[unreadable record]"})
					continue
				}

				if !pipeline.MatchesPost(post.Text, post.Langs, post.Embed != nil) {
					stale = append(stale, stalePost{URI: row.Uri, Text: post.Text})
				}
			}
		}

		if len(rows) < pageSize {
			break
		}

		last := rows[len(rows)-1]
		params.CursorIndexedAt.String = last.IndexedAt
		params.CursorIndexedAt.Valid = true
		params.CursorCid.String = last.Cid
		params.CursorCid.Valid = true
	}

	return stale, nil
}

func deletePost(ctx context.Context, host, token, postURI string) error {
	encoded := url.PathEscape(postURI)
	endpoint := strings.TrimRight(host, "/") + "/admin/posts/" + encoded

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("server returned %d", resp.StatusCode)
	}
	return nil
}
