package feed_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/kdwils/baldsky/db/gen"
	"github.com/kdwils/baldsky/feed"
	"github.com/kdwils/baldsky/feed/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

const (
	testServiceDID   = "did:web:test.example.com"
	testHostname     = "test.example.com"
	testPublisherDID = "did:plc:publisher123"
	testDidContext   = "https://www.w3.org/ns/did/v1"
	testServiceID    = "#bsky_fg"
)

var testEntries = []feed.FeedEntry{
	{
		ShortName:      "test-feed",
		DisplayName:    "Test Feed",
		Description:    "A test feed",
		CollectionName: "app.bsky.feed.generator",
	},
	{
		ShortName:      "another-feed",
		DisplayName:    "Another Feed",
		Description:    "Another feed",
		CollectionName: "app.bsky.feed.generator",
	},
}

func newTestFeedService(ctrl *gomock.Controller) (*feed.Service, *mocks.MockQuerier) {
	q := mocks.NewMockQuerier(ctrl)
	svc := feed.New(q, testServiceDID, testHostname, testPublisherDID, testDidContext, testServiceID, testEntries)
	return svc, q
}

func feedURI(shortName string) string {
	return "at://" + testPublisherDID + "/app.bsky.feed.generator/" + shortName
}

func TestHostname(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc, _ := newTestFeedService(ctrl)
	got := svc.Hostname()
	want := testHostname
	assert.Equal(t, want, got)
}

func TestGetDIDDocument(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc, _ := newTestFeedService(ctrl)
	got := svc.GetDIDDocument()
	want := feed.DIDDocument{
		Context: []string{testDidContext},
		ID:      testServiceDID,
		Service: []feed.DIDServiceEntry{
			{
				ID:              testServiceID,
				Type:            "BskyFeedGenerator",
				ServiceEndpoint: "https://" + testHostname,
			},
		},
	}
	assert.Equal(t, want, got)
}

func TestGetFeedDescription(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc, _ := newTestFeedService(ctrl)
	got := svc.GetFeedDescription()
	want := feed.FeedDescription{
		DID: testServiceDID,
		Feeds: []feed.FeedDescriptionEntry{
			{
				URI:         feedURI("test-feed"),
				DisplayName: "Test Feed",
				Description: "A test feed",
			},
			{
				URI:         feedURI("another-feed"),
				DisplayName: "Another Feed",
				Description: "Another feed",
			},
		},
	}
	assert.ElementsMatch(t, want.Feeds, got.Feeds)
	assert.Equal(t, want.DID, got.DID)
}

func TestInsertPost(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, q := newTestFeedService(ctrl)
		ctx := t.Context()

		var captured gen.InsertPostParams
		q.EXPECT().InsertPost(ctx, gomock.Any()).Do(func(_ context.Context, arg gen.InsertPostParams) {
			captured = arg
		}).Return(nil)

		err := svc.InsertPost(ctx, "test-feed", "at://some-uri", "bafy cid")
		require.NoError(t, err)

		assert.Equal(t, "test-feed", captured.FeedName)
		assert.Equal(t, "at://some-uri", captured.Uri)
		assert.Equal(t, "bafy cid", captured.Cid)

		parsed, err := time.Parse(time.RFC3339, captured.IndexedAt)
		require.NoError(t, err)
		assert.WithinDuration(t, time.Now().UTC(), parsed, time.Second)
	})

	t.Run("error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, q := newTestFeedService(ctrl)
		ctx := t.Context()

		q.EXPECT().InsertPost(ctx, gomock.Any()).Return(errors.New("db error"))

		err := svc.InsertPost(ctx, "test-feed", "at://some-uri", "bafy cid")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "db error")
	})
}

func TestDeletePosts(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, q := newTestFeedService(ctrl)
		ctx := t.Context()

		gomock.InOrder(
			q.EXPECT().DeletePost(ctx, "uri-1").Return(nil),
			q.EXPECT().DeletePost(ctx, "uri-2").Return(nil),
			q.EXPECT().DeletePost(ctx, "uri-3").Return(nil),
		)

		err := svc.DeletePosts(ctx, []string{"uri-1", "uri-2", "uri-3"})
		require.NoError(t, err)
	})

	t.Run("empty list", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, _ := newTestFeedService(ctrl)
		ctx := t.Context()

		err := svc.DeletePosts(ctx, []string{})
		require.NoError(t, err)
	})

	t.Run("error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, q := newTestFeedService(ctrl)
		ctx := t.Context()

		q.EXPECT().DeletePost(ctx, "uri-1").Return(errors.New("delete failed"))

		err := svc.DeletePosts(ctx, []string{"uri-1", "uri-2"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "delete failed")
	})
}

func TestGetFeedPage(t *testing.T) {
	t.Run("empty URI", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, _ := newTestFeedService(ctrl)
		ctx := t.Context()

		got, err := svc.GetFeedPage(ctx, "", "", "")
		require.ErrorIs(t, err, feed.ErrUnknownFeed)
		assert.Equal(t, feed.FeedResponse{}, got)
	})

	t.Run("invalid URI parts", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, _ := newTestFeedService(ctrl)
		ctx := t.Context()

		got, err := svc.GetFeedPage(ctx, "at://did/collection", "", "")
		require.ErrorIs(t, err, feed.ErrUnknownFeed)
		assert.Equal(t, feed.FeedResponse{}, got)
	})

	t.Run("wrong DID", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, _ := newTestFeedService(ctrl)
		ctx := t.Context()

		got, err := svc.GetFeedPage(ctx, "at://did:web:wrong.com/app.bsky.feed.generator/test-feed", "", "")
		require.ErrorIs(t, err, feed.ErrUnknownFeed)
		assert.Equal(t, feed.FeedResponse{}, got)
	})

	t.Run("unknown feed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, _ := newTestFeedService(ctrl)
		ctx := t.Context()

		got, err := svc.GetFeedPage(ctx, "at://"+testPublisherDID+"/app.bsky.feed.generator/nonexistent", "", "")
		require.ErrorIs(t, err, feed.ErrUnknownFeed)
		assert.Equal(t, feed.FeedResponse{}, got)
	})

	t.Run("wrong collection", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, _ := newTestFeedService(ctrl)
		ctx := t.Context()

		got, err := svc.GetFeedPage(ctx, "at://"+testPublisherDID+"/app.bsky.feed.badcollection/test-feed", "", "")
		require.ErrorIs(t, err, feed.ErrUnknownFeed)
		assert.Equal(t, feed.FeedResponse{}, got)
	})

	t.Run("full page with cursor", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, q := newTestFeedService(ctrl)
		ctx := t.Context()

		rows := []gen.GetFeedPageRow{
			{Uri: "at://post1", Cid: "cid1", IndexedAt: "2024-01-01T00:00:00Z"},
			{Uri: "at://post2", Cid: "cid2", IndexedAt: "2024-01-02T00:00:00Z"},
		}

		q.EXPECT().GetFeedPage(ctx, gen.GetFeedPageParams{
			FeedName:        "test-feed",
			CursorIndexedAt: sql.NullString{},
			CursorCid:       sql.NullString{},
			Limit:           2,
		}).Return(rows, nil)

		got, err := svc.GetFeedPage(ctx, feedURI("test-feed"), "2", "")
		require.NoError(t, err)
		want := feed.FeedResponse{
			Feed: []feed.FeedItem{
				{Post: "at://post1"},
				{Post: "at://post2"},
			},
			Cursor: new("2024-01-02T00:00:00Z::cid2"),
		}
		assert.Equal(t, want, got)
	})

	t.Run("with cursor", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, q := newTestFeedService(ctrl)
		ctx := t.Context()

		rows := []gen.GetFeedPageRow{
			{Uri: "at://post2", Cid: "cid2", IndexedAt: "2024-01-02T00:00:00Z"},
		}

		q.EXPECT().GetFeedPage(ctx, gen.GetFeedPageParams{
			FeedName:        "test-feed",
			CursorIndexedAt: sql.NullString{String: "2024-01-01T00:00:00Z", Valid: true},
			CursorCid:       sql.NullString{String: "cid1", Valid: true},
			Limit:           50,
		}).Return(rows, nil)

		got, err := svc.GetFeedPage(ctx, feedURI("test-feed"), "", "2024-01-01T00:00:00Z::cid1")
		require.NoError(t, err)
		want := feed.FeedResponse{
			Feed: []feed.FeedItem{
				{Post: "at://post2"},
			},
			Cursor: nil,
		}
		assert.Equal(t, want, got)
	})

	t.Run("invalid cursor", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, _ := newTestFeedService(ctrl)
		ctx := t.Context()

		got, err := svc.GetFeedPage(ctx, feedURI("test-feed"), "", "bad-cursor")
		require.ErrorIs(t, err, feed.ErrInvalidCursor)
		assert.Equal(t, feed.FeedResponse{}, got)
	})

	t.Run("cursor missing CID", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, _ := newTestFeedService(ctrl)
		ctx := t.Context()

		got, err := svc.GetFeedPage(ctx, feedURI("test-feed"), "", "2024-01-01T00:00:00Z::")
		require.ErrorIs(t, err, feed.ErrInvalidCursor)
		assert.Equal(t, feed.FeedResponse{}, got)
	})

	t.Run("cursor missing indexed at", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, _ := newTestFeedService(ctrl)
		ctx := t.Context()

		got, err := svc.GetFeedPage(ctx, feedURI("test-feed"), "", "::cid1")
		require.ErrorIs(t, err, feed.ErrInvalidCursor)
		assert.Equal(t, feed.FeedResponse{}, got)
	})

	t.Run("default limit", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, q := newTestFeedService(ctrl)
		ctx := t.Context()

		q.EXPECT().GetFeedPage(ctx, gen.GetFeedPageParams{
			FeedName:        "test-feed",
			CursorIndexedAt: sql.NullString{},
			CursorCid:       sql.NullString{},
			Limit:           50,
		}).Return([]gen.GetFeedPageRow{}, nil)

		got, err := svc.GetFeedPage(ctx, feedURI("test-feed"), "", "")
		require.NoError(t, err)
		want := feed.FeedResponse{
			Feed:   []feed.FeedItem{},
			Cursor: nil,
		}
		assert.Equal(t, want, got)
	})

	t.Run("custom limit", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, q := newTestFeedService(ctrl)
		ctx := t.Context()

		q.EXPECT().GetFeedPage(ctx, gen.GetFeedPageParams{
			FeedName:        "test-feed",
			CursorIndexedAt: sql.NullString{},
			CursorCid:       sql.NullString{},
			Limit:           25,
		}).Return([]gen.GetFeedPageRow{}, nil)

		got, err := svc.GetFeedPage(ctx, feedURI("test-feed"), "25", "")
		require.NoError(t, err)
		want := feed.FeedResponse{
			Feed:   []feed.FeedItem{},
			Cursor: nil,
		}
		assert.Equal(t, want, got)
	})

	t.Run("limit capped at 100", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, q := newTestFeedService(ctrl)
		ctx := t.Context()

		q.EXPECT().GetFeedPage(ctx, gen.GetFeedPageParams{
			FeedName:        "test-feed",
			CursorIndexedAt: sql.NullString{},
			CursorCid:       sql.NullString{},
			Limit:           100,
		}).Return([]gen.GetFeedPageRow{}, nil)

		got, err := svc.GetFeedPage(ctx, feedURI("test-feed"), "200", "")
		require.NoError(t, err)
		want := feed.FeedResponse{
			Feed:   []feed.FeedItem{},
			Cursor: nil,
		}
		assert.Equal(t, want, got)
	})

	t.Run("invalid limit string", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, q := newTestFeedService(ctrl)
		ctx := t.Context()

		q.EXPECT().GetFeedPage(ctx, gen.GetFeedPageParams{
			FeedName:        "test-feed",
			CursorIndexedAt: sql.NullString{},
			CursorCid:       sql.NullString{},
			Limit:           50,
		}).Return([]gen.GetFeedPageRow{}, nil)

		got, err := svc.GetFeedPage(ctx, feedURI("test-feed"), "not-a-number", "")
		require.NoError(t, err)
		want := feed.FeedResponse{
			Feed:   []feed.FeedItem{},
			Cursor: nil,
		}
		assert.Equal(t, want, got)
	})

	t.Run("negative limit", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, q := newTestFeedService(ctrl)
		ctx := t.Context()

		q.EXPECT().GetFeedPage(ctx, gen.GetFeedPageParams{
			FeedName:        "test-feed",
			CursorIndexedAt: sql.NullString{},
			CursorCid:       sql.NullString{},
			Limit:           50,
		}).Return([]gen.GetFeedPageRow{}, nil)

		got, err := svc.GetFeedPage(ctx, feedURI("test-feed"), "-5", "")
		require.NoError(t, err)
		want := feed.FeedResponse{
			Feed:   []feed.FeedItem{},
			Cursor: nil,
		}
		assert.Equal(t, want, got)
	})

	t.Run("empty results", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, q := newTestFeedService(ctrl)
		ctx := t.Context()

		q.EXPECT().GetFeedPage(ctx, gen.GetFeedPageParams{
			FeedName:        "test-feed",
			CursorIndexedAt: sql.NullString{},
			CursorCid:       sql.NullString{},
			Limit:           50,
		}).Return([]gen.GetFeedPageRow{}, nil)

		got, err := svc.GetFeedPage(ctx, feedURI("test-feed"), "", "")
		require.NoError(t, err)
		want := feed.FeedResponse{
			Feed:   []feed.FeedItem{},
			Cursor: nil,
		}
		assert.Equal(t, want, got)
	})

	t.Run("partial page no cursor", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, q := newTestFeedService(ctrl)
		ctx := t.Context()

		rows := []gen.GetFeedPageRow{
			{Uri: "at://post1", Cid: "cid1", IndexedAt: "2024-01-01T00:00:00Z"},
		}

		q.EXPECT().GetFeedPage(ctx, gen.GetFeedPageParams{
			FeedName:        "test-feed",
			CursorIndexedAt: sql.NullString{},
			CursorCid:       sql.NullString{},
			Limit:           50,
		}).Return(rows, nil)

		got, err := svc.GetFeedPage(ctx, feedURI("test-feed"), "", "")
		require.NoError(t, err)
		want := feed.FeedResponse{
			Feed: []feed.FeedItem{
				{Post: "at://post1"},
			},
			Cursor: nil,
		}
		assert.Equal(t, want, got)
	})

	t.Run("db error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, q := newTestFeedService(ctrl)
		ctx := t.Context()

		q.EXPECT().GetFeedPage(ctx, gomock.Any()).Return(nil, errors.New("db connection lost"))

		got, err := svc.GetFeedPage(ctx, feedURI("test-feed"), "", "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "get feed page")
		assert.Equal(t, feed.FeedResponse{}, got)
	})
}

func TestGetCursor(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, q := newTestFeedService(ctrl)
		ctx := t.Context()

		q.EXPECT().GetCursor(ctx, "bsky-appview").Return(int64(42), nil)

		got, err := svc.GetCursor(ctx, "bsky-appview")
		require.NoError(t, err)
		want := int64(42)
		assert.Equal(t, want, got)
	})

	t.Run("no rows", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, q := newTestFeedService(ctrl)
		ctx := t.Context()

		q.EXPECT().GetCursor(ctx, "bsky-appview").Return(int64(0), sql.ErrNoRows)

		got, err := svc.GetCursor(ctx, "bsky-appview")
		require.NoError(t, err)
		want := int64(0)
		assert.Equal(t, want, got)
	})

	t.Run("other error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, q := newTestFeedService(ctrl)
		ctx := t.Context()

		q.EXPECT().GetCursor(ctx, "bsky-appview").Return(int64(0), errors.New("timeout"))

		_, err := svc.GetCursor(ctx, "bsky-appview")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "timeout")
	})
}

func TestBuildDescription(t *testing.T) {
	t.Run("no link URL", func(t *testing.T) {
		gotDesc, gotFacets := feed.BuildDescription("Just a description", "", "")
		wantDesc := "Just a description"
		assert.Equal(t, wantDesc, gotDesc)
		assert.Nil(t, gotFacets)
	})

	t.Run("with link URL", func(t *testing.T) {
		gotDesc, gotFacets := feed.BuildDescription("Curated feed", "My Link", "https://example.com")
		wantDesc := "Curated feed\n\nMy Link"
		assert.Equal(t, wantDesc, gotDesc)
		require.Len(t, gotFacets, 1)

		facet := gotFacets[0]
		require.NotNil(t, facet.Index)
		assert.Equal(t, int64(14), facet.Index.ByteStart) // byte offset of "My Link"
		assert.Equal(t, int64(21), facet.Index.ByteEnd)
		require.Len(t, facet.Features, 1)

		elem := facet.Features[0]
		require.NotNil(t, elem.RichtextFacet_Link)
		assert.Equal(t, "https://example.com", elem.RichtextFacet_Link.Uri)
	})

	t.Run("description with unicode characters", func(t *testing.T) {
		gotDesc, gotFacets := feed.BuildDescription("café au lait", "Projet", "https://github.com/user/repo")
		wantDesc := "café au lait\n\nProjet"
		assert.Equal(t, wantDesc, gotDesc)
		require.Len(t, gotFacets, 1)

		facet := gotFacets[0]
		require.NotNil(t, facet.Index)
		// "café" is 5 bytes (c a f é = c3 a9), plus " au lait" = 8 bytes,
		// plus "\n\n" is 2 bytes = 15 bytes total before the link label
		assert.Equal(t, int64(15), facet.Index.ByteStart)
		assert.Equal(t, int64(21), facet.Index.ByteEnd)
	})

	t.Run("empty description with link", func(t *testing.T) {
		gotDesc, gotFacets := feed.BuildDescription("", "GitHub", "https://github.com/kdwils/baldsky")
		wantDesc := "\n\nGitHub"
		assert.Equal(t, wantDesc, gotDesc)
		require.Len(t, gotFacets, 1)

		facet := gotFacets[0]
		require.NotNil(t, facet.Index)
		assert.Equal(t, int64(2), facet.Index.ByteStart)
		assert.Equal(t, int64(8), facet.Index.ByteEnd)
	})

	t.Run("long multi-line description with link", func(t *testing.T) {
		gotDesc, gotFacets := feed.BuildDescription("Line one\nLine two\nLine three", "Click Here", "https://example.com/path?q=1")
		wantDesc := "Line one\nLine two\nLine three\n\nClick Here"
		assert.Equal(t, wantDesc, gotDesc)
		require.Len(t, gotFacets, 1)

		facet := gotFacets[0]
		require.NotNil(t, facet.Index)
		assert.Equal(t, int64(30), facet.Index.ByteStart)
		assert.Equal(t, int64(40), facet.Index.ByteEnd)
	})
}

func TestUpsertCursor(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, q := newTestFeedService(ctrl)
		ctx := t.Context()

		q.EXPECT().UpsertCursor(ctx, gen.UpsertCursorParams{
			Service: "bsky-appview",
			Cursor:  100,
		}).Return(nil)

		err := svc.UpsertCursor(ctx, "bsky-appview", 100)
		require.NoError(t, err)
	})

	t.Run("error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc, q := newTestFeedService(ctrl)
		ctx := t.Context()

		q.EXPECT().UpsertCursor(ctx, gen.UpsertCursorParams{
			Service: "bsky-appview",
			Cursor:  100,
		}).Return(errors.New("upsert failed"))

		err := svc.UpsertCursor(ctx, "bsky-appview", 100)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "upsert failed")
	})
}
