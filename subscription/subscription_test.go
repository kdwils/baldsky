package subscription

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	bsky "github.com/bluesky-social/indigo/api/bsky"
	fh "github.com/kdwils/baldsky/firehose"
	firehosemocks "github.com/kdwils/baldsky/firehose/mocks"
	"github.com/kdwils/baldsky/subscription/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestBuildUserAgent(t *testing.T) {
	t.Run("builds user agent string", func(t *testing.T) {
		got := BuildUserAgent("baldsky", "https://github.com/kdwils/baldsky")
		want := "baldsky/dev (+https://github.com/kdwils/baldsky)"
		assert.Equal(t, want, got)
	})

	t.Run("empty name", func(t *testing.T) {
		got := BuildUserAgent("", "https://example.com")
		want := "/dev (+https://example.com)"
		assert.Equal(t, want, got)
	})

	t.Run("empty url", func(t *testing.T) {
		got := BuildUserAgent("myapp", "")
		want := "myapp/dev"
		assert.Equal(t, want, got)
	})
}

func TestNewPipeline(t *testing.T) {
	t.Run("basic pipeline", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		store := mocks.NewMockPipelineStore(ctrl)
		p, err := NewPipeline(
			"test-pipeline",
			[]string{"bald", "hair loss"},
			nil,
			nil,
			nil,
			nil,
			nil,
			false,
			store,
		)
		require.NoError(t, err)
		assert.Equal(t, "test-pipeline", p.Name)
		assert.Equal(t, []string{"bald", "hair loss"}, p.keywords)
		assert.Empty(t, p.excludeKeywords)
		assert.Empty(t, p.contextKeywords)
		assert.Empty(t, p.contextWords)
		assert.False(t, p.requireMedia)
		assert.Equal(t, store, p.Store)
	})

	t.Run("pipeline with all options", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		store := mocks.NewMockPipelineStore(ctrl)
		p, err := NewPipeline(
			"full-pipeline",
			[]string{"bald"},
			[]string{"bald eagle"},
			[]string{"minoxidil"},
			[]string{"head", "spot"},
			[]string{"did:plc:blocked1", "did:plc:blocked2"},
			[]string{"en", "es"},
			true,
			store,
		)
		require.NoError(t, err)
		assert.Equal(t, "full-pipeline", p.Name)
		assert.Equal(t, []string{"bald"}, p.keywords)
		assert.Equal(t, []string{"bald eagle"}, p.excludeKeywords)
		assert.Equal(t, []string{"minoxidil"}, p.contextKeywords)
		assert.Equal(t, []string{"head", "spot"}, p.contextWords)
		assert.Equal(t, map[string]struct{}{"did:plc:blocked1": {}, "did:plc:blocked2": {}}, p.blockedDIDs)
		assert.Equal(t, map[string]struct{}{"en": {}, "es": {}}, p.languages)
		assert.True(t, p.requireMedia)
		assert.Len(t, p.keywordRegexps, 1)
		assert.Len(t, p.excludeRegexps, 1)
		assert.Len(t, p.contextKeywordRegexps, 1)
		assert.Len(t, p.contextWordRegexps, 2)
	})

	t.Run("lowercases keywords", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		store := mocks.NewMockPipelineStore(ctrl)
		p, err := NewPipeline(
			"lower-pipeline",
			[]string{"BALD", "HairLoss"},
			[]string{"BALD Eagle"},
			[]string{"Minoxidil"},
			[]string{"HEAD"},
			nil,
			nil,
			false,
			store,
		)
		require.NoError(t, err)
		assert.Equal(t, []string{"bald", "hairloss"}, p.keywords)
		assert.Equal(t, []string{"bald eagle"}, p.excludeKeywords)
		assert.Equal(t, []string{"minoxidil"}, p.contextKeywords)
		assert.Equal(t, []string{"head"}, p.contextWords)
	})

	t.Run("lowercases languages", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		store := mocks.NewMockPipelineStore(ctrl)
		p, err := NewPipeline(
			"lang-pipeline",
			nil,
			nil,
			nil,
			nil,
			nil,
			[]string{"EN", "Es"},
			false,
			store,
		)
		require.NoError(t, err)
		assert.Equal(t, map[string]struct{}{"en": {}, "es": {}}, p.languages)
	})

	t.Run("empty pipeline", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		store := mocks.NewMockPipelineStore(ctrl)
		p, err := NewPipeline(
			"empty-pipeline",
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			false,
			store,
		)
		require.NoError(t, err)
		assert.Equal(t, "empty-pipeline", p.Name)
		assert.Empty(t, p.keywords)
		assert.Empty(t, p.excludeKeywords)
		assert.Empty(t, p.contextKeywords)
		assert.Empty(t, p.contextWords)
		assert.Empty(t, p.keywordRegexps)
		assert.Empty(t, p.excludeRegexps)
		assert.Empty(t, p.contextKeywordRegexps)
		assert.Empty(t, p.contextWordRegexps)
		assert.Empty(t, p.blockedDIDs)
		assert.Empty(t, p.languages)
	})
}

func TestShouldInsert(t *testing.T) {
	t.Run("keyword match inserts", func(t *testing.T) {
		p := Pipeline{
			keywords:       []string{"bald"},
			keywordRegexps: mustCompileRegexps([]string{"bald"}),
		}
		post := &bsky.FeedPost{
			Text:  "I'm going bald",
			Langs: []string{"en"},
		}
		got := p.shouldInsert("did:plc:actor1", post)
		assert.True(t, got)
	})

	t.Run("hyphenated compound does not match", func(t *testing.T) {
		p := Pipeline{
			keywords:       []string{"bald"},
			keywordRegexps: mustCompileRegexps([]string{"bald"}),
		}
		post := &bsky.FeedPost{
			Text:  "I'm un-bald now",
			Langs: []string{"en"},
		}
		got := p.shouldInsert("did:plc:actor1", post)
		assert.False(t, got)
	})

	t.Run("leading hyphen does not match", func(t *testing.T) {
		p := Pipeline{
			keywords:       []string{"bald"},
			keywordRegexps: mustCompileRegexps([]string{"bald"}),
		}
		post := &bsky.FeedPost{
			Text:  "-bald is not a word",
			Langs: []string{"en"},
		}
		got := p.shouldInsert("did:plc:actor1", post)
		assert.False(t, got)
	})

	t.Run("trailing hyphen does not match", func(t *testing.T) {
		p := Pipeline{
			keywords:       []string{"bald"},
			keywordRegexps: mustCompileRegexps([]string{"bald"}),
		}
		post := &bsky.FeedPost{
			Text:  "bald- is not a word",
			Langs: []string{"en"},
		}
		got := p.shouldInsert("did:plc:actor1", post)
		assert.False(t, got)
	})

	t.Run("punctuation boundary matches", func(t *testing.T) {
		p := Pipeline{
			keywords:       []string{"bald"},
			keywordRegexps: mustCompileRegexps([]string{"bald"}),
		}
		post := &bsky.FeedPost{
			Text:  "look (bald) please",
			Langs: []string{"en"},
		}
		got := p.shouldInsert("did:plc:actor1", post)
		assert.True(t, got)
	})

	t.Run("exact word match", func(t *testing.T) {
		p := Pipeline{
			keywords:       []string{"bald"},
			keywordRegexps: mustCompileRegexps([]string{"bald"}),
		}
		post := &bsky.FeedPost{
			Text:  "bald",
			Langs: []string{"en"},
		}
		got := p.shouldInsert("did:plc:actor1", post)
		assert.True(t, got)
	})

	t.Run("blocked DID rejects", func(t *testing.T) {
		p := Pipeline{
			keywords:       []string{"bald"},
			keywordRegexps: mustCompileRegexps([]string{"bald"}),
			blockedDIDs:    map[string]struct{}{"did:plc:blocked1": {}},
		}
		post := &bsky.FeedPost{
			Text:  "I'm going bald",
			Langs: []string{"en"},
		}
		got := p.shouldInsert("did:plc:blocked1", post)
		assert.False(t, got)
	})

	t.Run("requireMedia with no embed rejects", func(t *testing.T) {
		p := Pipeline{
			keywords:       []string{"bald"},
			keywordRegexps: mustCompileRegexps([]string{"bald"}),
			requireMedia:   true,
		}
		post := &bsky.FeedPost{
			Text:  "I'm going bald",
			Langs: []string{"en"},
		}
		got := p.shouldInsert("did:plc:actor1", post)
		assert.False(t, got)
	})

	t.Run("requireMedia with embed allows", func(t *testing.T) {
		p := Pipeline{
			keywords:       []string{"bald"},
			keywordRegexps: mustCompileRegexps([]string{"bald"}),
			requireMedia:   true,
		}
		post := &bsky.FeedPost{
			Text:  "I'm going bald",
			Langs: []string{"en"},
			Embed: &bsky.FeedPost_Embed{},
		}
		got := p.shouldInsert("did:plc:actor1", post)
		assert.True(t, got)
	})

	t.Run("language filter matches", func(t *testing.T) {
		p := Pipeline{
			keywords:       []string{"bald"},
			keywordRegexps: mustCompileRegexps([]string{"bald"}),
			languages:      map[string]struct{}{"en": {}},
		}
		post := &bsky.FeedPost{
			Text:  "I'm going bald",
			Langs: []string{"en"},
		}
		got := p.shouldInsert("did:plc:actor1", post)
		assert.True(t, got)
	})

	t.Run("language filter rejects", func(t *testing.T) {
		p := Pipeline{
			keywords:       []string{"bald"},
			keywordRegexps: mustCompileRegexps([]string{"bald"}),
			languages:      map[string]struct{}{"en": {}},
		}
		post := &bsky.FeedPost{
			Text:  "I'm going bald",
			Langs: []string{"de"},
		}
		got := p.shouldInsert("did:plc:actor1", post)
		assert.False(t, got)
	})

	t.Run("no keyword match rejects", func(t *testing.T) {
		p := Pipeline{
			keywords:       []string{"bald"},
			keywordRegexps: mustCompileRegexps([]string{"bald"}),
		}
		post := &bsky.FeedPost{
			Text:  "I have great hair",
			Langs: []string{"en"},
		}
		got := p.shouldInsert("did:plc:actor1", post)
		assert.False(t, got)
	})

	t.Run("language filter passes post with no langs field", func(t *testing.T) {
		p := Pipeline{
			keywords:       []string{"bald"},
			keywordRegexps: mustCompileRegexps([]string{"bald"}),
			languages:      map[string]struct{}{"en": {}},
		}
		post := &bsky.FeedPost{
			Text:  "I'm going bald",
			Langs: nil,
		}
		got := p.shouldInsert("did:plc:actor1", post)
		assert.True(t, got)
	})

	t.Run("language filter passes post with empty langs slice", func(t *testing.T) {
		p := Pipeline{
			keywords:       []string{"bald"},
			keywordRegexps: mustCompileRegexps([]string{"bald"}),
			languages:      map[string]struct{}{"en": {}},
		}
		post := &bsky.FeedPost{
			Text:  "I'm going bald",
			Langs: []string{},
		}
		got := p.shouldInsert("did:plc:actor1", post)
		assert.True(t, got)
	})

	t.Run("empty languages allows all", func(t *testing.T) {
		p := Pipeline{
			keywords:       []string{"bald"},
			keywordRegexps: mustCompileRegexps([]string{"bald"}),
			languages:      map[string]struct{}{},
		}
		post := &bsky.FeedPost{
			Text:  "I'm going bald",
			Langs: []string{"fr"},
		}
		got := p.shouldInsert("did:plc:actor1", post)
		assert.True(t, got)
	})
}

func TestMatches(t *testing.T) {
	t.Run("context keyword without context word does not match", func(t *testing.T) {
		p := Pipeline{
			contextKeywords:       []string{"bald"},
			contextWords:          []string{"head", "spot", "my"},
			contextKeywordRegexps: mustCompileRegexps([]string{"bald"}),
			contextWordRegexps:    mustCompileRegexps([]string{"head", "spot", "my"}),
		}
		got := p.matches("bald patches in the yard")
		assert.False(t, got)
	})

	t.Run("context keyword with context word matches", func(t *testing.T) {
		p := Pipeline{
			contextKeywords:       []string{"bald"},
			contextWords:          []string{"head", "spot", "my"},
			contextKeywordRegexps: mustCompileRegexps([]string{"bald"}),
			contextWordRegexps:    mustCompileRegexps([]string{"head", "spot", "my"}),
		}
		got := p.matches("bald spot on my head")
		assert.True(t, got)
	})

	t.Run("unique keyword matches unconditionally", func(t *testing.T) {
		p := Pipeline{
			keywords:              []string{"minoxidil"},
			contextKeywords:       []string{"bald"},
			contextWords:          []string{"head", "spot", "my"},
			keywordRegexps:        mustCompileRegexps([]string{"minoxidil"}),
			contextKeywordRegexps: mustCompileRegexps([]string{"bald"}),
			contextWordRegexps:    mustCompileRegexps([]string{"head", "spot", "my"}),
		}
		got := p.matches("minoxidil saved my hairline")
		assert.True(t, got)
	})

	t.Run("exclude keyword blocks match", func(t *testing.T) {
		p := Pipeline{
			keywords:        []string{"bald"},
			excludeKeywords: []string{"bald eagle"},
			keywordRegexps:  mustCompileRegexps([]string{"bald"}),
			excludeRegexps:  mustCompileRegexps([]string{"bald eagle"}),
		}
		got := p.matches("the bald eagle flew overhead")
		assert.False(t, got)
	})

	t.Run("german bald without context does not match", func(t *testing.T) {
		p := Pipeline{
			contextKeywords:       []string{"bald"},
			contextWords:          []string{"head", "spot", "my"},
			contextKeywordRegexps: mustCompileRegexps([]string{"bald"}),
			contextWordRegexps:    mustCompileRegexps([]string{"head", "spot", "my"}),
		}
		got := p.matches("Hoffentlich kommt der Schlaf bald wieder")
		assert.False(t, got)
	})

	t.Run("ambiguous keyword with multiple context words matches", func(t *testing.T) {
		p := Pipeline{
			contextKeywords:       []string{"balding"},
			contextWords:          []string{"hair", "head", "loss"},
			contextKeywordRegexps: mustCompileRegexps([]string{"balding"}),
			contextWordRegexps:    mustCompileRegexps([]string{"hair", "head", "loss"}),
		}
		got := p.matches("my hair loss is making me balding")
		assert.True(t, got)
	})

	t.Run("context keyword with context word in exclude does not match", func(t *testing.T) {
		p := Pipeline{
			contextKeywords:       []string{"bald"},
			contextWords:          []string{"head", "spot"},
			excludeKeywords:       []string{"bald eagle"},
			contextKeywordRegexps: mustCompileRegexps([]string{"bald"}),
			contextWordRegexps:    mustCompileRegexps([]string{"head", "spot"}),
			excludeRegexps:        mustCompileRegexps([]string{"bald eagle"}),
		}
		got := p.matches("bald eagle spot on my head")
		assert.False(t, got)
	})

	t.Run("no keywords configured does not match", func(t *testing.T) {
		p := Pipeline{}
		got := p.matches("anything at all")
		assert.False(t, got)
	})
}

func TestHandleDelete(t *testing.T) {
	t.Run("deletes from all pipelines", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		store1 := mocks.NewMockPipelineStore(ctrl)
		store2 := mocks.NewMockPipelineStore(ctrl)
		ctx := t.Context()

		gomock.InOrder(
			store1.EXPECT().DeletePosts(ctx, []string{"at://did:plc:actor1/app.bsky.feed.post/abc123"}).Return(nil),
			store2.EXPECT().DeletePosts(ctx, []string{"at://did:plc:actor1/app.bsky.feed.post/abc123"}).Return(nil),
		)

		s := &Subscription{
			Processor: &Processor{
				pipelines: []Pipeline{
					{Name: "pipeline-1", Store: store1},
					{Name: "pipeline-2", Store: store2},
				},
			},
		}

		s.handleDelete(ctx, "at://did:plc:actor1/app.bsky.feed.post/abc123")
	})

	t.Run("continues on store error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		store1 := mocks.NewMockPipelineStore(ctrl)
		store2 := mocks.NewMockPipelineStore(ctrl)
		ctx := t.Context()

		gomock.InOrder(
			store1.EXPECT().DeletePosts(ctx, []string{"at://uri"}).Return(errors.New("db error")),
			store2.EXPECT().DeletePosts(ctx, []string{"at://uri"}).Return(nil),
		)

		s := &Subscription{
			Processor: &Processor{
				pipelines: []Pipeline{
					{Name: "failing-pipeline", Store: store1},
					{Name: "ok-pipeline", Store: store2},
				},
			},
		}

		s.handleDelete(ctx, "at://uri")
	})

	t.Run("single pipeline", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		store := mocks.NewMockPipelineStore(ctrl)
		ctx := t.Context()

		store.EXPECT().DeletePosts(ctx, []string{"at://uri"}).Return(nil)

		s := &Subscription{
			Processor: &Processor{
				pipelines: []Pipeline{
					{Name: "only-pipeline", Store: store},
				},
			},
		}

		s.handleDelete(ctx, "at://uri")
	})
}

func TestBuildURI(t *testing.T) {
	t.Run("builds AT URI", func(t *testing.T) {
		got := buildURI("did:plc:ohuaaaaaadsxc5k6c3ajezjq", "app.bsky.feed.post/aaaa3svycxc2r")
		want := "at://did:plc:ohuaaaaaadsxc5k6c3ajezjq/app.bsky.feed.post/aaaa3svycxc2r"
		assert.Equal(t, want, got)
	})

	t.Run("minimal path", func(t *testing.T) {
		got := buildURI("did:plc:test", "app.bsky.feed.post/123")
		want := "at://did:plc:test/app.bsky.feed.post/123"
		assert.Equal(t, want, got)
	})
}

func TestNewSubscription(t *testing.T) {
	t.Run("creates subscription with all fields", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		store := mocks.NewMockPipelineStore(ctrl)
		pipelines := []Pipeline{
			{Name: "p1", Store: store},
		}
		cursorStore := firehosemocks.NewMockCursorStore(ctrl)
		firehose := fh.NewFirehoseConn(nil, "wss://bsky.network", "baldsky/dev", 4, 100)

		got := New(pipelines, cursorStore, firehose, 5*time.Second)
		want := &Subscription{
			Processor:      &Processor{pipelines: pipelines},
			firehose:       firehose,
			cursorStore:    cursorStore,
			reconnectDelay: 5 * time.Second,
		}
		assert.Equal(t, want, got)
	})

	t.Run("creates subscription with defaults", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		store := mocks.NewMockPipelineStore(ctrl)
		pipelines := []Pipeline{
			{Name: "p1", Store: store},
		}

		got := New(pipelines, nil, nil, 0)
		want := &Subscription{
			Processor: &Processor{pipelines: pipelines},
		}
		assert.Equal(t, want, got)
	})
}

func TestSubscriptionSubscribe(t *testing.T) {
	t.Run("returns error when GetCursor fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		cursorStore := firehosemocks.NewMockCursorStore(ctrl)
		cursorStore.EXPECT().GetCursor(gomock.Any(), "wss://bsky.network").Return(int64(0), errors.New("cursor read failed"))
		firehose := fh.NewFirehoseConn(nil, "wss://bsky.network", "test", 4, 100)
		s := New(nil, cursorStore, firehose, 5*time.Second)
		ctx := t.Context()

		err := s.subscribe(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "getting cursor")
		assert.Contains(t, err.Error(), "cursor read failed")
	})

	t.Run("returns error when dial fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		cursorStore := firehosemocks.NewMockCursorStore(ctrl)
		cursorStore.EXPECT().GetCursor(gomock.Any(), "wss://bsky.network").Return(int64(0), nil)
		dialer := firehosemocks.NewMockDialer(ctrl)
		dialer.EXPECT().DialContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil, errors.New("connection refused"))
		firehose := fh.NewFirehoseConn(dialer, "wss://bsky.network", "test", 4, 100)
		s := New(nil, cursorStore, firehose, 5*time.Second)
		ctx := t.Context()

		err := s.subscribe(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "dialing firehose")
		assert.Contains(t, err.Error(), "connection refused")
	})

	t.Run("passes cursor to URL", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		cursorStore := firehosemocks.NewMockCursorStore(ctrl)
		cursorStore.EXPECT().GetCursor(gomock.Any(), "wss://bsky.network").Return(int64(42), nil)
		dialer := firehosemocks.NewMockDialer(ctrl)
		dialer.EXPECT().DialContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil, errors.New("expected dial error"))
		firehose := fh.NewFirehoseConn(dialer, "wss://bsky.network", "test", 4, 100)
		s := New(nil, cursorStore, firehose, 5*time.Second)
		ctx := t.Context()

		err := s.subscribe(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "dialing firehose")
	})

	t.Run("invalid service URL returns error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		cursorStore := firehosemocks.NewMockCursorStore(ctrl)
		cursorStore.EXPECT().GetCursor(gomock.Any(), "://invalid-url").Return(int64(0), nil)
		firehose := fh.NewFirehoseConn(nil, "://invalid-url", "test", 4, 100)
		s := New(nil, cursorStore, firehose, 5*time.Second)
		ctx := t.Context()

		err := s.subscribe(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parsing service URL")
	})
}

func mustCompileRegexps(keywords []string) []*regexp.Regexp {
	regexps := make([]*regexp.Regexp, len(keywords))
	for i, kw := range keywords {
		regexps[i] = regexp.MustCompile(`(?:^|[^a-zA-Z0-9-])` + regexp.QuoteMeta(strings.ToLower(kw)) + `(?:[^a-zA-Z0-9-]|$)`)
	}
	return regexps
}

func TestHandleCreate(t *testing.T) {
	t.Run("nil event is skipped", func(t *testing.T) {
		s := &Subscription{Processor: &Processor{}}
		err := s.HandleCommit(t.Context(), nil)
		require.NoError(t, err)
	})

	t.Run("too big event is skipped", func(t *testing.T) {
		s := &Subscription{Processor: &Processor{}}
		evt := &comatproto.SyncSubscribeRepos_Commit{
			Repo:   "did:plc:actor1",
			Seq:    1,
			TooBig: true,
		}
		err := s.HandleCommit(t.Context(), evt)
		require.NoError(t, err)
	})
}

func TestSubscriptionListen(t *testing.T) {
	t.Run("stops on context cancellation", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		cursorStore := firehosemocks.NewMockCursorStore(ctrl)
		cursorStore.EXPECT().GetCursor(gomock.Any(), "wss://bsky.network").Return(int64(0), errors.New("no db"))
		firehose := fh.NewFirehoseConn(nil, "wss://bsky.network", "test", 4, 100)
		s := New(nil, cursorStore, firehose, 5*time.Second)

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		done := make(chan struct{})
		go func() {
			s.Listen(ctx)
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("Listen did not return after context cancellation")
		}
	})
}
