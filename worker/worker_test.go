package worker

import (
	"bytes"
	"context"
	"errors"
	"testing"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessEventCalled(t *testing.T) {
	t.Run("processor receives deserialized event", func(t *testing.T) {
		processor := &mockProcessor{}

		commitCid, err := cid.Decode("bafyreia2y6udp7scevz4z3zpu7je76yvr5n5rfqndww3bk5rm3v5r5tbjy")
		require.NoError(t, err)

		evt := &comatproto.SyncSubscribeRepos_Commit{
			Repo:   "did:plc:test",
			Seq:    1,
			Time:   "2024-01-01T00:00:00Z",
			Commit: lexutil.LexLink(commitCid),
		}

		var buf bytes.Buffer
		err = evt.MarshalCBOR(&buf)
		require.NoError(t, err)

		err = processor.ProcessEvent(context.Background(), evt)
		require.NoError(t, err)
		assert.Equal(t, 1, processor.callCount)
		assert.Equal(t, int64(1), processor.lastEvt.Seq)
	})

	t.Run("processor error is captured", func(t *testing.T) {
		processor := &mockProcessor{err: errors.New("process failed")}

		evt := &comatproto.SyncSubscribeRepos_Commit{
			Repo: "did:plc:test",
			Seq:  2,
		}

		err := processor.ProcessEvent(context.Background(), evt)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "process failed")
	})
}

func TestCursorStoreInterface(t *testing.T) {
	t.Run("mock cursor store records updates", func(t *testing.T) {
		store := &mockCursorStore{}
		err := store.UpsertCursor(context.Background(), "firehose.events", 42)
		require.NoError(t, err)
		assert.Equal(t, "firehose.events", store.lastService)
		assert.Equal(t, int64(42), store.lastCursor)
	})

	t.Run("cursor store error is propagated", func(t *testing.T) {
		store := &mockCursorStore{err: errors.New("db error")}
		err := store.UpsertCursor(context.Background(), "firehose.events", 1)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "db error")
	})
}

type mockProcessor struct {
	callCount int
	lastEvt   *comatproto.SyncSubscribeRepos_Commit
	err       error
}

func (m *mockProcessor) ProcessEvent(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Commit) error {
	m.callCount++
	m.lastEvt = evt
	return m.err
}

type mockCursorStore struct {
	lastService string
	lastCursor  int64
	err         error
}

func (m *mockCursorStore) UpsertCursor(ctx context.Context, service string, cursor int64) error {
	if m.err != nil {
		return m.err
	}
	m.lastService = service
	m.lastCursor = cursor
	return nil
}
