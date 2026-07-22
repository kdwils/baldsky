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
