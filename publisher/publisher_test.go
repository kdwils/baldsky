package publisher

import (
	"bytes"
	"testing"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/ipfs/go-cid"
	"github.com/kdwils/baldsky/config"
	fh "github.com/kdwils/baldsky/firehose"
	"github.com/kdwils/baldsky/firehose/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestNew(t *testing.T) {
	t.Run("returns error when NATS connection fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		cursorStore := mocks.NewMockCursorStore(ctrl)
		firehoseConn := fh.NewFirehoseConn(nil, "wss://bsky.network", "test/1.0", 4, 100)

		_, err := New(cursorStore, firehoseConn, config.NATSConfig{
			URL:           "nats://invalid-host:9999",
			Subject:       "firehose.events",
			QueueGroup:    "baldsky-workers",
			ReconnectWait: 2 * time.Second,
		}, 5*time.Second, 5*time.Second, "baldsky-publisher")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "connect to NATS")
	})
}

func TestCBORRoundTrip(t *testing.T) {
	t.Run("marshal and unmarshal preserves event", func(t *testing.T) {
		commitCid, err := cid.Decode("bafyreia2y6udp7scevz4z3zpu7je76yvr5n5rfqndww3bk5rm3v5r5tbjy")
		require.NoError(t, err)

		opCid, err := cid.Decode("bafyreia2y6udp7scevz4z3zpu7je76yvr5n5rfqndww3bk5rm3v5r5tbjy")
		require.NoError(t, err)
		opCidLink := lexutil.LexLink(opCid)

		original := &comatproto.SyncSubscribeRepos_Commit{
			Repo:   "did:plc:test123",
			Seq:    42,
			Rev:    "3l...",
			Commit: lexutil.LexLink(commitCid),
			TooBig: false,
			Time:   "2024-01-01T00:00:00Z",
			Blocks: lexutil.LexBytes([]byte{0x00, 0x01, 0x02, 0x03}),
			Ops: []*comatproto.SyncSubscribeRepos_RepoOp{
				{
					Action: "create",
					Cid:    &opCidLink,
					Path:   "app.bsky.feed.post/3jqkl2abc7k2a",
				},
				{
					Action: "delete",
					Path:   "app.bsky.feed.post/3jqkl2abc7k2b",
				},
			},
		}

		var buf bytes.Buffer
		err = original.MarshalCBOR(&buf)
		require.NoError(t, err)
		assert.NotEmpty(t, buf.Bytes())

		var decoded comatproto.SyncSubscribeRepos_Commit
		err = decoded.UnmarshalCBOR(bytes.NewReader(buf.Bytes()))
		require.NoError(t, err)

		assert.Equal(t, original.Repo, decoded.Repo)
		assert.Equal(t, original.Seq, decoded.Seq)
		assert.Equal(t, original.Rev, decoded.Rev)
		assert.Equal(t, original.Time, decoded.Time)
		assert.Equal(t, original.TooBig, decoded.TooBig)
		assert.Equal(t, original.Blocks, decoded.Blocks)
		require.Len(t, decoded.Ops, 2)
		assert.Equal(t, "create", decoded.Ops[0].Action)
		assert.Equal(t, "app.bsky.feed.post/3jqkl2abc7k2a", decoded.Ops[0].Path)
		assert.Equal(t, *original.Ops[0].Cid, *decoded.Ops[0].Cid)
		assert.Equal(t, "delete", decoded.Ops[1].Action)
		assert.Equal(t, "app.bsky.feed.post/3jqkl2abc7k2b", decoded.Ops[1].Path)
		assert.Nil(t, decoded.Ops[1].Cid)
	})

	t.Run("marshal empty event produces valid CBOR", func(t *testing.T) {
		commitCid, err := cid.Decode("bafyreia2y6udp7scevz4z3zpu7je76yvr5n5rfqndww3bk5rm3v5r5tbjy")
		require.NoError(t, err)

		evt := &comatproto.SyncSubscribeRepos_Commit{
			Repo:   "did:plc:empty",
			Seq:    1,
			Commit: lexutil.LexLink(commitCid),
		}

		var buf bytes.Buffer
		err = evt.MarshalCBOR(&buf)
		require.NoError(t, err)
		assert.NotEmpty(t, buf.Bytes())

		var decoded comatproto.SyncSubscribeRepos_Commit
		err = decoded.UnmarshalCBOR(bytes.NewReader(buf.Bytes()))
		require.NoError(t, err)
		assert.Equal(t, evt.Repo, decoded.Repo)
		assert.Equal(t, evt.Seq, decoded.Seq)
	})
}
