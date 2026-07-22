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
			URL:        "nats://invalid-host:9999",
			Subject:    "firehose.events",
			QueueGroup: "baldsky-workers",
		}, 5*time.Second)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "connect to NATS")
	})
}

func TestCBORRoundTrip(t *testing.T) {
	t.Run("marshal and unmarshal preserves event", func(t *testing.T) {
		commitCid, err := cid.Decode("bafyreia2y6udp7scevz4z3zpu7je76yvr5n5rfqndww3bk5rm3v5r5tbjy")
		require.NoError(t, err)

		original := &comatproto.SyncSubscribeRepos_Commit{
			Repo:   "did:plc:test123",
			Seq:    42,
			Rev:    "3l...",
			Commit: lexutil.LexLink(commitCid),
			TooBig: false,
			Time:   "2024-01-01T00:00:00Z",
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
