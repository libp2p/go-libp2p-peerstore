package pstoremem

import (
	"testing"

	"github.com/stretchr/testify/require"

	pstore "github.com/libp2p/go-libp2p-core/peerstore"
	pt "github.com/libp2p/go-libp2p-peerstore/test"

	"go.uber.org/goleak"
)

func TestFuzzInMemoryPeerstore(t *testing.T) {
	// Just create and close a bunch of peerstores. If this leaks, we'll
	// catch it in the leak check below.
	for i := 0; i < 100; i++ {
		ps, err := NewPeerstore()
		require.NoError(t, err)
		ps.Close()
	}
}

func TestInMemoryPeerstore(t *testing.T) {
	pt.TestPeerstore(t, func() (pstore.Peerstore, func()) {
		ps, err := NewPeerstore()
		require.NoError(t, err)
		return ps, func() { ps.Close() }
	})
}

func TestPeerstoreProtoStoreLimits(t *testing.T) {
	const limit = 10
	ps, err := NewPeerstore(WithMaxProtocols(limit))
	require.NoError(t, err)
	defer ps.Close()
	pt.TestPeerstoreProtoStoreLimits(t, ps, limit)
}

func TestInMemoryAddrBook(t *testing.T) {
	pt.TestAddrBook(t, func() (pstore.AddrBook, func()) {
		ps, err := NewPeerstore()
		require.NoError(t, err)
		return ps, func() { ps.Close() }
	})
}

func TestInMemoryKeyBook(t *testing.T) {
	pt.TestKeyBook(t, func() (pstore.KeyBook, func()) {
		ps, err := NewPeerstore()
		require.NoError(t, err)
		return ps, func() { ps.Close() }
	})
}

func BenchmarkInMemoryPeerstore(b *testing.B) {
	pt.BenchmarkPeerstore(b, func() (pstore.Peerstore, func()) {
		ps, err := NewPeerstore()
		require.NoError(b, err)
		return ps, func() { ps.Close() }
	}, "InMem")
}

func BenchmarkInMemoryKeyBook(b *testing.B) {
	pt.BenchmarkKeyBook(b, func() (pstore.KeyBook, func()) {
		ps, err := NewPeerstore()
		require.NoError(b, err)
		return ps, func() { ps.Close() }
	})
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(
		m,
		goleak.IgnoreTopFunction("github.com/ipfs/go-log/v2/writer.(*MirrorWriter).logRoutine"),
	)
}
