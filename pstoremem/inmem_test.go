package pstoremem

import (
	"testing"

	pstore "github.com/libp2p/go-libp2p-peerstore"
	"github.com/libp2p/go-libp2p-peerstore/test"
)

func TestInMemoryPeerstore(t *testing.T) {
	test.TestPeerstore(t, func() (pstore.Peerstore, func()) {
		return NewPeerstore(), nil
	})
}

func TestInMemoryAddrMgr(t *testing.T) {
	test.TestAddrMgr(t, func() (pstore.AddrBook, func()) {
		return NewAddrBook(), nil
	})
}

func BenchmarkInMemoryPeerstore(b *testing.B) {
	test.BenchmarkPeerstore(b, func() (pstore.Peerstore, func()) {
		return NewPeerstore(), nil
	})
}
