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

func TestInMemoryAddrBook(t *testing.T) {
	test.TestAddrBook(t, func() (pstore.AddrBook, func()) {
		return NewAddrBook(), nil
	})
}

func TestInMemoryKeyBook(t *testing.T) {
	test.TestKeyBook(t, func() (pstore.KeyBook, func()) {
		return NewKeyBook(), nil
	})
}

func BenchmarkInMemoryPeerstore(b *testing.B) {
	test.BenchmarkPeerstore(b, func() (pstore.Peerstore, func()) {
		return NewPeerstore(), nil
	})
}
