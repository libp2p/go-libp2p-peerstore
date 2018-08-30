package peerstore

import (
	"testing"

	"github.com/libp2p/go-libp2p-peerstore/test"
)

func TestInMemoryPeerstore(t *testing.T) {
	test.TestPeerstore(t, func() (Peerstore, func()) {
		return NewPeerstore(), nil
	})
}

func TestInMemoryAddrMgr(t *testing.T) {
	test.TestAddrMgr(t, func() (AddrBook, func()) {
		return &AddrManager{}, nil
	})
}

func BenchmarkInMemoryPeerstore(b *testing.B) {
	test.BenchmarkPeerstore(b, func() (Peerstore, func()) {
		return NewPeerstore(), nil
	})
}