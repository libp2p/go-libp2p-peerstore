package test

import (
	"testing"

	"github.com/libp2p/go-libp2p-peer"
	ma "github.com/multiformats/go-multiaddr"
)

func peerId(t *testing.T, ids string) peer.ID {
	t.Helper()
	id, err := peer.IDB58Decode(ids)
	if err != nil {
		t.Fatalf("id %q is bad: %s", ids, err)
	}
	return id
}

func multiaddr(t *testing.T, m string) ma.Multiaddr {
	t.Helper()
	maddr, err := ma.NewMultiaddr(m)
	if err != nil {
		t.Fatal(err)
	}
	return maddr
}