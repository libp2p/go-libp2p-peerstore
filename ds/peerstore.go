package ds

import (
	"context"
	"time"

	"github.com/ipfs/go-datastore"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	"github.com/libp2p/go-libp2p-peerstore/mem"
)

// NewPeerstore creates a peerstore backed by the provided persistent datastore.
func NewPeerstore(ctx context.Context, ds datastore.Batching) (pstore.Peerstore, error) {
	addrBook, err := NewAddrManager(ctx, ds, time.Second)
	if err != nil {
		return nil, err
	}

	ps := pstore.NewPeerstoreWith(mem.NewKeybook(), addrBook, mem.NewPeerMetadata())
	return ps, nil
}
