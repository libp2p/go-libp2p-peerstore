package ds

import (
	"context"
	"time"

	"github.com/ipfs/go-datastore"
	pstore "github.com/libp2p/go-libp2p-peerstore"
)

// NewPeerstore creates a peerstore backed by the provided persistent datastore.
func NewPeerstore(ctx context.Context, ds datastore.Batching) (pstore.Peerstore, error) {
	addrBook, err := NewAddrManager(ctx, ds, time.Second)
	if err != nil {
		return nil, err
	}

	ps := pstore.NewPeerstoreWith(pstore.NewKeybook(), addrBook, pstore.NewPeerMetadata())
	return ps, nil
}
