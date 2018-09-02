package pstoreds

import (
	"context"
	"time"

	"gx/ipfs/QmSpg1CvpXQQow5ernt1gNBXaXV6yxyNqi7XoeerWfzB5w/go-datastore"

	pstore "github.com/libp2p/go-libp2p-peerstore"
	"github.com/libp2p/go-libp2p-peerstore/mem"
)

// NewPeerstore creates a peerstore backed by the provided persistent datastore.
func NewPeerstore(ctx context.Context, ds datastore.Batching) (pstore.Peerstore, error) {
	addrBook, err := NewAddrBook(ctx, ds, time.Second)
	if err != nil {
		return nil, err
	}

	ps := pstore.NewPeerstoreWith(mem.NewKeyBook(), addrBook, mem.NewPeerMetadata())
	return ps, nil
}
