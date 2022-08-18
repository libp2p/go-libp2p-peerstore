package pstoreds

import (
	"context"

	ds "github.com/ipfs/go-datastore"
	pstore "github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoreds"
)

// Deprecated: use github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoreds.NewKeyBook instead
func NewKeyBook(ctx context.Context, store ds.Datastore, opts Options) (pstore.KeyBook, error) {
	return pstoreds.NewKeyBook(ctx, store, opts)
}
