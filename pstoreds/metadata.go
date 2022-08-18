package pstoreds

import (
	"context"

	pstore "github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoreds"

	ds "github.com/ipfs/go-datastore"
)

// NewPeerMetadata creates a metadata store backed by a persistent db. It uses gob for serialisation.
//
// See `init()` to learn which types are registered by default. Modules wishing to store
// values of other types will need to `gob.Register()` them explicitly, or else callers
// will receive runtime errors.
// Deprecated: use github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoreds.NewPeerMetadata instead
func NewPeerMetadata(ctx context.Context, store ds.Datastore, opts Options) (pstore.PeerMetadata, error) {
	return pstoreds.NewPeerMetadata(ctx, store, opts)
}
