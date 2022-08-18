// Deprecated: This package has moved into go-libp2p as a sub-package: github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoreds.
package pstoreds

import (
	"context"

	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoreds"

	ds "github.com/ipfs/go-datastore"
)

// Configuration object for the peerstore.
// Deprecated: use github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoreds.Options instead
type Options = pstoreds.Options

// DefaultOpts returns the default options for a persistent peerstore, with the full-purge GC algorithm:
//
// * Cache size: 1024.
// * MaxProtocols: 1024.
// * GC purge interval: 2 hours.
// * GC lookahead interval: disabled.
// * GC initial delay: 60 seconds.
// Deprecated: use github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoreds.DefaultOpts instead
func DefaultOpts() Options {
	return pstoreds.DefaultOpts()
}

// NewPeerstore creates a peerstore backed by the provided persistent datastore.
// It's the caller's responsibility to call RemovePeer to ensure
// that memory consumption of the peerstore doesn't grow unboundedly.
// Deprecated: use github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoreds.NewPeerstore instead
func NewPeerstore(ctx context.Context, store ds.Batching, opts Options) (peerstore.Peerstore, error) {
	return pstoreds.NewPeerstore(ctx, store, opts)
}
