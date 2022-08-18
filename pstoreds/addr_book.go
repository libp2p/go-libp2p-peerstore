package pstoreds

import (
	"context"

	pstore "github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoreds"

	ds "github.com/ipfs/go-datastore"
)

// NewAddrBook initializes a new datastore-backed address book. It serves as a drop-in replacement for pstoremem
// (memory-backed peerstore), and works with any datastore implementing the ds.Batching interface.
//
// Addresses and peer records are serialized into protobuf, storing one datastore entry per peer, along with metadata
// to control address expiration. To alleviate disk access and serde overhead, we internally use a read/write-through
// ARC cache, the size of which is adjustable via Options.CacheSize.
//
// The user has a choice of two GC algorithms:
//
//   - lookahead GC: minimises the amount of full store traversals by maintaining a time-indexed list of entries that
//     need to be visited within the period specified in Options.GCLookaheadInterval. This is useful in scenarios with
//     considerable TTL variance, coupled with datastores whose native iterators return entries in lexicographical key
//     order. Enable this mode by passing a value Options.GCLookaheadInterval > 0. Lookahead windows are jumpy, not
//     sliding. Purges operate exclusively over the lookahead window with periodicity Options.GCPurgeInterval.
//
//   - full-purge GC (default): performs a full visit of the store with periodicity Options.GCPurgeInterval. Useful when
//     the range of possible TTL values is small and the values themselves are also extreme, e.g. 10 minutes or
//     permanent, popular values used in other libp2p modules. In this cited case, optimizing with lookahead windows
//     makes little sense.
//
// Deprecated: use github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoreds.NewAddrBook instead
func NewAddrBook(ctx context.Context, store ds.Batching, opts Options) (pstore.AddrBook, error) {
	return pstoreds.NewAddrBook(ctx, store, opts)
}
