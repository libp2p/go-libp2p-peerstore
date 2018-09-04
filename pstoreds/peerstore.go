package pstoreds

import (
	"context"
	"time"

	"github.com/ipfs/go-datastore"

	pstore "github.com/libp2p/go-libp2p-peerstore"
	"github.com/libp2p/go-libp2p-peerstore/pstoremem"
)

// Configuration object for the peerstore.
type PeerstoreOpts struct {
	// The size of the in-memory cache. A value of 0 or lower disables the cache.
	CacheSize uint

	// Sweep interval to expire entries when TTL is not managed by underlying datastore.
	TTLInterval time.Duration

	// Number of times to retry transactional writes.
	WriteRetries uint
}

// DefaultOpts returns the default options for a persistent peerstore:
// * Cache size: 1024
// * TTL sweep interval: 1 second
// * WriteRetries: 5
func DefaultOpts() PeerstoreOpts {
	return PeerstoreOpts{
		CacheSize:    1024,
		TTLInterval:  time.Second,
		WriteRetries: 5,
	}
}

// NewPeerstore creates a peerstore backed by the provided persistent datastore.
func NewPeerstore(ctx context.Context, ds datastore.TxnDatastore, opts PeerstoreOpts) (pstore.Peerstore, error) {
	addrBook, err := NewAddrBook(ctx, ds, opts)
	if err != nil {
		return nil, err
	}

	ps := pstore.NewPeerstore(pstoremem.NewKeyBook(), addrBook, pstoremem.NewPeerMetadata())
	return ps, nil
}
