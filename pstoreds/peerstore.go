package pstoreds

import (
	"context"
	"time"

	ds "github.com/ipfs/go-datastore"

	pstore "github.com/libp2p/go-libp2p-peerstore"
	pstoremem "github.com/libp2p/go-libp2p-peerstore/pstoremem"
)

// Configuration object for the peerstore.
type Options struct {
	// The size of the in-memory cache. A value of 0 or lower disables the cache.
	CacheSize uint

	// Sweep interval to expire entries, only used when TTL is *not* natively managed
	// by the underlying datastore.
	TTLInterval time.Duration

	// Number of times to retry transactional writes.
	WriteRetries uint
}

// DefaultOpts returns the default options for a persistent peerstore:
// * Cache size: 1024
// * TTL sweep interval: 1 second
// * WriteRetries: 5
func DefaultOpts() Options {
	return Options{
		CacheSize:    1024,
		TTLInterval:  time.Second,
		WriteRetries: 5,
	}
}

// NewPeerstore creates a peerstore backed by the provided persistent datastore.
func NewPeerstore(ctx context.Context, store ds.TxnDatastore, opts Options) (pstore.Peerstore, error) {
	addrBook, err := NewAddrBook(ctx, store, opts)
	if err != nil {
		return nil, err
	}

	ps := pstore.NewPeerstore(pstoremem.NewKeyBook(), addrBook, pstoremem.NewPeerMetadata())
	return ps, nil
}
