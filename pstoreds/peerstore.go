package pstoreds

import (
	"context"
	"time"

	base32 "github.com/whyrusleeping/base32"

	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"

	peer "github.com/libp2p/go-libp2p-peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"
)

// Configuration object for the peerstore.
type Options struct {
	// The size of the in-memory cache. A value of 0 or lower disables the cache.
	CacheSize uint

	// Sweep interval to purge expired addresses from the datastore.
	GCPurgeInterval time.Duration

	// Interval to renew the GC lookahead window.
	GCLookaheadInterval time.Duration

	// Initial delay before GC routines start. Intended to give the system time to initialise before starting GC.
	GCInitialDelay time.Duration
}

// DefaultOpts returns the default options for a persistent peerstore:
// * Cache size: 1024
// * GC prune interval: 5 minutes
// * GC lookahead interval: 12 hours
// * WriteRetries: 5
func DefaultOpts() Options {
	return Options{
		CacheSize:           1024,
		GCPurgeInterval:     5 * time.Minute,
		GCLookaheadInterval: 12 * time.Hour,
		GCInitialDelay:      60 * time.Second,
	}
}

// NewPeerstore creates a peerstore backed by the provided persistent datastore.
func NewPeerstore(ctx context.Context, store ds.TxnDatastore, opts Options) (pstore.Peerstore, error) {
	addrBook, err := NewAddrBook(ctx, store, opts)
	if err != nil {
		return nil, err
	}

	keyBook, err := NewKeyBook(ctx, store, opts)
	if err != nil {
		return nil, err
	}

	peerMetadata, err := NewPeerMetadata(ctx, store, opts)
	if err != nil {
		return nil, err
	}

	ps := pstore.NewPeerstore(keyBook, addrBook, peerMetadata)
	return ps, nil
}

// uniquePeerIds extracts and returns unique peer IDs from database keys.
func uniquePeerIds(ds ds.TxnDatastore, prefix ds.Key, extractor func(result query.Result) string) (peer.IDSlice, error) {
	var (
		q       = query.Query{Prefix: prefix.String(), KeysOnly: true}
		results query.Results
		err     error
	)

	txn, err := ds.NewTransaction(true)
	if err != nil {
		return nil, err
	}
	defer txn.Discard()

	if results, err = txn.Query(q); err != nil {
		log.Error(err)
		return nil, err
	}

	defer results.Close()

	idset := make(map[string]struct{})
	for result := range results.Next() {
		k := extractor(result)
		idset[k] = struct{}{}
	}

	if len(idset) == 0 {
		return peer.IDSlice{}, nil
	}

	ids := make(peer.IDSlice, len(idset))
	i := 0
	for id := range idset {
		pid, _ := base32.RawStdEncoding.DecodeString(id)
		ids[i], _ = peer.IDFromBytes(pid)
		i++
	}
	return ids, nil
}
