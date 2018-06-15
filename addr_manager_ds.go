package peerstore

import (
	"context"
	"sync"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/libp2p/go-libp2p-peer"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
)

// DatastoreAddrManager is an address manager backed by a Datastore with both an
// in-memory TTL manager and an in-memory address stream manager.
type DatastoreAddrManager struct {
	ds          ds.Batching
	ttlManager  *ttlmanager
	subsManager *AddrSubManager
}

// NewDatastoreAddrManager initializes a new DatastoreAddrManager given a
// Datastore instance, a context for managing the TTL manager, and the interval
// at which the TTL manager should sweep the Datastore.
func NewDatastoreAddrManager(ctx context.Context, ds ds.Batching, ttlInterval time.Duration) *DatastoreAddrManager {
	mgr := &DatastoreAddrManager{
		ds:          ds,
		ttlManager:  newTTLManager(ctx, ds, ttlInterval),
		subsManager: NewAddrSubManager(),
	}
	return mgr
}

// Stop will signal the TTL manager to stop and block until it returns.
func (mgr *DatastoreAddrManager) Stop() {
	mgr.ttlManager.stop()
}

func peerAddressKey(p *peer.ID, addr *ma.Multiaddr) (ds.Key, error) {
	hash, err := mh.Sum((*addr).Bytes(), mh.MURMUR3, -1)
	if err != nil {
		return ds.Key{}, nil
	}
	return ds.NewKey(peer.IDB58Encode(*p)).ChildString(hash.B58String()), nil
}

func peerIDFromKey(key ds.Key) (peer.ID, error) {
	idstring := key.Parent().Name()
	return peer.IDB58Decode(idstring)
}

// AddAddr will add a new address if it's not already in the AddrBook.
func (mgr *DatastoreAddrManager) AddAddr(p peer.ID, addr ma.Multiaddr, ttl time.Duration) {
	mgr.AddAddrs(p, []ma.Multiaddr{addr}, ttl)
}

// AddAddrs will add many new addresses if they're not already in the AddrBook.
func (mgr *DatastoreAddrManager) AddAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration) {
	if ttl <= 0 {
		return
	}

	mgr.setAddrs(p, addrs, ttl, true)
}

// SetAddr will add or update the TTL of an address in the AddrBook.
func (mgr *DatastoreAddrManager) SetAddr(p peer.ID, addr ma.Multiaddr, ttl time.Duration) {
	mgr.SetAddrs(p, []ma.Multiaddr{addr}, ttl)
}

// SetAddrs will add or update the TTLs of addresses in the AddrBook.
func (mgr *DatastoreAddrManager) SetAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration) {
	mgr.setAddrs(p, addrs, ttl, false)
}

func (mgr *DatastoreAddrManager) setAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration, add bool) {
	var keys []ds.Key
	batch, err := mgr.ds.Batch()
	if err != nil {
		log.Error(err)
		return
	}
	defer batch.Commit()
	for _, addr := range addrs {
		if addr == nil {
			continue
		}

		key, err := peerAddressKey(&p, &addr)
		if err != nil {
			log.Error(err)
			continue
		}
		keys = append(keys, key)

		if ttl <= 0 {
			batch.Delete(key)
			continue
		}
		has, err := mgr.ds.Has(key)
		if err != nil || !has {
			mgr.subsManager.BroadcastAddr(p, addr)
		}

		if !has || !add {
			if err := batch.Put(key, addr.Bytes()); err != nil {
				log.Error(err)
			}
		}
	}
	mgr.ttlManager.setTTLs(keys, ttl)
}

// UpdateAddrs will update any addresses for a given peer and TTL combination to
// have a new TTL.
func (mgr *DatastoreAddrManager) UpdateAddrs(p peer.ID, oldTTL time.Duration, newTTL time.Duration) {
	prefix := ds.NewKey(p.Pretty())
	mgr.ttlManager.updateTTLs(prefix, oldTTL, newTTL)
}

// Addrs Returns all of the non-expired addresses for a given peer.
func (mgr *DatastoreAddrManager) Addrs(p peer.ID) []ma.Multiaddr {
	prefix := ds.NewKey(p.Pretty())
	q := query.Query{Prefix: prefix.String()}
	results, err := mgr.ds.Query(q)
	if err != nil {
		log.Error(err)
		return []ma.Multiaddr{}
	}

	var addrs []ma.Multiaddr
	for result := range results.Next() {
		addrbytes := result.Value.([]byte)
		addr, err := ma.NewMultiaddrBytes(addrbytes)
		if err != nil {
			continue
		}
		addrs = append(addrs, addr)
	}

	return addrs
}

// Peers returns all of the peer IDs for which the AddrBook has addresses.
func (mgr *DatastoreAddrManager) Peers() []peer.ID {
	q := query.Query{}
	results, err := mgr.ds.Query(q)
	if err != nil {
		log.Error(err)
		return []peer.ID{}
	}

	idset := make(map[peer.ID]struct{})
	for result := range results.Next() {
		key := ds.RawKey(result.Key)
		id, err := peerIDFromKey(key)
		if err != nil {
			continue
		}
		idset[id] = struct{}{}
	}

	ids := make([]peer.ID, 0, len(idset))
	for id := range idset {
		ids = append(ids, id)
	}
	return ids
}

// AddrStream returns a channel on which all new addresses discovered for a
// given peer ID will be published.
func (mgr *DatastoreAddrManager) AddrStream(ctx context.Context, p peer.ID) <-chan ma.Multiaddr {
	initial := mgr.Addrs(p)
	return mgr.subsManager.AddrStream(ctx, p, initial)
}

// ClearAddrs will delete all known addresses for a peer ID.
func (mgr *DatastoreAddrManager) ClearAddrs(p peer.ID) {
	prefix := ds.NewKey(p.Pretty())
	q := query.Query{Prefix: prefix.String()}
	results, err := mgr.ds.Query(q)
	if err != nil {
		log.Error(err)
		return
	}
	batch, err := mgr.ds.Batch()
	if err != nil {
		log.Error(err)
		return
	}
	defer batch.Commit()

	for result := range results.Next() {
		batch.Delete(ds.NewKey(result.Key))
	}
	mgr.ttlManager.clear(ds.NewKey(p.Pretty()))
}

// ttlmanager

type ttlentry struct {
	TTL       time.Duration
	ExpiresAt time.Time
}

type ttlmanager struct {
	sync.RWMutex
	entries map[ds.Key]*ttlentry
	ctx     context.Context
	cancel  context.CancelFunc
	ticker  *time.Ticker
	done    chan struct{}
	ds      ds.Datastore
}

func newTTLManager(parent context.Context, d ds.Datastore, tick time.Duration) *ttlmanager {
	ctx, cancel := context.WithCancel(parent)
	mgr := &ttlmanager{
		entries: make(map[ds.Key]*ttlentry),
		ctx:     ctx,
		cancel:  cancel,
		ticker:  time.NewTicker(tick),
		ds:      d,
		done:    make(chan struct{}),
	}

	go func() {
		for {
			select {
			case <-mgr.ctx.Done():
				mgr.ticker.Stop()
				mgr.done <- struct{}{}
				return
			case <-mgr.ticker.C:
				mgr.tick()
			}
		}
	}()

	return mgr
}

func (mgr *ttlmanager) stop() {
	mgr.cancel()
	<-mgr.done
}

// To be called by TTL manager's coroutine only.
func (mgr *ttlmanager) tick() {
	mgr.RLock()
	defer mgr.RUnlock()

	now := time.Now()
	for key, entry := range mgr.entries {
		if entry.ExpiresAt.Before(now) {
			if err := mgr.ds.Delete(key); err != nil {
				log.Error(err)
			}
			delete(mgr.entries, key)
		}
	}
}

func (mgr *ttlmanager) setTTLs(keys []ds.Key, ttl time.Duration) {
	mgr.Lock()
	defer mgr.Unlock()

	expiration := time.Now().Add(ttl)
	for _, key := range keys {
		if ttl <= 0 {
			delete(mgr.entries, key)
		} else {
			mgr.entries[key] = &ttlentry{TTL: ttl, ExpiresAt: expiration}
		}
	}
}

func (mgr *ttlmanager) updateTTLs(prefix ds.Key, oldTTL, newTTL time.Duration) {
	mgr.Lock()
	defer mgr.Unlock()

	now := time.Now()
	var keys []ds.Key
	for key, entry := range mgr.entries {
		if key.IsDescendantOf(prefix) && entry.TTL == oldTTL {
			keys = append(keys, key)
			entry.TTL = newTTL
			entry.ExpiresAt = now.Add(newTTL)
		}
	}
}

func (mgr *ttlmanager) clear(prefix ds.Key) {
	mgr.Lock()
	defer mgr.Unlock()

	for key := range mgr.entries {
		if key.IsDescendantOf(prefix) {
			delete(mgr.entries, key)
		}
	}
}
