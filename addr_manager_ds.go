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

type DatastoreAddrManager struct {
	ds         ds.Datastore
	ttlManager *ttlmanager
	addrSubs   map[peer.ID][]*addrSub
}

func NewDatastoreAddrManager(ctx context.Context, ds ds.Datastore, ttlInterval time.Duration) *DatastoreAddrManager {
	mgr := &DatastoreAddrManager{
		ds:         ds,
		ttlManager: newTTLManager(ctx, ds, ttlInterval),
		addrSubs:   make(map[peer.ID][]*addrSub),
	}
	return mgr
}

func (mgr *DatastoreAddrManager) Stop() {
	mgr.ttlManager.stop()
}

func peerAddressKey(p *peer.ID, addr *ma.Multiaddr) (ds.Key, error) {
	hash, err := mh.Sum((*addr).Bytes(), mh.MURMUR3, -1)
	if err != nil {
		return ds.Key{}, nil
	}
	return ds.NewKey(p.Pretty()).ChildString(hash.B58String()), nil
}

func (mgr *DatastoreAddrManager) AddAddr(p peer.ID, addr ma.Multiaddr, ttl time.Duration) {
	mgr.AddAddrs(p, []ma.Multiaddr{addr}, ttl)
}

func (mgr *DatastoreAddrManager) AddAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration) {
	if ttl <= 0 {
		return
	}

	mgr.SetAddrs(p, addrs, ttl)
}

func (mgr *DatastoreAddrManager) SetAddr(p peer.ID, addr ma.Multiaddr, ttl time.Duration) {
	mgr.SetAddrs(p, []ma.Multiaddr{addr}, ttl)
}

func (mgr *DatastoreAddrManager) SetAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration) {
	var keys []ds.Key
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
			mgr.ds.Delete(key)
		} else {
			if err := mgr.ds.Put(key, addr.Bytes()); err != nil {
				log.Error(err)
			}
		}
	}
	mgr.ttlManager.setTTLs(keys, ttl)
}

func (mgr *DatastoreAddrManager) UpdateAddrs(p peer.ID, oldTTL time.Duration, newTTL time.Duration) {
	prefix := ds.NewKey(p.Pretty())
	mgr.ttlManager.updateTTLs(prefix, oldTTL, newTTL)
}

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

func (mgr *DatastoreAddrManager) AddrStream(context.Context, peer.ID) <-chan ma.Multiaddr {
	panic("implement me")
	stream := make(chan ma.Multiaddr)
	return stream
}

func (mgr *DatastoreAddrManager) ClearAddrs(p peer.ID) {
	prefix := ds.NewKey(p.Pretty())
	q := query.Query{Prefix: prefix.String()}
	results, err := mgr.ds.Query(q)
	if err != nil {
		log.Error(err)
		return
	}

	for result := range results.Next() {
		mgr.ds.Delete(ds.NewKey(result.Key))
	}
	mgr.ttlManager.clear(ds.NewKey(p.Pretty()))
}

// ttlmanager

type ttlentry struct {
	TTL       time.Duration
	ExpiresAt time.Time
}

type ttlmanager struct {
	sync.Mutex
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
		Mutex:   sync.Mutex{},
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

// For internal use only
func (mgr *ttlmanager) tick() {
	mgr.Lock()
	defer mgr.Unlock()

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
