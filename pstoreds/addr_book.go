package pstoreds

import (
	"context"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p-peer"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"

	pstore "github.com/libp2p/go-libp2p-peerstore"
	"github.com/libp2p/go-libp2p-peerstore/pstoremem"
)

var (
	log = logging.Logger("peerstore/ds")
)

var _ pstore.AddrBook = (*dsAddrBook)(nil)

// dsAddrBook is an address book backed by a Datastore with both an
// in-memory TTL manager and an in-memory address stream manager.
type dsAddrBook struct {
	cache        cache
	ds           ds.TxnDatastore
	ttlManager   *ttlManager
	subsManager  *pstoremem.AddrSubManager
	writeRetries int
}

// NewAddrBook initializes a new address book given a
// Datastore instance, a context for managing the TTL manager,
// and the interval at which the TTL manager should sweep the Datastore.
func NewAddrBook(ctx context.Context, ds ds.TxnDatastore, opts PeerstoreOpts) (*dsAddrBook, error) {
	var (
		cache cache = &noopCache{}
		err   error
	)

	if opts.CacheSize > 0 {
		if cache, err = lru.NewARC(int(opts.CacheSize)); err != nil {
			return nil, err
		}
	}

	mgr := &dsAddrBook{
		cache:        cache,
		ds:           ds,
		ttlManager:   newTTLManager(ctx, ds, &cache, opts.TTLInterval),
		subsManager:  pstoremem.NewAddrSubManager(),
		writeRetries: int(opts.WriteRetries),
	}
	return mgr, nil
}

// Stop will signal the TTL manager to stop and block until it returns.
func (mgr *dsAddrBook) Stop() {
	mgr.ttlManager.cancel()
}

func keysAndAddrs(p peer.ID, addrs []ma.Multiaddr) ([]ds.Key, []ma.Multiaddr, error) {
	var (
		keys      = make([]ds.Key, len(addrs))
		clean     = make([]ma.Multiaddr, len(addrs))
		parentKey = ds.NewKey(peer.IDB58Encode(p))
		i         = 0
	)

	for _, addr := range addrs {
		if addr == nil {
			continue
		}

		hash, err := mh.Sum((addr).Bytes(), mh.MURMUR3, -1)
		if err != nil {
			return nil, nil, err
		}

		keys[i] = parentKey.ChildString(hash.B58String())
		clean[i] = addr
		i++
	}

	return keys[:i], clean[:i], nil
}

func peerIDFromKey(key ds.Key) (peer.ID, error) {
	idstring := key.Parent().Name()
	return peer.IDB58Decode(idstring)
}

// AddAddr will add a new address if it's not already in the AddrBook.
func (mgr *dsAddrBook) AddAddr(p peer.ID, addr ma.Multiaddr, ttl time.Duration) {
	mgr.AddAddrs(p, []ma.Multiaddr{addr}, ttl)
}

// AddAddrs will add many new addresses if they're not already in the AddrBook.
func (mgr *dsAddrBook) AddAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	mgr.setAddrs(p, addrs, ttl, false)
}

// SetAddr will add or update the TTL of an address in the AddrBook.
func (mgr *dsAddrBook) SetAddr(p peer.ID, addr ma.Multiaddr, ttl time.Duration) {
	addrs := []ma.Multiaddr{addr}
	mgr.SetAddrs(p, addrs, ttl)
}

// SetAddrs will add or update the TTLs of addresses in the AddrBook.
func (mgr *dsAddrBook) SetAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration) {
	if ttl <= 0 {
		mgr.deleteAddrs(p, addrs)
		return
	}
	mgr.setAddrs(p, addrs, ttl, true)
}

func (mgr *dsAddrBook) deleteAddrs(p peer.ID, addrs []ma.Multiaddr) error {
	// Keys and cleaned up addresses.
	keys, addrs, err := keysAndAddrs(p, addrs)
	if err != nil {
		return err
	}

	// Evict all keys from cache.
	for _, key := range keys {
		mgr.cache.Remove(key)
	}

	// Attempt transactional KV deletion.
	for i := 0; i < mgr.writeRetries; i++ {
		if err = mgr.dbDelete(keys); err == nil {
			break
		}
		log.Errorf("failed to delete addresses for peer %s: %s\n", p.Pretty(), err)
	}

	if err != nil {
		log.Errorf("failed to avoid write conflict for peer %s after %d retries: %v\n", p.Pretty(), mgr.writeRetries, err)
		return err
	}

	mgr.ttlManager.deleteTTLs(keys)
	return nil
}

func (mgr *dsAddrBook) setAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration, ttlReset bool) error {
	// Keys and cleaned up addresses.
	keys, addrs, err := keysAndAddrs(p, addrs)
	if err != nil {
		return err
	}

	// Evict all keys from cache before the update.
	for _, key := range keys {
		mgr.cache.Remove(key)
	}

	// Attempt transactional KV insertion.
	var existed []bool
	for i := 0; i < mgr.writeRetries; i++ {
		if existed, err = mgr.dbInsert(keys, addrs); err == nil {
			break
		}
		log.Errorf("failed to write addresses for peer %s: %s\n", p.Pretty(), err)
	}

	if err != nil {
		log.Errorf("failed to avoid write conflict for peer %s after %d retries: %v\n", p.Pretty(), mgr.writeRetries, err)
		return err
	}

	// Successful. Update cache and broadcast event.
	for i, key := range keys {
		addr := addrs[i]
		mgr.cache.Add(key, addr.Bytes())

		if !existed[i] {
			mgr.subsManager.BroadcastAddr(p, addr)
		}
	}

	// Force update TTLs only if TTL reset was requested; otherwise
	// insert the appropriate TTL entries if they don't already exist.
	if ttlReset {
		mgr.ttlManager.setTTLs(keys, ttl)
	} else {
		mgr.ttlManager.insertTTLs(keys, ttl)
	}

	return nil
}

// dbDelete performs a transactional delete of the provided keys.
func (mgr *dsAddrBook) dbDelete(keys []ds.Key) error {
	var err error

	txn := mgr.ds.NewTransaction(false)
	defer txn.Discard()

	// Attempt to delete all keys.
	for _, key := range keys {
		if err = txn.Delete(key); err != nil {
			log.Errorf("transaction failed and aborted while deleting key: %s, cause: %v", key.String(), err)
			return err
		}
	}

	if err = txn.Commit(); err != nil {
		log.Errorf("failed to commit transaction when deleting keys, cause: %v", err)
		return err
	}
	return nil
}

// dbInsert performs a transactional insert of the provided keys and values.
func (mgr *dsAddrBook) dbInsert(keys []ds.Key, addrs []ma.Multiaddr) ([]bool, error) {
	var (
		err     error
		existed = make([]bool, len(keys))
	)

	txn := mgr.ds.NewTransaction(false)
	defer txn.Discard()

	for i, key := range keys {
		// Check if the key existed previously.
		if existed[i], err = txn.Has(key); err != nil {
			log.Errorf("transaction failed and aborted while checking key existence: %s, cause: %v", key.String(), err)
			return nil, err
		}

		// The key embeds a hash of the value, so if it existed, we can safely skip the insert.
		if existed[i] {
			continue
		}

		// Attempt to add the key.
		if err = txn.Put(key, addrs[i].Bytes()); err != nil {
			log.Errorf("transaction failed and aborted while setting key: %s, cause: %v", key.String(), err)
			return nil, err
		}
	}

	if err = txn.Commit(); err != nil {
		log.Errorf("failed to commit transaction when setting keys, cause: %v", err)
		return nil, err
	}

	return existed, nil
}

// UpdateAddrs will update any addresses for a given peer and TTL combination to
// have a new TTL.
func (mgr *dsAddrBook) UpdateAddrs(p peer.ID, oldTTL time.Duration, newTTL time.Duration) {
	prefix := ds.NewKey(p.Pretty())
	mgr.ttlManager.adjustTTLs(prefix, oldTTL, newTTL)
}

// Addrs Returns all of the non-expired addresses for a given peer.
func (mgr *dsAddrBook) Addrs(p peer.ID) []ma.Multiaddr {
	prefix := ds.NewKey(p.Pretty())
	q := query.Query{Prefix: prefix.String(), KeysOnly: true}
	results, err := mgr.ds.Query(q)

	if err != nil {
		log.Error(err)
		return nil
	}

	var addrs []ma.Multiaddr
	for result := range results.Next() {
		key := ds.RawKey(result.Key)
		var addri interface{}
		addri, ok := mgr.cache.Get(key)
		if !ok {
			addri, err = mgr.ds.Get(key)
			if err != nil {
				log.Error(err)
				continue
			}
		}
		addrbytes := addri.([]byte)
		addr, err := ma.NewMultiaddrBytes(addrbytes)
		if err != nil {
			log.Error(err)
			continue
		}
		addrs = append(addrs, addr)
	}

	return addrs
}

// Peers returns all of the peer IDs for which the AddrBook has addresses.
func (mgr *dsAddrBook) PeersWithAddrs() peer.IDSlice {
	q := query.Query{KeysOnly: true}
	results, err := mgr.ds.Query(q)

	if err != nil {
		log.Error(err)
		return peer.IDSlice{}
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

	ids := make(peer.IDSlice, 0, len(idset))
	for id := range idset {
		ids = append(ids, id)
	}
	return ids
}

// AddrStream returns a channel on which all new addresses discovered for a
// given peer ID will be published.
func (mgr *dsAddrBook) AddrStream(ctx context.Context, p peer.ID) <-chan ma.Multiaddr {
	initial := mgr.Addrs(p)
	return mgr.subsManager.AddrStream(ctx, p, initial)
}

// ClearAddrs will delete all known addresses for a peer ID.
func (mgr *dsAddrBook) ClearAddrs(p peer.ID) {
	var (
		err    error
		keys   []ds.Key
		prefix = ds.NewKey(p.Pretty())
	)

	// Attempt transactional KV deletion.
	for i := 0; i < mgr.writeRetries; i++ {
		if keys, err = mgr.dbClear(prefix); err == nil {
			break
		}
		log.Errorf("failed to clear addresses for peer %s: %s\n", p.Pretty(), err)
	}

	if err != nil {
		log.Errorf("failed to clear addresses for peer %s after %d attempts\n", p.Pretty(), mgr.writeRetries)
		// TODO: return error
	}

	// Perform housekeeping.
	mgr.ttlManager.clear(prefix)
	for _, key := range keys {
		mgr.cache.Remove(key)
	}
}

func (mgr *dsAddrBook) dbClear(prefix ds.Key) ([]ds.Key, error) {
	q := query.Query{Prefix: prefix.String(), KeysOnly: true}

	txn := mgr.ds.NewTransaction(false)
	defer txn.Discard()

	results, err := txn.Query(q)
	if err != nil {
		log.Errorf("failed to fetch all keys prefixed with: %s, cause: %v", prefix.String(), err)
		return nil, err
	}

	var keys []ds.Key
	for result := range results.Next() {
		key := ds.RawKey(result.Key)
		keys = append(keys, key)

		if err = txn.Delete(key); err != nil {
			log.Errorf("failed to delete key: %s, cause: %v", key.String(), err)
			return nil, err
		}
	}

	if err := results.Close(); err != nil {
		log.Errorf("failed to close cursor: %s, cause: %v", err)
		return nil, err
	}

	if err = txn.Commit(); err != nil {
		log.Errorf("failed to commit transaction when deleting keys, cause: %v", err)
		return nil, err
	}

	return keys, nil
}

func (mgr *dsAddrBook) doInTransaction(readOnly bool, op func(txn ds.Txn) error) error {
	txn := mgr.ds.NewTransaction(false)
	defer txn.Discard()

	if err := op(txn); err != nil {
		return err
	}

	if err := txn.Commit(); err != nil {
		return err
	}

	return nil
}

type ttlEntry struct {
	TTL       time.Duration
	ExpiresAt time.Time
}

type ttlManager struct {
	sync.RWMutex
	entries map[ds.Key]*ttlEntry

	ctx    context.Context
	cancel context.CancelFunc
	ticker *time.Ticker
	ds     ds.TxnDatastore
	cache  cache
}

func newTTLManager(parent context.Context, d ds.Datastore, c *cache, tick time.Duration) *ttlManager {
	ctx, cancel := context.WithCancel(parent)
	txnDs, ok := d.(ds.TxnDatastore)
	if !ok {
		panic("must construct ttlManager with transactional datastore")
	}
	mgr := &ttlManager{
		entries: make(map[ds.Key]*ttlEntry),
		ctx:     ctx,
		cancel:  cancel,
		ticker:  time.NewTicker(tick),
		ds:      txnDs,
		cache:   *c,
	}

	go func() {
		for {
			select {
			case <-mgr.ctx.Done():
				mgr.ticker.Stop()
				return
			case <-mgr.ticker.C:
				mgr.tick()
			}
		}
	}()

	return mgr
}

// To be called by TTL manager's coroutine only.
func (mgr *ttlManager) tick() {
	mgr.Lock()
	defer mgr.Unlock()

	txn := mgr.ds.NewTransaction(false)
	defer txn.Discard()

	now := time.Now()
	for key, entry := range mgr.entries {
		if entry.ExpiresAt.After(now) {
			continue
		}
		if err := txn.Delete(key); err != nil {
			log.Error("failed to delete TTL key: %v, cause: %v", key.String(), err)
			break
		}
		mgr.cache.Remove(key)
		delete(mgr.entries, key)
	}

	if err := txn.Commit(); err != nil {
		log.Error("failed to commit TTL deletion, cause: %v", err)
	}
}

func (mgr *ttlManager) deleteTTLs(keys []ds.Key) {
	mgr.Lock()
	defer mgr.Unlock()

	for _, key := range keys {
		delete(mgr.entries, key)
	}
}

func (mgr *ttlManager) insertTTLs(keys []ds.Key, ttl time.Duration) {
	mgr.Lock()
	defer mgr.Unlock()

	expiration := time.Now().Add(ttl)
	for _, key := range keys {
		if entry, ok := mgr.entries[key]; !ok || (ok && entry.ExpiresAt.Before(expiration)) {
			mgr.entries[key] = &ttlEntry{TTL: ttl, ExpiresAt: expiration}
		}
	}
}

func (mgr *ttlManager) setTTLs(keys []ds.Key, ttl time.Duration) {
	mgr.Lock()
	defer mgr.Unlock()

	expiration := time.Now().Add(ttl)
	for _, key := range keys {
		mgr.entries[key] = &ttlEntry{TTL: ttl, ExpiresAt: expiration}
	}
}

func (mgr *ttlManager) adjustTTLs(prefix ds.Key, oldTTL, newTTL time.Duration) {
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

func (mgr *ttlManager) clear(prefix ds.Key) {
	mgr.Lock()
	defer mgr.Unlock()

	for key := range mgr.entries {
		if key.IsDescendantOf(prefix) {
			delete(mgr.entries, key)
		}
	}
}
