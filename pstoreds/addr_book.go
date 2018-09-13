package pstoreds

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
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
	// The maximum representable value in time.Time is time.Unix(1<<63-62135596801, 999999999).
	// But it's too brittle and implementation-dependent, so we prefer to use 1<<62, which is in the
	// year 146138514283. We're safe.
	maxTime = time.Unix(1<<62, 0)

	ErrTTLDatastore = errors.New("datastore must provide TTL support")
)

var _ pstore.AddrBook = (*dsAddrBook)(nil)

// dsAddrBook is an address book backed by a Datastore with both an
// in-memory TTL manager and an in-memory address stream manager.
type dsAddrBook struct {
	cache        cache
	ds           ds.TxnDatastore
	subsManager  *pstoremem.AddrSubManager
	writeRetries int
}

type ttlWriteMode int

const (
	ttlOverride ttlWriteMode = iota
	ttlExtend
)

type cacheEntry struct {
	expiration time.Time
	addrs      []ma.Multiaddr
}

// NewAddrBook initializes a new address book given a
// Datastore instance, a context for managing the TTL manager,
// and the interval at which the TTL manager should sweep the Datastore.
func NewAddrBook(ctx context.Context, store ds.TxnDatastore, opts PeerstoreOpts) (*dsAddrBook, error) {
	if _, ok := store.(ds.TTLDatastore); !ok {
		return nil, ErrTTLDatastore
	}

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
		ds:           store,
		subsManager:  pstoremem.NewAddrSubManager(),
		writeRetries: int(opts.WriteRetries),
	}
	return mgr, nil
}

// Stop will signal the TTL manager to stop and block until it returns.
func (mgr *dsAddrBook) Stop() {
	// noop
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

// AddAddr will add a new address if it's not already in the AddrBook.
func (mgr *dsAddrBook) AddAddr(p peer.ID, addr ma.Multiaddr, ttl time.Duration) {
	mgr.AddAddrs(p, []ma.Multiaddr{addr}, ttl)
}

// AddAddrs will add many new addresses if they're not already in the AddrBook.
func (mgr *dsAddrBook) AddAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	mgr.setAddrs(p, addrs, ttl, ttlExtend)
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
	mgr.setAddrs(p, addrs, ttl, ttlOverride)
}

func (mgr *dsAddrBook) deleteAddrs(p peer.ID, addrs []ma.Multiaddr) error {
	// Keys and cleaned up addresses.
	keys, addrs, err := keysAndAddrs(p, addrs)
	if err != nil {
		return err
	}

	mgr.cache.Remove(p.Pretty())
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

	return nil
}

func (mgr *dsAddrBook) setAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration, mode ttlWriteMode) error {
	// Keys and cleaned up addresses.
	keys, addrs, err := keysAndAddrs(p, addrs)
	if err != nil {
		return err
	}

	mgr.cache.Remove(p.Pretty())

	// Attempt transactional KV insertion.
	var existed []bool
	for i := 0; i < mgr.writeRetries; i++ {
		if existed, err = mgr.dbInsert(keys, addrs, ttl, mode); err == nil {
			break
		}
		log.Errorf("failed to write addresses for peer %s: %s\n", p.Pretty(), err)
	}

	if err != nil {
		log.Errorf("failed to avoid write conflict for peer %s after %d retries: %v\n", p.Pretty(), mgr.writeRetries, err)
		return err
	}

	// Successful. Update cache and broadcast event.
	for i, _ := range keys {
		if !existed[i] {
			mgr.subsManager.BroadcastAddr(p, addrs[i])
		}
	}

	return nil
}

// dbInsert performs a transactional insert of the provided keys and values.
func (mgr *dsAddrBook) dbInsert(keys []ds.Key, addrs []ma.Multiaddr, ttl time.Duration, mode ttlWriteMode) ([]bool, error) {
	var (
		err     error
		existed = make([]bool, len(keys))
		exp     = time.Now().Add(ttl)
		ttlB    = make([]byte, 8)
	)

	binary.LittleEndian.PutUint64(ttlB, uint64(ttl))

	txn := mgr.ds.NewTransaction(false)
	defer txn.Discard()

	ttltxn := txn.(ds.TTLDatastore)
	for i, key := range keys {
		// Check if the key existed previously.
		if existed[i], err = ttltxn.Has(key); err != nil {
			log.Errorf("transaction failed and aborted while checking key existence: %s, cause: %v", key.String(), err)
			return nil, err
		}

		// The key embeds a hash of the value, so if it existed, we can safely skip the insert and
		// just update the TTL.
		if existed[i] {
			switch mode {
			case ttlOverride:
				err = ttltxn.SetTTL(key, ttl)
			case ttlExtend:
				var curr time.Time
				if curr, err = ttltxn.GetExpiration(key); err != nil && exp.After(curr) {
					err = ttltxn.SetTTL(key, ttl)
				}
			}
			if err != nil {
				// mode will be printed as an int
				log.Errorf("failed while updating the ttl for key: %s, mode: %v, cause: %v", key.String(), mode, err)
				return nil, err
			}
			continue
		}

		// format: bytes(ttl) || bytes(multiaddr)
		value := append(ttlB, addrs[i].Bytes()...)
		if err = ttltxn.PutWithTTL(key, value, ttl); err != nil {
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
	mgr.cache.Remove(p.Pretty())

	var err error
	for i := 0; i < mgr.writeRetries; i++ {
		if err = mgr.dbUpdateTTL(p, oldTTL, newTTL); err == nil {
			break
		}
		log.Errorf("failed to update ttlsfor peer %s: %s\n", p.Pretty(), err)
	}

	if err != nil {
		log.Errorf("failed to avoid write conflict when updating ttls for peer %s after %d retries: %v\n",
			p.Pretty(), mgr.writeRetries, err)
	}
}

func (mgr *dsAddrBook) dbUpdateTTL(p peer.ID, oldTTL time.Duration, newTTL time.Duration) error {
	var (
		prefix     = ds.NewKey(p.Pretty())
		q          = query.Query{Prefix: prefix.String(), KeysOnly: false}
		oldb, newb = make([]byte, 8), make([]byte, 8)
		results    query.Results
		err        error
	)

	binary.LittleEndian.PutUint64(oldb, uint64(oldTTL))
	binary.LittleEndian.PutUint64(newb, uint64(newTTL))

	txn := mgr.ds.NewTransaction(false)
	defer txn.Discard()

	if results, err = txn.Query(q); err != nil {
		return err
	}
	defer results.Close()

	ttltxn := txn.(ds.TTLDatastore)
	for result := range results.Next() {
		// format: bytes(ttl) || bytes(multiaddr)
		if curr := result.Value[:8]; !bytes.Equal(curr, oldb) {
			continue
		}
		newVal := append(newb, result.Value[8:]...)
		if err = ttltxn.PutWithTTL(ds.RawKey(result.Key), newVal, newTTL); err != nil {
			return err
		}
	}

	if err := txn.Commit(); err != nil {
		log.Errorf("failed to commit transaction when updating ttls, cause: %v", err)
		return err
	}

	return nil
}

// Addrs returns all of the non-expired addresses for a given peer.
func (mgr *dsAddrBook) Addrs(p peer.ID) []ma.Multiaddr {
	var (
		prefix  = ds.NewKey(p.Pretty())
		q       = query.Query{Prefix: prefix.String(), KeysOnly: false, ReturnExpirations: true}
		results query.Results
		err     error
	)

	// Check the cache and return the entry only if it hasn't expired; if expired, remove.
	if e, ok := mgr.cache.Get(p.Pretty()); ok {
		entry := e.(cacheEntry)
		if entry.expiration.After(time.Now()) {
			addrs := make([]ma.Multiaddr, len(entry.addrs))
			copy(addrs, entry.addrs)
			return addrs
		} else {
			mgr.cache.Remove(p.Pretty())
		}
	}

	txn := mgr.ds.NewTransaction(true)
	defer txn.Discard()

	if results, err = txn.Query(q); err != nil {
		log.Error(err)
		return nil
	}
	defer results.Close()

	var addrs []ma.Multiaddr
	// used to set the expiration for the entire cache entry
	earliestExp := maxTime
	for result := range results.Next() {
		// extract multiaddr from value: bytes(ttl) || bytes(multiaddr)
		if addr, err := ma.NewMultiaddrBytes(result.Value[8:]); err == nil {
			addrs = append(addrs, addr)
		}

		if exp := result.Expiration; !exp.IsZero() && exp.Before(earliestExp) {
			earliestExp = exp
		}
	}

	// Store a copy in the cache.
	addrsCpy := make([]ma.Multiaddr, len(addrs))
	copy(addrsCpy, addrs)
	entry := cacheEntry{addrs: addrsCpy, expiration: earliestExp}
	mgr.cache.Add(p.Pretty(), entry)

	return addrs
}

// Peers returns all of the peer IDs for which the AddrBook has addresses.
func (mgr *dsAddrBook) PeersWithAddrs() peer.IDSlice {
	var (
		q       = query.Query{KeysOnly: true}
		results query.Results
		err     error
	)

	txn := mgr.ds.NewTransaction(true)
	defer txn.Discard()

	if results, err = txn.Query(q); err != nil {
		log.Error(err)
		return peer.IDSlice{}
	}

	defer results.Close()

	idset := make(map[string]struct{})
	for result := range results.Next() {
		key := ds.RawKey(result.Key)
		idset[key.Parent().Name()] = struct{}{}
	}

	if len(idset) == 0 {
		return peer.IDSlice{}
	}

	ids, i := make(peer.IDSlice, len(idset)), 0
	for id := range idset {
		pid, _ := peer.IDB58Decode(id)
		ids[i] = pid
		i++
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
		err      error
		prefix   = ds.NewKey(p.Pretty())
		deleteFn func() error
	)

	if e, ok := mgr.cache.Peek(p.Pretty()); ok {
		mgr.cache.Remove(p.Pretty())
		keys, _, _ := keysAndAddrs(p, e.(cacheEntry).addrs)
		deleteFn = func() error {
			return mgr.dbDelete(keys)
		}
	} else {
		deleteFn = func() error {
			_, err := mgr.dbDeleteIter(prefix)
			return err
		}
	}

	// Attempt transactional KV deletion.
	for i := 0; i < mgr.writeRetries; i++ {
		if err = deleteFn(); err == nil {
			break
		}
		log.Errorf("failed to clear addresses for peer %s: %s\n", p.Pretty(), err)
	}

	if err != nil {
		log.Errorf("failed to clear addresses for peer %s after %d attempts\n", p.Pretty(), mgr.writeRetries)
	}
}

// dbDelete transactionally deletes the provided keys.
func (mgr *dsAddrBook) dbDelete(keys []ds.Key) error {
	var err error

	txn := mgr.ds.NewTransaction(false)
	defer txn.Discard()

	for _, key := range keys {
		if err = txn.Delete(key); err != nil {
			log.Errorf("failed to delete key: %s, cause: %v", key.String(), err)
			return err
		}
	}

	if err = txn.Commit(); err != nil {
		log.Errorf("failed to commit transaction when deleting keys, cause: %v", err)
		return err
	}

	return nil
}

// dbDeleteIter removes all entries whose keys are prefixed with the argument.
// it returns a slice of the removed keys in case it's needed
func (mgr *dsAddrBook) dbDeleteIter(prefix ds.Key) ([]ds.Key, error) {
	q := query.Query{Prefix: prefix.String(), KeysOnly: true}

	txn := mgr.ds.NewTransaction(false)
	defer txn.Discard()

	results, err := txn.Query(q)
	if err != nil {
		log.Errorf("failed to fetch all keys prefixed with: %s, cause: %v", prefix.String(), err)
		return nil, err
	}

	var keys = make([]ds.Key, 0, 4) // cap: 4 to reduce allocs
	var key ds.Key
	for result := range results.Next() {
		key = ds.RawKey(result.Key)
		keys = append(keys, key)

		if err = txn.Delete(key); err != nil {
			log.Errorf("failed to delete key: %s, cause: %v", key.String(), err)
			return nil, err
		}
	}

	if err = results.Close(); err != nil {
		log.Errorf("failed to close cursor, cause: %v", err)
		return nil, err
	}

	if err = txn.Commit(); err != nil {
		log.Errorf("failed to commit transaction when deleting keys, cause: %v", err)
		return nil, err
	}

	return keys, nil
}
