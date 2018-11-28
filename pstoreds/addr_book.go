package pstoreds

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log"

	peer "github.com/libp2p/go-libp2p-peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	pb "github.com/libp2p/go-libp2p-peerstore/pb"
	pstoremem "github.com/libp2p/go-libp2p-peerstore/pstoremem"

	ma "github.com/multiformats/go-multiaddr"
	lru "github.com/raulk/golang-lru"
	b32 "github.com/whyrusleeping/base32"
)

type ttlWriteMode int

const (
	ttlOverride ttlWriteMode = iota
	ttlExtend
)

var (
	log = logging.Logger("peerstore/ds")
	// Peer addresses are stored under the following db key pattern:
	// /peers/addrs/<b32 peer id no padding>
	addrBookBase = ds.NewKey("/peers/addrs")
	// GC lookahead entries are stored in keys with pattern:
	// /peers/gc/addrs/<unix timestamp of next visit>/<peer ID b32> => nil
	gcLookaheadBase = ds.NewKey("/peers/gc/addrs")
	arPool          = &sync.Pool{
		New: func() interface{} {
			return &addrsRecord{
				AddrBookRecord: &pb.AddrBookRecord{},
				dirty:          false,
			}
		},
	}
)

// addrsRecord decorates the AddrBookRecord with locks and metadata.
type addrsRecord struct {
	sync.RWMutex
	*pb.AddrBookRecord
	dirty bool
}

func (r *addrsRecord) Reset() {
	r.AddrBookRecord.Reset()
	r.dirty = false
}

// FlushInTxn writes the record to the datastore by calling ds.Put, unless the record is
// marked for deletion, in which case the deletion is executed via ds.Delete.
func (r *addrsRecord) FlushInTxn(txn ds.Txn) (err error) {
	key := addrBookBase.ChildString(b32.RawStdEncoding.EncodeToString([]byte(r.Id.ID)))
	if len(r.Addrs) == 0 {
		return txn.Delete(key)
	}
	// cannot use a buffer pool because data is retained in the txn until it's committed or discarded.
	data, err := r.Marshal()
	if err != nil {
		return err
	}
	if err = txn.Put(key, data); err != nil {
		return err
	}
	// write succeeded; record is no longer dirty.
	r.dirty = false
	return nil
}

// Flush creates a ds.Txn, and calls FlushInTxn with it.
func (r *addrsRecord) Flush(ds ds.TxnDatastore) (err error) {
	txn, err := ds.NewTransaction(false)
	if err != nil {
		return err
	}
	defer txn.Discard()

	if err = r.FlushInTxn(txn); err != nil {
		return err
	}
	return txn.Commit()
}

// Refresh is called on records to perform housekeeping. The return value signals if the record was changed
// as a result of the refresh.
//
// Refresh does the following:
// * sorts the addresses by expiration (soonest expiring first).
// * removes the addresses that have expired.
//
// It short-circuits optimistically when we know there's nothing to do.
//
// Refresh is called from several points:
// * when accessing and loading an entry.
// * when performing periodic GC.
// * after an entry has been modified (e.g. addresses have been added or removed,
//   TTLs updated, etc.)
//
// If the return value is true, the caller can perform a flush immediately, or can schedule an async
// flush, depending on the context.
func (r *addrsRecord) Refresh() (chgd bool) {
	now := time.Now().Unix()
	if !r.dirty && len(r.Addrs) > 0 && r.Addrs[0].Expiry > now {
		// record is not dirty, and we have no expired entries to purge.
		return false
	}

	if len(r.Addrs) == 0 {
		// this is a ghost record; let's signal it has to be written.
		// Flush() will take care of doing the deletion.
		return true
	}

	if r.dirty && len(r.Addrs) > 1 {
		// the record has been modified, so it may need resorting.
		// we keep addresses sorted by expiration, where 0 is the soonest expiring.
		sort.Slice(r.Addrs, func(i, j int) bool {
			return r.Addrs[i].Expiry < r.Addrs[j].Expiry
		})
	}

	// since addresses are sorted by expiration, we find the first survivor and split the
	// slice on its index.
	pivot := -1
	for i, addr := range r.Addrs {
		if addr.Expiry > now {
			break
		}
		pivot = i
	}

	r.Addrs = r.Addrs[pivot+1:]
	return r.dirty || pivot >= 0
}

// dsAddrBook is an address book backed by a Datastore with a GC-like procedure
// to purge expired entries. It uses an in-memory address stream manager.
type dsAddrBook struct {
	ctx context.Context

	opts Options

	cache       cache
	ds          ds.TxnDatastore
	subsManager *pstoremem.AddrSubManager

	flushJobCh chan *addrsRecord
	cancelFn   func()
	closedCh   chan struct{}

	gcCurrWindowEnd    int64
	gcLookaheadRunning int32
}

var _ pstore.AddrBook = (*dsAddrBook)(nil)

// NewAddrBook initializes a new address book given a Datastore instance, a context for managing the TTL manager,
// and the interval at which the TTL manager should sweep the Datastore.
func NewAddrBook(ctx context.Context, store ds.TxnDatastore, opts Options) (ab *dsAddrBook, err error) {
	var cache cache = new(noopCache)
	if opts.CacheSize > 0 {
		evictCallback := func(key interface{}, value interface{}) {
			value.(*addrsRecord).Reset()
			arPool.Put(value)
		}
		if cache, err = lru.NewARCWithEvict(int(opts.CacheSize), evictCallback); err != nil {
			return nil, err
		}
	}

	ctx, cancelFn := context.WithCancel(ctx)
	mgr := &dsAddrBook{
		ctx:         ctx,
		cancelFn:    cancelFn,
		opts:        opts,
		cache:       cache,
		ds:          store,
		subsManager: pstoremem.NewAddrSubManager(),
		flushJobCh:  make(chan *addrsRecord, 32),
		closedCh:    make(chan struct{}),
	}

	// kick off periodic GC.
	go mgr.background()

	return mgr, nil
}

func (ab *dsAddrBook) Close() {
	ab.cancelFn()
	<-ab.closedCh
}

func (ab *dsAddrBook) asyncFlush(pr *addrsRecord) {
	select {
	case ab.flushJobCh <- pr:
	default:
		log.Warningf("flush queue is full; could not flush peer %v", pr.Id.ID.Pretty())
	}
}

// loadRecord is a read-through fetch. It fetches a record from cache, falling back to the
// datastore upon a miss, and returning a newly initialized record if the peer doesn't exist.
//
// loadRecord calls Refresh() on the record before returning it. If the record changes
// as a result and `update=true`, an async flush is scheduled.
//
// If `cache=true`, the record is inserted in the cache when loaded from the datastore.
func (ab *dsAddrBook) loadRecord(id peer.ID, cache bool, update bool) (pr *addrsRecord, err error) {
	if e, ok := ab.cache.Get(id); ok {
		pr = e.(*addrsRecord)
		if pr.Refresh() && update {
			ab.asyncFlush(pr)
		}
		return pr, nil
	}

	txn, err := ab.ds.NewTransaction(true)
	if err != nil {
		return nil, err
	}
	defer txn.Discard()

	key := addrBookBase.ChildString(b32.RawStdEncoding.EncodeToString([]byte(id)))
	data, err := txn.Get(key)

	if err != nil && err != ds.ErrNotFound {
		return nil, err
	}

	if err == nil {
		pr = arPool.Get().(*addrsRecord)
		if err = pr.Unmarshal(data); err != nil {
			return nil, err
		}
		if pr.Refresh() && update {
			ab.asyncFlush(pr)
		}
	} else {
		pr = arPool.Get().(*addrsRecord)
		pr.Id = &pb.ProtoPeerID{ID: id}
	}

	if cache {
		ab.cache.Add(id, pr)
	}
	return pr, nil
}

// background runs the housekeeping process that takes care of:
//
// * GCing expired addresses from the datastore at regular intervals.
// * persisting asynchronous flushes to the datastore.
func (ab *dsAddrBook) background() {
	// placeholder tickers.
	pruneTimer, lookaheadTimer := new(time.Ticker), new(time.Ticker)

	// populate the tickers after the initial delay has passed.
	go func() {
		select {
		case <-time.After(ab.opts.GCInitialDelay):
			pruneTimer = time.NewTicker(ab.opts.GCPurgeInterval)
			lookaheadTimer = time.NewTicker(ab.opts.GCLookaheadInterval)
		}
	}()

	for {
		select {
		case fj := <-ab.flushJobCh:
			if cached, ok := ab.cache.Peek(fj.Id.ID); ok {
				// Only continue flushing if the record we have in memory is the same as for which the flush
				// job was requested. If it's not in memory, it has been evicted and we don't know if we hold
				// the latest state or not. Similarly, if it's cached but the pointer is different, it means
				// it was evicted and has been reloaded, so we're also uncertain if we hold the latest state.
				if pr := cached.(*addrsRecord); pr == fj {
					pr.RLock()
					pr.Flush(ab.ds)
					pr.RUnlock()
				}
			}

		case <-pruneTimer.C:
			ab.purgeCycle()

		case <-lookaheadTimer.C:
			ab.populateLookahead()

		case <-ab.ctx.Done():
			pruneTimer.Stop()
			lookaheadTimer.Stop()
			close(ab.closedCh)
			return
		}
	}
}

var purgeQuery = query.Query{Prefix: gcLookaheadBase.String(), KeysOnly: true}

// purgeCycle runs a single GC cycle, operating within the lookahead window.
//
// It scans the lookahead region for entries that need to be visited, and performs a refresh on them. An errors trigger
// the removal of the GC entry, in order to prevent unactionable items from accumulating. If the error happened to be
// temporary, the entry will be revisited in the next lookahead window.
func (ab *dsAddrBook) purgeCycle() {
	if atomic.LoadInt32(&ab.gcLookaheadRunning) > 0 {
		// yield if lookahead is running.
		return
	}

	var id peer.ID
	record := arPool.Get().(*addrsRecord)
	record.Reset()
	defer arPool.Put(record)

	txn, err := ab.ds.NewTransaction(false)
	if err != nil {
		log.Warningf("failed while purging entries: %v", err)
		return
	}
	defer txn.Discard()

	// This function drops an unparseable GC entry; this is for safety. It is an escape hatch in case
	// we modify the format of keys going forward. If a user runs a new version against an old DB,
	// if we don't clean up unparseable entries we'll end up accumulating garbage.
	dropInError := func(key ds.Key, err error, msg string) {
		if err != nil {
			log.Warningf("failed while %s with GC key: %v, err: %v", msg, key, err)
		}
		if err = txn.Delete(key); err != nil {
			log.Warningf("failed to delete corrupt GC lookahead entry: %v, err: %v", key, err)
		}
	}

	// This function drops a GC key if the entry is refreshed correctly. It may reschedule another visit
	// if the next earliest expiry falls within the current window again.
	dropOrReschedule := func(key ds.Key, ar *addrsRecord) {
		if err = txn.Delete(key); err != nil {
			log.Warningf("failed to delete lookahead entry: %v, err: %v", key, err)
		}

		// re-add the record if it needs to be visited again in this window.
		if len(ar.Addrs) != 0 && ar.Addrs[0].Expiry <= ab.gcCurrWindowEnd {
			gcKey := gcLookaheadBase.ChildString(fmt.Sprintf("%d/%s", ar.Addrs[0].Expiry, key.Name()))
			if err = txn.Put(gcKey, []byte{}); err != nil {
				log.Warningf("failed to add new GC key: %v, err: %v", gcKey, err)
			}
		}
	}

	results, err := txn.Query(purgeQuery)
	if err != nil {
		log.Warningf("failed while fetching entries to purge: %v", err)
		return
	}
	defer results.Close()

	now := time.Now().Unix()

	// keys: 	/peers/gc/addrs/<unix timestamp of next visit>/<peer ID b32>
	// values: 	nil
	for result := range results.Next() {
		gcKey := ds.RawKey(result.Key)

		ts, err := strconv.ParseInt(gcKey.Parent().Name(), 10, 64)
		if err != nil {
			dropInError(gcKey, err, "parsing timestamp")
			log.Warningf("failed while parsing timestamp from key: %v, err: %v", result.Key, err)
			continue
		} else if ts > now {
			// this is an ordered cursor; when we hit an entry with a timestamp beyond now, we can break.
			break
		}

		idb32, err := b32.RawStdEncoding.DecodeString(gcKey.Name())
		if err != nil {
			dropInError(gcKey, err, "parsing peer ID")
			log.Warningf("failed while parsing b32 peer ID from key: %v, err: %v", result.Key, err)
			continue
		}

		id, err = peer.IDFromBytes(idb32)
		if err != nil {
			dropInError(gcKey, err, "decoding peer ID")
			log.Warningf("failed while decoding peer ID from key: %v, err: %v", result.Key, err)
			continue
		}

		// if the record is in cache, we refresh it and flush it if necessary.
		if e, ok := ab.cache.Peek(id); ok {
			cached := e.(*addrsRecord)
			cached.Lock()
			if cached.Refresh() {
				if err = cached.FlushInTxn(txn); err != nil {
					log.Warningf("failed to flush entry modified by GC for peer: &v, err: %v", id.Pretty(), err)
				}
			}
			dropOrReschedule(gcKey, cached)
			cached.Unlock()
			continue
		}

		record.Reset()

		// otherwise, fetch it from the store, refresh it and flush it.
		entryKey := addrBookBase.ChildString(gcKey.Name())
		val, err := txn.Get(entryKey)
		if err != nil {
			// captures all errors, including ErrNotFound.
			dropInError(gcKey, err, "fetching entry")
			continue
		}
		err = record.Unmarshal(val)
		if err != nil {
			dropInError(gcKey, err, "unmarshalling entry")
			continue
		}
		if record.Refresh() {
			err = record.FlushInTxn(txn)
			if err != nil {
				log.Warningf("failed to flush entry modified by GC for peer: &v, err: %v", id.Pretty(), err)
			}
		}
		dropOrReschedule(gcKey, record)
	}

	err = txn.Commit()
	if err != nil {
		log.Warningf("failed to commit GC prune transaction: %v", err)
	}
}

var populateLookaheadQuery = query.Query{Prefix: addrBookBase.String(), KeysOnly: true}

// populateLookahead populates the lookahead window by scanning the entire store and picking entries whose earliest
// expiration falls within the new window.
//
// Those entries are stored in the lookahead region in the store, indexed by the timestamp when they need to be
// visited, to facilitate temporal range scans.
func (ab *dsAddrBook) populateLookahead() {
	if !atomic.CompareAndSwapInt32(&ab.gcLookaheadRunning, 0, 1) {
		return
	}

	until := time.Now().Add(ab.opts.GCLookaheadInterval).Unix()

	var id peer.ID
	record := arPool.Get().(*addrsRecord)
	defer arPool.Put(record)

	txn, err := ab.ds.NewTransaction(false)
	if err != nil {
		log.Warningf("failed while filling lookahead GC region: %v", err)
		return
	}
	defer txn.Discard()

	results, err := txn.Query(populateLookaheadQuery)
	if err != nil {
		log.Warningf("failed while filling lookahead GC region: %v", err)
		return
	}
	defer results.Close()

	for result := range results.Next() {
		idb32 := ds.RawKey(result.Key).Name()
		k, err := b32.RawStdEncoding.DecodeString(idb32)
		if err != nil {
			log.Warningf("failed while decoding peer ID from key: %v, err: %v", result.Key, err)
			continue
		}
		if id, err = peer.IDFromBytes(k); err != nil {
			log.Warningf("failed while decoding peer ID from key: %v, err: %v", result.Key, err)
		}

		// if the record is in cache, use the cached version.
		if e, ok := ab.cache.Peek(id); ok {
			cached := e.(*addrsRecord)
			cached.RLock()
			if len(cached.Addrs) == 0 || cached.Addrs[0].Expiry > until {
				cached.RUnlock()
				continue
			}
			gcKey := gcLookaheadBase.ChildString(fmt.Sprintf("%d/%s", cached.Addrs[0].Expiry, idb32))
			if err = txn.Put(gcKey, []byte{}); err != nil {
				log.Warningf("failed while inserting GC entry for peer: %v, err: %v", id.Pretty(), err)
			}
			cached.RUnlock()
			continue
		}

		record.Reset()

		val, err := txn.Get(ds.RawKey(result.Key))
		if err != nil {
			log.Warningf("failed which getting record from store for peer: %v, err: %v", id.Pretty(), err)
			continue
		}
		if err := record.Unmarshal(val); err != nil {
			log.Warningf("failed while unmarshalling record from store for peer: %v, err: %v", id.Pretty(), err)
			continue
		}
		if len(record.Addrs) > 0 && record.Addrs[0].Expiry <= until {
			gcKey := gcLookaheadBase.ChildString(fmt.Sprintf("%d/%s", record.Addrs[0].Expiry, idb32))
			if err = txn.Put(gcKey, []byte{}); err != nil {
				log.Warningf("failed while inserting GC entry for peer: %v, err: %v", id.Pretty(), err)
			}
		}
	}

	if err = txn.Commit(); err != nil {
		log.Warningf("failed to commit GC lookahead transaction: %v", err)
	}

	ab.gcCurrWindowEnd = until
	atomic.StoreInt32(&ab.gcLookaheadRunning, 0)
}

// AddAddr will add a new address if it's not already in the AddrBook.
func (ab *dsAddrBook) AddAddr(p peer.ID, addr ma.Multiaddr, ttl time.Duration) {
	ab.AddAddrs(p, []ma.Multiaddr{addr}, ttl)
}

// AddAddrs will add many new addresses if they're not already in the AddrBook.
func (ab *dsAddrBook) AddAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	addrs = cleanAddrs(addrs)
	ab.setAddrs(p, addrs, ttl, ttlExtend)
}

// SetAddr will add or update the TTL of an address in the AddrBook.
func (ab *dsAddrBook) SetAddr(p peer.ID, addr ma.Multiaddr, ttl time.Duration) {
	ab.SetAddrs(p, []ma.Multiaddr{addr}, ttl)
}

// SetAddrs will add or update the TTLs of addresses in the AddrBook.
func (ab *dsAddrBook) SetAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration) {
	addrs = cleanAddrs(addrs)
	if ttl <= 0 {
		ab.deleteAddrs(p, addrs)
		return
	}
	ab.setAddrs(p, addrs, ttl, ttlOverride)
}

// UpdateAddrs will update any addresses for a given peer and TTL combination to
// have a new TTL.
func (ab *dsAddrBook) UpdateAddrs(p peer.ID, oldTTL time.Duration, newTTL time.Duration) {
	pr, err := ab.loadRecord(p, true, false)
	if err != nil {
		log.Errorf("failed to update ttls for peer %s: %s\n", p.Pretty(), err)
	}

	pr.Lock()
	defer pr.Unlock()

	newExp := time.Now().Add(newTTL).Unix()
	for _, entry := range pr.Addrs {
		if entry.Ttl != int64(oldTTL) {
			continue
		}
		entry.Ttl, entry.Expiry = int64(newTTL), newExp
		pr.dirty = true
	}

	if pr.Refresh() {
		pr.Flush(ab.ds)
	}
}

// Addrs returns all of the non-expired addresses for a given peer.
func (ab *dsAddrBook) Addrs(p peer.ID) []ma.Multiaddr {
	pr, err := ab.loadRecord(p, true, true)
	if err != nil {
		log.Warning("failed to load peerstore entry for peer %v while querying addrs, err: %v", p, err)
		return nil
	}

	pr.RLock()
	defer pr.RUnlock()

	addrs := make([]ma.Multiaddr, 0, len(pr.Addrs))
	for _, a := range pr.Addrs {
		addrs = append(addrs, a.Addr)
	}
	return addrs
}

// Peers returns all of the peer IDs for which the AddrBook has addresses.
func (ab *dsAddrBook) PeersWithAddrs() peer.IDSlice {
	ids, err := uniquePeerIds(ab.ds, addrBookBase, func(result query.Result) string {
		return ds.RawKey(result.Key).Name()
	})
	if err != nil {
		log.Errorf("error while retrieving peers with addresses: %v", err)
	}
	return ids
}

// AddrStream returns a channel on which all new addresses discovered for a
// given peer ID will be published.
func (ab *dsAddrBook) AddrStream(ctx context.Context, p peer.ID) <-chan ma.Multiaddr {
	initial := ab.Addrs(p)
	return ab.subsManager.AddrStream(ctx, p, initial)
}

// ClearAddrs will delete all known addresses for a peer ID.
func (ab *dsAddrBook) ClearAddrs(p peer.ID) {
	ab.cache.Remove(p)

	key := addrBookBase.ChildString(b32.RawStdEncoding.EncodeToString([]byte(p)))
	txn, err := ab.ds.NewTransaction(false)
	if err != nil {
		log.Errorf("failed to clear addresses for peer %s: %v", p.Pretty(), err)
	}
	defer txn.Discard()

	if err := txn.Delete(key); err != nil {
		log.Errorf("failed to clear addresses for peer %s: %v", p.Pretty(), err)
	}

	if err = txn.Commit(); err != nil {
		log.Errorf("failed to commit transaction when deleting keys, cause: %v", err)
	}
}

func (ab *dsAddrBook) setAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration, mode ttlWriteMode) (err error) {
	pr, err := ab.loadRecord(p, true, false)
	if err != nil {
		return fmt.Errorf("failed to load peerstore entry for peer %v while setting addrs, err: %v", p, err)
	}

	pr.Lock()
	defer pr.Unlock()

	newExp := time.Now().Add(ttl).Unix()
	existed := make([]bool, len(addrs)) // keeps track of which addrs we found

Outer:
	for i, incoming := range addrs {
		for _, have := range pr.Addrs {
			if incoming.Equal(have.Addr) {
				existed[i] = true
				if mode == ttlExtend && have.Expiry > newExp {
					// if we're only extending TTLs but the addr already has a longer one, we skip it.
					continue Outer
				}
				have.Expiry = newExp
				// we found the address, and addresses cannot be duplicate,
				// so let's move on to the next.
				continue Outer
			}
		}
	}

	// add addresses we didn't hold.
	var added []*pb.AddrBookRecord_AddrEntry
	for i, e := range existed {
		if e {
			continue
		}
		addr := addrs[i]
		entry := &pb.AddrBookRecord_AddrEntry{
			Addr:   &pb.ProtoAddr{Multiaddr: addr},
			Ttl:    int64(ttl),
			Expiry: newExp,
		}
		added = append(added, entry)
		// TODO: should we only broadcast if we updated the store successfully?
		// we have no way of rolling back the state of the in-memory record, although we
		// could at the expense of allocs. But is it worthwhile?
		ab.subsManager.BroadcastAddr(p, addr)
	}

	pr.Addrs = append(pr.Addrs, added...)
	pr.dirty = true
	pr.Refresh()
	return pr.Flush(ab.ds)
}

func (ab *dsAddrBook) deleteAddrs(p peer.ID, addrs []ma.Multiaddr) (err error) {
	pr, err := ab.loadRecord(p, false, false)
	if err != nil {
		return fmt.Errorf("failed to load peerstore entry for peer %v while deleting addrs, err: %v", p, err)
	}

	if pr.Addrs == nil {
		return nil
	}

	pr.Lock()
	defer pr.Unlock()

	// deletes addresses in place, and avoiding copies until we encounter the first deletion.
	survived := 0
	for i, addr := range pr.Addrs {
		for _, del := range addrs {
			if addr.Addr.Equal(del) {
				continue
			}
			if i != survived {
				pr.Addrs[survived] = pr.Addrs[i]
			}
			survived++
		}
	}
	pr.Addrs = pr.Addrs[:survived]

	pr.dirty = true
	pr.Refresh()
	return pr.Flush(ab.ds)
}

func cleanAddrs(addrs []ma.Multiaddr) []ma.Multiaddr {
	clean := make([]ma.Multiaddr, 0, len(addrs))
	for _, addr := range addrs {
		if addr == nil {
			continue
		}
		clean = append(clean, addr)
	}
	return clean
}
