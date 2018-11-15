package pstoreds

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log"

	pool "github.com/libp2p/go-buffer-pool"
	peer "github.com/libp2p/go-libp2p-peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	pb "github.com/libp2p/go-libp2p-peerstore/pb"
	pstoremem "github.com/libp2p/go-libp2p-peerstore/pstoremem"

	lru "github.com/hashicorp/golang-lru"
	ma "github.com/multiformats/go-multiaddr"
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
	// /peers/addr/<b32 peer id no padding>/<hash of maddr>
	addrBookBase = ds.NewKey("/peers/addrs")
)

// addrsRecord decorates the AddrBookRecord with locks and metadata.
type addrsRecord struct {
	sync.RWMutex
	*pb.AddrBookRecord
	dirty bool
}

// FlushInTxn writes the record to the datastore by calling ds.Put, unless the record is
// marked for deletion, in which case the deletion is executed via ds.Delete.
func (r *addrsRecord) FlushInTxn(txn ds.Txn) (err error) {
	key := addrBookBase.ChildString(b32.RawStdEncoding.EncodeToString([]byte(r.Id.ID)))
	if len(r.Addrs) == 0 {
		return txn.Delete(key)
	}
	data := pool.Get(r.Size())
	defer pool.Put(data)

	// i is the number of bytes that were effectively written.
	i, err := r.MarshalTo(data)
	if err != nil {
		return err
	}
	if err := txn.Put(key, data[:i]); err != nil {
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
	ctx                context.Context
	gcInterval         time.Duration
	gcMaxPurgePerCycle int

	cache       cache
	ds          ds.TxnDatastore
	subsManager *pstoremem.AddrSubManager

	flushJobCh chan *addrsRecord
	cancelFn   func()
	closedCh   chan struct{}
}

var _ pstore.AddrBook = (*dsAddrBook)(nil)

// NewAddrBook initializes a new address book given a Datastore instance, a context for managing the TTL manager,
// and the interval at which the TTL manager should sweep the Datastore.
func NewAddrBook(ctx context.Context, store ds.TxnDatastore, opts Options) (ab *dsAddrBook, err error) {
	var cache cache = new(noopCache)
	if opts.CacheSize > 0 {
		if cache, err = lru.NewARC(int(opts.CacheSize)); err != nil {
			return nil, err
		}
	}

	ctx, cancelFn := context.WithCancel(ctx)
	mgr := &dsAddrBook{
		ctx:         ctx,
		cancelFn:    cancelFn,
		gcInterval:  opts.GCInterval,
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
// datastore upon a miss, and returning an newly initialized record if the peer doesn't exist.
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
		pr = &addrsRecord{AddrBookRecord: &pb.AddrBookRecord{}}
		if err = pr.Unmarshal(data); err != nil {
			return nil, err
		}
		if pr.Refresh() && update {
			ab.asyncFlush(pr)
		}
	} else {
		pr = &addrsRecord{AddrBookRecord: &pb.AddrBookRecord{Id: &pb.ProtoPeerID{ID: id}}}
	}

	if cache {
		ab.cache.Add(id, pr)
	}
	return pr, nil
}

// background runs the housekeeping process that takes care of:
//
// * purging expired addresses from the datastore at regular intervals.
// * persisting asynchronous flushes to the datastore.
func (ab *dsAddrBook) background() {
	timer := time.NewTicker(ab.gcInterval)
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

		case <-timer.C:
			ab.purgeCycle()

		case <-ab.ctx.Done():
			timer.Stop()
			close(ab.closedCh)
			return
		}
	}
}

var purgeQuery = query.Query{Prefix: addrBookBase.String()}

// purgeCycle runs a GC cycle
func (ab *dsAddrBook) purgeCycle() {
	var id peer.ID
	record := &addrsRecord{AddrBookRecord: &pb.AddrBookRecord{}}
	txn, err := ab.ds.NewTransaction(false)
	if err != nil {
		log.Warningf("failed while purging entries: %v\n", err)
		return
	}
	defer txn.Discard()

	results, err := txn.Query(purgeQuery)
	if err != nil {
		log.Warningf("failed while purging entries: %v\n", err)
		return
	}
	defer results.Close()

	for result := range results.Next() {
		k, err := b32.RawStdEncoding.DecodeString(ds.RawKey(result.Key).Name())
		if err != nil {
			// TODO: drop the record? this will keep failing forever.
			log.Warningf("failed while purging record: %v, err: %v\n", result.Key, err)
			continue
		}
		id, err = peer.IDFromBytes(k)
		if err != nil {
			// TODO: drop the record? this will keep failing forever.
			log.Warningf("failed to get extract peer ID from bytes (hex): %x, err: %v\n", k, err)
			continue
		}
		// if the record is in cache, we refresh it and flush it if necessary.
		if e, ok := ab.cache.Peek(id); ok {
			cached := e.(*addrsRecord)
			cached.Lock()
			if cached.Refresh() {
				cached.FlushInTxn(txn)
			}
			cached.Unlock()
			continue
		}

		if err := record.Unmarshal(result.Value); err != nil {
			// TODO: drop the record? this will keep failing forever.
			log.Warningf("failed while deserializing entry with key: %v, err: %v\n", result.Key, err)
			continue
		}
		if record.Refresh() {
			record.FlushInTxn(txn)
		}
		record.Reset()
	}

	if err = txn.Commit(); err != nil {
		log.Warningf("failed to commit GC transaction: %v\n", err)
	}
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
		log.Errorf("failed to clear addresses for peer %s: %v\n", p.Pretty(), err)
	}
	defer txn.Discard()

	if err := txn.Delete(key); err != nil {
		log.Errorf("failed to clear addresses for peer %s: %v\n", p.Pretty(), err)
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
