package pstoreds

import (
	"context"
	"fmt"
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

	delete bool
}

// FlushInTxn flushes the record to the datastore by calling ds.Put, unless the record is
// marked for deletion, in which case the deletion is executed.
func (r *addrsRecord) FlushInTxn(txn ds.Txn) (err error) {
	key := addrBookBase.ChildString(b32.RawStdEncoding.EncodeToString([]byte(r.Id)))
	if r.delete {
		return txn.Delete(key)
	}
	data := pool.Get(r.Size())
	defer pool.Put(data)
	i, err := r.MarshalTo(data)
	if err != nil {
		return err
	}
	return txn.Put(key, data[:i])
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

// Refresh is called on in-memory entries to perform housekeeping. Refresh does the following:
// * removes all expired addresses.
// * recalculates the date in which the record needs to be revisited (earliest expiration of survivors).
// * marks the record for deletion if no addresses are left.
//
// A `true` value of `force` tells us to proceed with housekeeping even if the `NextVisit` date has not arrived.
//
// Refresh is called in several occasions:
// * with force=false, when accessing and loading an entry, or when performing GC.
// * with force=true, after an entry has been modified (e.g. addresses have been added or removed,
//   TTLs updated, etc.)
func (r *addrsRecord) Refresh(force bool) (chgd bool) {
	if len(r.Addrs) == 0 {
		r.delete = true
		r.NextVisit = nil
		return true
	}

	now := time.Now()
	if !force && r.NextVisit != nil && !r.NextVisit.IsZero() && now.Before(*r.NextVisit) {
		// no expired entries to purge, and no forced housekeeping.
		return false
	}

	// nv stores the next visit for surviving addresses following the purge
	var nv time.Time
	for addr, entry := range r.Addrs {
		if entry.Expiry == nil {
			continue
		}
		if nv.IsZero() {
			nv = *entry.Expiry
		}
		if now.After(*entry.Expiry) {
			// this entry is expired; remove it.
			delete(r.Addrs, addr)
			chgd = true
		} else if nv.After(*entry.Expiry) {
			// keep track of the earliest expiry across survivors.
			nv = *entry.Expiry
		}
	}

	if len(r.Addrs) == 0 {
		r.delete = true
		r.NextVisit = nil
		return true
	}

	chgd = chgd || r.NextVisit == nil || nv != *r.NextVisit
	r.NextVisit = &nv
	return chgd
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

// NewAddrBook initializes a new address book given a
// Datastore instance, a context for managing the TTL manager,
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

	go mgr.background()

	return mgr, nil
}

func (ab *dsAddrBook) Close() {
	ab.cancelFn()
	<-ab.closedCh
}

func (ab *dsAddrBook) tryFlush(pr *addrsRecord) {
	select {
	case ab.flushJobCh <- pr:
	default:
		id, _ := peer.IDFromBytes(pr.Id)
		log.Warningf("flush queue is full; could not flush peer %v", id)
	}
}

func (ab *dsAddrBook) loadRecord(id peer.ID, cache bool, update bool) (pr *addrsRecord, err error) {
	if e, ok := ab.cache.Get(id); ok {
		pr = e.(*addrsRecord)
		if pr.Refresh(false) && update {
			ab.tryFlush(pr)
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

	if err == nil {
		pr = &addrsRecord{AddrBookRecord: &pb.AddrBookRecord{}}
		if err = pr.Unmarshal(data); err != nil {
			return nil, err
		}
		if pr.Refresh(false) {
			ab.tryFlush(pr)
		}
		if cache {
			ab.cache.Add(id, pr)
		}
		return pr, nil
	}

	if err == ds.ErrNotFound {
		pr = &addrsRecord{AddrBookRecord: &pb.AddrBookRecord{
			Id:    []byte(id),
			Addrs: make(map[string]*pb.AddrBookRecord_AddrEntry),
		}}
		if cache {
			ab.cache.Add(id, pr)
		}
		return pr, nil
	}

	log.Error(err)
	return nil, err
}

// background is the peerstore process that takes care of:
// * purging expired addresses and peers with no addresses from the datastore at regular intervals.
// * asynchronously flushing cached entries with expired addresses to the datastore.
func (ab *dsAddrBook) background() {
	timer := time.NewTicker(ab.gcInterval)

	for {
		select {
		case fj := <-ab.flushJobCh:
			id, _ := peer.IDFromBytes(fj.Id)
			if cached, ok := ab.cache.Peek(id); ok {
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

func (ab *dsAddrBook) purgeCycle() {
	var id peer.ID
	record := &addrsRecord{AddrBookRecord: &pb.AddrBookRecord{}}
	q := query.Query{Prefix: addrBookBase.String()}

	txn, err := ab.ds.NewTransaction(false)
	if err != nil {
		log.Warningf("failed while purging entries: %v\n", err)
		return
	}

	defer txn.Discard()

	results, err := txn.Query(q)
	if err != nil {
		log.Warningf("failed while purging entries: %v\n", err)
		return
	}

	defer results.Close()

	for result := range results.Next() {
		id, _ = peer.IDFromBytes(record.Id)

		// if the record is in cache, let's refresh that one and flush it if necessary.
		if e, ok := ab.cache.Peek(id); ok {
			cached := e.(*addrsRecord)
			cached.Lock()
			if cached.Refresh(false) {
				cached.FlushInTxn(txn)
			}
			cached.Unlock()
			continue
		}

		if err := record.Unmarshal(result.Value); err != nil {
			log.Warningf("failed while purging entries: %v\n", err)
			continue
		}

		if record.Refresh(false) {
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

	chgd, newExp := false, time.Now().Add(newTTL)
	for _, entry := range pr.Addrs {
		if entry.Ttl == nil || *entry.Ttl != oldTTL {
			continue
		}
		entry.Ttl, entry.Expiry = &newTTL, &newExp
		chgd = true
	}

	if chgd {
		pr.Refresh(true)
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
	for k, _ := range pr.Addrs {
		if a, err := ma.NewMultiaddr(k); err == nil {
			addrs = append(addrs, a)
		}
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
		return fmt.Errorf("failed to load peerstore entry for peer %v while deleting addrs, err: %v", p, err)
	}

	pr.Lock()
	defer pr.Unlock()

	now := time.Now()
	broadcast := make([]bool, len(addrs))
	newExp := now.Add(ttl)
	for i, addr := range addrs {
		e, ok := pr.Addrs[addr.String()]
		if ok && mode == ttlExtend && e.Expiry.After(newExp) {
			continue
		}
		pr.Addrs[addr.String()] = &pb.AddrBookRecord_AddrEntry{Expiry: &newExp, Ttl: &ttl}
		broadcast[i] = !ok
	}

	// Update was successful, so broadcast event only for new addresses.
	for i, v := range broadcast {
		if v {
			ab.subsManager.BroadcastAddr(p, addrs[i])
		}
	}

	pr.Refresh(true)
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

	for _, addr := range addrs {
		delete(pr.Addrs, addr.String())
	}

	pr.Refresh(true)
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
