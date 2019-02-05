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
	// /peers/addrs/<b32 peer id no padding>
	addrBookBase = ds.NewKey("/peers/addrs")
)

// addrsRecord decorates the AddrBookRecord with locks and metadata.
type addrsRecord struct {
	sync.RWMutex
	*pb.AddrBookRecord
	dirty bool
}

// flush writes the record to the datastore by calling ds.Put, unless the record is
// marked for deletion, in which case we call ds.Delete.
func (r *addrsRecord) flush(write ds.Write) (err error) {
	key := addrBookBase.ChildString(b32.RawStdEncoding.EncodeToString([]byte(r.Id.ID)))
	if len(r.Addrs) == 0 {
		return write.Delete(key)
	}

	data, err := r.Marshal()
	if err != nil {
		return err
	}
	if err = write.Put(key, data); err != nil {
		return err
	}
	// write succeeded; record is no longer dirty.
	r.dirty = false
	return nil
}

// Clean is called on records to perform housekeeping. The return value signals if the record was changed
// as a result of the cleaning.
//
// Clean does the following:
// * sorts the addresses by expiration (soonest expiring first).
// * removes the addresses that have expired.
//
// It short-circuits optimistically when we know there's nothing to do.
//
// Clean is called from several points:
// * when accessing and loading an entry.
// * when performing periodic GC.
// * after an entry has been modified (e.g. addresses have been added or removed,
//   TTLs updated, etc.)
//
// If the return value is true, the caller can perform a flush immediately, or can schedule an async
// flush, depending on the context.
func (r *addrsRecord) Clean() (chgd bool) {
	now := time.Now().Unix()
	if !r.dirty && len(r.Addrs) > 0 && r.Addrs[0].Expiry > now {
		// record is not dirty, and we have no expired entries to purge.
		return false
	}

	if len(r.Addrs) == 0 {
		// this is a ghost record; let's signal it has to be written.
		// flush() will take care of doing the deletion.
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
	ds          ds.Batching
	subsManager *pstoremem.AddrSubManager

	flushJobCh chan *addrsRecord
	cancelFn   func()
	closeDone  sync.WaitGroup

	gcCurrWindowEnd    int64
	gcLookaheadRunning int32
}

var _ pstore.AddrBook = (*dsAddrBook)(nil)

// NewAddrBook initializes a new address book given a Datastore instance, a context for managing the TTL manager,
// and the interval at which the TTL manager should sweep the Datastore.
func NewAddrBook(ctx context.Context, store ds.Batching, opts Options) (ab *dsAddrBook, err error) {
	var cache cache = new(noopCache)
	if opts.CacheSize > 0 {
		if cache, err = lru.NewARC(int(opts.CacheSize)); err != nil {
			return nil, err
		}
	}

	ctx, cancelFn := context.WithCancel(ctx)
	ab = &dsAddrBook{
		ctx:         ctx,
		cancelFn:    cancelFn,
		opts:        opts,
		cache:       cache,
		ds:          store,
		subsManager: pstoremem.NewAddrSubManager(),
		flushJobCh:  make(chan *addrsRecord, 32),
	}

	// kick off background processes.
	go ab.flusher()
	go ab.gc()

	return ab, nil
}

func (ab *dsAddrBook) Close() {
	ab.cancelFn()
	ab.closeDone.Wait()
}

func (ab *dsAddrBook) asyncFlush(pr *addrsRecord) {
	select {
	case ab.flushJobCh <- pr:
	default:
		log.Warningf("flush queue is full; could not flush record for peer %s", pr.Id.ID)
	}
}

// loadRecord is a read-through fetch. It fetches a record from cache, falling back to the
// datastore upon a miss, and returning a newly initialized record if the peer doesn't exist.
//
// loadRecord calls Clean() on the record before returning it. If the record changes
// as a result and the update argument is true, an async flush is queued.
//
// If the cache argument is true, the record is inserted in the cache when loaded from the datastore.
func (ab *dsAddrBook) loadRecord(id peer.ID, cache bool, update bool) (pr *addrsRecord, err error) {
	if e, ok := ab.cache.Get(id); ok {
		pr = e.(*addrsRecord)
		if pr.Clean() && update {
			ab.asyncFlush(pr)
		}
		return pr, nil
	}

	pr = &addrsRecord{AddrBookRecord: &pb.AddrBookRecord{}}
	key := addrBookBase.ChildString(b32.RawStdEncoding.EncodeToString([]byte(id)))
	data, err := ab.ds.Get(key)

	switch err {
	case ds.ErrNotFound:
		pr.Id = &pb.ProtoPeerID{ID: id}
	case nil:
		if err = pr.Unmarshal(data); err != nil {
			return nil, err
		}
		if pr.Clean() && update {
			ab.asyncFlush(pr)
		}
	default:
		return nil, err
	}

	if cache {
		ab.cache.Add(id, pr)
	}
	return pr, nil
}

// flusher is a goroutine that takes care of persisting asynchronous flushes to the datastore.
func (ab *dsAddrBook) flusher() {
	ab.closeDone.Add(1)
	for {
		select {
		case fj := <-ab.flushJobCh:
			if cached, ok := ab.cache.Peek(fj.Id.ID); ok {
				// Only continue flushing if the record we have in memory is the same as for which the flush
				// job was requested. If it's not in memory, it has been evicted and we don't know if we hold
				// the latest state or not. Similarly, if it's cached but the pointer is different, it means
				// it was evicted and has been reloaded, so we're also uncertain if we hold the latest state.
				if pr := cached.(*addrsRecord); pr != fj {
					pr.RLock()
					pr.flush(ab.ds)
					pr.RUnlock()
				}
			}

		case <-ab.ctx.Done():
			ab.closeDone.Done()
			return
		}
	}
}

// gc is a goroutine that prunes expired addresses from the datastore at regular intervals.
func (ab *dsAddrBook) gc() {
	select {
	case <-time.After(ab.opts.GCInitialDelay):
	case <-ab.ctx.Done():
		// yield if we have been cancelled/closed before the delay elapses.
		return
	}

	ab.closeDone.Add(1)
	purgeTimer := time.NewTicker(ab.opts.GCPurgeInterval)
	lookaheadTimer := time.NewTicker(ab.opts.GCLookaheadInterval)

	for {
		select {
		case <-purgeTimer.C:
			ab.purgeCycle()

		case <-lookaheadTimer.C:
			ab.populateLookahead()

		case <-ab.ctx.Done():
			purgeTimer.Stop()
			lookaheadTimer.Stop()
			ab.closeDone.Done()
			return
		}
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

	if pr.Clean() {
		pr.flush(ab.ds)
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
	if err := ab.ds.Delete(key); err != nil {
		log.Errorf("failed to clear addresses for peer %s: %v", p.Pretty(), err)
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
	existed := make([]bool, len(addrs)) // keeps track of which addrs we found.

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
		// note: there's a minor chance that writing the record will fail, in which case we would've broadcast
		// the addresses without persisting them. This is very unlikely and not much of an issue.
		ab.subsManager.BroadcastAddr(p, addr)
	}

	pr.Addrs = append(pr.Addrs, added...)
	pr.dirty = true
	pr.Clean()
	return pr.flush(ab.ds)
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
	pr.Clean()
	return pr.flush(ab.ds)
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
