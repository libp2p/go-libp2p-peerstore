package pstoreds

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	errors "github.com/pkg/errors"

	peer "github.com/libp2p/go-libp2p-peer"
	pb "github.com/libp2p/go-libp2p-peerstore/pb"

	b32 "github.com/whyrusleeping/base32"
)

var (
	// GC lookahead entries are stored in keys with pattern:
	// /peers/gc/addrs/<unix timestamp of next visit>/<peer ID b32> => nil
	gcLookaheadBase = ds.NewKey("/peers/gc/addrs")
	// in GC routines, how many operations do we place in a batch before it's committed.
	gcOpsPerBatch = 20
	// queries
	purgeQuery = query.Query{
		Prefix:   gcLookaheadBase.String(),
		Orders:   []query.Order{query.OrderByKey{}},
		KeysOnly: true,
	}
	populateLookaheadQuery = query.Query{
		Prefix:   addrBookBase.String(),
		Orders:   []query.Order{query.OrderByKey{}},
		KeysOnly: true,
	}
)

// cyclicBatch buffers datastore write operations and automatically flushes them after gcOpsPerBatch (20) have been
// queued. An explicit `Commit()` closes this cyclic batch, erroring all further operations.
//
// It is similar to go-datastore autobatch, but it's driven by an actual Batch facility offered by the
// datastore.
type cyclicBatch struct {
	ds.Batch
	ds      ds.Batching
	pending int
}

func newCyclicBatch(ds ds.Batching) (ds.Batch, error) {
	batch, err := ds.Batch()
	if err != nil {
		return nil, err
	}
	return &cyclicBatch{Batch: batch, ds: ds}, nil
}

func (cb *cyclicBatch) cycle() (err error) {
	if cb.Batch == nil {
		return errors.New("cyclic batch is closed")
	}
	if cb.pending < gcOpsPerBatch {
		// we haven't reached the threshold yet.
		return nil
	}
	// commit and renew the batch.
	if err = cb.Batch.Commit(); err != nil {
		return errors.Wrap(err, "failed while committing cyclic batch")
	}
	if cb.Batch, err = cb.ds.Batch(); err != nil {
		return errors.Wrap(err, "failed while renewing cyclic batch")
	}
	return nil
}

func (cb *cyclicBatch) Put(key ds.Key, val []byte) error {
	if err := cb.cycle(); err != nil {
		return err
	}
	cb.pending++
	return cb.Batch.Put(key, val)
}

func (cb *cyclicBatch) Delete(key ds.Key) error {
	if err := cb.cycle(); err != nil {
		return err
	}
	cb.pending++
	return cb.Batch.Delete(key)
}

func (cb *cyclicBatch) Commit() error {
	if cb.Batch == nil {
		return errors.New("cyclic batch is closed")
	}
	if err := cb.Batch.Commit(); err != nil {
		return err
	}
	cb.pending = 0
	cb.Batch = nil
	return nil
}

// purgeCycle runs a single GC purge cycle. It operates within the lookahead window if lookahead is enabled; else it
// visits all entries in the datastore, deleting the addresses that have expired.
func (ab *dsAddrBook) purgeCycle() {
	if atomic.LoadInt32(&ab.gcLookaheadRunning) > 0 {
		// yield if lookahead is running.
		return
	}

	var id peer.ID
	record := &addrsRecord{AddrBookRecord: &pb.AddrBookRecord{}} // empty record to reuse and avoid allocs.
	batch, err := newCyclicBatch(ab.ds)
	if err != nil {
		log.Warningf("failed while creating batch to purge GC entries: %v", err)
	}

	// This function drops an unparseable GC entry; this is for safety. It is an escape hatch in case
	// we modify the format of keys going forward. If a user runs a new version against an old DB,
	// if we don't clean up unparseable entries we'll end up accumulating garbage.
	dropInError := func(key ds.Key, err error, msg string) {
		if err != nil {
			log.Warningf("failed while %s with GC key: %v, err: %v", msg, key, err)
		}
		if err = batch.Delete(key); err != nil {
			log.Warningf("failed to delete corrupt GC lookahead entry: %v, err: %v", key, err)
		}
	}

	// This function drops a GC key if the entry is cleaned correctly. It may reschedule another visit
	// if the next earliest expiry falls within the current window again.
	dropOrReschedule := func(key ds.Key, ar *addrsRecord) {
		if err := batch.Delete(key); err != nil {
			log.Warningf("failed to delete lookahead entry: %v, err: %v", key, err)
		}

		// re-add the record if it needs to be visited again in this window.
		if len(ar.Addrs) != 0 && ar.Addrs[0].Expiry <= ab.gcCurrWindowEnd {
			gcKey := gcLookaheadBase.ChildString(fmt.Sprintf("%d/%s", ar.Addrs[0].Expiry, key.Name()))
			if err := batch.Put(gcKey, []byte{}); err != nil {
				log.Warningf("failed to add new GC key: %v, err: %v", gcKey, err)
			}
		}
	}

	results, err := ab.ds.Query(purgeQuery)
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

		// if the record is in cache, we clean it and flush it if necessary.
		if e, ok := ab.cache.Peek(id); ok {
			cached := e.(*addrsRecord)
			cached.Lock()
			if cached.Clean() {
				if err = cached.flush(batch); err != nil {
					log.Warningf("failed to flush entry modified by GC for peer: &v, err: %v", id.Pretty(), err)
				}
			}
			dropOrReschedule(gcKey, cached)
			cached.Unlock()
			continue
		}

		record.Reset()

		// otherwise, fetch it from the store, clean it and flush it.
		entryKey := addrBookBase.ChildString(gcKey.Name())
		val, err := ab.ds.Get(entryKey)
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
		if record.Clean() {
			err = record.flush(batch)
			if err != nil {
				log.Warningf("failed to flush entry modified by GC for peer: &v, err: %v", id.Pretty(), err)
			}
		}
		dropOrReschedule(gcKey, record)
	}

	if err = batch.Commit(); err != nil {
		log.Warningf("failed to commit GC purge batch: %v", err)
	}
}

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
	record := &addrsRecord{AddrBookRecord: &pb.AddrBookRecord{}}
	results, err := ab.ds.Query(populateLookaheadQuery)
	if err != nil {
		log.Warningf("failed while querying to populate lookahead GC window: %v", err)
		return
	}
	defer results.Close()

	batch, err := newCyclicBatch(ab.ds)
	if err != nil {
		log.Warningf("failed while creating batch to populate lookahead GC window: %v", err)
		return
	}

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
			if err = batch.Put(gcKey, []byte{}); err != nil {
				log.Warningf("failed while inserting GC entry for peer: %v, err: %v", id.Pretty(), err)
			}
			cached.RUnlock()
			continue
		}

		record.Reset()

		val, err := ab.ds.Get(ds.RawKey(result.Key))
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
			if err = batch.Put(gcKey, []byte{}); err != nil {
				log.Warningf("failed while inserting GC entry for peer: %v, err: %v", id.Pretty(), err)
			}
		}
	}

	if err = batch.Commit(); err != nil {
		log.Warningf("failed to commit GC lookahead batch: %v", err)
	}

	ab.gcCurrWindowEnd = until
	atomic.StoreInt32(&ab.gcLookaheadRunning, 0)
}
