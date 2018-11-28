package pstoreds

import (
	"testing"
	"time"

	"github.com/ipfs/go-datastore/query"
	"github.com/libp2p/go-libp2p-peerstore"
	"github.com/libp2p/go-libp2p-peerstore/test"
)

var lookaheadQuery = query.Query{Prefix: gcLookaheadBase.String(), KeysOnly: true}

type testProbe struct {
	t  *testing.T
	ab peerstore.AddrBook
}

func (tp *testProbe) countLookaheadEntries() (i int) {
	results, err := tp.ab.(*dsAddrBook).ds.Query(lookaheadQuery)
	if err != nil {
		tp.t.Fatal(err)
	}

	defer results.Close()
	for range results.Next() {
		i++
	}
	return i
}
func (tp *testProbe) clearCache() {
	for _, k := range tp.ab.(*dsAddrBook).cache.Keys() {
		tp.ab.(*dsAddrBook).cache.Remove(k)
	}
}

func TestGCLookahead(t *testing.T) {
	opts := DefaultOpts()

	// effectively disable automatic GC for this test.
	opts.GCInitialDelay = 90 * time.Hour
	opts.GCLookaheadInterval = 10 * time.Second
	opts.GCPurgeInterval = 1 * time.Minute

	factory := addressBookFactory(t, badgerStore, opts)
	ab, closeFn := factory()
	defer closeFn()

	tp := &testProbe{t, ab}

	ids := test.GeneratePeerIDs(10)
	addrs := test.GenerateAddrs(100)

	// lookahead is 10 seconds, so these entries will be outside the lookahead window.
	ab.AddAddrs(ids[0], addrs[:10], time.Hour)
	ab.AddAddrs(ids[1], addrs[10:20], time.Hour)
	ab.AddAddrs(ids[2], addrs[20:30], time.Hour)
	ab.(*dsAddrBook).populateLookahead()
	if i := tp.countLookaheadEntries(); i != 0 {
		t.Errorf("expected no GC lookahead entries, got: %v", i)
	}

	// change addresses of a peer to have TTL 1 second, placing them in the lookahead window.
	ab.UpdateAddrs(ids[1], time.Hour, time.Second)

	// Purge the cache, to exercise a different path in the lookahead cycle.
	tp.clearCache()

	ab.(*dsAddrBook).populateLookahead()
	if i := tp.countLookaheadEntries(); i != 1 {
		t.Errorf("expected 1 GC lookahead entry, got: %v", i)
	}

	// change addresses of another to have TTL 5 second, placing them in the lookahead window.
	ab.UpdateAddrs(ids[2], time.Hour, 5*time.Second)
	ab.(*dsAddrBook).populateLookahead()
	if i := tp.countLookaheadEntries(); i != 2 {
		t.Errorf("expected 2 GC lookahead entries, got: %v", i)
	}
}

func TestGCPurging(t *testing.T) {
	opts := DefaultOpts()

	// effectively disable automatic GC for this test.
	opts.GCInitialDelay = 90 * time.Hour
	opts.GCLookaheadInterval = 20 * time.Second
	opts.GCPurgeInterval = 1 * time.Minute

	factory := addressBookFactory(t, badgerStore, opts)
	ab, closeFn := factory()
	defer closeFn()

	tp := &testProbe{t, ab}

	ids := test.GeneratePeerIDs(10)
	addrs := test.GenerateAddrs(100)

	// stagger addresses within the lookahead window, but stagger them.
	ab.AddAddrs(ids[0], addrs[:10], 1*time.Second)
	ab.AddAddrs(ids[1], addrs[30:40], 1*time.Second)
	ab.AddAddrs(ids[2], addrs[60:70], 1*time.Second)

	ab.AddAddrs(ids[0], addrs[10:20], 4*time.Second)
	ab.AddAddrs(ids[1], addrs[40:50], 4*time.Second)

	ab.AddAddrs(ids[0], addrs[20:30], 10*time.Second)
	ab.AddAddrs(ids[1], addrs[50:60], 10*time.Second)

	// this is inside the window, but it will survive the purges we do in the test.
	ab.AddAddrs(ids[3], addrs[70:80], 15*time.Second)

	ab.(*dsAddrBook).populateLookahead()
	if i := tp.countLookaheadEntries(); i != 4 {
		t.Errorf("expected 4 GC lookahead entries, got: %v", i)
	}

	<-time.After(2 * time.Second)
	ab.(*dsAddrBook).purgeCycle()
	if i := tp.countLookaheadEntries(); i != 3 {
		t.Errorf("expected 3 GC lookahead entries, got: %v", i)
	}

	// Purge the cache, to exercise a different path in the purge cycle.
	tp.clearCache()

	<-time.After(5 * time.Second)
	ab.(*dsAddrBook).purgeCycle()
	if i := tp.countLookaheadEntries(); i != 3 {
		t.Errorf("expected 3 GC lookahead entries, got: %v", i)
	}

	<-time.After(5 * time.Second)
	ab.(*dsAddrBook).purgeCycle()
	if i := tp.countLookaheadEntries(); i != 1 {
		t.Errorf("expected 1 GC lookahead entries, got: %v", i)
	}
	if i := len(ab.PeersWithAddrs()); i != 1 {
		t.Errorf("expected 1 entries in database, got: %v", i)
	}
	if p := ab.PeersWithAddrs()[0]; p != ids[3] {
		t.Errorf("expected remaining peer to be #3, got: %v, expected: %v", p, ids[3])
	}
}
