package pstoreds

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-ds-badger"

	"github.com/libp2p/go-libp2p-peerstore"
	"github.com/libp2p/go-libp2p-peerstore/test"
)

func setupBadgerDatastore(t testing.TB) (datastore.TxnDatastore, func()) {
	dataPath, err := ioutil.TempDir(os.TempDir(), "badger")
	if err != nil {
		t.Fatal(err)
	}
	ds, err := badger.NewDatastore(dataPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	closer := func() {
		ds.Close()
		os.RemoveAll(dataPath)
	}
	return ds, closer
}

func cachingPeerstore(tb testing.TB) test.PeerstoreFactory {
	opts := DefaultOpts()
	opts.CacheSize = 1024
	opts.TTLInterval = 100 * time.Microsecond

	return func() (peerstore.Peerstore, func()) {
		ds, closeFunc := setupBadgerDatastore(tb)

		ps, err := NewPeerstore(context.Background(), ds, opts)
		if err != nil {
			tb.Fatal(err)
		}

		return ps, closeFunc
	}
}

func cachelessPeerstore(tb testing.TB) test.PeerstoreFactory {
	opts := DefaultOpts()
	opts.CacheSize = 0
	opts.TTLInterval = 100 * time.Microsecond

	return func() (peerstore.Peerstore, func()) {
		ds, closeFunc := setupBadgerDatastore(tb)

		ps, err := NewPeerstore(context.Background(), ds, opts)
		if err != nil {
			tb.Fatal(err)
		}

		return ps, closeFunc
	}
}

func TestBadgerDsPeerstore(t *testing.T) {
	test.TestPeerstore(t, cachingPeerstore(t))
}

func TestBadgerDsAddrBook(t *testing.T) {
	opts := DefaultOpts()
	opts.CacheSize = 0
	opts.TTLInterval = 100 * time.Microsecond

	test.TestAddrBook(t, func() (peerstore.AddrBook, func()) {
		ds, closeDB := setupBadgerDatastore(t)

		mgr, err := NewAddrBook(context.Background(), ds, opts)
		if err != nil {
			t.Fatal(err)
		}

		closeFunc := func() {
			mgr.Stop()
			closeDB()
		}
		return mgr, closeFunc
	})
}

func BenchmarkBadgerDsPeerstore(b *testing.B) {
	test.BenchmarkPeerstore(b, cachingPeerstore(b), "Caching")
	test.BenchmarkPeerstore(b, cachelessPeerstore(b), "Cacheless")
}
