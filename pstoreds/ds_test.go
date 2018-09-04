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

func TestBadgerDsPeerstore(t *testing.T) {
	test.TestPeerstore(t, peerstoreFactory(t, DefaultOpts()))
}

func TestBadgerDsAddrBook(t *testing.T) {
	opts := DefaultOpts()
	opts.TTLInterval = 100 * time.Microsecond

	test.TestAddrBook(t, addressBookFactory(t, opts))
}

func BenchmarkBadgerDsPeerstore(b *testing.B) {
	caching := DefaultOpts()
	caching.CacheSize = 1024

	cacheless := DefaultOpts()
	cacheless.CacheSize = 0

	test.BenchmarkPeerstore(b, peerstoreFactory(b, caching), "Caching")
	test.BenchmarkPeerstore(b, peerstoreFactory(b, cacheless), "Cacheless")
}

func badgerStore(t testing.TB) (datastore.TxnDatastore, func()) {
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

func peerstoreFactory(tb testing.TB, opts PeerstoreOpts) test.PeerstoreFactory {
	return func() (peerstore.Peerstore, func()) {
		ds, closeFunc := badgerStore(tb)

		ps, err := NewPeerstore(context.Background(), ds, opts)
		if err != nil {
			tb.Fatal(err)
		}

		return ps, closeFunc
	}
}

func addressBookFactory(tb testing.TB, opts PeerstoreOpts) test.AddrBookFactory {
	return func() (peerstore.AddrBook, func()) {
		ds, closeDB := badgerStore(tb)

		mgr, err := NewAddrBook(context.Background(), ds, opts)
		if err != nil {
			tb.Fatal(err)
		}

		closeFunc := func() {
			mgr.Stop()
			closeDB()
		}
		return mgr, closeFunc
	}
}
