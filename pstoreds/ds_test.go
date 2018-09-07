package pstoreds

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-ds-badger"

	pstore "github.com/libp2p/go-libp2p-peerstore"
	pt "github.com/libp2p/go-libp2p-peerstore/test"
)

func setupBadgerDatastore(t testing.TB) (ds.Batching, func()) {
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

func newPeerstoreFactory(tb testing.TB) pt.PeerstoreFactory {
	return func() (pstore.Peerstore, func()) {
		ds, closeFunc := setupBadgerDatastore(tb)

		ps, err := NewPeerstore(context.Background(), ds)
		if err != nil {
			tb.Fatal(err)
		}

		return ps, closeFunc
	}
}

func TestBadgerDsPeerstore(t *testing.T) {
	pt.TestPeerstore(t, newPeerstoreFactory(t))
}

func TestBadgerDsAddrBook(t *testing.T) {
	pt.TestAddrBook(t, func() (pstore.AddrBook, func()) {
		ds, closeDB := setupBadgerDatastore(t)

		mgr, err := NewAddrBook(context.Background(), ds, 100*time.Microsecond)
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
	pt.BenchmarkPeerstore(b, newPeerstoreFactory(b))
}
