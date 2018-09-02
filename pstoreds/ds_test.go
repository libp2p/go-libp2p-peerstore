package pstoreds

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"gx/ipfs/QmSpg1CvpXQQow5ernt1gNBXaXV6yxyNqi7XoeerWfzB5w/go-datastore"
	"gx/ipfs/QmUCfrikzKVGAfpE31RPwPd32fu1DYxSG7HTGCadba5Wza/go-ds-badger"

	"github.com/libp2p/go-libp2p-peerstore"
	"github.com/libp2p/go-libp2p-peerstore/test"
)

func setupBadgerDatastore(t testing.TB) (datastore.Batching, func()) {
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

func newPeerstoreFactory(tb testing.TB) test.PeerstoreFactory {
	return func() (peerstore.Peerstore, func()) {
		ds, closeFunc := setupBadgerDatastore(tb)

		ps, err := NewPeerstore(context.Background(), ds)
		if err != nil {
			tb.Fatal(err)
		}

		return ps, closeFunc
	}
}

func TestBadgerDsPeerstore(t *testing.T) {
	test.TestPeerstore(t, newPeerstoreFactory(t))
}

func TestBadgerDsAddrBook(t *testing.T) {
	test.TestAddrMgr(t, func() (peerstore.AddrBook, func()) {
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
	test.BenchmarkPeerstore(b, newPeerstoreFactory(b))
}
