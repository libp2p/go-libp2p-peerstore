package pstoreds

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	bd "github.com/dgraph-io/badger"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-ds-badger"

	"github.com/libp2p/go-libp2p-peerstore"
	"github.com/libp2p/go-libp2p-peerstore/test"
)

func BenchmarkBaselineBadgerDatastorePutEntry(b *testing.B) {
	bds, closer := badgerStore(b)
	defer closer()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := bds.NewTransaction(false)

		key := datastore.RawKey(fmt.Sprintf("/key/%d", i))
		txn.Put(key, []byte(fmt.Sprintf("/value/%d", i)))

		txn.Commit()
		txn.Discard()
	}
}

func BenchmarkBaselineBadgerDatastoreGetEntry(b *testing.B) {
	bds, closer := badgerStore(b)
	defer closer()

	txn := bds.NewTransaction(false)
	keys := make([]datastore.Key, 1000)
	for i := 0; i < 1000; i++ {
		key := datastore.RawKey(fmt.Sprintf("/key/%d", i))
		txn.Put(key, []byte(fmt.Sprintf("/value/%d", i)))
		keys[i] = key
	}
	if err := txn.Commit(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := bds.NewTransaction(true)
		if _, err := txn.Get(keys[i%1000]); err != nil {
			b.Fatal(err)
		}
		txn.Discard()
	}
}

func BenchmarkBaselineBadgerDirectPutEntry(b *testing.B) {
	opts := bd.DefaultOptions

	dataPath, err := ioutil.TempDir(os.TempDir(), "badger")
	if err != nil {
		b.Fatal(err)
	}

	opts.Dir = dataPath
	opts.ValueDir = dataPath
	opts.SyncWrites = false

	db, err := bd.Open(opts)
	if err != nil {
		b.Fatal(err)
	}

	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := db.NewTransaction(true)
		txn.Set([]byte(fmt.Sprintf("/key/%d", i)), []byte(fmt.Sprintf("/value/%d", i)))
		txn.Commit(nil)
	}
}

func BenchmarkBaselineBadgerDirectGetEntry(b *testing.B) {
	opts := bd.DefaultOptions

	dataPath, err := ioutil.TempDir(os.TempDir(), "badger")
	if err != nil {
		b.Fatal(err)
	}

	opts.Dir = dataPath
	opts.ValueDir = dataPath

	db, err := bd.Open(opts)
	if err != nil {
		b.Fatal(err)
	}

	defer db.Close()

	txn := db.NewTransaction(true)
	for i := 0; i < 1000; i++ {
		txn.Set([]byte(fmt.Sprintf("/key/%d", i)), []byte(fmt.Sprintf("/value/%d", i)))
	}
	txn.Commit(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := db.NewTransaction(false)
		txn.Get([]byte(fmt.Sprintf("/key/%d", i%1000)))
		txn.Discard()
	}
}

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
