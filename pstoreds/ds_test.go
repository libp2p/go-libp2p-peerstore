package pstoreds

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	bd "github.com/dgraph-io/badger"

	ds "github.com/ipfs/go-datastore"
	badger "github.com/ipfs/go-ds-badger"

	pstore "github.com/libp2p/go-libp2p-peerstore"
	pt "github.com/libp2p/go-libp2p-peerstore/test"
)

func BenchmarkBaselineBadgerDatastorePutEntry(b *testing.B) {
	bds, closer := badgerStore(b)
	defer closer()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn, _ := bds.NewTransaction(false)

		key := ds.RawKey(fmt.Sprintf("/key/%d", i))
		txn.Put(key, []byte(fmt.Sprintf("/value/%d", i)))

		txn.Commit()
		txn.Discard()
	}
}

func BenchmarkBaselineBadgerDatastoreGetEntry(b *testing.B) {
	bds, closer := badgerStore(b)
	defer closer()

	txn, _ := bds.NewTransaction(false)
	keys := make([]ds.Key, 1000)
	for i := 0; i < 1000; i++ {
		key := ds.RawKey(fmt.Sprintf("/key/%d", i))
		txn.Put(key, []byte(fmt.Sprintf("/value/%d", i)))
		keys[i] = key
	}
	if err := txn.Commit(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn, _ := bds.NewTransaction(true)
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
	pt.TestPeerstore(t, peerstoreFactory(t, DefaultOpts()))
}

func TestBadgerDsAddrBook(t *testing.T) {
	t.Run("Cacheful", func(t *testing.T) {
		t.Parallel()

		opts := DefaultOpts()
		opts.TTLInterval = 100 * time.Microsecond
		opts.CacheSize = 1024

		pt.TestAddrBook(t, addressBookFactory(t, opts))
	})

	t.Run("Cacheless", func(t *testing.T) {
		t.Parallel()

		opts := DefaultOpts()
		opts.TTLInterval = 100 * time.Microsecond
		opts.CacheSize = 0

		pt.TestAddrBook(t, addressBookFactory(t, opts))
	})
}

func BenchmarkBadgerDsPeerstore(b *testing.B) {
	caching := DefaultOpts()
	caching.CacheSize = 1024

	cacheless := DefaultOpts()
	cacheless.CacheSize = 0

	pt.BenchmarkPeerstore(b, peerstoreFactory(b, caching), "Caching")
	pt.BenchmarkPeerstore(b, peerstoreFactory(b, cacheless), "Cacheless")
}

func badgerStore(t testing.TB) (ds.TxnDatastore, func()) {
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

func peerstoreFactory(tb testing.TB, opts Options) pt.PeerstoreFactory {
	return func() (pstore.Peerstore, func()) {
		store, closeFunc := badgerStore(tb)

		ps, err := NewPeerstore(context.Background(), store, opts)
		if err != nil {
			tb.Fatal(err)
		}

		return ps, closeFunc
	}
}

func addressBookFactory(tb testing.TB, opts Options) pt.AddrBookFactory {
	return func() (pstore.AddrBook, func()) {
		store, closeFunc := badgerStore(tb)

		ab, err := NewAddrBook(context.Background(), store, opts)
		if err != nil {
			tb.Fatal(err)
		}

		return ab, closeFunc
	}
}
