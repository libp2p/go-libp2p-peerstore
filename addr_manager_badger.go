package peerstore

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p-peer"
	ma "github.com/multiformats/go-multiaddr"
	//"github.com/hashicorp/golang-lru"
	"github.com/dgraph-io/badger"
	"github.com/multiformats/go-multihash"
	"encoding/gob"
	"bytes"
	"encoding/binary"
)

type addrmanager_badger struct {
	//cache *lru.Cache
	db *badger.DB
}

type addrentry struct {
	Addr []byte
	TTL time.Duration
}

func NewBadgerAddrManager() *addrmanager_badger {
	db, err := badger.Open(badger.DefaultOptions)
	if err != nil {
		panic(err)
	}
	return &addrmanager_badger{db: db}
}

func (mgr *addrmanager_badger) AddAddr(p peer.ID, addr ma.Multiaddr, ttl time.Duration) {
	mgr.AddAddrs(p, []ma.Multiaddr{addr}, ttl)
}

// use murmur3 because it's the most compact
func hashMultiaddr(addr *ma.Multiaddr) ([]byte, error) {
	return multihash.Encode((*addr).Bytes(), multihash.MURMUR3)
}

// not relevant w/ key prefixing
func createAddrEntry(addr ma.Multiaddr, ttl time.Duration) ([]byte, error) {
	entry := addrentry{Addr: addr.Bytes(), TTL: ttl}
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(entry); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func createKeyPrefix(p *peer.ID, ttl time.Duration) ([]byte, error) {
	buf := bytes.NewBufferString(string(*p))
	if err := binary.Write(buf, binary.LittleEndian, ttl); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func createUniqueKey(p *peer.ID, addr *ma.Multiaddr, ttl time.Duration) ([]byte, error) {
	prefix, err := createKeyPrefix(p, ttl)
	if err != nil {
		return nil, err
	}
	addrHash, err := hashMultiaddr(addr)
	if err != nil {
		return nil, err
	}

	return append(prefix, addrHash...), nil
}

func addAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration, txn *badger.Txn) {
	for _, addr := range addrs {
		key, err := createUniqueKey(&p, &addr, ttl)
		if err != nil {
			log.Error(err)
			txn.Discard()
			return
		}
		txn.SetWithTTL(key, addr.Bytes(), ttl)
	}
}

func (mgr *addrmanager_badger) AddAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration) {
	// if ttl is zero, exit. nothing to do.
	if ttl <= 0 {
		log.Debugf("short circuiting AddAddrs with ttl %d", ttl)
		return
	}

	txn := mgr.db.NewTransaction(true)
	defer txn.Discard()

	addAddrs(p, addrs, ttl, txn)

	txn.Commit(func (err error) {
		if err != nil {
			log.Error(err)
		}
	})
}

func (mgr *addrmanager_badger) SetAddr(p peer.ID, addr ma.Multiaddr, ttl time.Duration) {
	mgr.AddAddr(p, addr, ttl)
}

func (mgr *addrmanager_badger) SetAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration) {
	mgr.AddAddrs(p, addrs, ttl)
}

func (mgr *addrmanager_badger) UpdateAddrs(p peer.ID, oldTTL time.Duration, newTTL time.Duration) {
	prefix, err := createKeyPrefix(&p, oldTTL)
	if err != nil {
		log.Error(err)
		return
	}
	txn := mgr.db.NewTransaction(true)
	defer txn.Discard()
	opts := badger.DefaultIteratorOptions
	iter := txn.NewIterator(opts)

	var addrs []ma.Multiaddr
	iter.Seek(prefix)
	for iter.ValidForPrefix(prefix) {
		item := iter.Item()
		addrbytes, err := item.Value()
		if err != nil {
			log.Error(err)
			return
		}
		addrs = append(addrs, ma.Cast(addrbytes))

		iter.Next()
	}

	addAddrs(p, addrs, newTTL, txn)

	txn.Commit(func (err error) {
		if err != nil {
			log.Error(err)
		}
	})
}

func (mgr *addrmanager_badger) Addrs(p peer.ID) []ma.Multiaddr {
	txn := mgr.db.NewTransaction(false)
	defer txn.Discard()

	prefix := []byte(p)
	opts := badger.DefaultIteratorOptions
	iter := txn.NewIterator(opts)
	iter.Seek(prefix)

	var addrs []ma.Multiaddr

	for iter.ValidForPrefix(prefix) {
		item := iter.Item()

		if !item.IsDeletedOrExpired() {
			value, err := item.Value()
			if err != nil {
				log.Error(err)
			} else {
				addrs = append(addrs, ma.Cast(value))
			}
		}

		iter.Next()
	}

	txn.Commit(nil)

	return addrs
}

func (mgr *addrmanager_badger) AddrStream(ctx context.Context, p peer.ID) <-chan ma.Multiaddr {
	addrs := make(chan ma.Multiaddr)

	//mgr.db.View(func (txn *badger.Txn) error {
	//	defer close(addrs)
	//
	//	return nil
	//})

	return addrs
}

func (mgr *addrmanager_badger) ClearAddrs(p peer.ID) {
	err := mgr.db.Update(func (txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		prefix := []byte(p)
		iter.Seek(prefix)

		for iter.ValidForPrefix(prefix) {
			txn.Delete(iter.Item().Key())
			iter.Next()
		}

		return nil
	})
	if err != nil {
		log.Error(err)
	}
}
