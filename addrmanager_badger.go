package peerstore

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p-peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/hashicorp/golang-lru"
	"github.com/dgraph-io/badger"
	"github.com/multiformats/go-multihash"
	"encoding/gob"
	"bytes"
	"encoding/binary"
)

type addrmanager_badger struct {
	cache *lru.Cache
	db *badger.DB
}

type addrentry struct {
	Addr []byte
	TTL time.Duration
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

func (mgr *addrmanager_badger) AddAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration) {
	// if ttl is zero, exit. nothing to do.
	if ttl <= 0 {
		log.Debugf("short circuiting AddAddrs with ttl %d", ttl)
		return
	}

	txn := mgr.db.NewTransaction(true)
	defer txn.Discard()

	for _, addr := range addrs {
		key, err := createUniqueKey(&p, &addr, ttl)
		if err != nil {
			log.Error(err)
			return
		}
		txn.SetWithTTL(key, addr.Bytes(), ttl)
	}

	txn.Commit(func (err error) {
		log.Error(err)
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
	}
	txn := mgr.db.NewTransaction(true)
	opts := badger.DefaultIteratorOptions
	iter := txn.NewIterator(opts)

	iter.Seek(prefix)
	for iter.Valid() && iter.ValidForPrefix(prefix) {
		item := iter.Item()
		// TODO:

		iter.Next()
	}

}

func (mgr *addrmanager_badger) Addrs(p peer.ID) []ma.Multiaddr {
	panic("implement me")
}

func (mgr *addrmanager_badger) AddrStream(context.Context, peer.ID) <-chan ma.Multiaddr {
	panic("implement me")
}

func (mgr *addrmanager_badger) ClearAddrs(p peer.ID) {
	panic("implement me")
}
