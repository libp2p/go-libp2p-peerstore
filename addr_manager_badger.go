package peerstore

import (
	"context"
	"time"

	"bytes"
	"encoding/gob"
	"github.com/dgraph-io/badger"
	"github.com/libp2p/go-libp2p-peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

type BadgerAddrManager struct {
	DB       *badger.DB
	addrSubs map[peer.ID][]*addrSub
}

type addrentry struct {
	Addr []byte
	TTL  time.Duration
}

func (mgr *BadgerAddrManager) sendSubscriptionUpdates(p *peer.ID, addrs []ma.Multiaddr) {
	subs := mgr.addrSubs[*p]
	for _, sub := range subs {
		for _, addr := range addrs {
			sub.pubAddr(addr)
		}
	}
}

func (mgr *BadgerAddrManager) Close() {
	if err := mgr.DB.Close(); err != nil {
		log.Error(err)
	}
}

func NewBadgerAddrManager(dataPath string) (*BadgerAddrManager, error) {
	opts := badger.DefaultOptions
	opts.Dir = dataPath
	opts.ValueDir = dataPath
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &BadgerAddrManager{DB: db}, nil
}

func (mgr *BadgerAddrManager) AddAddr(p peer.ID, addr ma.Multiaddr, ttl time.Duration) {
	mgr.AddAddrs(p, []ma.Multiaddr{addr}, ttl)
}

// use murmur3 because it's the most compact
func hashMultiaddr(addr *ma.Multiaddr) ([]byte, error) {
	return multihash.Encode((*addr).Bytes(), multihash.MURMUR3)
}

func createUniqueKey(p *peer.ID, addr *ma.Multiaddr) ([]byte, error) {
	prefix := []byte(*p)
	addrHash, err := hashMultiaddr(addr)
	if err != nil {
		return nil, err
	}

	return append(prefix, addrHash...), nil
}

func addAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration, txn *badger.Txn) {
	for _, addr := range addrs {
		if addr == nil {
			continue
		}
		entry := &addrentry{Addr: addr.Bytes(), TTL: ttl}
		key, err := createUniqueKey(&p, &addr)
		if err != nil {
			log.Error(err)
			txn.Discard()
			return
		}
		buf := &bytes.Buffer{}
		enc := gob.NewEncoder(buf)
		if err := enc.Encode(entry); err != nil {
			log.Error(err)
			txn.Discard()
			return
		}
		txn.SetWithTTL(key, buf.Bytes(), ttl)
	}
}

func (mgr *BadgerAddrManager) AddAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration) {
	if ttl <= 0 {
		log.Debugf("short circuiting AddAddrs with ttl %d", ttl)
		return
	}

	txn := mgr.DB.NewTransaction(true)
	defer txn.Discard()

	go mgr.sendSubscriptionUpdates(&p, addrs)
	addAddrs(p, addrs, ttl, txn)

	txn.Commit(nil)
}

func (mgr *BadgerAddrManager) SetAddr(p peer.ID, addr ma.Multiaddr, ttl time.Duration) {
	mgr.SetAddrs(p, []ma.Multiaddr{addr}, ttl)
}

func (mgr *BadgerAddrManager) SetAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration) {
	txn := mgr.DB.NewTransaction(true)
	defer txn.Discard()

	for _, addr := range addrs {
		if addr == nil {
			continue
		}
		key, err := createUniqueKey(&p, &addr)
		if err != nil {
			log.Error(err)
			continue
		}
		if ttl <= 0 {
			if err := txn.Delete(key); err != nil {
				log.Error(err)
				continue
			}
		}
		entry := &addrentry{Addr: addr.Bytes(), TTL: ttl}
		buf := &bytes.Buffer{}
		enc := gob.NewEncoder(buf)
		if err := enc.Encode(entry); err != nil {
			log.Error(err)
			continue
		}
		txn.SetWithTTL(key, buf.Bytes(), ttl)
	}

	txn.Commit(nil)
}

func (mgr *BadgerAddrManager) UpdateAddrs(p peer.ID, oldTTL time.Duration, newTTL time.Duration) {
	prefix := []byte(p)
	txn := mgr.DB.NewTransaction(true)
	defer txn.Discard()
	opts := badger.DefaultIteratorOptions
	iter := txn.NewIterator(opts)

	iter.Seek(prefix)
	for iter.ValidForPrefix(prefix) {
		item := iter.Item()
		addrbytes, err := item.Value()
		if err != nil {
			log.Error(err)
			return
		}
		entry := &addrentry{}
		buf := bytes.NewBuffer(addrbytes)
		dec := gob.NewDecoder(buf)
		if err := dec.Decode(&entry); err != nil {
			log.Error(err)
			return
		}
		if entry.TTL == oldTTL {
			entry.TTL = newTTL
			buf := &bytes.Buffer{}
			enc := gob.NewEncoder(buf)
			if err := enc.Encode(&entry); err != nil {
				log.Error(err)
				return
			}
			txn.SetWithTTL(item.Key(), buf.Bytes(), newTTL)
		}

		iter.Next()
	}

	txn.Commit(nil)
}

func (mgr *BadgerAddrManager) Addrs(p peer.ID) []ma.Multiaddr {
	txn := mgr.DB.NewTransaction(false)
	defer txn.Discard()

	prefix := []byte(p)
	opts := badger.DefaultIteratorOptions
	iter := txn.NewIterator(opts)
	iter.Seek(prefix)

	var addrs []ma.Multiaddr

	for ; iter.ValidForPrefix(prefix); iter.Next() {
		item := iter.Item()
		if item.IsDeletedOrExpired() {
			continue
		}

		value, err := item.Value()
		if err != nil {
			log.Error(err)
			continue
		}
		entry := &addrentry{}
		buf := bytes.NewBuffer(value)
		dec := gob.NewDecoder(buf)
		if err := dec.Decode(&entry); err != nil {
			log.Error("deleting bad entry in peerstore for peer", p.String())
			txn.Delete(item.Key())
		} else {
			addrs = append(addrs, ma.Cast(entry.Addr))
		}
	}

	txn.Commit(nil)

	return addrs
}

func (mgr *BadgerAddrManager) AddrStream(ctx context.Context, p peer.ID) <-chan ma.Multiaddr {
	addrs := make(chan ma.Multiaddr)

	// TODO: impl

	return addrs
}

func (mgr *BadgerAddrManager) ClearAddrs(p peer.ID) {
	txn := mgr.DB.NewTransaction(true)
	defer txn.Discard()
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	prefix := []byte(p)
	it.Seek(prefix)

	count := 0
	for it.ValidForPrefix(prefix) {
		count++
		if err := txn.Delete(it.Item().Key()); err != nil {
			log.Error(err)
		}
		it.Next()
	}
	txn.Commit(nil)
}
