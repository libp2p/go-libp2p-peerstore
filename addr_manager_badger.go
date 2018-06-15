package peerstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/libp2p/go-libp2p-peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

type BadgerAddrManager struct {
	DB          *badger.DB
	subsManager *AddrSubManager
}

type addrentry struct {
	Addr []byte
	TTL  time.Duration
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
	return &BadgerAddrManager{DB: db, subsManager: NewAddrSubManager()}, nil
}

// use murmur3 because it's the most compact
func hashMultiaddr(addr *ma.Multiaddr) ([]byte, error) {
	return multihash.Encode((*addr).Bytes(), multihash.MURMUR3)
}

func createKeyPrefix(p *peer.ID) ([]byte, error) {
	buf := &bytes.Buffer{}
	prefix := []byte(*p)
	prefixlen := uint64(len(prefix))
	if err := binary.Write(buf, binary.LittleEndian, prefixlen); err != nil {
		return nil, err
	}
	buf.Write(prefix)
	return buf.Bytes(), nil
}

func createAddressKey(p *peer.ID, addr *ma.Multiaddr) ([]byte, error) {
	buf := &bytes.Buffer{}
	prefix := []byte(*p)
	prefixlen := uint64(len(prefix))
	if err := binary.Write(buf, binary.LittleEndian, prefixlen); err != nil {
		return nil, err
	}
	buf.Write(prefix)
	addrhash, err := hashMultiaddr(addr)
	if err != nil {
		return nil, err
	}
	addrhashlen := uint64(len(addrhash))

	if err := binary.Write(buf, binary.LittleEndian, addrhashlen); err != nil {
		return nil, err
	}

	buf.Write(addrhash)
	return buf.Bytes(), nil
}

func peerIDFromBinaryKey(key []byte) (peer.ID, error) {
	buf := bytes.NewBuffer(key)
	idlen, err := binary.ReadUvarint(buf)
	if err != nil {
		return peer.ID(""), err
	}
	idbytes := make([]byte, idlen)
	read, err := buf.Read(idbytes)
	if err != nil {
		return peer.ID(""), err
	}
	if uint64(read) != idlen {
		return peer.ID(""), errors.New("invalid key length")
	}
	return peer.IDFromBytes(idbytes)
}

func (mgr *BadgerAddrManager) AddAddr(p peer.ID, addr ma.Multiaddr, ttl time.Duration) {
	mgr.AddAddrs(p, []ma.Multiaddr{addr}, ttl)
}

func (mgr *BadgerAddrManager) AddAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration) {
	if ttl <= 0 {
		log.Debugf("short circuiting AddAddrs with ttl %d", ttl)
		return
	}

	mgr.SetAddrs(p, addrs, ttl)
}

func (mgr *BadgerAddrManager) SetAddr(p peer.ID, addr ma.Multiaddr, ttl time.Duration) {
	mgr.SetAddrs(p, []ma.Multiaddr{addr}, ttl)
}

func (mgr *BadgerAddrManager) SetAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration) {
	// Attempt to commit five times
	for i := 0; i < 5; i++ {
		txn := mgr.DB.NewTransaction(true)
		defer txn.Discard()

		for _, addr := range addrs {
			if addr == nil {
				continue
			}
			key, err := createAddressKey(&p, &addr)
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
			if item, err := txn.Get(key); err != nil || item.IsDeletedOrExpired() {
				mgr.subsManager.BroadcastAddr(p, addr)
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

		err := txn.Commit(nil)
		if err != badger.ErrConflict {
			return
		}
	}
}

func (mgr *BadgerAddrManager) UpdateAddrs(p peer.ID, oldTTL time.Duration, newTTL time.Duration) {
	prefix, err := createKeyPrefix(&p)
	if err != nil {
		log.Error(err)
		return
	}
	txn := mgr.DB.NewTransaction(true)
	defer txn.Discard()
	opts := badger.DefaultIteratorOptions
	iter := txn.NewIterator(opts)

	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
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
	}

	txn.Commit(nil)
}

func (mgr *BadgerAddrManager) Addrs(p peer.ID) []ma.Multiaddr {
	txn := mgr.DB.NewTransaction(false)
	defer txn.Discard()

	prefix, err := createKeyPrefix(&p)
	if err != nil {
		log.Error(err)
		return []ma.Multiaddr{}
	}
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

func (mgr *BadgerAddrManager) Peers() []peer.ID {
	txn := mgr.DB.NewTransaction(false)
	defer txn.Commit(nil)
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	idset := make(map[peer.ID]struct{})
	for ; it.Valid(); it.Next() {
		item := it.Item()
		id, err := peerIDFromBinaryKey(item.Key())
		if err != nil {
			continue
		}
		idset[id] = struct{}{}
	}
	ids := make([]peer.ID, 0, len(idset))
	for id := range idset {
		ids = append(ids, id)
	}
	return ids
}

func (mgr *BadgerAddrManager) AddrStream(ctx context.Context, p peer.ID) <-chan ma.Multiaddr {
	initial := mgr.Addrs(p)
	return mgr.subsManager.AddrStream(ctx, p, initial)
}

func (mgr *BadgerAddrManager) ClearAddrs(p peer.ID) {
	txn := mgr.DB.NewTransaction(true)
	defer txn.Discard()
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	prefix, err := createKeyPrefix(&p)
	if err != nil {
		log.Error(err)
		return
	}
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
