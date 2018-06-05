package peerstore

import (
	"github.com/dgraph-io/badger"
	"github.com/libp2p/go-libp2p-crypto"
	"time"
	"github.com/libp2p/go-libp2p-peer"
)

type keybook_badger struct {

}

func (kb *keybook_badger) PubKey(peer.ID) crypto.PubKey {
	panic("implement me")
}

func (kb *keybook_badger) AddPubKey(peer.ID, crypto.PubKey) error {
	panic("implement me")
}

func (kb *keybook_badger) PrivKey(peer.ID) crypto.PrivKey {
	panic("implement me")
}

func (kb *keybook_badger) AddPrivKey(peer.ID, crypto.PrivKey) error {
	panic("implement me")
}

type peerstore_badger struct {
	*addrmanager_badger
	*keybook_badger
	*badger.DB
}

func (store *peerstore_badger) PubKey(peer.ID) crypto.PubKey {
	panic("implement me")
}

func (store *peerstore_badger) AddPubKey(peer.ID, crypto.PubKey) error {
	panic("implement me")
}

func (store *peerstore_badger) PrivKey(peer.ID) crypto.PrivKey {
	panic("implement me")
}

func (store *peerstore_badger) AddPrivKey(peer.ID, crypto.PrivKey) error {
	panic("implement me")
}

func (store *peerstore_badger) RecordLatency(peer.ID, time.Duration) {
	panic("implement me")
}

func (store *peerstore_badger) LatencyEWMA(peer.ID) time.Duration {
	panic("implement me")
}

func (store *peerstore_badger) Peers() []peer.ID {
	panic("implement me")
}

func (store *peerstore_badger) PeerInfo(peer.ID) PeerInfo {
	panic("implement me")
}

func (store *peerstore_badger) Get(id peer.ID, key string) (interface{}, error) {
	panic("implement me")
}

func (store *peerstore_badger) Put(id peer.ID, key string, val interface{}) error {
	panic("implement me")
}

func (store *peerstore_badger) GetProtocols(peer.ID) ([]string, error) {
	panic("implement me")
}

func (store *peerstore_badger) AddProtocols(peer.ID, ...string) error {
	panic("implement me")
}

func (store *peerstore_badger) SetProtocols(peer.ID, ...string) error {
	panic("implement me")
}

func (store *peerstore_badger) SupportsProtocols(peer.ID, ...string) ([]string, error) {
	panic("implement me")
}

