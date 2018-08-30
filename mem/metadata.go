package mem

import (
	"sync"

	pstore "github.com/libp2p/go-libp2p-peerstore"
	"gx/ipfs/QmQsErDt8Qgw1XrsXf2BpEzDgGWtB1YLsTAARBup5b6B9W/go-libp2p-peer"
)

type memoryPeerMetadata struct {
	// store other data, like versions
	//ds ds.ThreadSafeDatastore
	// TODO: use a datastore for this
	ds     map[string]interface{}
	dslock sync.Mutex
}

func NewPeerMetadata() pstore.PeerMetadata {
	return &memoryPeerMetadata{
		ds: make(map[string]interface{}),
	}
}

func (ps *memoryPeerMetadata) Put(p peer.ID, key string, val interface{}) error {
	//dsk := ds.NewKey(string(p) + "/" + key)
	//return ps.ds.Put(dsk, val)
	ps.dslock.Lock()
	defer ps.dslock.Unlock()
	ps.ds[string(p)+"/"+key] = val
	return nil
}

func (ps *memoryPeerMetadata) Get(p peer.ID, key string) (interface{}, error) {
	//dsk := ds.NewKey(string(p) + "/" + key)
	//return ps.ds.Get(dsk)

	ps.dslock.Lock()
	defer ps.dslock.Unlock()
	i, ok := ps.ds[string(p)+"/"+key]
	if !ok {
		return nil, pstore.ErrNotFound
	}
	return i, nil
}
