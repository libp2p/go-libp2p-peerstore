package pstoremem

import (
	"sync"

	"github.com/libp2p/go-libp2p-core/peer"
	pstore "github.com/libp2p/go-libp2p-core/peerstore"
)

type memoryPeerMetadata struct {
	// store other data, like versions
	// ds ds.ThreadSafeDatastore
	ds       map[peer.ID]map[string]interface{}
	dslock   sync.RWMutex
	interned map[string]interface{}
}

var _ pstore.PeerMetadata = (*memoryPeerMetadata)(nil)

func NewPeerMetadata() *memoryPeerMetadata {
	return &memoryPeerMetadata{
		ds:       make(map[peer.ID]map[string]interface{}),
		interned: make(map[string]interface{}),
	}
}

func (ps *memoryPeerMetadata) Put(p peer.ID, key string, val interface{}) error {
	if err := p.Validate(); err != nil {
		return err
	}
	ps.dslock.Lock()
	defer ps.dslock.Unlock()
	if vals, ok := val.(string); ok && (key == "AgentVersion" || key == "ProtocolVersion") {
		if interned, ok := ps.interned[vals]; ok {
			val = interned
		} else {
			ps.interned[vals] = val
		}
	}
	m, ok := ps.ds[p]
	if !ok {
		m = make(map[string]interface{})
		ps.ds[p] = m
	}
	m[key] = val
	return nil
}

func (ps *memoryPeerMetadata) Get(p peer.ID, key string) (interface{}, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	ps.dslock.RLock()
	defer ps.dslock.RUnlock()
	m, ok := ps.ds[p]
	if !ok {
		return nil, pstore.ErrNotFound
	}
	val, ok := m[key]
	if !ok {
		return nil, pstore.ErrNotFound
	}
	return val, nil
}

func (ps *memoryPeerMetadata) RemovePeer(p peer.ID) {
	ps.dslock.Lock()
	delete(ps.ds, p)
	ps.dslock.Unlock()
}
