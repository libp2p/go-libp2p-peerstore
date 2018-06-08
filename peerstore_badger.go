package peerstore

import (
	"fmt"
	"sync"

	"github.com/libp2p/go-libp2p-peer"
)

type PeerstoreBadger struct {
	*keybook
	*metrics
	*AddrManagerBadger

	ds     map[string]interface{}
	dslock sync.Mutex

	// lock for protocol information, separate from datastore lock
	protolock sync.Mutex
}

func NewPeerstoreBadger(dataPath string) *PeerstoreBadger {
	mgr, err := NewBadgerAddrManager(dataPath)
	if err != nil {
		panic(err)
	}
	return &PeerstoreBadger{
		keybook:           newKeybook(),
		metrics:           NewMetrics(),
		AddrManagerBadger: mgr,
		ds:                make(map[string]interface{}),
	}
}

func (ps *PeerstoreBadger) Close() error {
	ps.AddrManagerBadger.Close()
	return nil
}

func (ps *PeerstoreBadger) PeerInfo(p peer.ID) PeerInfo {
	return PeerInfo{
		ID:    p,
		Addrs: ps.AddrManagerBadger.Addrs(p),
	}
}

func (ps *PeerstoreBadger) Get(p peer.ID, key string) (interface{}, error) {
	ps.dslock.Lock()
	defer ps.dslock.Unlock()
	i, ok := ps.ds[string(p)+"/"+key]
	if !ok {
		return nil, ErrNotFound
	}
	return i, nil
}

func (ps *PeerstoreBadger) Put(p peer.ID, key string, val interface{}) error {
	ps.dslock.Lock()
	defer ps.dslock.Unlock()
	ps.ds[string(p)+"/"+key] = val
	return nil
}

func (ps *PeerstoreBadger) getProtocolMap(p peer.ID) (map[string]struct{}, error) {
	iprotomap, err := ps.Get(p, "protocols")
	switch err {
	default:
		return nil, err
	case ErrNotFound:
		return make(map[string]struct{}), nil
	case nil:
		cast, ok := iprotomap.(map[string]struct{})
		if !ok {
			return nil, fmt.Errorf("stored protocol set was not a map")
		}

		return cast, nil
	}
}

func (ps *PeerstoreBadger) GetProtocols(p peer.ID) ([]string, error) {
	ps.protolock.Lock()
	defer ps.protolock.Unlock()
	pmap, err := ps.getProtocolMap(p)
	if err != nil {
		return nil, err
	}

	var out []string
	for k, _ := range pmap {
		out = append(out, k)
	}

	return out, nil
}

func (ps *PeerstoreBadger) AddProtocols(p peer.ID, protos ...string) error {
	ps.protolock.Lock()
	defer ps.protolock.Unlock()
	protomap, err := ps.getProtocolMap(p)
	if err != nil {
		return err
	}

	for _, proto := range protos {
		protomap[proto] = struct{}{}
	}

	return ps.Put(p, "protocols", protomap)
}

func (ps *PeerstoreBadger) SetProtocols(p peer.ID, protos ...string) error {
	ps.protolock.Lock()
	defer ps.protolock.Unlock()

	protomap := make(map[string]struct{})
	for _, proto := range protos {
		protomap[proto] = struct{}{}
	}

	return ps.Put(p, "protocols", protomap)
}

func (ps *PeerstoreBadger) SupportsProtocols(p peer.ID, protos ...string) ([]string, error) {
	ps.protolock.Lock()
	defer ps.protolock.Unlock()
	pmap, err := ps.getProtocolMap(p)
	if err != nil {
		return nil, err
	}

	var out []string
	for _, proto := range protos {
		if _, ok := pmap[proto]; ok {
			out = append(out, proto)
		}
	}

	return out, nil
}
