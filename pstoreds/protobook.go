package pstoreds

import (
	"fmt"
	"sync"

	peer "github.com/libp2p/go-libp2p-peer"

	pstore "github.com/libp2p/go-libp2p-peerstore"
)

type dsProtoBook struct {
	lks  [256]sync.RWMutex
	meta pstore.PeerMetadata
}

var _ pstore.ProtoBook = (*dsProtoBook)(nil)

func NewProtoBook(meta pstore.PeerMetadata) pstore.ProtoBook {
	return &dsProtoBook{meta: meta}
}

func (pb *dsProtoBook) Lock(p peer.ID) {
	b := []byte(p)
	pb.lks[b[len(b)-1]].Lock()
}

func (pb *dsProtoBook) Unlock(p peer.ID) {
	b := []byte(p)
	pb.lks[b[len(b)-1]].Unlock()
}

func (pb *dsProtoBook) RLock(p peer.ID) {
	b := []byte(p)
	pb.lks[b[len(b)-1]].RLock()
}

func (pb *dsProtoBook) RUnlock(p peer.ID) {
	b := []byte(p)
	pb.lks[b[len(b)-1]].RUnlock()
}

func (pb *dsProtoBook) SetProtocols(p peer.ID, protos ...string) error {
	pb.Lock(p)
	defer pb.Unlock(p)

	protomap := make(map[string]struct{}, len(protos))
	for _, proto := range protos {
		protomap[proto] = struct{}{}
	}

	return pb.meta.Put(p, "protocols", protomap)
}

func (pb *dsProtoBook) AddProtocols(p peer.ID, protos ...string) error {
	pb.Lock(p)
	defer pb.Unlock(p)

	pmap, err := pb.getProtocolMap(p)
	if err != nil {
		return err
	}

	for _, proto := range protos {
		pmap[proto] = struct{}{}
	}

	return pb.meta.Put(p, "protocols", pmap)
}

func (pb *dsProtoBook) GetProtocols(p peer.ID) ([]string, error) {
	pb.RLock(p)
	defer pb.RUnlock(p)

	pmap, err := pb.getProtocolMap(p)
	if err != nil {
		return nil, err
	}

	res := make([]string, 0, len(pmap))
	for proto := range pmap {
		res = append(res, proto)
	}

	return res, nil
}

func (pb *dsProtoBook) SupportsProtocols(p peer.ID, protos ...string) ([]string, error) {
	pb.RLock(p)
	defer pb.RUnlock(p)

	pmap, err := pb.getProtocolMap(p)
	if err != nil {
		return nil, err
	}

	res := make([]string, 0, len(protos))
	for _, proto := range protos {
		if _, ok := pmap[proto]; ok {
			res = append(res, proto)
		}
	}

	return res, nil
}

func (pb *dsProtoBook) getProtocolMap(p peer.ID) (map[string]struct{}, error) {
	iprotomap, err := pb.meta.Get(p, "protocols")
	switch err {
	default:
		return nil, err
	case pstore.ErrNotFound:
		return make(map[string]struct{}), nil
	case nil:
		cast, ok := iprotomap.(map[string]struct{})
		if !ok {
			return nil, fmt.Errorf("stored protocol set was not a map")
		}

		return cast, nil
	}
}
