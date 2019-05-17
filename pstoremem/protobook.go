package pstoremem

import (
	"sync"

	peer "github.com/libp2p/go-libp2p-peer"

	pstore "github.com/libp2p/go-libp2p-peerstore"
)

const maxInternedProtocols = 64
const maxInternedProtocolSize = 128

type protoSegment struct {
	lk        sync.RWMutex
	interned  map[string]string
	protocols map[peer.ID]map[string]struct{}
}

type protoSegments [256]*protoSegment

func (s *protoSegments) get(id peer.ID) *protoSegment {
	b := []byte(id)
	return s[b[len(b)-1]]
}

func (s *protoSegment) internProtocol(proto string) string {
	if len(proto) > maxInternedProtocolSize {
		return proto
	}

	if interned, ok := s.interned[proto]; ok {
		return interned
	}

	if len(s.interned) >= maxInternedProtocols {
		s.interned = make(map[string]string, maxInternedProtocols)
	}

	s.interned[proto] = proto
	return proto
}

type memoryProtoBook struct {
	segments protoSegments
}

var _ pstore.ProtoBook = (*memoryProtoBook)(nil)

func NewProtoBook() pstore.ProtoBook {
	return &memoryProtoBook{
		segments: func() (ret protoSegments) {
			for i := range ret {
				ret[i] = &protoSegment{
					interned:  make(map[string]string),
					protocols: make(map[peer.ID]map[string]struct{}),
				}
			}
			return ret
		}(),
	}
}

func (pb *memoryProtoBook) SetProtocols(p peer.ID, protos ...string) error {
	s := pb.segments.get(p)
	s.lk.Lock()
	defer s.lk.Unlock()

	newprotos := make(map[string]struct{}, len(protos))
	for _, proto := range protos {
		newprotos[s.internProtocol(proto)] = struct{}{}
	}

	s.protocols[p] = newprotos

	return nil
}

func (pb *memoryProtoBook) AddProtocols(p peer.ID, protos ...string) error {
	s := pb.segments.get(p)
	s.lk.Lock()
	defer s.lk.Unlock()

	protomap, ok := s.protocols[p]
	if !ok {
		protomap = make(map[string]struct{})
		s.protocols[p] = protomap
	}

	for _, proto := range protos {
		protomap[s.internProtocol(proto)] = struct{}{}
	}

	return nil
}

func (pb *memoryProtoBook) GetProtocols(p peer.ID) ([]string, error) {
	s := pb.segments.get(p)
	s.lk.RLock()
	defer s.lk.RUnlock()

	out := make([]string, 0, len(s.protocols))
	for k := range s.protocols[p] {
		out = append(out, k)
	}

	return out, nil
}

func (pb *memoryProtoBook) SupportsProtocols(p peer.ID, protos ...string) ([]string, error) {
	s := pb.segments.get(p)
	s.lk.RLock()
	defer s.lk.RUnlock()

	out := make([]string, 0, len(protos))
	for _, proto := range protos {
		if _, ok := s.protocols[p][proto]; ok {
			out = append(out, proto)
		}
	}

	return out, nil
}
