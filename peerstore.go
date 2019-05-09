package peerstore

import (
	"fmt"
	"io"
	"sync"

	peer "github.com/libp2p/go-libp2p-peer"
)

var _ Peerstore = (*peerstore)(nil)

const maxInternedProtocols = 64
const maxInternedProtocolSize = 128

type segment struct {
	lk        sync.RWMutex
	interned  map[string]string
	protocols map[peer.ID]map[string]struct{}
}

type segments [256]*segment

func (s *segments) get(id peer.ID) *segment {
	b := []byte(id)
	return s[b[len(b)-1]]
}

func (s *segment) internProtocol(proto string) string {
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

type peerstore struct {
	Metrics

	KeyBook
	AddrBook
	PeerMetadata

	// segments for protocol information
	segments segments
}

// NewPeerstore creates a data structure that stores peer data, backed by the
// supplied implementations of KeyBook, AddrBook and PeerMetadata.
func NewPeerstore(kb KeyBook, ab AddrBook, md PeerMetadata) Peerstore {
	return &peerstore{
		KeyBook:      kb,
		AddrBook:     ab,
		PeerMetadata: md,
		Metrics:      NewMetrics(),
		segments: func() (ret segments) {
			for i := range ret {
				ret[i] = &segment{
					interned:  make(map[string]string),
					protocols: make(map[peer.ID]map[string]struct{}),
				}
			}
			return ret
		}(),
	}
}

func (ps *peerstore) Close() (err error) {
	var errs []error
	weakClose := func(name string, c interface{}) {
		if cl, ok := c.(io.Closer); ok {
			if err = cl.Close(); err != nil {
				errs = append(errs, fmt.Errorf("%s error: %s", name, err))
			}
		}
	}

	weakClose("keybook", ps.KeyBook)
	weakClose("addressbook", ps.AddrBook)
	weakClose("peermetadata", ps.PeerMetadata)

	if len(errs) > 0 {
		return fmt.Errorf("failed while closing peerstore; err(s): %q", errs)
	}
	return nil
}

func (ps *peerstore) Peers() peer.IDSlice {
	set := map[peer.ID]struct{}{}
	for _, p := range ps.PeersWithKeys() {
		set[p] = struct{}{}
	}
	for _, p := range ps.PeersWithAddrs() {
		set[p] = struct{}{}
	}

	pps := make(peer.IDSlice, 0, len(set))
	for p := range set {
		pps = append(pps, p)
	}
	return pps
}

func (ps *peerstore) PeerInfo(p peer.ID) PeerInfo {
	return PeerInfo{
		ID:    p,
		Addrs: ps.AddrBook.Addrs(p),
	}
}

func (ps *peerstore) SetProtocols(p peer.ID, protos ...string) error {
	s := ps.segments.get(p)
	s.lk.Lock()
	defer s.lk.Unlock()

	newprotos := make(map[string]struct{}, len(protos))
	for _, proto := range protos {
		newprotos[s.internProtocol(proto)] = struct{}{}
	}

	s.protocols[p] = newprotos

	return nil
}

func (ps *peerstore) AddProtocols(p peer.ID, protos ...string) error {
	s := ps.segments.get(p)
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

func (ps *peerstore) GetProtocols(p peer.ID) ([]string, error) {
	s := ps.segments.get(p)
	s.lk.RLock()
	defer s.lk.RUnlock()

	out := make([]string, 0, len(s.protocols))
	for k := range s.protocols[p] {
		out = append(out, k)
	}

	return out, nil
}

func (ps *peerstore) SupportsProtocols(p peer.ID, protos ...string) ([]string, error) {
	s := ps.segments.get(p)
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

func PeerInfos(ps Peerstore, peers peer.IDSlice) []PeerInfo {
	pi := make([]PeerInfo, len(peers))
	for i, p := range peers {
		pi[i] = ps.PeerInfo(p)
	}
	return pi
}

func PeerInfoIDs(pis []PeerInfo) peer.IDSlice {
	ps := make(peer.IDSlice, len(pis))
	for i, pi := range pis {
		ps[i] = pi.ID
	}
	return ps
}
