package pstoremem

import (
	"errors"
	"fmt"
	"io"

	"github.com/libp2p/go-libp2p-core/event"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"

	pstore "github.com/libp2p/go-libp2p-peerstore"
	"github.com/libp2p/go-libp2p-peerstore/pstoremanager"
)

type pstoremem struct {
	peerstore.Metrics

	*memoryKeyBook
	*memoryAddrBook
	*memoryProtoBook
	*memoryPeerMetadata

	eventBus event.Bus
	manager  *pstoremanager.PeerstoreManager
}

var _ peerstore.Peerstore = &pstoremem{}

type Option interface{}
type PeerstoreOption func(*pstoremem) error

// WithEventBus sets the eventBus that is used to subscribe to EvtPeerConnectednessChanged events.
// This allows the automatic clean up when a peer disconnect.
func WithEventBus(eventBus event.Bus) PeerstoreOption {
	return func(ps *pstoremem) error {
		ps.eventBus = eventBus
		return nil
	}
}

// NewPeerstore creates an in-memory threadsafe collection of peers.
// It is recommended to construct the peerstore with an event bus, using the WithEventBus option.
// In that case, the peerstore will automatically perform cleanups when a peer disconnects
// (see the pstoremanager package for details).
// If constructed without an event bus, it's the caller's responsibility to call RemovePeer to ensure
// that memory consumption of the peerstore doesn't grow unboundedly.
func NewPeerstore(opts ...Option) (*pstoremem, error) {
	var (
		protoBookOpts []ProtoBookOption
		peerstoreOpts []PeerstoreOption
		managerOpts   []pstoremanager.Option
	)
	for _, opt := range opts {
		switch o := opt.(type) {
		case PeerstoreOption:
			peerstoreOpts = append(peerstoreOpts, o)
		case ProtoBookOption:
			protoBookOpts = append(protoBookOpts, o)
		case pstoremanager.Option:
			managerOpts = append(managerOpts, o)
		default:
			return nil, fmt.Errorf("unexpected peer store option: %v", o)
		}
	}
	pb, err := NewProtoBook(protoBookOpts...)
	if err != nil {
		return nil, err
	}
	pstore := &pstoremem{
		Metrics:            pstore.NewMetrics(),
		memoryKeyBook:      NewKeyBook(),
		memoryAddrBook:     NewAddrBook(),
		memoryProtoBook:    pb,
		memoryPeerMetadata: NewPeerMetadata(),
	}
	for _, opt := range peerstoreOpts {
		if err := opt(pstore); err != nil {
			return nil, err
		}
	}
	if pstore.eventBus == nil && len(managerOpts) > 0 {
		return nil, errors.New("peer store manager options set an event bus")
	}
	if pstore.eventBus != nil {
		manager, err := pstoremanager.NewPeerstoreManager(pstore, pstore.eventBus, managerOpts...)
		if err != nil {
			pstore.Close()
			return nil, err
		}
		pstore.manager = manager
	}
	return pstore, nil
}

func (ps *pstoremem) Start() {
	ps.manager.Start()
}

func (ps *pstoremem) Close() (err error) {
	var errs []error
	weakClose := func(name string, c interface{}) {
		if cl, ok := c.(io.Closer); ok {
			if err = cl.Close(); err != nil {
				errs = append(errs, fmt.Errorf("%s error: %s", name, err))
			}
		}
	}
	if ps.manager != nil {
		weakClose("manager", ps.manager)
	}
	weakClose("keybook", ps.memoryKeyBook)
	weakClose("addressbook", ps.memoryAddrBook)
	weakClose("protobook", ps.memoryProtoBook)
	weakClose("peermetadata", ps.memoryPeerMetadata)

	if len(errs) > 0 {
		return fmt.Errorf("failed while closing peerstore; err(s): %q", errs)
	}
	return nil
}

func (ps *pstoremem) Peers() peer.IDSlice {
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

func (ps *pstoremem) PeerInfo(p peer.ID) peer.AddrInfo {
	return peer.AddrInfo{
		ID:    p,
		Addrs: ps.memoryAddrBook.Addrs(p),
	}
}

// RemovePeer removes entries associated with a peer from:
// * the KeyBook
// * the ProtoBook
// * the PeerMetadata
// * the Metrics
// It DOES NOT remove the peer from the AddrBook.
// It is only necessary to call this function if the peerstore was constructed without an event bus.
// If the peerstore was constructed with an event bus, peers are removed
// automatically when they disconnect (after a grace period).
func (ps *pstoremem) RemovePeer(p peer.ID) {
	ps.memoryKeyBook.RemovePeer(p)
	ps.memoryProtoBook.RemovePeer(p)
	ps.memoryPeerMetadata.RemovePeer(p)
	ps.Metrics.RemovePeer(p)
}
