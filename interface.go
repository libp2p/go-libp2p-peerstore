package peerstore

import (
	"context"
	"time"

	"gx/ipfs/QmVmDhyTTUcQXFD1rRQ64fGLMSAoaQvNH3hwuaCFAPq2hy/errors"

	ic "github.com/libp2p/go-libp2p-crypto"
	"github.com/libp2p/go-libp2p-peer"
	ma "github.com/multiformats/go-multiaddr"
)

var ErrNotFound = errors.New("item not found")

// Peerstore provides a threadsafe store of Peer related
// information.
type Peerstore interface {
	AddrBook
	KeyBook
	PeerMetadata
	Metrics

	// PeerInfo returns a peer.PeerInfo struct for given peer.ID.
	// This is a small slice of the information Peerstore has on
	// that peer, useful to other services.
	PeerInfo(peer.ID) PeerInfo

	GetProtocols(peer.ID) ([]string, error)
	AddProtocols(peer.ID, ...string) error
	SetProtocols(peer.ID, ...string) error
	SupportsProtocols(peer.ID, ...string) ([]string, error)

	// Peers returns all of the peer IDs stored across all inner stores.
	Peers() []peer.ID
}

type PeerMetadata interface {
	// Get/Put is a simple registry for other peer-related key/value pairs.
	// if we find something we use often, it should become its own set of
	// methods. this is a last resort.
	Get(p peer.ID, key string) (interface{}, error)
	Put(p peer.ID, key string, val interface{}) error
}

// AddrBook is an interface that fits the new AddrManager. I'm patching
// it up in here to avoid changing a ton of the codebase.
type AddrBook interface {

	// AddAddr calls AddAddrs(p, []ma.Multiaddr{addr}, ttl)
	AddAddr(p peer.ID, addr ma.Multiaddr, ttl time.Duration)

	// AddAddrs gives AddrManager addresses to use, with a given ttl
	// (time-to-live), after which the address is no longer valid.
	// If the manager has a longer TTL, the operation is a no-op for that address
	AddAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration)

	// SetAddr calls mgr.SetAddrs(p, addr, ttl)
	SetAddr(p peer.ID, addr ma.Multiaddr, ttl time.Duration)

	// SetAddrs sets the ttl on addresses. This clears any TTL there previously.
	// This is used when we receive the best estimate of the validity of an address.
	SetAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration)

	// UpdateAddrs updates the addresses associated with the given peer that have
	// the given oldTTL to have the given newTTL.
	UpdateAddrs(p peer.ID, oldTTL time.Duration, newTTL time.Duration)

	// Addresses returns all known (and valid) addresses for a given peer
	Addrs(p peer.ID) []ma.Multiaddr

	// AddrStream returns a channel that gets all addresses for a given
	// peer sent on it. If new addresses are added after the call is made
	// they will be sent along through the channel as well.
	AddrStream(context.Context, peer.ID) <-chan ma.Multiaddr

	// ClearAddresses removes all previously stored addresses
	ClearAddrs(p peer.ID)

	// Peers returns all of the peer IDs stored in the AddrBook
	AddrsPeers() []peer.ID
}

// KeyBook tracks the Public keys of Peers.
type KeyBook interface {
	KeyBookPeers() []peer.ID

	PubKey(peer.ID) ic.PubKey
	AddPubKey(peer.ID, ic.PubKey) error

	PrivKey(peer.ID) ic.PrivKey
	AddPrivKey(peer.ID, ic.PrivKey) error
}
