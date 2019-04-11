package peerstore

import (
	moved "github.com/libp2p/go-libp2p/skel/peerstore"
)

// Deprecated: use github.com/libp2p/go-libp2p/skel/peerstore.ErrNotFound instead.
var ErrNotFound = moved.ErrNotFound

var (
	// Deprecated: use github.com/libp2p/go-libp2p/skel/peerstore.AddressTTL instead.
	AddressTTL = moved.AddressTTL

	// Deprecated: use github.com/libp2p/go-libp2p/skel/peerstore.TempAddrTTL instead.
	TempAddrTTL = moved.TempAddrTTL

	// Deprecated: use github.com/libp2p/go-libp2p/skel/peerstore.ProviderAddrTTL instead.
	ProviderAddrTTL = moved.ProviderAddrTTL

	// Deprecated: use github.com/libp2p/go-libp2p/skel/peerstore.RecentlyConnectedAddrTTL instead.
	RecentlyConnectedAddrTTL = moved.RecentlyConnectedAddrTTL

	// Deprecated: use github.com/libp2p/go-libp2p/skel/peerstore.OwnObservedAddrTTL instead.
	OwnObservedAddrTTL = moved.OwnObservedAddrTTL
)

const (
	// Deprecated: use github.com/libp2p/go-libp2p/skel/peerstore.PermanentAddrTTL instead.
	PermanentAddrTTL = moved.PermanentAddrTTL

	// Deprecated: use github.com/libp2p/go-libp2p/skel/peerstore.ConnectedAddrTTL instead.
	ConnectedAddrTTL = moved.ConnectedAddrTTL
)

// Deprecated: use github.com/libp2p/go-libp2p/skel/peerstore.Peerstore instead.
type Peerstore = moved.Peerstore

// Deprecated: use github.com/libp2p/go-libp2p/skel/peerstore.Peerstore instead.
type PeerMetadata = moved.PeerMetadata

// Deprecated: use github.com/libp2p/go-libp2p/skel/peerstore.AddrBook instead.
type AddrBook = moved.AddrBook

// Deprecated: use github.com/libp2p/go-libp2p/skel/peerstore.KeyBook instead.
type KeyBook = moved.KeyBook
