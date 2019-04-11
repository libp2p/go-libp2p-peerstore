package peerstore

import (
	moved "github.com/libp2p/go-libp2p-core/peerstore"
)

// Deprecated: use github.com/libp2p/go-libp2p-core/peerstore.ErrNotFound instead.
var ErrNotFound = moved.ErrNotFound

var (
	// Deprecated: use github.com/libp2p/go-libp2p-core/peerstore.AddressTTL instead.
	AddressTTL = moved.AddressTTL

	// Deprecated: use github.com/libp2p/go-libp2p-core/peerstore.TempAddrTTL instead.
	TempAddrTTL = moved.TempAddrTTL

	// Deprecated: use github.com/libp2p/go-libp2p-core/peerstore.ProviderAddrTTL instead.
	ProviderAddrTTL = moved.ProviderAddrTTL

	// Deprecated: use github.com/libp2p/go-libp2p-core/peerstore.RecentlyConnectedAddrTTL instead.
	RecentlyConnectedAddrTTL = moved.RecentlyConnectedAddrTTL

	// Deprecated: use github.com/libp2p/go-libp2p-core/peerstore.OwnObservedAddrTTL instead.
	OwnObservedAddrTTL = moved.OwnObservedAddrTTL
)

const (
	// Deprecated: use github.com/libp2p/go-libp2p-core/peerstore.PermanentAddrTTL instead.
	PermanentAddrTTL = moved.PermanentAddrTTL

	// Deprecated: use github.com/libp2p/go-libp2p-core/peerstore.ConnectedAddrTTL instead.
	ConnectedAddrTTL = moved.ConnectedAddrTTL
)

// Deprecated: use github.com/libp2p/go-libp2p-core/peerstore.Peerstore instead.
type Peerstore = moved.Peerstore

// Deprecated: use github.com/libp2p/go-libp2p-core/peerstore.Peerstore instead.
type PeerMetadata = moved.PeerMetadata

// Deprecated: use github.com/libp2p/go-libp2p-core/peerstore.AddrBook instead.
type AddrBook = moved.AddrBook

// Deprecated: use github.com/libp2p/go-libp2p-core/peerstore.KeyBook instead.
type KeyBook = moved.KeyBook
