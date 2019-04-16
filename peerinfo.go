package peerstore

import moved "github.com/libp2p/go-libp2p-core/peer"

// Deprecated: Use github.com/libp2p/go-libp2p-core/peer.Info instead.
type PeerInfo = moved.Info

// Deprecated: Use github.com/libp2p/go-libp2p-core/peer.ErrInvalidAddr instead.
var ErrInvalidAddr = moved.ErrInvalidAddr

// Deprecated: Use github.com/libp2p/go-libp2p-core/peer.InfoFromP2pAddr instead.
var InfoFromP2pAddr = moved.InfoFromP2pAddr

// Deprecated: Use github.com/libp2p/go-libp2p-core/peer.InfoToP2pAddrs instead.
var InfoToP2pAddrs = moved.InfoToP2pAddrs
