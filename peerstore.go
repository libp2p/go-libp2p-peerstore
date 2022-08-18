// Deprecated: This package has moved into go-libp2p as a sub-package: github.com/libp2p/go-libp2p/p2p/host/peerstore.
package peerstore

import (
	"github.com/libp2p/go-libp2p/core/peer"
	pstore "github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore"
)

// Deprecated: use github.com/libp2p/go-libp2p/p2p/host/peerstore.PeerInfos instead
func PeerInfos(ps pstore.Peerstore, peers peer.IDSlice) []peer.AddrInfo {
	return peerstore.PeerInfos(ps, peers)
}

// Deprecated: use github.com/libp2p/go-libp2p/p2p/host/peerstore.PeerInfoIDs instead
func PeerInfoIDs(pis []peer.AddrInfo) peer.IDSlice {
	return peerstore.PeerInfoIDs(pis)
}
