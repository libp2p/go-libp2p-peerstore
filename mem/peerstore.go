package mem

import pstore "github.com/libp2p/go-libp2p-peerstore"

// NewPeerstore creates an in-memory threadsafe collection of peers.
func NewPeerstore() pstore.Peerstore {
	return pstore.NewPeerstoreWith(
		NewKeyBook(),
		NewAddrBook(),
		NewPeerMetadata())
}
