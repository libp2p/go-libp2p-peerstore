package pstoremem

import pstore "github.com/libp2p/go-libp2p-peerstore/v3"

// NewPeerstore creates an in-memory threadsafe collection of peers.
func NewPeerstore() pstore.Peerstore {
	return pstore.NewPeerstore(
		NewKeyBook(),
		NewAddrBook(),
		NewPeerMetadata())
}
