// Deprecated: This package has moved into go-libp2p as a sub-package: github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem.
package pstoremem

import (
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
)

// Deprecated: use github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem.Option instead
type Option = pstoremem.Option

// NewPeerstore creates an in-memory threadsafe collection of peers.
// It's the caller's responsibility to call RemovePeer to ensure
// that memory consumption of the peerstore doesn't grow unboundedly.
// Deprecated: use github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem.NewPeerstore instead
func NewPeerstore(opts ...Option) (ps peerstore.Peerstore, err error) {
	return pstoremem.NewPeerstore(opts...)
}
