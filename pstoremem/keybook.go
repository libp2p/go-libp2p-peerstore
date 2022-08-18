package pstoremem

import (
	pstore "github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
)

// Deprecated: use github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem.NewKeyBook instead
func NewKeyBook() pstore.KeyBook {
	return pstoremem.NewKeyBook()
}
