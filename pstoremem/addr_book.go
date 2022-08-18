package pstoremem

import (
	"time"

	pstore "github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
)

// Deprecated: use github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem.NewAddrBook instead
func NewAddrBook() pstore.AddrBook {
	return pstoremem.NewAddrBook()
}

// Deprecated: use github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem.AddrBookOption instead
type AddrBookOption = pstoremem.AddrBookOption

type clock interface {
	Now() time.Time
}

// Deprecated: use github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem.WithClock instead
func WithClock(clock clock) AddrBookOption {
	return pstoremem.WithClock(clock)
}

// An abstracted, pub-sub manager for address streams. Extracted from
// memoryAddrBook in order to support additional implementations.
// Deprecated: use github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem.AddrSubManager instead
type AddrSubManager = pstoremem.AddrSubManager

// NewAddrSubManager initializes an AddrSubManager.
// Deprecated: use github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem.NewAddrSubManager instead
func NewAddrSubManager() *AddrSubManager {
	return pstoremem.NewAddrSubManager()
}
