package peerstore

import (
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore"
)

// LatencyEWMASmoothing governs the decay of the EWMA (the speed
// at which it changes). This must be a normalized (0-1) value.
// 1 is 100% change, 0 is no change.
// Deprecated: use github.com/libp2p/go-libp2p/p2p/host/peerstore.LatencyEWMASmoothing instead
var LatencyEWMASmoothing = peerstore.LatencyEWMASmoothing

type metrics interface {
	RecordLatency(peer.ID, time.Duration)
	LatencyEWMA(peer.ID) time.Duration
	RemovePeer(p peer.ID)
}

// Deprecated: use github.com/libp2p/go-libp2p/p2p/host/peerstore.NewMetrics instead
func NewMetrics() metrics {
	return peerstore.NewMetrics()
}
