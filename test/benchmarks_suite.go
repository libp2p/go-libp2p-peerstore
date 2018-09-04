package test

import (
	"context"
	"fmt"
	"testing"

	pstore "github.com/libp2p/go-libp2p-peerstore"
)

var peerstoreBenchmarks = map[string]func(pstore.Peerstore, chan *peerpair) func(*testing.B){
	"AddAddr":  benchmarkAddAddr,
	"AddAddrs": benchmarkAddAddrs,
	"SetAddrs": benchmarkSetAddrs,
	"GetAddrs": benchmarkGetAddrs,
}

func BenchmarkPeerstore(b *testing.B, factory PeerstoreFactory, variant string) {
	// Parameterises benchmarks to tackle peers with 1, 10, 100 multiaddrs.
	params := []struct {
		n  int
		ch chan *peerpair
	}{
		{1, make(chan *peerpair, 100)},
		{10, make(chan *peerpair, 100)},
		{100, make(chan *peerpair, 100)},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start all test peer producing goroutines, where each produces peers with as many
	// multiaddrs as the n field in the param struct.
	for _, p := range params {
		go addressProducer(ctx, b, p.ch, p.n)
	}

	for name, bench := range peerstoreBenchmarks {
		for _, p := range params {
			// Create a new peerstore.
			ps, closeFunc := factory()

			// Run the test.
			b.Run(fmt.Sprintf("%s-%dAddrs-%s", name, p.n, variant), bench(ps, p.ch))

			// Cleanup.
			if closeFunc != nil {
				closeFunc()
			}
		}
	}
}
