package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/libp2p/go-libp2p-peer"
	pt "github.com/libp2p/go-libp2p-peer/test"
	ma "github.com/multiformats/go-multiaddr"
)

func peerId(ids string) peer.ID {
	id, err := peer.IDB58Decode(ids)
	if err != nil {
		panic(err)
	}
	return id
}

func multiaddr(m string) ma.Multiaddr {
	maddr, err := ma.NewMultiaddr(m)
	if err != nil {
		panic(err)
	}
	return maddr
}

type peerpair struct {
	ID   peer.ID
	Addr ma.Multiaddr
}

func randomPeer(b *testing.B) *peerpair {
	var pid peer.ID
	var err error
	var addr ma.Multiaddr

	if pid, err = pt.RandPeerID(); err != nil {
		b.Fatal(err)
	}

	if addr, err = ma.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/6666/ipfs/%s", pid.Pretty())); err != nil {
		b.Fatal(err)
	}

	return &peerpair{pid, addr}
}

func addressProducer(ctx context.Context, b *testing.B, addrs chan *peerpair) {
	defer close(addrs)
	for {
		p := randomPeer(b)
		select {
		case addrs <- p:
		case <-ctx.Done():
			return
		}
	}
}