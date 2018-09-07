package test

import (
	"github.com/libp2p/go-libp2p-peer"
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
