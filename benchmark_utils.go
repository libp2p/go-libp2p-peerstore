package peerstore

import (
	cr "crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/mr-tron/base58/base58"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
	"math/rand"
)

type peerpair struct {
	ID   string
	Addr ma.Multiaddr
}

func randomPeer(b *testing.B) *peerpair {
	buf := make([]byte, 50)

	for {
		n, err := cr.Read(buf)
		if err != nil {
			b.Fatal(err)
		}
		if n > 0 {
			break
		}
	}

	id, err := mh.Encode(buf, mh.SHA2_256)
	if err != nil {
		b.Fatal(err)
	}
	b58ID := base58.Encode(id)

	addr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/6666/ipfs/%s", b58ID))
	if err != nil {
		b.Fatal(err)
	}

	return &peerpair{b58ID, addr}
}

func addressProducer(b *testing.B, addrs chan *peerpair, n int) {
	defer close(addrs)
	for i := 0; i < n; i++ {
		p := randomPeer(b)
		addrs <- p
	}
}

func rateLimitedAddressProducer(b *testing.B, addrs chan *peerpair, producer chan *peerpair, n int, avgTime time.Duration, errBound time.Duration) {
	defer close(addrs)

	go addressProducer(b, producer, n)

	eb := int64(errBound)
	for {
		addr, ok := <-producer
		if !ok {
			break
		}
		addrs <- addr
		wiggle := time.Duration(0)
		if eb > 0 {
			wiggle = time.Duration(rand.Int63n(eb*2) - eb)
		}
		sleepDur := avgTime + wiggle
		time.Sleep(sleepDur)
	}
}
