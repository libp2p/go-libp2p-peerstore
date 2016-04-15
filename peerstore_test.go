package peer

import (
	"fmt"
	"testing"
	"time"

	ma "github.com/jbenet/go-multiaddr"
	"golang.org/x/net/context"
)

func TestAddrStream(t *testing.T) {
	var addrs []ma.Multiaddr
	for i := 0; i < 100; i++ {
		a, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", i))
		if err != nil {
			t.Fatal(err)
		}

		addrs = append(addrs, a)
	}

	pid := ID("testpeer")

	ps := NewPeerstore()

	ps.AddAddrs(pid, addrs[:10], time.Hour)

	ctx, cancel := context.WithCancel(context.Background())

	addrch := ps.AddrStream(ctx, pid)

	// while that subscription is active, publish ten more addrs
	// this tests that it doesnt hang
	for i := 10; i < 20; i++ {
		ps.AddAddr(pid, addrs[i], time.Hour)
	}

	// now receive them (without hanging)
	timeout := time.After(time.Second * 10)
	for i := 0; i < 20; i++ {
		select {
		case <-addrch:
		case <-timeout:
			t.Fatal("timed out")
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		// now send the rest of the addresses
		for _, a := range addrs[20:80] {
			ps.AddAddr(pid, a, time.Hour)
		}
	}()

	// receive some concurrently with the goroutine
	timeout = time.After(time.Second * 10)
	for i := 0; i < 40; i++ {
		select {
		case <-addrch:
		case <-timeout:
		}
	}

	<-done

	// receive some more after waiting for that goroutine to complete
	timeout = time.After(time.Second * 10)
	for i := 0; i < 20; i++ {
		select {
		case <-addrch:
		case <-timeout:
		}
	}

	// now cancel it, and add a few more addresses it doesnt hang afterwards
	cancel()

	for _, a := range addrs[80:] {
		ps.AddAddr(pid, a, time.Hour)
	}
}
