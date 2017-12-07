package peerstore

import (
	"context"
	"testing"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-peer"
	ma "github.com/multiformats/go-multiaddr"
)

func IDS(t *testing.T, ids string) peer.ID {
	id, err := peer.IDB58Decode(ids)
	if err != nil {
		t.Fatalf("id %q is bad: %s", ids, err)
	}
	return id
}

func MA(t *testing.T, m string) ma.Multiaddr {
	maddr, err := ma.NewMultiaddr(m)
	if err != nil {
		t.Fatal(err)
	}
	return maddr
}

func testHas(t *testing.T, exp, act []ma.Multiaddr) {
	if len(exp) != len(act) {
		t.Fatal("lengths not the same")
	}

	for _, a := range exp {
		found := false

		for _, b := range act {
			if a.Equal(b) {
				found = true
				break
			}
		}

		if !found {
			t.Fatalf("expected address %s not found", a)
		}
	}
}

func testPeerHas(t *testing.T, mgr *AddrManager, exp []ma.Multiaddr, p peer.ID) {
	addrs, _ := mgr.Addrs(p)
	testHas(t, exp, addrs)
}

func TestAddresses(t *testing.T) {

	id1 := IDS(t, "QmcNstKuwBBoVTpSCSDrwzjgrRcaYXK833Psuz2EMHwyQN")
	id2 := IDS(t, "QmRmPL3FDZKE3Qiwv1RosLdwdvbvg17b2hB39QPScgWKKZ")
	id3 := IDS(t, "QmPhi7vBsChP7sjRoZGgg7bcKqF6MmCcQwvRbDte8aJ6Kn")
	id4 := IDS(t, "QmPhi7vBsChP7sjRoZGgg7bcKqF6MmCcQwvRbDte8aJ5Kn")
	id5 := IDS(t, "QmPhi7vBsChP7sjRoZGgg7bcKqF6MmCcQwvRbDte8aJ5Km")

	ma11 := MA(t, "/ip4/1.2.3.1/tcp/1111")
	ma21 := MA(t, "/ip4/2.2.3.2/tcp/1111")
	ma22 := MA(t, "/ip4/2.2.3.2/tcp/2222")
	ma31 := MA(t, "/ip4/3.2.3.3/tcp/1111")
	ma32 := MA(t, "/ip4/3.2.3.3/tcp/2222")
	ma33 := MA(t, "/ip4/3.2.3.3/tcp/3333")
	ma41 := MA(t, "/ip4/4.2.3.3/tcp/1111")
	ma42 := MA(t, "/ip4/4.2.3.3/tcp/2222")
	ma43 := MA(t, "/ip4/4.2.3.3/tcp/3333")
	ma44 := MA(t, "/ip4/4.2.3.3/tcp/4444")
	ma51 := MA(t, "/ip4/5.2.3.3/tcp/1111")
	ma52 := MA(t, "/ip4/5.2.3.3/tcp/2222")
	ma53 := MA(t, "/ip4/5.2.3.3/tcp/3333")
	ma54 := MA(t, "/ip4/5.2.3.3/tcp/4444")
	ma55 := MA(t, "/ip4/5.2.3.3/tcp/5555")

	ttl := time.Hour
	m := newAddrManager(context.Background(), ds.NewMapDatastore())
	err := m.AddAddr(id1, ma11, ttl)
	if err != nil {
		t.Fatal("error adding address:", err)
	}

	m.AddAddrs(id2, []ma.Multiaddr{ma21, ma22}, ttl)
	m.AddAddrs(id2, []ma.Multiaddr{ma21, ma22}, ttl) // idempotency

	m.AddAddr(id3, ma31, ttl)
	m.AddAddr(id3, ma32, ttl)
	m.AddAddr(id3, ma33, ttl)
	m.AddAddr(id3, ma33, ttl) // idempotency
	m.AddAddr(id3, ma33, ttl)

	m.AddAddrs(id4, []ma.Multiaddr{ma41, ma42, ma43, ma44}, ttl) // multiple

	m.AddAddrs(id5, []ma.Multiaddr{ma21, ma22}, ttl)             // clearing
	m.AddAddrs(id5, []ma.Multiaddr{ma41, ma42, ma43, ma44}, ttl) // clearing
	m.ClearAddrs(id5)
	m.AddAddrs(id5, []ma.Multiaddr{ma51, ma52, ma53, ma54, ma55}, ttl) // clearing

	peers, _ := m.Peers()
	if len(peers) != 5 {
		t.Fatalf("should have %d peers in the address book, has %d", 5, len(peers))
	}

	// test the Addresses return value
	testPeerHas(t, m, []ma.Multiaddr{ma11}, id1)
	testPeerHas(t, m, []ma.Multiaddr{ma21, ma22}, id2)
	testPeerHas(t, m, []ma.Multiaddr{ma31, ma32, ma33}, id3)
	testPeerHas(t, m, []ma.Multiaddr{ma41, ma42, ma43, ma44}, id4)
	testPeerHas(t, m, []ma.Multiaddr{ma51, ma52, ma53, ma54, ma55}, id5)

}

func TestAddressesExpire(t *testing.T) {

	id1 := IDS(t, "QmcNstKuwBBoVTpSCSDrwzjgrRcaYXK833Psuz2EMHwyQN")
	id2 := IDS(t, "QmcNstKuwBBoVTpSCSDrwzjgrRcaYXK833Psuz2EMHwyQM")
	ma11 := MA(t, "/ip4/1.2.3.1/tcp/1111")
	ma12 := MA(t, "/ip4/2.2.3.2/tcp/2222")
	ma13 := MA(t, "/ip4/3.2.3.3/tcp/3333")
	ma24 := MA(t, "/ip4/4.2.3.3/tcp/4444")
	ma25 := MA(t, "/ip4/5.2.3.3/tcp/5555")

	m := newAddrManager(context.Background(), ds.NewMapDatastore())
	m.AddAddr(id1, ma11, time.Hour)
	m.AddAddr(id1, ma12, time.Hour)
	m.AddAddr(id1, ma13, time.Hour)
	m.AddAddr(id2, ma24, time.Hour)
	m.AddAddr(id2, ma25, time.Hour)

	peers, _ := m.Peers()
	if len(peers) != 2 {
		t.Fatalf("should have %d peers in the address book, has %d", 2, len(peers))
	}

	testPeerHas(t, m, []ma.Multiaddr{ma11, ma12, ma13}, id1)
	testPeerHas(t, m, []ma.Multiaddr{ma24, ma25}, id2)

	m.SetAddr(id1, ma11, 2*time.Hour)
	m.SetAddr(id1, ma12, 2*time.Hour)
	m.SetAddr(id1, ma13, 2*time.Hour)
	m.SetAddr(id2, ma24, 2*time.Hour)
	m.SetAddr(id2, ma25, 2*time.Hour)

	testPeerHas(t, m, []ma.Multiaddr{ma11, ma12, ma13}, id1)
	testPeerHas(t, m, []ma.Multiaddr{ma24, ma25}, id2)

	m.SetAddr(id1, ma11, time.Millisecond)
	<-time.After(time.Millisecond)
	testPeerHas(t, m, []ma.Multiaddr{ma12, ma13}, id1)
	testPeerHas(t, m, []ma.Multiaddr{ma24, ma25}, id2)

	m.SetAddr(id1, ma13, time.Millisecond)
	<-time.After(time.Millisecond)
	testPeerHas(t, m, []ma.Multiaddr{ma12}, id1)
	testPeerHas(t, m, []ma.Multiaddr{ma24, ma25}, id2)

	m.SetAddr(id2, ma24, time.Millisecond)
	<-time.After(time.Millisecond)
	testPeerHas(t, m, []ma.Multiaddr{ma12}, id1)
	testPeerHas(t, m, []ma.Multiaddr{ma25}, id2)

	m.SetAddr(id2, ma25, time.Millisecond)
	<-time.After(time.Millisecond)
	testPeerHas(t, m, []ma.Multiaddr{ma12}, id1)
	testPeerHas(t, m, nil, id2)

	m.SetAddr(id1, ma12, time.Millisecond)
	<-time.After(time.Millisecond)
	testPeerHas(t, m, nil, id1)
	testPeerHas(t, m, nil, id2)
}

func TestClearWorks(t *testing.T) {

	id1 := IDS(t, "QmcNstKuwBBoVTpSCSDrwzjgrRcaYXK833Psuz2EMHwyQN")
	id2 := IDS(t, "QmcNstKuwBBoVTpSCSDrwzjgrRcaYXK833Psuz2EMHwyQM")
	ma11 := MA(t, "/ip4/1.2.3.1/tcp/1111")
	ma12 := MA(t, "/ip4/2.2.3.2/tcp/2222")
	ma13 := MA(t, "/ip4/3.2.3.3/tcp/3333")
	ma24 := MA(t, "/ip4/4.2.3.3/tcp/4444")
	ma25 := MA(t, "/ip4/5.2.3.3/tcp/5555")

	m := newAddrManager(context.Background(), ds.NewMapDatastore())
	m.AddAddr(id1, ma11, time.Hour)
	m.AddAddr(id1, ma12, time.Hour)
	m.AddAddr(id1, ma13, time.Hour)
	m.AddAddr(id2, ma24, time.Hour)
	m.AddAddr(id2, ma25, time.Hour)

	testPeerHas(t, m, []ma.Multiaddr{ma11, ma12, ma13}, id1)
	testPeerHas(t, m, []ma.Multiaddr{ma24, ma25}, id2)

	m.ClearAddrs(id1)
	m.ClearAddrs(id2)

	testPeerHas(t, m, nil, id1)
	testPeerHas(t, m, nil, id2)
}

func TestClearWorksForDatastoreResidentRecords(t *testing.T) {
	old := lruCacheSize
	defer func() { lruCacheSize = old }()

	id1 := IDS(t, "QmQ4PU94ZKknywdLuyZ9SVuZd8tTkGPGFfbYN2nJuNZbWP")
	id2 := IDS(t, "QmU7WhKm6Y3T4ZrYeg4BHnAApHEqNci1fMxTAvsjaynSJN")
	ma1 := MA(t, "/ip4/1.2.3.1/tcp/1111")
	ma2 := MA(t, "/ip4/2.4.3.1/tcp/2222")

	m := newAddrManager(context.Background(), ds.NewMapDatastore())
	m.AddAddr(id1, ma1, time.Hour)
	// make record for id1 go to datastore
	m.AddAddr(id2, ma2, time.Hour)

	m.ClearAddrs(id1)

	testPeerHas(t, m, nil, id1)
	testPeerHas(t, m, []ma.Multiaddr{ma2}, id2)
}

func TestSetNegativeTTLClears(t *testing.T) {
	id1 := IDS(t, "QmcNstKuwBBoVTpSCSDrwzjgrRcaYXK833Psuz2EMHwyQN")
	ma11 := MA(t, "/ip4/1.2.3.1/tcp/1111")

	m := newAddrManager(context.Background(), ds.NewMapDatastore())
	m.SetAddr(id1, ma11, time.Hour)

	testPeerHas(t, m, []ma.Multiaddr{ma11}, id1)

	m.SetAddr(id1, ma11, -1)

	testPeerHas(t, m, nil, id1)
}

func TestNilAddrsDontBreak(t *testing.T) {
	id1 := IDS(t, "QmcNstKuwBBoVTpSCSDrwzjgrRcaYXK833Psuz2EMHwyQN")
	m := newAddrManager(context.Background(), ds.NewMapDatastore())
	m.SetAddr(id1, nil, time.Hour)
	m.AddAddr(id1, nil, time.Hour)
}
