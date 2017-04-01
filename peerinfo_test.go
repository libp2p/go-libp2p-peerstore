package peerstore

import (
	"testing"

	peer "github.com/libp2p/go-libp2p-peer"
	ma "github.com/multiformats/go-multiaddr"
)

func mustAddr(t *testing.T, s string) ma.Multiaddr {
	addr, err := ma.NewMultiaddr(s)
	if err != nil {
		t.Fatal(err)
	}

	return addr
}

func TestPeerInfoMarshal(t *testing.T) {
	a := mustAddr(t, "/ip4/1.2.3.4/tcp/4536")
	b := mustAddr(t, "/ip4/1.2.3.8/udp/7777")
	id, err := peer.IDB58Decode("QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ")
	if err != nil {
		t.Fatal(err)
	}

	pi := &PeerInfo{
		ID:    id,
		Addrs: []ma.Multiaddr{a, b},
	}

	data, err := pi.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	pi2 := new(PeerInfo)
	if err := pi2.UnmarshalJSON(data); err != nil {
		t.Fatal(err)
	}

	if pi2.ID != pi.ID {
		t.Fatal("ids didnt match after marshal")
	}

	if !pi.Addrs[0].Equal(pi2.Addrs[0]) {
		t.Fatal("wrong addrs")
	}

	if !pi.Addrs[1].Equal(pi2.Addrs[1]) {
		t.Fatal("wrong addrs")
	}

	lgbl := pi2.Loggable()
	if lgbl["peerID"] != id.Pretty() {
		t.Fatal("loggables gave wrong peerID output")
	}
}

func TestP2pAddrParsing(t *testing.T) {
	a := mustAddr(t, "/ip4/1.2.3.4/tcp/4536")
	id, err := peer.IDB58Decode("QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ")
	if err != nil {
		t.Fatal(err)
	}

	p2pa := a.String() + "/ipfs/" + id.Pretty()
	p2pma, err := ma.NewMultiaddr(p2pa)
	if err != nil {
		t.Fatal(err)
	}

	pinfo, err := InfoFromP2pAddr(p2pma)
	if err != nil {
		t.Fatal(err)
	}

	if pinfo.ID != id {
		t.Fatal("didnt get expected peerID")
	}

	if !a.Equal(pinfo.Addrs[0]) {
		t.Fatal("didnt get expected address")
	}
}
