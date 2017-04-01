package peerstore

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/libp2p/go-libp2p-peer"
	ma "github.com/multiformats/go-multiaddr"
)

// PeerInfo is a small struct used to pass around a peer with
// a set of addresses (and later, keys?). This is not meant to be
// a complete view of the system, but rather to model updates to
// the peerstore. It is used by things like the routing system.
type PeerInfo struct {
	ID    peer.ID
	Addrs []ma.Multiaddr
}

var ErrInvalidAddr = fmt.Errorf("invalid p2p multiaddr")

func InfoFromP2pAddr(m ma.Multiaddr) (*PeerInfo, error) {
	if m == nil {
		return nil, ErrInvalidAddr
	}

	// make sure it's an IPFS addr
	parts := ma.Split(m)
	if len(parts) < 1 {
		return nil, ErrInvalidAddr
	}

	ipfspart := parts[len(parts)-1] // last part
	if ipfspart.Protocols()[0].Code != ma.P_IPFS {
		return nil, ErrInvalidAddr
	}

	// make sure 'ipfs id' parses as a peer.ID
	peerIdParts := strings.Split(ipfspart.String(), "/")
	peerIdStr := peerIdParts[len(peerIdParts)-1]
	id, err := peer.IDB58Decode(peerIdStr)
	if err != nil {
		return nil, err
	}

	return &PeerInfo{
		ID:    id,
		Addrs: []ma.Multiaddr{ma.Join(parts[:len(parts)-1]...)},
	}, nil
}

func (pi *PeerInfo) Loggable() map[string]interface{} {
	return map[string]interface{}{
		"peerID": pi.ID.Pretty(),
		"addrs":  pi.Addrs,
	}
}

func (pi *PeerInfo) MarshalJSON() ([]byte, error) {
	out := make(map[string]interface{})
	out["ID"] = pi.ID.Pretty()
	var addrs []string
	for _, a := range pi.Addrs {
		addrs = append(addrs, a.String())
	}
	out["Addrs"] = addrs
	return json.Marshal(out)
}

func (pi *PeerInfo) UnmarshalJSON(b []byte) error {
	var data map[string]interface{}
	err := json.Unmarshal(b, &data)
	if err != nil {
		return err
	}
	pid, err := peer.IDB58Decode(data["ID"].(string))
	if err != nil {
		return err
	}
	pi.ID = pid
	addrs, ok := data["Addrs"].([]interface{})
	if ok {
		for _, a := range addrs {
			pi.Addrs = append(pi.Addrs, ma.StringCast(a.(string)))
		}
	}
	return nil
}
