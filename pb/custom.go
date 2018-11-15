package pstore_pb

import (
	"encoding/json"

	peer "github.com/libp2p/go-libp2p-peer"
	pt "github.com/libp2p/go-libp2p-peer/test"
	ma "github.com/multiformats/go-multiaddr"
)

type ProtoPeerID struct {
	peer.ID
}

func NewPopulatedProtoPeerID(r randyPstore) *ProtoPeerID {
	id, _ := pt.RandPeerID()
	return &ProtoPeerID{ID: id}
}

func (id ProtoPeerID) Marshal() ([]byte, error) {
	return []byte(id.ID), nil
}

func (id ProtoPeerID) MarshalTo(data []byte) (n int, err error) {
	return copy(data, []byte(id.ID)), nil
}

func (id ProtoPeerID) MarshalJSON() ([]byte, error) {
	m, _ := id.Marshal()
	return json.Marshal(m)
}

func (id *ProtoPeerID) Unmarshal(data []byte) (err error) {
	id.ID = peer.ID(string(data))
	return nil
}

func (id *ProtoPeerID) UnmarshalJSON(data []byte) error {
	v := new([]byte)
	err := json.Unmarshal(data, v)
	if err != nil {
		return err
	}
	return id.Unmarshal(*v)
}

func (id ProtoPeerID) Size() int {
	return len([]byte(id.ID))
}

type ProtoAddr struct {
	ma.Multiaddr
}

func NewPopulatedProtoAddr(r randyPstore) *ProtoAddr {
	a, _ := ma.NewMultiaddr("/ip4/123.123.123.123/tcp/7001")
	return &ProtoAddr{Multiaddr: a}
}

func (a ProtoAddr) Marshal() ([]byte, error) {
	return a.Bytes(), nil
}

func (a ProtoAddr) MarshalTo(data []byte) (n int, err error) {
	return copy(data, a.Bytes()), nil
}

func (a ProtoAddr) MarshalJSON() ([]byte, error) {
	m, _ := a.Marshal()
	return json.Marshal(m)
}

func (a *ProtoAddr) Unmarshal(data []byte) (err error) {
	a.Multiaddr, err = ma.NewMultiaddrBytes(data)
	return err
}

func (a *ProtoAddr) UnmarshalJSON(data []byte) error {
	v := new([]byte)
	err := json.Unmarshal(data, v)
	if err != nil {
		return err
	}
	return a.Unmarshal(*v)
}

func (a ProtoAddr) Size() int {
	return len(a.Bytes())
}
