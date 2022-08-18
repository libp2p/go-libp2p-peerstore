package pstoremem

import (
	pstore "github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
)

// Deprecated: use github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem.ProtoBookOption instead
type ProtoBookOption = pstoremem.ProtoBookOption

// Deprecated: use github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem.WithMaxProtocols instead
func WithMaxProtocols(num int) ProtoBookOption {
	return pstoremem.WithMaxProtocols(num)
}

// Deprecated: use github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem.NewProtoBook instead
func NewProtoBook(opts ...ProtoBookOption) (pstore.ProtoBook, error) {
	return pstoremem.NewProtoBook(opts...)
}
