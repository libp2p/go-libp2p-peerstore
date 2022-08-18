package pstoreds

import (
	"github.com/libp2p/go-libp2p/core/peerstore"
	pstore "github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoreds"
)

// Deprecated: use github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoreds.ProtoBookOption instead
type ProtoBookOption = pstoreds.ProtoBookOption

// Deprecated: use github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoreds.WithMaxProtocols instead
func WithMaxProtocols(num int) ProtoBookOption {
	return pstoreds.WithMaxProtocols(num)
}

// Deprecated: use github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoreds.NewProtoBook instead
func NewProtoBook(meta pstore.PeerMetadata, opts ...ProtoBookOption) (peerstore.ProtoBook, error) {
	return pstoreds.NewProtoBook(meta, opts...)
}
