package testutil

import (
	"io"

	ci "gx/ipfs/QmTuX6VtWTbWgPwd5PMXHyp411RKsW5nBqLKVVRfJMNneb/go-libp2p-crypto"
	u "gx/ipfs/QmZuY8aV7zbNXVy6DyN9SmnuH3o9nG852F4aTiSBpts8d1/go-ipfs-util"
	peer "gx/ipfs/QmbKtZxyDqUJp7Ad8tGr5nrLqoi9nfgqFxcNbmLJbfaHPe/go-libp2p-peer"
)

func RandPeerID() (peer.ID, error) {
	buf := make([]byte, 16)
	if _, err := io.ReadFull(u.NewTimeSeededRand(), buf); err != nil {
		return "", err
	}
	h := u.Hash(buf)
	return peer.ID(h), nil
}

func RandTestKeyPair(bits int) (ci.PrivKey, ci.PubKey, error) {
	return ci.GenerateKeyPairWithReader(ci.RSA, bits, u.NewTimeSeededRand())
}

func SeededTestKeyPair(seed int64) (ci.PrivKey, ci.PubKey, error) {
	return ci.GenerateKeyPairWithReader(ci.RSA, 512, u.NewSeededRand(seed))
}
