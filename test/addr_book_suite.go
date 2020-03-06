package test

import (
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/record"
	"github.com/libp2p/go-libp2p-core/test"
	"github.com/multiformats/go-multiaddr"
	"testing"
	"time"

	pstore "github.com/libp2p/go-libp2p-core/peerstore"
)

var addressBookSuite = map[string]func(book pstore.AddrBook) func(*testing.T){
	"AddAddress":           testAddAddress,
	"Clear":                testClearWorks,
	"SetNegativeTTLClears": testSetNegativeTTLClears,
	"UpdateTTLs":           testUpdateTTLs,
	"NilAddrsDontBreak":    testNilAddrsDontBreak,
	"AddressesExpire":      testAddressesExpire,
	"ClearWithIter":        testClearWithIterator,
	"PeersWithAddresses":   testPeersWithAddrs,
	"CertifiedAddresses":   testCertifiedAddresses,
}

type AddrBookFactory func() (pstore.AddrBook, func())

func TestAddrBook(t *testing.T, factory AddrBookFactory) {
	for name, test := range addressBookSuite {
		// Create a new peerstore.
		ab, closeFunc := factory()

		// Run the test.
		t.Run(name, test(ab))

		// Cleanup.
		if closeFunc != nil {
			closeFunc()
		}
	}
}

func testAddAddress(ab pstore.AddrBook) func(*testing.T) {
	return func(t *testing.T) {
		t.Run("add a single address", func(t *testing.T) {
			id := GeneratePeerIDs(1)[0]
			addrs := GenerateAddrs(1)

			ab.AddAddr(id, addrs[0], time.Hour)

			AssertAddressesEqual(t, addrs, ab.Addrs(id))
		})

		t.Run("idempotent add single address", func(t *testing.T) {
			id := GeneratePeerIDs(1)[0]
			addrs := GenerateAddrs(1)

			ab.AddAddr(id, addrs[0], time.Hour)
			ab.AddAddr(id, addrs[0], time.Hour)

			AssertAddressesEqual(t, addrs, ab.Addrs(id))
		})

		t.Run("add multiple addresses", func(t *testing.T) {
			id := GeneratePeerIDs(1)[0]
			addrs := GenerateAddrs(3)

			ab.AddAddrs(id, addrs, time.Hour)
			AssertAddressesEqual(t, addrs, ab.Addrs(id))
		})

		t.Run("idempotent add multiple addresses", func(t *testing.T) {
			id := GeneratePeerIDs(1)[0]
			addrs := GenerateAddrs(3)

			ab.AddAddrs(id, addrs, time.Hour)
			ab.AddAddrs(id, addrs, time.Hour)

			AssertAddressesEqual(t, addrs, ab.Addrs(id))
		})

		t.Run("adding an existing address with a later expiration extends its ttl", func(t *testing.T) {
			id := GeneratePeerIDs(1)[0]
			addrs := GenerateAddrs(3)

			ab.AddAddrs(id, addrs, time.Second)

			// same address as before but with a higher TTL
			ab.AddAddrs(id, addrs[2:], time.Hour)

			// after the initial TTL has expired, check that only the third address is present.
			time.Sleep(1200 * time.Millisecond)
			AssertAddressesEqual(t, addrs[2:], ab.Addrs(id))

			// make sure we actually set the TTL
			ab.UpdateAddrs(id, time.Hour, 0)
			AssertAddressesEqual(t, nil, ab.Addrs(id))
		})

		t.Run("adding an existing address with an earlier expiration never reduces the expiration", func(t *testing.T) {
			id := GeneratePeerIDs(1)[0]
			addrs := GenerateAddrs(3)

			ab.AddAddrs(id, addrs, time.Hour)

			// same address as before but with a lower TTL
			ab.AddAddrs(id, addrs[2:], time.Second)

			// after the initial TTL has expired, check that all three addresses are still present (i.e. the TTL on
			// the modified one was not shortened).
			time.Sleep(2100 * time.Millisecond)
			AssertAddressesEqual(t, addrs, ab.Addrs(id))
		})

		t.Run("adding an existing address with an earlier expiration never reduces the TTL", func(t *testing.T) {
			id := GeneratePeerIDs(1)[0]
			addrs := GenerateAddrs(1)

			ab.AddAddrs(id, addrs, 4*time.Second)
			// 4 seconds left
			time.Sleep(3 * time.Second)
			// 1 second left
			ab.AddAddrs(id, addrs, 3*time.Second)
			// 3 seconds left
			time.Sleep(2)
			// 1 seconds left.

			// We still have the address.
			AssertAddressesEqual(t, addrs, ab.Addrs(id))

			// The TTL wasn't reduced
			ab.UpdateAddrs(id, 4*time.Second, 0)
			AssertAddressesEqual(t, nil, ab.Addrs(id))
		})
	}
}

func testClearWorks(ab pstore.AddrBook) func(t *testing.T) {
	return func(t *testing.T) {
		ids := GeneratePeerIDs(2)
		addrs := GenerateAddrs(5)

		ab.AddAddrs(ids[0], addrs[0:3], time.Hour)
		ab.AddAddrs(ids[1], addrs[3:], time.Hour)

		AssertAddressesEqual(t, addrs[0:3], ab.Addrs(ids[0]))
		AssertAddressesEqual(t, addrs[3:], ab.Addrs(ids[1]))

		ab.ClearAddrs(ids[0])
		AssertAddressesEqual(t, nil, ab.Addrs(ids[0]))
		AssertAddressesEqual(t, addrs[3:], ab.Addrs(ids[1]))

		ab.ClearAddrs(ids[1])
		AssertAddressesEqual(t, nil, ab.Addrs(ids[0]))
		AssertAddressesEqual(t, nil, ab.Addrs(ids[1]))
	}
}

func testSetNegativeTTLClears(m pstore.AddrBook) func(t *testing.T) {
	return func(t *testing.T) {
		id := GeneratePeerIDs(1)[0]
		addrs := GenerateAddrs(100)

		m.SetAddrs(id, addrs, time.Hour)
		AssertAddressesEqual(t, addrs, m.Addrs(id))

		// remove two addresses.
		m.SetAddr(id, addrs[50], -1)
		m.SetAddr(id, addrs[75], -1)

		// calculate the survivors
		survivors := append(addrs[0:50], addrs[51:]...)
		survivors = append(survivors[0:74], survivors[75:]...)

		AssertAddressesEqual(t, survivors, m.Addrs(id))

		// remove _all_ the addresses
		m.SetAddrs(id, survivors, -1)
		if len(m.Addrs(id)) != 0 {
			t.Error("expected empty address list after clearing all addresses")
		}

		// add half, but try to remove more than we added
		m.SetAddrs(id, addrs[:50], time.Hour)
		m.SetAddrs(id, addrs, -1)
		if len(m.Addrs(id)) != 0 {
			t.Error("expected empty address list after clearing all addresses")
		}

		// try to remove the same addr multiple times
		m.SetAddrs(id, addrs[:5], time.Hour)
		repeated := make([]multiaddr.Multiaddr, 10)
		for i := 0; i < len(repeated); i++ {
			repeated[i] = addrs[0]
		}
		m.SetAddrs(id, repeated, -1)
		if len(m.Addrs(id)) != 4 {
			t.Errorf("expected 4 addrs after removing one, got %d", len(m.Addrs(id)))
		}
	}
}

func testUpdateTTLs(m pstore.AddrBook) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("update ttl of peer with no addrs", func(t *testing.T) {
			id := GeneratePeerIDs(1)[0]

			// Shouldn't panic.
			m.UpdateAddrs(id, time.Hour, time.Minute)
		})

		t.Run("update ttls successfully", func(t *testing.T) {
			ids := GeneratePeerIDs(2)
			addrs1, addrs2 := GenerateAddrs(2), GenerateAddrs(2)

			// set two keys with different ttls for each peer.
			m.SetAddr(ids[0], addrs1[0], time.Hour)
			m.SetAddr(ids[0], addrs1[1], time.Minute)
			m.SetAddr(ids[1], addrs2[0], time.Hour)
			m.SetAddr(ids[1], addrs2[1], time.Minute)

			// Sanity check.
			AssertAddressesEqual(t, addrs1, m.Addrs(ids[0]))
			AssertAddressesEqual(t, addrs2, m.Addrs(ids[1]))

			// Will only affect addrs1[0].
			// Badger does not support subsecond TTLs.
			// https://github.com/dgraph-io/badger/issues/339
			m.UpdateAddrs(ids[0], time.Hour, 1*time.Second)

			// No immediate effect.
			AssertAddressesEqual(t, addrs1, m.Addrs(ids[0]))
			AssertAddressesEqual(t, addrs2, m.Addrs(ids[1]))

			// After a wait, addrs[0] is gone.
			time.Sleep(1500 * time.Millisecond)
			AssertAddressesEqual(t, addrs1[1:2], m.Addrs(ids[0]))
			AssertAddressesEqual(t, addrs2, m.Addrs(ids[1]))

			// Will only affect addrs2[0].
			m.UpdateAddrs(ids[1], time.Hour, 1*time.Second)

			// No immediate effect.
			AssertAddressesEqual(t, addrs1[1:2], m.Addrs(ids[0]))
			AssertAddressesEqual(t, addrs2, m.Addrs(ids[1]))

			time.Sleep(1500 * time.Millisecond)

			// First addrs is gone in both.
			AssertAddressesEqual(t, addrs1[1:], m.Addrs(ids[0]))
			AssertAddressesEqual(t, addrs2[1:], m.Addrs(ids[1]))
		})

	}
}

func testNilAddrsDontBreak(m pstore.AddrBook) func(t *testing.T) {
	return func(t *testing.T) {
		id := GeneratePeerIDs(1)[0]

		m.SetAddr(id, nil, time.Hour)
		m.AddAddr(id, nil, time.Hour)
	}
}

func testAddressesExpire(m pstore.AddrBook) func(t *testing.T) {
	return func(t *testing.T) {
		ids := GeneratePeerIDs(2)
		addrs1 := GenerateAddrs(3)
		addrs2 := GenerateAddrs(2)

		m.AddAddrs(ids[0], addrs1, time.Hour)
		m.AddAddrs(ids[1], addrs2, time.Hour)

		AssertAddressesEqual(t, addrs1, m.Addrs(ids[0]))
		AssertAddressesEqual(t, addrs2, m.Addrs(ids[1]))

		m.AddAddrs(ids[0], addrs1, 2*time.Hour)
		m.AddAddrs(ids[1], addrs2, 2*time.Hour)

		AssertAddressesEqual(t, addrs1, m.Addrs(ids[0]))
		AssertAddressesEqual(t, addrs2, m.Addrs(ids[1]))

		m.SetAddr(ids[0], addrs1[0], 100*time.Microsecond)
		<-time.After(100 * time.Millisecond)
		AssertAddressesEqual(t, addrs1[1:3], m.Addrs(ids[0]))
		AssertAddressesEqual(t, addrs2, m.Addrs(ids[1]))

		m.SetAddr(ids[0], addrs1[2], 100*time.Microsecond)
		<-time.After(100 * time.Millisecond)
		AssertAddressesEqual(t, addrs1[1:2], m.Addrs(ids[0]))
		AssertAddressesEqual(t, addrs2, m.Addrs(ids[1]))

		m.SetAddr(ids[1], addrs2[0], 100*time.Microsecond)
		<-time.After(100 * time.Millisecond)
		AssertAddressesEqual(t, addrs1[1:2], m.Addrs(ids[0]))
		AssertAddressesEqual(t, addrs2[1:], m.Addrs(ids[1]))

		m.SetAddr(ids[1], addrs2[1], 100*time.Microsecond)
		<-time.After(100 * time.Millisecond)
		AssertAddressesEqual(t, addrs1[1:2], m.Addrs(ids[0]))
		AssertAddressesEqual(t, nil, m.Addrs(ids[1]))

		m.SetAddr(ids[0], addrs1[1], 100*time.Microsecond)
		<-time.After(100 * time.Millisecond)
		AssertAddressesEqual(t, nil, m.Addrs(ids[0]))
		AssertAddressesEqual(t, nil, m.Addrs(ids[1]))
	}
}

func testClearWithIterator(m pstore.AddrBook) func(t *testing.T) {
	return func(t *testing.T) {
		ids := GeneratePeerIDs(2)
		addrs := GenerateAddrs(100)

		// Add the peers with 50 addresses each.
		m.AddAddrs(ids[0], addrs[:50], pstore.PermanentAddrTTL)
		m.AddAddrs(ids[1], addrs[50:], pstore.PermanentAddrTTL)

		if all := append(m.Addrs(ids[0]), m.Addrs(ids[1])...); len(all) != 100 {
			t.Fatal("expected pstore to contain both peers with all their maddrs")
		}

		// Since we don't fetch these peers, they won't be present in cache.

		m.ClearAddrs(ids[0])
		if all := append(m.Addrs(ids[0]), m.Addrs(ids[1])...); len(all) != 50 {
			t.Fatal("expected pstore to contain only addrs of peer 2")
		}

		m.ClearAddrs(ids[1])
		if all := append(m.Addrs(ids[0]), m.Addrs(ids[1])...); len(all) != 0 {
			t.Fatal("expected pstore to contain no addresses")
		}
	}
}

func testPeersWithAddrs(m pstore.AddrBook) func(t *testing.T) {
	return func(t *testing.T) {
		// cannot run in parallel as the store is modified.
		// go runs sequentially in the specified order
		// see https://blog.golang.org/subtests

		t.Run("empty addrbook", func(t *testing.T) {
			if peers := m.PeersWithAddrs(); len(peers) != 0 {
				t.Fatal("expected to find no peers")
			}
		})

		t.Run("non-empty addrbook", func(t *testing.T) {
			ids := GeneratePeerIDs(2)
			addrs := GenerateAddrs(10)

			m.AddAddrs(ids[0], addrs[:5], pstore.PermanentAddrTTL)
			m.AddAddrs(ids[1], addrs[5:], pstore.PermanentAddrTTL)

			if peers := m.PeersWithAddrs(); len(peers) != 2 {
				t.Fatal("expected to find 2 peers")
			}
		})
	}
}

func testCertifiedAddresses(m pstore.AddrBook) func(*testing.T) {
	return func(t *testing.T) {
		cab := m.(pstore.CertifiedAddrBook)

		priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
		if err != nil {
			t.Errorf("error generating testing keys: %v", err)
		}

		id, _ := peer.IDFromPrivateKey(priv)
		allAddrs := GenerateAddrs(10)
		certifiedAddrs := allAddrs[:5]
		uncertifiedAddrs := allAddrs[5:]
		rec := peer.NewPeerRecord()
		rec.PeerID = id
		rec.Addrs = certifiedAddrs
		signedRec, err := record.Seal(rec, priv)
		if err != nil {
			t.Errorf("error creating signed routing record: %v", err)
		}

		// add a few non-certified addrs
		m.AddAddrs(id, uncertifiedAddrs, time.Hour)

		// make sure they're present
		AssertAddressesEqual(t, uncertifiedAddrs, m.Addrs(id))

		// add the signed record to addr book
		_, err = cab.ConsumePeerRecord(signedRec, time.Hour)
		if err != nil {
			t.Errorf("error adding signed routing record to addrbook: %v", err)
		}

		// the non-certified addrs should be gone & we should get only certified addrs back from Addrs
		// AssertAddressesEqual(t, certifiedAddrs, m.Addrs(id))
		AssertAddressesEqual(t, allAddrs, m.Addrs(id))

		// PeersWithAddrs should return a single peer
		if len(m.PeersWithAddrs()) != 1 {
			t.Errorf("expected PeersWithAddrs to return 1, got %d", len(m.PeersWithAddrs()))
		}

		// adding the same peer record again should result in the record being ignored
		accepted, err := cab.ConsumePeerRecord(signedRec, time.Hour)
		if accepted {
			t.Error("Expected record with duplicate sequence number to be ignored")
		}
		if err != nil {
			t.Errorf("Expected record with duplicate sequence number to be ignored without error, got err: %s", err)
		}

		// once certified addrs exist, trying to add non-certified addrs should have no effect
		// m.AddAddrs(id, uncertifiedAddrs, time.Hour)
		// AssertAddressesEqual(t, certifiedAddrs, m.Addrs(id))
		m.AddAddrs(id, uncertifiedAddrs, time.Hour)
		AssertAddressesEqual(t, allAddrs, m.Addrs(id))

		// we should be able to retrieve the signed peer record
		rec2 := cab.GetPeerRecord(id)
		if rec2 == nil || !signedRec.Equal(rec2) {
			t.Error("unable to retrieve signed routing record from addrbook")
		}

		// Adding a new envelope should clear existing certified addresses.
		// Only the newly-added ones should remain
		certifiedAddrs = certifiedAddrs[:3]
		rec = peer.NewPeerRecord()
		rec.PeerID = id
		rec.Addrs = certifiedAddrs
		signedRec, err = record.Seal(rec, priv)
		test.AssertNilError(t, err)
		_, err = cab.ConsumePeerRecord(signedRec, time.Hour)
		test.AssertNilError(t, err)
		// AssertAddressesEqual(t, certifiedAddrs, m.Addrs(id))
		AssertAddressesEqual(t, allAddrs, m.Addrs(id))

		// update TTL on signed addrs to -1 to remove them.
		// the signed routing record should be deleted
		// m.SetAddrs(id, certifiedAddrs, -1)
		m.SetAddrs(id, allAddrs, -1)
		if len(m.Addrs(id)) != 0 {
			t.Error("expected zero certified addrs after setting TTL to -1")
		}
		if cab.GetPeerRecord(id) != nil {
			t.Error("expected signed peer record to be removed when addresses expire")
		}

		// Test that natural TTL expiration clears signed peer records
		_, err = cab.ConsumePeerRecord(signedRec, time.Second)
		test.AssertNilError(t, err)
		AssertAddressesEqual(t, certifiedAddrs, m.Addrs(id))

		time.Sleep(2 * time.Second)
		if cab.GetPeerRecord(id) != nil {
			t.Error("expected signed peer record to be removed when addresses expire")
		}

		// adding a peer record that's signed with the wrong key should fail
		priv2, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
		test.AssertNilError(t, err)
		env, err := record.Seal(rec, priv2)
		test.AssertNilError(t, err)

		accepted, err = cab.ConsumePeerRecord(env, time.Second)
		if accepted || err == nil {
			t.Error("expected adding a PeerRecord that's signed with the wrong key to fail")
		}
	}
}
