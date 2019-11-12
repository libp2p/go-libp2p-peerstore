package pstoremem

import (
	"context"
	"sort"
	"sync"
	"time"

	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	ma "github.com/multiformats/go-multiaddr"

	pstore "github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-peerstore/addr"
)

var log = logging.Logger("peerstore")

type expiringAddr struct {
	Addr    ma.Multiaddr
	TTL     time.Duration
	Expires time.Time
}

func (e *expiringAddr) ExpiredBy(t time.Time) bool {
	return t.After(e.Expires)
}

type addrSegments [256]*addrSegment

type addrSegment struct {
	sync.RWMutex

	// Use pointers to save memory. Maps always leave some fraction of their
	// space unused. storing the *values* directly in the map will
	// drastically increase the space waste. In our case, by 6x.
	addrs map[peer.ID]map[string]*expiringAddr
}

// returns the address map for the given peer, creating it if it doesn't exist.
// Note: the caller MUST hold the lock before calling!
func (s *addrSegment) addrMap(p peer.ID) map[string]*expiringAddr {
	m, found := s.addrs[p]
	if !found {
		m = make(map[string]*expiringAddr)
		s.addrs[p] = m
	}
	return m
}

func (s *addrSegment) validAddrs(p peer.ID) []ma.Multiaddr {
	s.RLock()
	defer s.RUnlock()

	amap, found := s.addrs[p]
	if !found {
		return nil
	}

	now := time.Now()
	good := make([]ma.Multiaddr, 0, len(amap))
	for _, m := range amap {
		if !m.ExpiredBy(now) {
			good = append(good, m.Addr)
		}
	}

	return good
}

func (s *addrSegment) updateAddrs(p peer.ID, oldTTL time.Duration, newTTL time.Duration) {
	s.Lock()
	defer s.Unlock()

	amap, found := s.addrs[p]
	if !found {
		return
	}

	exp := time.Now().Add(newTTL)
	for k, addr := range amap {
		if oldTTL == addr.TTL {
			addr.TTL = newTTL
			addr.Expires = exp
			amap[k] = addr
		}
	}
}

func (segments *addrSegments) get(p peer.ID) *addrSegment {
	return segments[byte(p[len(p)-1])]
}

func (segments *addrSegments) gc() {
	now := time.Now()
	for _, s := range segments {
		s.Lock()
		for p, amap := range s.addrs {
			for k, addr := range amap {
				if addr.ExpiredBy(now) {
					delete(amap, k)
				}
			}
			if len(amap) == 0 {
				delete(s.addrs, p)
			}
		}
		s.Unlock()
	}
}

func (segments *addrSegments) peersWithAddrs() peer.IDSlice {
	var pids peer.IDSlice
	for _, s := range segments {
		s.RLock()
		for pid, amap := range s.addrs {
			if amap != nil && len(amap) > 0 {
				pids = append(pids, pid)
			}
		}
		s.RUnlock()
	}
	return pids
}

func (segments *addrSegments) clear(p peer.ID) {
	s := segments.get(p)
	s.Lock()
	defer s.Unlock()

	delete(s.addrs, p)
}

type certifiedRecord struct {
	Seq      uint64
	Envelope *crypto.SignedEnvelope
}

// memoryAddrBook manages addresses.
type memoryAddrBook struct {
	segments       addrSegments
	signedSegments addrSegments

	signedRoutingStates map[peer.ID]*certifiedRecord

	ctx    context.Context
	cancel func()

	subManager *AddrSubManager
}

var _ pstore.AddrBook = (*memoryAddrBook)(nil)

func NewAddrBook() pstore.AddrBook {
	ctx, cancel := context.WithCancel(context.Background())

	makeSegments := func() (ret addrSegments) {
		for i, _ := range ret {
			ret[i] = &addrSegment{addrs: make(map[peer.ID]map[string]*expiringAddr)}
		}
		return ret
	}

	ab := &memoryAddrBook{
		segments:            makeSegments(),
		signedSegments:      makeSegments(),
		subManager:          NewAddrSubManager(),
		ctx:                 ctx,
		cancel:              cancel,
		signedRoutingStates: make(map[peer.ID]*certifiedRecord),
	}

	go ab.background()
	return ab
}

// background periodically schedules a gc
func (mab *memoryAddrBook) background() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			mab.gc()

		case <-mab.ctx.Done():
			return
		}
	}
}

func (mab *memoryAddrBook) Close() error {
	mab.cancel()
	return nil
}

// gc garbage collects the in-memory address book.
func (mab *memoryAddrBook) gc() {
	mab.segments.gc()
	mab.signedSegments.gc()

	// remove routing records for peers whose signed addrs have all been removed
	for _, p := range mab.signedSegments.peersWithAddrs() {
		if len(mab.CertifiedAddrs(p)) == 0 {
			delete(mab.signedRoutingStates, p)
		}
	}
}

func (mab *memoryAddrBook) PeersWithAddrs() peer.IDSlice {
	// deduplicate, since the same peer could have both signed & unsigned addrs
	s := peer.NewSet()
	out := mab.signedSegments.peersWithAddrs()
	for _, p := range out {
		s.Add(p)
	}
	for _, p := range mab.segments.peersWithAddrs() {
		if !s.Contains(p) {
			out = append(out, p)
		}
	}

	return out
}

// AddAddr calls AddAddrs(p, []ma.Multiaddr{addr}, ttl)
func (mab *memoryAddrBook) AddAddr(p peer.ID, addr ma.Multiaddr, ttl time.Duration) {
	mab.AddAddrs(p, []ma.Multiaddr{addr}, ttl)
}

// AddAddrs gives memoryAddrBook addresses to use, with a given ttl
// (time-to-live), after which the address is no longer valid.
// This function never reduces the TTL or expiration of an address.
func (mab *memoryAddrBook) AddAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration) {
	mab.addAddrs(p, addrs, ttl, false)
}

// AddCertifiedAddrs adds addresses from a routing.RoutingState record
// contained in the given SignedEnvelope.
func (mab *memoryAddrBook) AddCertifiedAddrs(envelope *crypto.SignedEnvelope, ttl time.Duration) error {
	state, err := routing.RoutingStateFromEnvelope(envelope)
	if err != nil {
		return err
	}

	// ensure seq is greater than last received
	lastState, found := mab.signedRoutingStates[state.PeerID]
	if found && lastState.Seq >= state.Seq {
		// TODO: should this be an error?
		return nil
	}
	mab.signedRoutingStates[state.PeerID] = &certifiedRecord{Seq: state.Seq, Envelope: envelope}
	mab.addAddrs(state.PeerID, state.Multiaddrs(), ttl, true)
	return nil
}

func (mab *memoryAddrBook) addAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration, signed bool) {
	// if ttl is zero, exit. nothing to do.
	if ttl <= 0 {
		return
	}

	s := mab.segments.get(p)
	s.Lock()
	defer s.Unlock()
	amap := s.addrMap(p)

	s = mab.signedSegments.get(p)
	s.Lock()
	defer s.Unlock()
	signedAddrMap := s.addrMap(p)

	exp := time.Now().Add(ttl)
	maxTTLAndExp := func(a *expiringAddr, t time.Duration, e time.Time) (time.Duration, time.Time) {
		if a == nil {
			return t, e
		}
		if a.TTL > ttl {
			t = a.TTL
		}
		if a.Expires.After(exp) {
			e = a.Expires
		}
		return t, e
	}
	for _, addr := range addrs {
		if addr == nil {
			log.Warningf("was passed nil multiaddr for %s", p)
			continue
		}
		k := string(addr.Bytes())

		// find the highest TTL and Expiry time between
		// existing records and function args
		a, found := amap[k] // won't allocate.
		b, foundSigned := signedAddrMap[k]
		maxTTL, maxExp := maxTTLAndExp(a, ttl, exp)
		maxTTL, maxExp = maxTTLAndExp(b, maxTTL, maxExp)

		entry := &expiringAddr{Addr: addr, Expires: maxExp, TTL: maxTTL}

		// if we're adding a signed addr, or if it was previously signed,
		// make sure it's not also in the unsigned addr list.
		if signed || foundSigned {
			signedAddrMap[k] = entry
			delete(amap, k)
		} else {
			amap[k] = entry
		}

		if !found && !foundSigned {
			// not found, announce it.
			mab.subManager.BroadcastAddr(p, addr)
		}
	}
}

// SetAddr calls mgr.SetAddrs(p, addr, ttl)
func (mab *memoryAddrBook) SetAddr(p peer.ID, addr ma.Multiaddr, ttl time.Duration) {
	mab.SetAddrs(p, []ma.Multiaddr{addr}, ttl)
}

// SetAddrs sets the ttl on addresses. This clears any TTL there previously.
// This is used when we receive the best estimate of the validity of an address.
func (mab *memoryAddrBook) SetAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration) {
	mab.setAddrs(p, addrs, ttl)
}

func (mab *memoryAddrBook) setAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration) {
	s := mab.segments.get(p)
	s.Lock()
	defer s.Unlock()
	signedSegment := mab.signedSegments.get(p)
	signedSegment.Lock()
	defer signedSegment.Unlock()

	amap := s.addrMap(p)
	signedAddrMap := signedSegment.addrMap(p)

	exp := time.Now().Add(ttl)
	for _, addr := range addrs {
		if addr == nil {
			log.Warningf("was passed nil multiaddr for %s", p)
			continue
		}
		aBytes := addr.Bytes()
		key := string(aBytes)

		// if the addr is in the signed addr set, update the ttl
		// and ensure it's not in the unsigned set. otherwise,
		// add to or update the unsigned set
		mapToUpdate := amap
		if _, signedExists := signedAddrMap[key]; signedExists {
			mapToUpdate = signedAddrMap
			delete(amap, key)
		}

		// re-set all of them for new ttl.
		if ttl > 0 {
			mapToUpdate[key] = &expiringAddr{Addr: addr, Expires: exp, TTL: ttl}
			mab.subManager.BroadcastAddr(p, addr)
		} else {
			delete(mapToUpdate, key)
		}
	}

	if len(signedAddrMap) == 0 {
		delete(mab.signedRoutingStates, p)
	}
}

// UpdateAddrs updates the addresses associated with the given peer that have
// the given oldTTL to have the given newTTL.
func (mab *memoryAddrBook) UpdateAddrs(p peer.ID, oldTTL time.Duration, newTTL time.Duration) {
	mab.segments.get(p).updateAddrs(p, oldTTL, newTTL)
	mab.signedSegments.get(p).updateAddrs(p, oldTTL, newTTL)

	// updating may have caused all our certified addrs to expire.
	// if so, remove the signed record we sourced them from.
	if len(mab.CertifiedAddrs(p)) == 0 {
		delete(mab.signedRoutingStates, p)
	}
}

// Addrs returns all known (and valid) addresses for a given peer
func (mab *memoryAddrBook) Addrs(p peer.ID) []ma.Multiaddr {
	return append(
		mab.signedSegments.get(p).validAddrs(p),
		mab.segments.get(p).validAddrs(p)...)
}

// CertifiedAddrs returns all known (and valid) addressed that have been
// certified by the given peer.
func (mab *memoryAddrBook) CertifiedAddrs(p peer.ID) []ma.Multiaddr {
	return mab.signedSegments.get(p).validAddrs(p)
}

func (mab *memoryAddrBook) SignedRoutingState(p peer.ID) *crypto.SignedEnvelope {
	r := mab.signedRoutingStates[p]
	if r != nil {
		return r.Envelope
	}
	return nil
}

// ClearAddrs removes all previously stored addresses
func (mab *memoryAddrBook) ClearAddrs(p peer.ID) {
	mab.segments.clear(p)
	mab.signedSegments.clear(p)
}

// AddrStream returns a channel on which all new addresses discovered for a
// given peer ID will be published.
func (mab *memoryAddrBook) AddrStream(ctx context.Context, p peer.ID) <-chan ma.Multiaddr {
	s := mab.segments.get(p)
	s.RLock()
	defer s.RUnlock()

	baseaddrslice := s.addrs[p]
	initial := make([]ma.Multiaddr, 0, len(baseaddrslice))
	for _, a := range baseaddrslice {
		initial = append(initial, a.Addr)
	}

	return mab.subManager.AddrStream(ctx, p, initial)
}

type addrSub struct {
	pubch  chan ma.Multiaddr
	lk     sync.Mutex
	buffer []ma.Multiaddr
	ctx    context.Context
}

func (s *addrSub) pubAddr(a ma.Multiaddr) {
	select {
	case s.pubch <- a:
	case <-s.ctx.Done():
	}
}

// An abstracted, pub-sub manager for address streams. Extracted from
// memoryAddrBook in order to support additional implementations.
type AddrSubManager struct {
	mu   sync.RWMutex
	subs map[peer.ID][]*addrSub
}

// NewAddrSubManager initializes an AddrSubManager.
func NewAddrSubManager() *AddrSubManager {
	return &AddrSubManager{
		subs: make(map[peer.ID][]*addrSub),
	}
}

// Used internally by the address stream coroutine to remove a subscription
// from the manager.
func (mgr *AddrSubManager) removeSub(p peer.ID, s *addrSub) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	subs := mgr.subs[p]
	if len(subs) == 1 {
		if subs[0] != s {
			return
		}
		delete(mgr.subs, p)
		return
	}

	for i, v := range subs {
		if v == s {
			subs[i] = subs[len(subs)-1]
			subs[len(subs)-1] = nil
			mgr.subs[p] = subs[:len(subs)-1]
			return
		}
	}
}

// BroadcastAddr broadcasts a new address to all subscribed streams.
func (mgr *AddrSubManager) BroadcastAddr(p peer.ID, addr ma.Multiaddr) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	if subs, ok := mgr.subs[p]; ok {
		for _, sub := range subs {
			sub.pubAddr(addr)
		}
	}
}

// AddrStream creates a new subscription for a given peer ID, pre-populating the
// channel with any addresses we might already have on file.
func (mgr *AddrSubManager) AddrStream(ctx context.Context, p peer.ID, initial []ma.Multiaddr) <-chan ma.Multiaddr {
	sub := &addrSub{pubch: make(chan ma.Multiaddr), ctx: ctx}
	out := make(chan ma.Multiaddr)

	mgr.mu.Lock()
	if _, ok := mgr.subs[p]; ok {
		mgr.subs[p] = append(mgr.subs[p], sub)
	} else {
		mgr.subs[p] = []*addrSub{sub}
	}
	mgr.mu.Unlock()

	sort.Sort(addr.AddrList(initial))

	go func(buffer []ma.Multiaddr) {
		defer close(out)

		sent := make(map[string]bool, len(buffer))
		var outch chan ma.Multiaddr

		for _, a := range buffer {
			sent[string(a.Bytes())] = true
		}

		var next ma.Multiaddr
		if len(buffer) > 0 {
			next = buffer[0]
			buffer = buffer[1:]
			outch = out
		}

		for {
			select {
			case outch <- next:
				if len(buffer) > 0 {
					next = buffer[0]
					buffer = buffer[1:]
				} else {
					outch = nil
					next = nil
				}
			case naddr := <-sub.pubch:
				if sent[string(naddr.Bytes())] {
					continue
				}

				sent[string(naddr.Bytes())] = true
				if next == nil {
					next = naddr
					outch = out
				} else {
					buffer = append(buffer, naddr)
				}
			case <-ctx.Done():
				mgr.removeSub(p, sub)
				return
			}
		}

	}(initial)

	return out
}
