package peerstore

import (
	"context"
	"encoding/base32"
	"encoding/binary"
	"errors"
	"sort"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	peer "github.com/libp2p/go-libp2p-peer"
	addr "github.com/libp2p/go-libp2p-peerstore/addr"
	ma "github.com/multiformats/go-multiaddr"
	autobatch "github.com/whyrusleeping/autobatch"
)

const (

	// TempAddrTTL is the ttl used for a short lived address
	TempAddrTTL = time.Second * 10

	// ProviderAddrTTL is the TTL of an address we've received from a provider.
	// This is also a temporary address, but lasts longer. After this expires,
	// the records we return will require an extra lookup.
	ProviderAddrTTL = time.Minute * 10

	// RecentlyConnectedAddrTTL is used when we recently connected to a peer.
	// It means that we are reasonably certain of the peer's address.
	RecentlyConnectedAddrTTL = time.Minute * 10

	// OwnObservedAddrTTL is used for our own external addresses observed by peers.
	OwnObservedAddrTTL = time.Minute * 10

	// PermanentAddrTTL is the ttl for a "permanent address" (e.g. bootstrap nodes)
	// if we haven't shipped you an update to ipfs in 356 days
	// we probably arent running the same bootstrap nodes...
	PermanentAddrTTL = time.Hour * 24 * 356

	// ConnectedAddrTTL is the ttl used for the addresses of a peer to whom
	// we're connected directly. This is basically permanent, as we will
	// clear them + re-add under a TempAddrTTL after disconnecting.
	ConnectedAddrTTL = PermanentAddrTTL

	// cleanupInterval sets how often automatic cleanup of stale addresses is performed
	cleanupInterval = time.Hour

	// batchBufferSize controls the size of batches which datastore writes are grouped into
	batchBufferSize = 256

	// peerNamespace, addrNamespace and expNamespace are the namespace components making up
	// the record keys in datastore. With 'peer', 'address' and 'expire' as values thereof,
	// the expiration time is stored under key '/peer:BASE58_OF_PEERID/address:BASE32_OF_MULTIADDR/expire'.
	peerNamespace = "peer"

	// see peerNamespace
	addrNamespace = "address"

	// see peerNamespace
	expNamespace = "expire"
)

var (
	// lruCacheSize is the limit on number of peers whos addresses are cached
	lruCacheSize = 64

	addrParseErr = errors.New("failed to parse expiring address entry")
)

type expiringAddr struct {
	Addr ma.Multiaddr
	TTL  time.Time
}

func (e *expiringAddr) ExpiredBy(t time.Time) bool {
	return t.After(e.TTL)
}

type addrSet map[string]expiringAddr

type AddrManager struct {
	addrmu   sync.Mutex
	cache    *lru.Cache
	dstore   ds.Datastore
	addrSubs map[peer.ID][]*addrSub
}

func newAddrManager(ctx context.Context, dstore ds.Batching) *AddrManager {
	mgr := new(AddrManager)
	mgr.addrSubs = make(map[peer.ID][]*addrSub)
	cache, err := lru.New(lruCacheSize)
	if err != nil {
		panic(err) // only happens if negative value is passed to lru constructor
	}
	mgr.cache = cache
	mgr.dstore = autobatch.NewAutoBatching(dstore, batchBufferSize)
	go mgr.cleanup(ctx)
	return mgr
}

func (mgr *AddrManager) cleanup(ctx context.Context) {
	tick := time.NewTicker(cleanupInterval)
	for {
		select {
		case <-tick.C:
			if err := mgr.deleteExpiredAddrs(); err != nil {
				log.Error("failed to delete expired peer addresses from datastore:", err)
			}
		case <-ctx.Done():
			tick.Stop()
			return
		}
	}
}

func (mgr *AddrManager) Peers() ([]peer.ID, error) {
	mgr.addrmu.Lock()
	defer mgr.addrmu.Unlock()

	res, err := mgr.dstore.Query(dsq.Query{
		Prefix:   ds.NewKey(peerNamespace).String(),
		KeysOnly: true,
	})
	if err != nil {
		return nil, err
	}
	peerSet := make(map[peer.ID]struct{})
	for e := range res.Next() {
		if e.Error != nil {
			return nil, e.Error
		}
		k := ds.NewKey(e.Key)
		p, _, err := parseExpAddrKey(k)
		if err != nil {
			return nil, err
		}
		peerSet[p] = struct{}{}
	}
	peers := make([]peer.ID, 0, len(peerSet))
	for p, _ := range peerSet {
		peers = append(peers, p)
	}
	return peers, nil
}

// AddAddr calls AddAddrs(p, []ma.Multiaddr{addr}, ttl)
func (mgr *AddrManager) AddAddr(p peer.ID, addr ma.Multiaddr, ttl time.Duration) error {
	return mgr.AddAddrs(p, []ma.Multiaddr{addr}, ttl)
}

// AddAddrs gives AddrManager addresses to use, with a given ttl
// (time-to-live), after which the address is no longer valid.
// If the manager has a longer TTL, the operation is a no-op for that address
func (mgr *AddrManager) AddAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration) error {
	mgr.addrmu.Lock()
	defer mgr.addrmu.Unlock()

	// if ttl is zero, exit. nothing to do.
	if ttl <= 0 {
		return nil
	}

	amap, err := mgr.warmupCache(p)
	if err != nil {
		return err
	}

	subs := mgr.addrSubs[p]

	// only expand ttls
	exp := time.Now().Add(ttl)
	for _, addr := range addrs {
		if addr == nil {
			log.Warningf("was passed nil multiaddr for %s", p)
			continue
		}

		addrstr := string(addr.Bytes())
		a, found := amap[addrstr]
		if !found || exp.After(a.TTL) {
			amap[addrstr] = expiringAddr{Addr: addr, TTL: exp}
			if err = mgr.saveAddrToDatastore(p, addr, exp); err != nil {
				return err
			}
			for _, sub := range subs {
				sub.pubAddr(addr)
			}
		}
	}
	return nil
}

// SetAddr calls mgr.SetAddrs(p, addr, ttl)
func (mgr *AddrManager) SetAddr(p peer.ID, addr ma.Multiaddr, ttl time.Duration) error {
	return mgr.SetAddrs(p, []ma.Multiaddr{addr}, ttl)
}

// SetAddrs sets the ttl on addresses. This clears any TTL there previously.
// This is used when we receive the best estimate of the validity of an address.
func (mgr *AddrManager) SetAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration) error {
	mgr.addrmu.Lock()
	defer mgr.addrmu.Unlock()

	amap, err := mgr.warmupCache(p)
	if err != nil {
		return err
	}

	subs := mgr.addrSubs[p]

	exp := time.Now().Add(ttl)
	for _, addr := range addrs {
		if addr == nil {
			log.Warningf("was passed nil multiaddr for %s", p)
			continue
		}
		// re-set all of them for new ttl.
		addrs := string(addr.Bytes())

		if ttl > 0 {
			amap[addrs] = expiringAddr{Addr: addr, TTL: exp}
			if err = mgr.saveAddrToDatastore(p, addr, exp); err != nil {
				return err
			}
			for _, sub := range subs {
				sub.pubAddr(addr)
			}
		} else {
			delete(amap, addrs)
			if err = mgr.deleteAddrFromDatastore(p, addr); err != nil {
				return err
			}
		}
	}
	return nil
}

// Addresses returns all known (and valid) addresses for a given
func (mgr *AddrManager) Addrs(p peer.ID) ([]ma.Multiaddr, error) {
	mgr.addrmu.Lock()
	defer mgr.addrmu.Unlock()
	return mgr.addrs(p)
}

func (mgr *AddrManager) addrs(p peer.ID) ([]ma.Multiaddr, error) {
	maddrs, err := mgr.warmupCache(p)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	good := make([]ma.Multiaddr, 0, len(maddrs))
	var expired []ma.Multiaddr
	for _, m := range maddrs {
		if m.ExpiredBy(now) {
			expired = append(expired, m.Addr)
		} else {
			good = append(good, m.Addr)
		}
	}

	// clean up the expired ones
	for _, a := range expired {
		delete(maddrs, string(a.Bytes()))
		if err = mgr.deleteAddrFromDatastore(p, a); err != nil {
			return nil, err
		}
	}
	return good, nil
}

// ClearAddresses removes all previously stored addresses
func (mgr *AddrManager) ClearAddrs(p peer.ID) {
	mgr.addrmu.Lock()
	defer mgr.addrmu.Unlock()

	mgr.cache.Add(p, make(addrSet))
	mgr.deleteAddrsFromDatastore(p)
}

func (mgr *AddrManager) warmupCache(p peer.ID) (addrSet, error) {
	cached, found := mgr.cache.Get(p)
	if found {
		return cached.(addrSet), nil
	}
	stored, err := mgr.loadAddresFromDatastore(p)
	if err != nil {
		return nil, err
	}
	mgr.cache.Add(p, stored)
	return stored, nil
}

func mkAddrKey(p peer.ID) ds.Key {
	peerEnc := base32.StdEncoding.EncodeToString([]byte(p))
	return ds.NewKey(peerNamespace).Instance(peerEnc).ChildString(addrNamespace)
}

func mkExpAddrKey(p peer.ID, addr ma.Multiaddr) ds.Key {
	addrEnc := base32.StdEncoding.EncodeToString(addr.Bytes())
	return mkAddrKey(p).Instance(addrEnc).ChildString(expNamespace)
}

func parseExpAddrKey(key ds.Key) (peer.ID, ma.Multiaddr, error) {
	if key.Name() != expNamespace {
		return "", nil, addrParseErr
	}
	if key = key.Parent(); key.Type() != addrNamespace {
		return "", nil, addrParseErr
	}
	addrBytes, err := base32.StdEncoding.DecodeString(key.Name())
	if err != nil {
		return "", nil, addrParseErr
	}
	addr, err := ma.NewMultiaddrBytes(addrBytes)
	if err != nil {
		return "", nil, addrParseErr
	}
	if key = key.Parent(); key.Type() != peerNamespace || !key.IsTopLevel() {
		return "", nil, addrParseErr
	}
	pBytes, err := base32.StdEncoding.DecodeString(key.Name())
	if err != nil {
		return "", nil, addrParseErr
	}
	p := peer.ID(string(pBytes))
	return p, addr, nil
}

func parseExpAddr(key ds.Key, value interface{}) (peer.ID, expiringAddr, error) {
	p, addr, err := parseExpAddrKey(key)
	if err != nil {
		return "", expiringAddr{}, err
	}
	expBytes, ok := value.([]byte)
	if !ok || len(expBytes) != 8 {
		return "", expiringAddr{}, addrParseErr
	}
	expUnixNano := int64(binary.LittleEndian.Uint64(expBytes))
	return p, expiringAddr{Addr: addr, TTL: time.Unix(0, expUnixNano)}, nil
}

func (mgr *AddrManager) saveAddrToDatastore(p peer.ID, addr ma.Multiaddr, expire time.Time) error {
	dsk := mkExpAddrKey(p, addr)
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(expire.UnixNano()))
	return mgr.dstore.Put(dsk, buf)
}

func (mgr *AddrManager) loadAddresFromDatastore(p peer.ID) (addrSet, error) {
	res, err := mgr.dstore.Query(dsq.Query{Prefix: mkAddrKey(p).String()})
	if err != nil {
		return nil, err
	}
	addrs := make(addrSet)
	for e := range res.Next() {
		if e.Error != nil {
			return nil, e.Error
		}
		_, addr, err := parseExpAddr(ds.NewKey(e.Key), e.Value)
		if err != nil {
			log.Warningf("failed to parse expiring address from datastore (key = %s): %s", e.Key, err)
			continue
		}
		addrs[string(addr.Addr.Bytes())] = addr
	}
	return addrs, nil
}

func (mgr *AddrManager) deleteAddrFromDatastore(p peer.ID, addr ma.Multiaddr) error {
	dsk := mkExpAddrKey(p, addr)
	return mgr.dstore.Delete(dsk)
}

func (mgr *AddrManager) deleteAddrsFromDatastore(p peer.ID) error {
	res, err := mgr.dstore.Query(dsq.Query{
		Prefix:   mkAddrKey(p).String(),
		KeysOnly: true,
	})
	if err != nil {
		return err
	}
	for e := range res.Next() {
		if e.Error != nil {
			return e.Error
		}
		_, addr, err := parseExpAddrKey(ds.NewKey(e.Key))
		if err != nil {
			log.Warningf("failed to parse expiring address from datastore (key = %s): %s", e.Key, err)
			continue
		}
		if err = mgr.deleteAddrFromDatastore(p, addr); err != nil {
			return err
		}
	}
	return nil
}

func (mgr *AddrManager) deleteExpiredAddrs() error {
	dsk := ds.NewKey(peerNamespace)
	res, err := mgr.dstore.Query(dsq.Query{Prefix: dsk.String()})
	if err != nil {
		return err
	}
	now := time.Now()
	for addrEnt := range res.Next() {
		if addrEnt.Error != nil {
			return addrEnt.Error
		}
		p, addr, err := parseExpAddr(ds.NewKey(addrEnt.Key), addrEnt.Value)
		if err != nil {
			log.Warningf("failed to parse expiring address from datastore (key = %s): %s", addrEnt.Key, err)
			continue
		}
		if addr.ExpiredBy(now) {
			if err = mgr.deleteAddrFromDatastore(p, addr.Addr); err != nil {
				return err
			}
			if cached, found := mgr.cache.Get(p); found {
				delete(cached.(addrSet), string(addr.Addr.Bytes()))
			}
		}
	}
	return nil
}

func (mgr *AddrManager) removeSub(p peer.ID, s *addrSub) {
	mgr.addrmu.Lock()
	defer mgr.addrmu.Unlock()
	subs := mgr.addrSubs[p]
	var filtered []*addrSub
	for _, v := range subs {
		if v != s {
			filtered = append(filtered, v)
		}
	}
	mgr.addrSubs[p] = filtered
}

type addrSub struct {
	pubch  chan ma.Multiaddr
	buffer []ma.Multiaddr
	ctx    context.Context
}

func (s *addrSub) pubAddr(a ma.Multiaddr) {
	select {
	case s.pubch <- a:
	case <-s.ctx.Done():
	}
}

func (mgr *AddrManager) AddrStream(ctx context.Context, p peer.ID) (<-chan ma.Multiaddr, error) {
	mgr.addrmu.Lock()
	defer mgr.addrmu.Unlock()

	sub := &addrSub{pubch: make(chan ma.Multiaddr), ctx: ctx}

	out := make(chan ma.Multiaddr)

	mgr.addrSubs[p] = append(mgr.addrSubs[p], sub)

	initial, err := mgr.addrs(p)
	if err != nil {
		return nil, err
	}

	sort.Sort(addr.AddrList(initial))

	go func(buffer []ma.Multiaddr) {
		defer close(out)

		sent := make(map[string]bool)
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

	return out, nil
}
