package pstoremanager

import (
	"context"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
)

type Option func(*PeerstoreManager) error

// WithGracePeriod sets the grace period.
// If a peer doesn't reconnect during the grace period, its data is removed.
// Default: 1 minute.
func WithGracePeriod(p time.Duration) Option {
	return func(m *PeerstoreManager) error {
		m.gracePeriod = p
		return nil
	}
}

// WithCleanupInterval set the clean up interval.
// During a clean up run peers that disconnected before the grace period are removed.
// If unset, the interval is set to half the grace period.
func WithCleanupInterval(t time.Duration) Option {
	return func(m *PeerstoreManager) error {
		m.cleanupInterval = t
		return nil
	}
}

type PeerstoreManager struct {
	pstore peerstore.Peerstore
	sub    event.Subscription

	ctx       context.Context
	ctxCancel context.CancelFunc
	refCount  sync.WaitGroup

	gracePeriod     time.Duration
	cleanupInterval time.Duration
}

func NewPeerstoreManager(pstore peerstore.Peerstore, eventBus event.Bus, opts ...Option) (*PeerstoreManager, error) {
	sub, err := eventBus.Subscribe(&event.EvtPeerConnectednessChanged{})
	if err != nil {
		return nil, err
	}
	m := &PeerstoreManager{
		pstore:      pstore,
		gracePeriod: time.Minute,
		sub:         sub,
	}
	m.ctx, m.ctxCancel = context.WithCancel(context.Background())
	for _, opt := range opts {
		if err := opt(m); err != nil {
			return nil, err
		}
	}
	if m.cleanupInterval == 0 {
		m.cleanupInterval = m.gracePeriod / 2
	}
	return m, nil
}

func (m *PeerstoreManager) Start() {
	m.refCount.Add(1)
	go m.background()
}

func (m *PeerstoreManager) background() {
	defer m.refCount.Done()
	disconnected := make(map[peer.ID]time.Time)

	ticker := time.NewTicker(m.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			for p := range disconnected {
				m.pstore.RemovePeer(p)
			}
			return
		case e, ok := <-m.sub.Out():
			if !ok {
				continue
			}
			ev := e.(event.EvtPeerConnectednessChanged)
			p := ev.Peer
			switch ev.Connectedness {
			case network.NotConnected:
				if _, ok := disconnected[p]; !ok {
					disconnected[p] = time.Now()
				}
			case network.Connected:
				// If we reconnect to the peer before we've cleared the information, keep it.
				delete(disconnected, p)
			}
		case now := <-ticker.C:
			for p, disconnectTime := range disconnected {
				if disconnectTime.Add(m.gracePeriod).Before(now) {
					m.pstore.RemovePeer(p)
					delete(disconnected, p)
				}
			}
		}
	}
}

func (m *PeerstoreManager) Close() error {
	m.ctxCancel()
	err := m.sub.Close()
	m.refCount.Wait()
	return err
}
