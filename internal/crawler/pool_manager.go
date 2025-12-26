package crawler

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

// RelayUsageInfo tracks when a relay was last accessed
type RelayUsageInfo struct {
	lastUsed time.Time
	relay    *nostr.Relay
}

// PoolManager manages Nostr relay connections and cleans up idle ones
type PoolManager struct {
	pool          *nostr.SimplePool
	relayUsage    map[string]*RelayUsageInfo
	usageMu       sync.RWMutex
	ctx           context.Context
	cancel        context.CancelFunc
	cleanupTicker *time.Ticker
	idleTimeout   time.Duration
}

// NewPoolManager creates a new pool manager with automatic idle connection cleanup
func NewPoolManager(ctx context.Context, relayOptions ...nostr.RelayOption) *PoolManager {
	poolCtx, cancel := context.WithCancel(ctx)

	pm := &PoolManager{
		pool:        nostr.NewSimplePool(poolCtx, nostr.WithRelayOptions(relayOptions...)),
		relayUsage:  make(map[string]*RelayUsageInfo),
		ctx:         poolCtx,
		cancel:      cancel,
		idleTimeout: time.Minute,
	}

	pm.cleanupTicker = time.NewTicker(30 * time.Second)
	go pm.cleanupLoop()

	return pm
}

// GetPool returns the SimplePool instance
func (pm *PoolManager) GetPool() *nostr.SimplePool {
	return pm.pool
}

// TrackRelayUsage records that a relay was just used
func (pm *PoolManager) TrackRelayUsage(relayURL string) {
	pm.usageMu.Lock()
	defer pm.usageMu.Unlock()

	relay, ok := pm.pool.Relays.Load(relayURL)
	if !ok || relay == nil {
		return
	}

	if info, exists := pm.relayUsage[relayURL]; exists {
		info.lastUsed = time.Now()
	} else {
		pm.relayUsage[relayURL] = &RelayUsageInfo{
			lastUsed: time.Now(),
			relay:    relay,
		}
	}
}

func (pm *PoolManager) cleanupLoop() {
	for {
		select {
		case <-pm.cleanupTicker.C:
			pm.cleanupIdleRelays()
		case <-pm.ctx.Done():
			return
		}
	}
}

func (pm *PoolManager) cleanupIdleRelays() {
	pm.usageMu.Lock()
	defer pm.usageMu.Unlock()

	now := time.Now()

	for url, info := range pm.relayUsage {
		if now.Sub(info.lastUsed) > pm.idleTimeout {
			if info.relay != nil && info.relay.IsConnected() {
				if err := info.relay.Close(); err != nil {
					log.Printf("[POOL] Error closing idle relay %s: %v", url, err)
				}
			}
			delete(pm.relayUsage, url)
		}
	}
}

// Stop stops the pool manager and closes all connections
func (pm *PoolManager) Stop() {
	pm.cleanupTicker.Stop()
	pm.cancel()

	pm.usageMu.Lock()
	defer pm.usageMu.Unlock()

	for url, info := range pm.relayUsage {
		if info.relay != nil && info.relay.IsConnected() {
			if err := info.relay.Close(); err != nil {
				log.Printf("[POOL] Error closing relay %s: %v", url, err)
			}
		}
		delete(pm.relayUsage, url)
	}
}

// Get current number of connected relays
func (pm *PoolManager) GetConnectedRelayCount() int {
	return len(pm.relayUsage)
}
