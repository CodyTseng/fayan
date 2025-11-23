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

// PoolManager manages Nostr relay connections and cleans up idle ones to prevent goroutine leaks
type PoolManager struct {
	pool          *nostr.SimplePool
	relayUsage    map[string]*RelayUsageInfo
	usageMu       sync.RWMutex
	ctx           context.Context
	cancel        context.CancelFunc
	cleanupTicker *time.Ticker
	idleTimeout   time.Duration // how long a relay can be idle before cleanup
}

// NewPoolManager creates a new pool manager with automatic idle connection cleanup
func NewPoolManager(ctx context.Context, relayOptions ...nostr.RelayOption) *PoolManager {
	poolCtx, cancel := context.WithCancel(ctx)

	pm := &PoolManager{
		pool:        nostr.NewSimplePool(poolCtx, nostr.WithRelayOptions(relayOptions...)),
		relayUsage:  make(map[string]*RelayUsageInfo),
		ctx:         poolCtx,
		cancel:      cancel,
		idleTimeout: time.Minute, // close relays idle for more than a minutes
	}

	// run cleanup every 30 seconds
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

	// get the relay from the pool
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

// cleanupLoop periodically closes idle relay connections
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

// cleanupIdleRelays closes relay connections that haven't been used recently
func (pm *PoolManager) cleanupIdleRelays() {
	pm.usageMu.Lock()
	defer pm.usageMu.Unlock()

	now := time.Now()

	for url, info := range pm.relayUsage {
		// check if relay has been idle too long
		if now.Sub(info.lastUsed) > pm.idleTimeout {
			if info.relay != nil && info.relay.IsConnected() {
				if err := info.relay.Close(); err != nil {
					log.Printf("[POOL] Error closing idle relay %s: %v", url, err)
				}
			}
			// remove from tracking
			delete(pm.relayUsage, url)
		}
	}
}

// Stop gracefully shuts down the pool manager
func (pm *PoolManager) Stop() {

	if pm.cleanupTicker != nil {
		pm.cleanupTicker.Stop()
	}

	pm.cancel()
}

// Get current number of connected relays
func (pm *PoolManager) GetConnectedRelayCount() int {
	return len(pm.relayUsage)
}
