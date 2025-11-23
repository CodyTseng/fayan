package crawler

import (
	"sync"
	"time"
)

const (
	maxRelayFailures     = 3                // Maximum failures before banning a relay
	relayBanDuration     = 30 * time.Minute // How long to ban a failed relay
	relayFailureResetAge = 10 * time.Minute // Reset failure count if last failure was this long ago
)

// relayFailureInfo tracks failure information for a relay
type relayFailureInfo struct {
	failureCount int
	lastFailure  time.Time
	bannedUntil  time.Time
}

// RelayHealthTracker manages relay failure tracking and banning
type RelayHealthTracker struct {
	failedRelays map[string]*relayFailureInfo
	mu           sync.RWMutex
}

// NewRelayHealthTracker creates a new relay health tracker
func NewRelayHealthTracker() *RelayHealthTracker {
	return &RelayHealthTracker{
		failedRelays: make(map[string]*relayFailureInfo),
	}
}

// IsRelayBanned checks if a relay is currently banned
func (t *RelayHealthTracker) IsRelayBanned(relay string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if info, exists := t.failedRelays[relay]; exists {
		if time.Now().Before(info.bannedUntil) {
			return true
		}
	}
	return false
}

// RecordFailure records a failure for a relay and potentially bans it
func (t *RelayHealthTracker) RecordFailure(relay string, reason string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	info, exists := t.failedRelays[relay]

	if !exists {
		info = &relayFailureInfo{
			failureCount: 1,
			lastFailure:  now,
		}
		t.failedRelays[relay] = info
		return
	}

	// Reset failure count if the last failure was long ago
	if now.Sub(info.lastFailure) > relayFailureResetAge {
		info.failureCount = 1
		info.lastFailure = now
		return
	}

	// Increment failure count
	info.failureCount++
	info.lastFailure = now

	if info.failureCount >= maxRelayFailures {
		info.bannedUntil = now.Add(relayBanDuration)
	}
}

// RecordSuccess records a successful connection to a relay (resets failures)
func (t *RelayHealthTracker) RecordSuccess(relay string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Remove from failed relays map if it exists
	delete(t.failedRelays, relay)
}

// GetStats returns statistics about failed relays
func (t *RelayHealthTracker) GetStats() (total int, banned int) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := time.Now()
	total = len(t.failedRelays)
	for _, info := range t.failedRelays {
		if now.Before(info.bannedUntil) {
			banned++
		}
	}
	return
}

// FilterBannedRelays removes banned relays from the list
func (t *RelayHealthTracker) FilterBannedRelays(relays []string) []string {
	filtered := make([]string, 0, len(relays))
	for _, relay := range relays {
		if !t.IsRelayBanned(relay) {
			filtered = append(filtered, relay)
		}
	}
	return filtered
}
