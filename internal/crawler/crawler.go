package crawler

import (
	"context"
	"encoding/json"
	"log"
	"slices"
	"strings"
	"sync"
	"time"

	"fayan/config"
	"fayan/internal/repository"

	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/sdk"
	"golang.org/x/time/rate"
)

const (
	batchSize      = 200
	reqRate        = 2 * time.Second
	numProcessors  = 4
	resultChanSize = 1000
)

// Crawler manages the recursive crawling of the Nostr network.
type Crawler struct {
	repo          *repository.Repository
	poolManager   *PoolManager
	relays        []string
	seedPubkeys   []string
	searchConfig  *config.SearchConfig
	resultsChan   chan *nostr.Event
	profilesChan  chan *nostr.Event
	crawled       map[string]bool
	crawledMu     sync.Mutex
	processed     map[string]time.Time
	processedMu   sync.Mutex
	relayLimiters map[string]*rate.Limiter
	limitersMu    sync.Mutex

	relayHealth *RelayHealthTracker

	consecutiveEmpty int
	sleepDuration    time.Duration

	paused   bool
	pausedMu sync.RWMutex
	pauseCh  chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
}

// NewCrawler creates a new Crawler instance.
func NewCrawler(repo *repository.Repository, relays []string, seedPubkeys []string, searchConfig *config.SearchConfig) *Crawler {
	ctx, cancel := context.WithCancel(context.Background())

	relayOptions := []nostr.RelayOption{
		nostr.WithNoticeHandler(func(notice string) {}),
	}

	poolManager := NewPoolManager(ctx, relayOptions...)

	return &Crawler{
		repo:             repo,
		poolManager:      poolManager,
		relays:           relays,
		seedPubkeys:      seedPubkeys,
		searchConfig:     searchConfig,
		resultsChan:      make(chan *nostr.Event, resultChanSize),
		profilesChan:     make(chan *nostr.Event, resultChanSize),
		crawled:          make(map[string]bool),
		processed:        make(map[string]time.Time),
		relayLimiters:    make(map[string]*rate.Limiter),
		relayHealth:      NewRelayHealthTracker(),
		consecutiveEmpty: 0,
		sleepDuration:    5 * time.Second,
		pauseCh:          make(chan struct{}),
		ctx:              ctx,
		cancel:           cancel,
	}
}

// Stop gracefully shuts down the crawler and cleans up connections
func (c *Crawler) Stop() {
	log.Println("[CRAWLER] Shutting down...")

	// Cancel the context to stop all operations
	c.cancel()

	// Stop the pool manager (this will close all relay connections)
	c.poolManager.Stop()

	// Close the results channel
	close(c.resultsChan)
	close(c.profilesChan)

	// Give some time for goroutines to finish
	time.Sleep(2 * time.Second)

	log.Println("[CRAWLER] Shutdown complete")
}

// Pause temporarily stops the crawler from fetching new data
func (c *Crawler) Pause() {
	c.pausedMu.Lock()
	defer c.pausedMu.Unlock()
	if !c.paused {
		c.paused = true
		c.pauseCh = make(chan struct{})
		log.Println("[CRAWLER] Paused")
	}
}

// Resume resumes the crawler after being paused
func (c *Crawler) Resume() {
	c.pausedMu.Lock()
	defer c.pausedMu.Unlock()
	if c.paused {
		c.paused = false
		close(c.pauseCh)
		log.Println("[CRAWLER] Resumed")
	}
}

// waitIfPaused blocks if the crawler is paused, returns true if context was cancelled
func (c *Crawler) waitIfPaused() bool {
	c.pausedMu.RLock()
	paused := c.paused
	pauseCh := c.pauseCh
	c.pausedMu.RUnlock()

	if paused {
		select {
		case <-pauseCh:
			return false
		case <-c.ctx.Done():
			return true
		}
	}
	return false
}

// getRelayLimiter returns a rate limiter for a specific relay, creating one if needed
func (c *Crawler) getRelayLimiter(relay string) *rate.Limiter {
	c.limitersMu.Lock()
	defer c.limitersMu.Unlock()

	if limiter, exists := c.relayLimiters[relay]; exists {
		return limiter
	}

	limiter := rate.NewLimiter(rate.Every(reqRate), 1)
	c.relayLimiters[relay] = limiter
	return limiter
}

// Start begins the crawling process.
func (c *Crawler) Start() {
	// Single goroutine for network operations to avoid rate limiting
	go c.networkWorker()

	// Multiple goroutines for processing results
	for range numProcessors {
		go c.resultProcessor()
	}

	// Profile processor for search functionality
	if c.searchConfig != nil && c.searchConfig.Enabled {
		go c.profileProcessor()
	}

	// Status reporter
	go c.statusReporter()
}

// networkWorker is the single goroutine that handles all network communication
func (c *Crawler) networkWorker() {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		// Check if paused
		if c.waitIfPaused() {
			return
		}

		randomPubkeys, err := c.repo.RandomPubkeys(batchSize)
		if err != nil || len(randomPubkeys) == 0 {
			c.fetchBatch(c.seedPubkeys)
		} else {
			pubkeys := make([]string, 0, len(randomPubkeys))
			for _, pk := range randomPubkeys {
				c.processedMu.Lock()
				lastProcessedTime, alreadyProcessed := c.processed[pk]
				c.processedMu.Unlock()
				if !alreadyProcessed || time.Since(lastProcessedTime) > time.Hour {
					pubkeys = append(pubkeys, pk)
				}
			}
			if len(pubkeys) == 0 {
				// No pubkeys to crawl, sleep and increase wait time exponentially
				c.consecutiveEmpty++
				sleepTime := min(c.sleepDuration*time.Duration(1<<(c.consecutiveEmpty-1)), time.Hour) // 1h (capped)
				log.Printf("[INFO] No new pubkeys to crawl (attempt %d). Sleeping for %v...", c.consecutiveEmpty, sleepTime)

				// Interruptible sleep
				select {
				case <-time.After(sleepTime):
				case <-c.ctx.Done():
					return
				}
				continue
			}
			// Reset consecutive empty counter when we find pubkeys
			c.consecutiveEmpty = 0
			c.fetchBatch(pubkeys)
		}
	}
}

// resultProcessor handles processing of contact events (database writes, queue management)
func (c *Crawler) resultProcessor() {
	for {
		select {
		case event, ok := <-c.resultsChan:
			if !ok {
				return // Channel closed, exit
			}
			if event != nil {
				c.processKind3Event(event)
			}
		case <-c.ctx.Done():
			return
		}

		// Check if paused
		if c.waitIfPaused() {
			return
		}
	}
}

// fetchBatch fetches relay lists and contacts for a batch of pubkeys
func (c *Crawler) fetchBatch(pubkeys []string) {
	// Check if context is cancelled
	select {
	case <-c.ctx.Done():
		return
	default:
	}

	// Step 1: Fetch relay lists from bootstrap relays (batch operation)
	ctx1, cancel1 := context.WithTimeout(c.ctx, 15*time.Second)
	defer cancel1()

	relayLists := c.fetchRelayLists(ctx1, pubkeys)

	// Step 2: Group users by individual relay (not by relay combination)
	// A user can appear in multiple relay groups
	relayToUsers := make(map[string][]string)

	for _, pubkey := range pubkeys {
		userRelays := c.calculateRelaysForPubkey(pubkey, relayLists[pubkey])

		if len(userRelays) == 0 {
			continue
		}

		// Add this user to each of their relays
		for _, relay := range userRelays {
			relayToUsers[relay] = append(relayToUsers[relay], pubkey)
		}
	}

	// Step 3: Fetch contacts (and profiles if search is enabled) from each relay concurrently
	// Collect results from all relays
	contactEvents := make(map[string]*nostr.Event)
	profileEvents := make(map[string]*nostr.Event)
	var wg sync.WaitGroup
	var eventsMu sync.Mutex // Protect concurrent map writes

	fetchProfiles := c.searchConfig != nil && c.searchConfig.Enabled

	for relay, users := range relayToUsers {
		wg.Add(1)
		go func(r string, u []string) {
			defer wg.Done()
			contacts, profiles := c.fetchEventsFromRelay(r, u, fetchProfiles)

			// Use mutex to protect map access
			eventsMu.Lock()
			for pubkey, event := range contacts {
				if existing, exists := contactEvents[pubkey]; !exists || event.CreatedAt > existing.CreatedAt {
					contactEvents[pubkey] = event
				}
			}
			for pubkey, event := range profiles {
				if existing, exists := profileEvents[pubkey]; !exists || event.CreatedAt > existing.CreatedAt {
					profileEvents[pubkey] = event
				}
			}
			eventsMu.Unlock()
		}(relay, users)
	}

	// Wait for all relays to finish
	wg.Wait()

	// Step 5: Check against global timestamps and send to processors
	for _, event := range contactEvents {
		select {
		case c.resultsChan <- event:
		case <-c.ctx.Done():
			return
		}
	}

	// Send profile events to profile processor
	if fetchProfiles {
		for _, event := range profileEvents {
			select {
			case c.profilesChan <- event:
			case <-c.ctx.Done():
				return
			}
		}
	}
}

// fetchEventsFromRelay fetches contacts (kind 3) and optionally profiles (kind 0) for multiple users from a single relay
// Returns maps of pubkey -> latest event for contacts and profiles
func (c *Crawler) fetchEventsFromRelay(relay string, pubkeys []string, fetchProfiles bool) (map[string]*nostr.Event, map[string]*nostr.Event) {
	if len(pubkeys) == 0 {
		return nil, nil
	}

	// Check if context is cancelled
	select {
	case <-c.ctx.Done():
		return nil, nil
	default:
	}

	// Skip if relay is banned
	if c.relayHealth.IsRelayBanned(relay) {
		return nil, nil
	}

	// Apply rate limiting for this specific relay
	limiter := c.getRelayLimiter(relay)
	if err := limiter.Wait(c.ctx); err != nil {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(c.ctx, 15*time.Second)
	defer cancel()

	// Build filter with kinds 3 (contacts) and optionally 0 (profiles)
	kinds := []int{3}
	if fetchProfiles {
		kinds = append(kinds, 0)
	}

	filter := nostr.Filter{
		Kinds:   kinds,
		Authors: pubkeys,
	}

	// Get the current pool from pool manager
	pool := c.poolManager.GetPool()

	// SubscribeMany returns a channel of RelayEvent
	eventsChan := pool.FetchMany(ctx, []string{relay}, filter)

	// Track relay usage
	c.poolManager.TrackRelayUsage(relay)

	// Collect events and keep only the latest for each pubkey
	contacts := make(map[string]*nostr.Event)
	profiles := make(map[string]*nostr.Event)
	timer := time.NewTimer(10 * time.Second) // Slightly less than context timeout
	defer timer.Stop()
	channelClosed := false

	for {
		select {
		case relayEvent, ok := <-eventsChan:
			if !ok {
				// Check for relay connection error
				if relayEvent.Relay != nil && relayEvent.Relay.ConnectionError != nil {
					c.relayHealth.RecordFailure(relay, "connection error: "+relayEvent.Relay.ConnectionError.Error())
				} else {
					channelClosed = true
					c.relayHealth.RecordSuccess(relay)
				}
				return contacts, profiles
			}

			ev := relayEvent.Event

			// Keep only the latest event for each pubkey from this relay
			switch ev.Kind {
			case 3:
				if existing, exists := contacts[ev.PubKey]; !exists || ev.CreatedAt > existing.CreatedAt {
					contacts[ev.PubKey] = ev
				}
			case 0:
				if existing, exists := profiles[ev.PubKey]; !exists || ev.CreatedAt > existing.CreatedAt {
					profiles[ev.PubKey] = ev
				}
			}
		case <-timer.C:
			// Timeout - this could indicate connection issues
			if !channelClosed {
				c.relayHealth.RecordFailure(relay, "timeout - no response")
			}
			return contacts, profiles
		case <-ctx.Done():
			// Context cancelled
			if !channelClosed {
				c.relayHealth.RecordFailure(relay, "context cancelled")
			}
			return contacts, profiles
		}
	}
}

// fetchRelayLists fetches kind:10002 events for a list of pubkeys.
func (c *Crawler) fetchRelayLists(ctx context.Context, pubkeys []string) map[string]*nostr.Event {
	filter := nostr.Filter{
		Kinds:   []int{10002},
		Authors: pubkeys,
	}

	// Get the current pool from pool manager
	pool := c.poolManager.GetPool()

	eventsChan := pool.FetchMany(ctx, c.relays, filter)

	// Track relay usage for bootstrap relays
	for _, relay := range c.relays {
		c.poolManager.TrackRelayUsage(relay)
	}

	latestEvents := make(map[string]*nostr.Event)
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	for {
		select {
		case relayEvent, ok := <-eventsChan:
			if !ok {
				return latestEvents
			}
			ev := relayEvent.Event
			if existing, ok := latestEvents[ev.PubKey]; !ok || ev.CreatedAt > existing.CreatedAt {
				latestEvents[ev.PubKey] = ev
			}
		case <-timer.C:
			return latestEvents
		case <-ctx.Done():
			return latestEvents
		}
	}
}

func (c *Crawler) calculateRelaysForPubkey(pubkey string, relayListEvent *nostr.Event) []string {
	if relayListEvent == nil {
		return c.relays
	}

	writeRelays := c.parseWriteRelays(relayListEvent)

	// Assume too many relays means misconfiguration
	if len(writeRelays) > 8 {
		return c.relays
	}

	// Start with the user's valid write relays
	finalRelays := c.relayHealth.FilterBannedRelays(writeRelays)

	// If the user has fewer than 4 relays, supplement with defaults
	if len(finalRelays) < 4 {
		for _, bootstrapRelay := range c.relays {
			if len(finalRelays) >= 4 {
				break
			}
			if !slices.Contains(finalRelays, bootstrapRelay) {
				finalRelays = append(finalRelays, bootstrapRelay)
			}
		}
	}

	return finalRelays
}

// parseWriteRelays extracts valid write relays from a kind:10002 event.
func (c *Crawler) parseWriteRelays(event *nostr.Event) []string {
	relays := []string{}
	for _, tag := range event.Tags {
		if len(tag) >= 2 && tag[0] == "r" {
			// Exclude relays marked as "read" only.
			if len(tag) > 2 && tag[2] == "read" {
				continue
			}
			url := nostr.NormalizeURL(tag[1])
			if c.isValidRelay(url) && !slices.Contains(relays, url) {
				relays = append(relays, url)
			}
		}
	}
	return relays
}

// isValidRelay performs basic validation on a relay URL.
func (c *Crawler) isValidRelay(url string) bool {
	if !nostr.IsValidRelayURL(url) {
		return false
	}

	// Exclude local and private network relays
	invalidPatterns := []string{"127.0.0.1", "192.168.", "localhost", ".onion"}
	for _, pattern := range invalidPatterns {
		if strings.Contains(url, pattern) {
			return false
		}
	}

	return true
}

// processKind3Event parses a kind:3 event and updates the database and work queue.
func (c *Crawler) processKind3Event(ev *nostr.Event) {
	if ev.Kind != 3 {
		return
	}

	c.repo.UpsertPubkey(ev.PubKey)

	c.processedMu.Lock()
	c.processed[ev.PubKey] = time.Now()
	c.processedMu.Unlock()

	targetCount := 0
	for _, tag := range ev.Tags {
		if len(tag) >= 2 && tag[0] == "p" {
			targetPubkey := tag[1]
			if !nostr.IsValidPublicKey(targetPubkey) {
				continue
			}
			if targetPubkey == ev.PubKey {
				continue
			}

			c.repo.UpsertPubkey(targetPubkey)
			c.repo.UpsertConnection(ev.PubKey, targetPubkey)

			targetCount++

			c.crawledMu.Lock()
			c.crawled[targetPubkey] = true
			c.crawledMu.Unlock()
		}
	}
}

// profileProcessor handles processing of profile events (kind 0)
func (c *Crawler) profileProcessor() {
	for {
		select {
		case event, ok := <-c.profilesChan:
			if !ok {
				return // Channel closed, exit
			}
			if event != nil {
				c.processKind0Event(event)
			}
		case <-c.ctx.Done():
			return
		}

		// Check if paused
		if c.waitIfPaused() {
			return
		}
	}
}

// processKind0Event parses a kind:0 event and stores the user profile
func (c *Crawler) processKind0Event(ev *nostr.Event) {
	if ev.Kind != 0 {
		return
	}

	// Check if search is enabled
	if c.searchConfig == nil || !c.searchConfig.Enabled {
		return
	}

	meta, err := sdk.ParseMetadata(ev)
	if err != nil {
		// Invalid metadata, skip
		return
	}

	// Skip if no useful profile information for search
	if meta.Name == "" && meta.DisplayName == "" && meta.NIP05 == "" {
		return
	}

	// If top percentile filtering is enabled
	if c.searchConfig.TopPercentile > 0 {
		// Check if user is in top percentile before saving
		inTop, err := c.repo.IsUserInTopPercentile(ev.PubKey, c.searchConfig.TopPercentile)
		if err != nil {
			// Log but don't save on error
			log.Printf("[CRAWLER] Error checking user percentile: %v", err)
			return
		}
		if !inTop {
			return
		}
	}

	// Serialize the full event to JSON
	eventJSON, err := json.Marshal(ev)
	if err != nil {
		return
	}

	// Store the profile with searchable fields and full event
	if err := c.repo.UpsertUserProfile(ev.PubKey, meta.Name, meta.DisplayName, meta.NIP05, string(eventJSON)); err != nil {
		log.Printf("[CRAWLER] Error storing profile for %s: %v", ev.PubKey, err)
	}
}

// --- Utility Functions ---

// statusReporter periodically logs crawler statistics
func (c *Crawler) statusReporter() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.processedMu.Lock()
			processedCount := len(c.processed)
			c.processedMu.Unlock()

			c.crawledMu.Lock()
			crawledCount := len(c.crawled)
			c.crawledMu.Unlock()

			totalFailed, banned := c.relayHealth.GetStats()
			connectedRelayCount := c.poolManager.GetConnectedRelayCount()

			if totalFailed > 0 {
				log.Printf("[STATUS] Crawled: %d | Processed: %d | Connected relays: %d | Failed relays: %d (%d banned)",
					crawledCount, processedCount, connectedRelayCount, totalFailed, banned)
			} else {
				log.Printf("[STATUS] Crawled: %d | Processed: %d | Connected relays: %d",
					crawledCount, processedCount, connectedRelayCount)
			}
		case <-c.ctx.Done():
			return
		}
	}
}
