package crawler

import (
	"context"
	"database/sql"
	"log"
	"slices"
	"strings"
	"sync"
	"time"

	"fayan/database"

	"github.com/nbd-wtf/go-nostr"
	"golang.org/x/time/rate"
)

const (
	batchSize      = 200             // Batch size for fetching
	reqRate        = 2 * time.Second // Rate limit per relay
	numProcessors  = 4               // Number of goroutines for processing results
	resultChanSize = 1000
)

// Crawler manages the recursive crawling of the Nostr network.
type Crawler struct {
	db            *sql.DB
	poolManager   *PoolManager
	relays        []string // These are the bootstrap relays
	seedPubkeys   []string
	resultsChan   chan *nostr.Event
	crawled       map[string]bool
	crawledMu     sync.Mutex
	processed     map[string]time.Time
	processedMu   sync.Mutex
	relayLimiters map[string]*rate.Limiter // Per-relay rate limiters
	limitersMu    sync.Mutex

	// Relay health tracking
	relayHealth *RelayHealthTracker

	// Sleep management when no pubkeys available
	consecutiveEmpty int
	sleepDuration    time.Duration

	// Context for graceful shutdown
	ctx    context.Context
	cancel context.CancelFunc
}

// NewCrawler creates a new Crawler instance.
func NewCrawler(db *sql.DB, relays []string, seedPubkeys []string) *Crawler {
	// Create context for managing crawler lifecycle
	ctx, cancel := context.WithCancel(context.Background())

	// Create relay options
	relayOptions := []nostr.RelayOption{
		nostr.WithNoticeHandler(func(notice string) {
			// Suppress NOTICE messages by doing nothing
		}),
	}

	// Create pool manager
	poolManager := NewPoolManager(ctx, relayOptions...)

	c := &Crawler{
		db:               db,
		poolManager:      poolManager,
		relays:           relays,
		seedPubkeys:      seedPubkeys,
		resultsChan:      make(chan *nostr.Event, resultChanSize),
		crawled:          make(map[string]bool),
		processed:        make(map[string]time.Time),
		relayLimiters:    make(map[string]*rate.Limiter),
		relayHealth:      NewRelayHealthTracker(),
		consecutiveEmpty: 0,
		sleepDuration:    5 * time.Second, // Initial sleep duration
		ctx:              ctx,
		cancel:           cancel,
	}
	return c
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

	// Give some time for goroutines to finish
	time.Sleep(2 * time.Second)

	log.Println("[CRAWLER] Shutdown complete")
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

		randomPubkeys, err := database.RandomPubkeys(c.db, batchSize)
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

	// Step 3: Fetch contacts from each relay concurrently
	// Collect results from all relays
	contactEvents := make(map[string]*nostr.Event)
	var wg sync.WaitGroup
	var eventsMu sync.Mutex // Protect concurrent map writes

	for relay, users := range relayToUsers {
		wg.Add(1)
		go func(r string, u []string) {
			defer wg.Done()
			events := c.fetchContactsFromRelay(r, u)

			// Use mutex to protect map access
			eventsMu.Lock()
			for pubkey, event := range events {
				if existing, exists := contactEvents[pubkey]; !exists || event.CreatedAt > existing.CreatedAt {
					contactEvents[pubkey] = event
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
}

// fetchContactsFromRelay fetches contacts for multiple users from a single relay
// Returns a map of pubkey -> latest event from this relay
func (c *Crawler) fetchContactsFromRelay(relay string, pubkeys []string) map[string]*nostr.Event {
	if len(pubkeys) == 0 {
		return nil
	}

	// Check if context is cancelled
	select {
	case <-c.ctx.Done():
		return nil
	default:
	}

	// Skip if relay is banned
	if c.relayHealth.IsRelayBanned(relay) {
		return nil
	}

	// Apply rate limiting for this specific relay
	limiter := c.getRelayLimiter(relay)
	if err := limiter.Wait(c.ctx); err != nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(c.ctx, 15*time.Second)
	defer cancel()

	filter := nostr.Filter{
		Kinds:   []int{3},
		Authors: pubkeys,
	}

	// Get the current pool from pool manager
	pool := c.poolManager.GetPool()

	// SubscribeMany returns a channel of RelayEvent
	eventsChan := pool.FetchMany(ctx, []string{relay}, filter)

	// Track relay usage
	c.poolManager.TrackRelayUsage(relay)

	// Collect events and keep only the latest for each pubkey
	events := make(map[string]*nostr.Event)
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
				return events
			}

			ev := relayEvent.Event

			// Keep only the latest event for each pubkey from this relay
			if existing, exists := events[ev.PubKey]; !exists || ev.CreatedAt > existing.CreatedAt {
				events[ev.PubKey] = ev
			}
		case <-timer.C:
			// Timeout - this could indicate connection issues
			if !channelClosed {
				c.relayHealth.RecordFailure(relay, "timeout - no response")
			}
			return events
		case <-ctx.Done():
			// Context cancelled
			if !channelClosed {
				c.relayHealth.RecordFailure(relay, "context cancelled")
			}
			return events
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

	database.UpsertPubkey(c.db, ev.PubKey)

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

			database.UpsertPubkey(c.db, targetPubkey)
			database.UpsertConnection(c.db, ev.PubKey, targetPubkey)

			targetCount++

			c.crawledMu.Lock()
			c.crawled[targetPubkey] = true
			c.crawledMu.Unlock()
		}
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
