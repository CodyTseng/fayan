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
	"fayan/internal/ingest"
	"fayan/internal/repository"

	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/sdk"
	"golang.org/x/time/rate"
)

// maxReportsPerQuery caps how many kind:1984 reports a single relay query pulls
// per author batch. Reports are append-only and a busy account can have many,
// so without a cap they would crowd out the replaceable events sharing a fetch.
// We deliberately do not paginate — successive crawl cycles re-fetch the batch,
// and reports are sparse, so the newest 50 per query suffice.
const maxReportsPerQuery = 50

// CrawlerConfig holds the crawler configuration parameters
type CrawlerConfig struct {
	BatchSize            int
	RequestInterval      time.Duration
	NumContactProcessors int
	NumProfileProcessors int
}

// Crawler manages the recursive crawling of the Nostr network.
type Crawler struct {
	repo          *repository.Repository
	poolManager   *PoolManager
	relays        []string
	seedPubkeys   []string
	searchConfig  *config.SearchConfig
	crawlerConfig *CrawlerConfig
	vouchEnabled  bool
	ingester      *ingest.Ingester
	contactsChan  chan *nostr.Event
	profilesChan  chan *nostr.Event
	reportsChan   chan *nostr.Event
	crawled       map[string]bool
	crawledMu     sync.Mutex
	relayLimiters map[string]*rate.Limiter
	limitersMu    sync.Mutex

	relayHealth *RelayHealthTracker

	consecutiveEmpty int
	sleepDuration    time.Duration

	// Pause control using sync.Cond for safer synchronization
	paused    bool
	pauseCond *sync.Cond

	// WaitGroup for tracking worker goroutines
	wg sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
}

// NewCrawler creates a new Crawler instance. When vouchEnabled is true the
// crawler also fetches kind:1984 reports and kind:10040 vouch sets.
func NewCrawler(repo *repository.Repository, relays []string, seedPubkeys []string, searchConfig *config.SearchConfig, crawlerConfig *CrawlerConfig, vouchEnabled bool) *Crawler {
	ctx, cancel := context.WithCancel(context.Background())

	relayOptions := []nostr.RelayOption{
		nostr.WithNoticeHandler(func(notice string) {}),
	}

	poolManager := NewPoolManager(ctx, relayOptions...)

	// Calculate channel buffer sizes based on batch size
	contactsChanSize := crawlerConfig.BatchSize * 3
	profilesChanSize := crawlerConfig.BatchSize * crawlerConfig.NumProfileProcessors * 2
	reportsChanSize := crawlerConfig.BatchSize * 2

	c := &Crawler{
		repo:             repo,
		poolManager:      poolManager,
		relays:           relays,
		seedPubkeys:      seedPubkeys,
		searchConfig:     searchConfig,
		crawlerConfig:    crawlerConfig,
		vouchEnabled:     vouchEnabled,
		ingester:         ingest.New(repo),
		contactsChan:     make(chan *nostr.Event, contactsChanSize),
		profilesChan:     make(chan *nostr.Event, profilesChanSize),
		reportsChan:      make(chan *nostr.Event, reportsChanSize),
		crawled:          make(map[string]bool),
		relayLimiters:    make(map[string]*rate.Limiter),
		relayHealth:      NewRelayHealthTracker(),
		consecutiveEmpty: 0,
		sleepDuration:    5 * time.Second,
		ctx:              ctx,
		cancel:           cancel,
	}
	c.pauseCond = sync.NewCond(&sync.Mutex{})
	return c
}

// Stop gracefully shuts down the crawler and cleans up connections
func (c *Crawler) Stop() {
	log.Println("[CRAWLER] Shutting down...")

	// Cancel the context to signal all goroutines to stop
	c.cancel()

	// Wake up any goroutines waiting on pause condition
	c.pauseCond.L.Lock()
	c.paused = false
	c.pauseCond.Broadcast()
	c.pauseCond.L.Unlock()

	// Wait for all worker goroutines to finish
	c.wg.Wait()

	// Now it's safe to close channels (no more senders)
	close(c.contactsChan)
	close(c.profilesChan)
	close(c.reportsChan)

	// Stop the pool manager (this will close all relay connections)
	c.poolManager.Stop()

	log.Println("[CRAWLER] Shutdown complete")
}

// Pause temporarily stops the crawler from fetching new data
func (c *Crawler) Pause() {
	c.pauseCond.L.Lock()
	defer c.pauseCond.L.Unlock()
	if !c.paused {
		c.paused = true
		log.Println("[CRAWLER] Paused")
	}
}

// Resume resumes the crawler after being paused
func (c *Crawler) Resume() {
	c.pauseCond.L.Lock()
	defer c.pauseCond.L.Unlock()
	if c.paused {
		c.paused = false
		c.pauseCond.Broadcast()
		log.Println("[CRAWLER] Resumed")
	}
}

// waitIfPaused blocks if the crawler is paused, returns true if context was cancelled
func (c *Crawler) waitIfPaused() bool {
	c.pauseCond.L.Lock()
	for c.paused {
		// Check context before waiting
		select {
		case <-c.ctx.Done():
			c.pauseCond.L.Unlock()
			return true
		default:
		}

		// Use a goroutine to check context while waiting
		done := make(chan struct{})
		go func() {
			select {
			case <-c.ctx.Done():
				c.pauseCond.Broadcast() // Wake up the waiting goroutine
			case <-done:
			}
		}()

		c.pauseCond.Wait()
		close(done)

		// Check if we were woken up due to context cancellation
		select {
		case <-c.ctx.Done():
			c.pauseCond.L.Unlock()
			return true
		default:
		}
	}
	c.pauseCond.L.Unlock()
	return false
}

// getRelayLimiter returns a rate limiter for a specific relay, creating one if needed
func (c *Crawler) getRelayLimiter(relay string) *rate.Limiter {
	c.limitersMu.Lock()
	defer c.limitersMu.Unlock()

	if limiter, exists := c.relayLimiters[relay]; exists {
		return limiter
	}

	limiter := rate.NewLimiter(rate.Every(c.crawlerConfig.RequestInterval), 1)
	c.relayLimiters[relay] = limiter
	return limiter
}

// Start begins the crawling process.
func (c *Crawler) Start() {
	// Single goroutine for network operations to avoid rate limiting
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.networkWorker()
	}()

	// Multiple goroutines for processing contact events
	for range c.crawlerConfig.NumContactProcessors {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.contactProcessor()
		}()
	}

	// Multiple goroutines for processing profile events (search functionality)
	if c.searchConfig != nil && c.searchConfig.Enabled {
		for range c.crawlerConfig.NumProfileProcessors {
			c.wg.Add(1)
			go func() {
				defer c.wg.Done()
				c.profileProcessor()
			}()
		}
	}

	// Processors for kind:1984 report events (only when the feature is enabled)
	if c.vouchEnabled {
		for range c.crawlerConfig.NumContactProcessors {
			c.wg.Add(1)
			go func() {
				defer c.wg.Done()
				c.reportProcessor()
			}()
		}
	}

	// Status reporter (not tracked in wg since it's non-critical)
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

		pubkeys, err := c.repo.OldestPubkeys(c.crawlerConfig.BatchSize)
		if err != nil || len(pubkeys) == 0 {
			c.fetchBatch(c.seedPubkeys)
		} else {
			c.fetchBatch(pubkeys)
			// Mark these pubkeys as crawled
			if err := c.repo.MarkPubkeysCrawled(pubkeys); err != nil {
				log.Printf("[CRAWLER] Error marking pubkeys as crawled: %v", err)
			}
		}
	}
}

// contactProcessor handles processing of contact events (kind 3)
func (c *Crawler) contactProcessor() {
	for {
		// Check if paused before processing
		if c.waitIfPaused() {
			// Context was cancelled while paused, drain remaining events
			c.drainContactsChan()
			return
		}

		select {
		case event, ok := <-c.contactsChan:
			if !ok {
				return // Channel closed, exit
			}
			c.dispatchRelationEvent(event)
		case <-c.ctx.Done():
			// Context cancelled, drain remaining events before exiting
			c.drainContactsChan()
			return
		}
	}
}

// dispatchRelationEvent routes a relation event from the contacts channel to
// its handler by kind (kind:3 follows or kind:10040 vouch sets).
func (c *Crawler) dispatchRelationEvent(event *nostr.Event) {
	if event == nil {
		return
	}
	switch event.Kind {
	case ingest.KindContacts:
		c.processKind3Event(event)
	case ingest.KindVouchSet:
		c.processVouchSetEvent(event)
	}
}

// drainContactsChan processes any remaining events in the contacts channel
func (c *Crawler) drainContactsChan() {
	for {
		select {
		case event, ok := <-c.contactsChan:
			if !ok {
				return
			}
			c.dispatchRelationEvent(event)
		default:
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

	// Step 3: Fetch contacts (and profiles/vouch sets) from each relay concurrently
	// Collect results from all relays
	contactEvents := make(map[string]*nostr.Event)
	profileEvents := make(map[string]*nostr.Event)
	vouchSetEvents := make(map[string]*nostr.Event)
	var reportEvents []*nostr.Event
	var wg sync.WaitGroup
	var eventsMu sync.Mutex // Protect concurrent map writes

	fetchProfiles := c.searchConfig != nil && c.searchConfig.Enabled

	for relay, users := range relayToUsers {
		wg.Add(1)
		go func(r string, u []string) {
			defer wg.Done()
			contacts, profiles, vouchSets := c.fetchEventsFromRelay(r, u, fetchProfiles)

			var reports []*nostr.Event
			if c.vouchEnabled {
				reports = c.fetchReportsFromRelay(r, u)
			}

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
			for pubkey, event := range vouchSets {
				if existing, exists := vouchSetEvents[pubkey]; !exists || event.CreatedAt > existing.CreatedAt {
					vouchSetEvents[pubkey] = event
				}
			}
			reportEvents = append(reportEvents, reports...)
			eventsMu.Unlock()
		}(relay, users)
	}

	// Wait for all relays to finish
	wg.Wait()

	// Step 5: Check against global timestamps and send to processors.
	// Contacts and vouch sets share the contacts channel (dispatched by kind).
	for _, event := range contactEvents {
		select {
		case c.contactsChan <- event:
		case <-c.ctx.Done():
			return
		}
	}

	for _, event := range vouchSetEvents {
		select {
		case c.contactsChan <- event:
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

	// Send report events to report processor
	for _, event := range reportEvents {
		select {
		case c.reportsChan <- event:
		case <-c.ctx.Done():
			return
		}
	}
}

// fetchEventsFromRelay fetches the replaceable events for multiple users from a
// single relay: kind 3 (contacts), optionally kind 0 (profiles), and — when the
// vouch feature is enabled — kind 10040 (vouch sets). All are one-per-author, so
// they share a single small-limit query. kind:1984 reports are NOT fetched here;
// being append-only and numerous, they get their own query (fetchReportsFromRelay).
// Returns maps of pubkey -> latest event for contacts, profiles, and vouch sets.
func (c *Crawler) fetchEventsFromRelay(relay string, pubkeys []string, fetchProfiles bool) (map[string]*nostr.Event, map[string]*nostr.Event, map[string]*nostr.Event) {
	if len(pubkeys) == 0 {
		return nil, nil, nil
	}

	// Check if context is cancelled
	select {
	case <-c.ctx.Done():
		return nil, nil, nil
	default:
	}

	// Skip if relay is banned
	if c.relayHealth.IsRelayBanned(relay) {
		return nil, nil, nil
	}

	// Apply rate limiting for this specific relay
	limiter := c.getRelayLimiter(relay)
	if err := limiter.Wait(c.ctx); err != nil {
		return nil, nil, nil
	}

	ctx, cancel := context.WithTimeout(c.ctx, 15*time.Second)
	defer cancel()

	// Build filter with kinds 3 (contacts), optionally 0 (profiles) and 10040 (vouch sets)
	kinds := []int{3}
	if fetchProfiles {
		kinds = append(kinds, 0)
	}
	if c.vouchEnabled {
		kinds = append(kinds, ingest.KindVouchSet)
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
	vouchSets := make(map[string]*nostr.Event)
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
				return contacts, profiles, vouchSets
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
			case ingest.KindVouchSet:
				if existing, exists := vouchSets[ev.PubKey]; !exists || ev.CreatedAt > existing.CreatedAt {
					vouchSets[ev.PubKey] = ev
				}
			}
		case <-timer.C:
			// Timeout - this could indicate connection issues
			if !channelClosed {
				c.relayHealth.RecordFailure(relay, "timeout - no response")
			}
			return contacts, profiles, vouchSets
		case <-ctx.Done():
			// Context cancelled
			if !channelClosed {
				c.relayHealth.RecordFailure(relay, "context cancelled")
			}
			return contacts, profiles, vouchSets
		}
	}
}

// fetchReportsFromRelay fetches kind:1984 reports for multiple users from a
// single relay in their own capped query, kept separate from the replaceable
// events so a flood of reports cannot crowd them out. Returns all matching
// events (an author may have many); de-duplication happens at ingest time.
func (c *Crawler) fetchReportsFromRelay(relay string, pubkeys []string) []*nostr.Event {
	if len(pubkeys) == 0 {
		return nil
	}

	select {
	case <-c.ctx.Done():
		return nil
	default:
	}

	if c.relayHealth.IsRelayBanned(relay) {
		return nil
	}

	limiter := c.getRelayLimiter(relay)
	if err := limiter.Wait(c.ctx); err != nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(c.ctx, 15*time.Second)
	defer cancel()

	limit := maxReportsPerQuery
	filter := nostr.Filter{
		Kinds:   []int{ingest.KindReport},
		Authors: pubkeys,
		Limit:   limit,
	}

	pool := c.poolManager.GetPool()
	eventsChan := pool.FetchMany(ctx, []string{relay}, filter)
	c.poolManager.TrackRelayUsage(relay)

	var reports []*nostr.Event
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	for {
		select {
		case relayEvent, ok := <-eventsChan:
			if !ok {
				return reports
			}
			reports = append(reports, relayEvent.Event)
		case <-timer.C:
			return reports
		case <-ctx.Done():
			return reports
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
// Uses batch writes to reduce lock contention.
func (c *Crawler) processKind3Event(ev *nostr.Event) {
	if ev.Kind != ingest.KindContacts {
		return
	}

	// Parse with the shared ingest parser so the crawler and POST /event paths
	// produce identical follow edges.
	pubkeys, connections := ingest.ParseContacts(ev)

	// Batch write all pubkeys and connections in a single transaction
	if err := c.repo.BatchUpsertPubkeysAndConnections(pubkeys, connections); err != nil {
		log.Printf("[CRAWLER] Error batch upserting for %s: %v", ev.PubKey, err)
	}

	// Update crawled map
	c.crawledMu.Lock()
	for _, pk := range pubkeys {
		c.crawled[pk] = true
	}
	c.crawledMu.Unlock()
}

// processVouchSetEvent verifies a kind:10040 event's signature and replaces the
// author's vouch edges with the listed pubkeys.
func (c *Crawler) processVouchSetEvent(ev *nostr.Event) {
	if ev.Kind != ingest.KindVouchSet {
		return
	}
	if ok, err := ev.CheckSignature(); err != nil || !ok {
		return
	}
	if err := c.ingester.ApplyVouchSet(ev); err != nil {
		log.Printf("[CRAWLER] Error applying vouch set for %s: %v", ev.PubKey, err)
	}
}

// processReportEvent verifies a kind:1984 event's signature and stores it as a
// report edge when it is a profile-level spam/impersonation report.
func (c *Crawler) processReportEvent(ev *nostr.Event) {
	if ev.Kind != ingest.KindReport {
		return
	}
	if ok, err := ev.CheckSignature(); err != nil || !ok {
		return
	}
	if _, err := c.ingester.ApplyReport(ev); err != nil {
		log.Printf("[CRAWLER] Error applying report from %s: %v", ev.PubKey, err)
	}
}

// reportProcessor handles processing of report events (kind 1984)
func (c *Crawler) reportProcessor() {
	for {
		if c.waitIfPaused() {
			c.drainReportsChan()
			return
		}

		select {
		case event, ok := <-c.reportsChan:
			if !ok {
				return
			}
			if event != nil {
				c.processReportEvent(event)
			}
		case <-c.ctx.Done():
			c.drainReportsChan()
			return
		}
	}
}

// drainReportsChan processes any remaining events in the reports channel
func (c *Crawler) drainReportsChan() {
	for {
		select {
		case event, ok := <-c.reportsChan:
			if !ok {
				return
			}
			if event != nil {
				c.processReportEvent(event)
			}
		default:
			return
		}
	}
}

// profileProcessor handles processing of profile events (kind 0)
func (c *Crawler) profileProcessor() {
	for {
		// Check if paused before processing
		if c.waitIfPaused() {
			// Context was cancelled while paused, drain remaining events
			c.drainProfilesChan()
			return
		}

		select {
		case event, ok := <-c.profilesChan:
			if !ok {
				return // Channel closed, exit
			}
			if event != nil {
				c.processKind0Event(event)
			}
		case <-c.ctx.Done():
			// Context cancelled, drain remaining events before exiting
			c.drainProfilesChan()
			return
		}
	}
}

// drainProfilesChan processes any remaining events in the profiles channel
func (c *Crawler) drainProfilesChan() {
	for {
		select {
		case event, ok := <-c.profilesChan:
			if !ok {
				return
			}
			if event != nil {
				c.processKind0Event(event)
			}
		default:
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
			c.crawledMu.Lock()
			crawledCount := len(c.crawled)
			c.crawledMu.Unlock()

			totalFailed, banned := c.relayHealth.GetStats()
			connectedRelayCount := c.poolManager.GetConnectedRelayCount()

			if totalFailed > 0 {
				log.Printf("[STATUS] Crawled: %d | Connected relays: %d | Failed relays: %d (%d banned)",
					crawledCount, connectedRelayCount, totalFailed, banned)
			} else {
				log.Printf("[STATUS] Crawled: %d | Connected relays: %d",
					crawledCount, connectedRelayCount)
			}
		case <-c.ctx.Done():
			return
		}
	}
}
