package crawler

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"fayan/internal/repository"

	"github.com/nbd-wtf/go-nostr"
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
	resultsChan   chan *nostr.Event
	crawled       map[string]bool
	crawledMu     sync.Mutex
	processed     map[string]time.Time
	processedMu   sync.Mutex
	relayLimiters map[string]*rate.Limiter
	limitersMu    sync.Mutex

	relayHealth *RelayHealthTracker

	consecutiveEmpty int
	sleepDuration    time.Duration

	ctx    context.Context
	cancel context.CancelFunc
}

// NewCrawler creates a new Crawler instance.
func NewCrawler(repo *repository.Repository, relays []string, seedPubkeys []string) *Crawler {
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
		resultsChan:      make(chan *nostr.Event, resultChanSize),
		crawled:          make(map[string]bool),
		processed:        make(map[string]time.Time),
		relayLimiters:    make(map[string]*rate.Limiter),
		relayHealth:      NewRelayHealthTracker(),
		consecutiveEmpty: 0,
		sleepDuration:    5 * time.Second,
		ctx:              ctx,
		cancel:           cancel,
	}
}

// Stop gracefully shuts down the crawler
func (c *Crawler) Stop() {
	log.Println("[CRAWLER] Shutting down...")
	c.cancel()
	c.poolManager.Stop()
	close(c.resultsChan)
	time.Sleep(2 * time.Second)
	log.Println("[CRAWLER] Shutdown complete")
}

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
	go c.networkWorker()

	for range numProcessors {
		go c.resultProcessor()
	}

	go c.statusReporter()
}

func (c *Crawler) networkWorker() {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
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
				c.handleEmptyBatch()
				continue
			}

			c.consecutiveEmpty = 0
			c.sleepDuration = 5 * time.Second
			c.fetchBatch(pubkeys)
		}
	}
}

func (c *Crawler) handleEmptyBatch() {
	c.consecutiveEmpty++
	if c.consecutiveEmpty > 5 {
		c.sleepDuration = min(c.sleepDuration*2, 5*time.Minute)
	}
	log.Printf("[CRAWLER] No new pubkeys to process, sleeping %v", c.sleepDuration)
	time.Sleep(c.sleepDuration)
}

func (c *Crawler) fetchBatch(pubkeys []string) {
	for _, pk := range pubkeys {
		c.processedMu.Lock()
		c.processed[pk] = time.Now()
		c.processedMu.Unlock()
	}

	healthyRelays := c.getHealthyRelays()
	if len(healthyRelays) == 0 {
		log.Println("[CRAWLER] No healthy relays available, waiting...")
		time.Sleep(30 * time.Second)
		return
	}

	filter := nostr.Filter{
		Authors: pubkeys,
		Kinds:   []int{nostr.KindFollowList},
	}

	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()

	for event := range c.poolManager.GetPool().SubManyEose(ctx, healthyRelays, nostr.Filters{filter}) {
		if event.Event != nil {
			c.resultsChan <- event.Event
		}
	}
}

func (c *Crawler) getHealthyRelays() []string {
	healthy := make([]string, 0, len(c.relays))
	for _, relay := range c.relays {
		if !c.relayHealth.IsRelayBanned(relay) {
			healthy = append(healthy, relay)
		}
	}
	return healthy
}

func (c *Crawler) resultProcessor() {
	for event := range c.resultsChan {
		c.processEvent(event)
	}
}

func (c *Crawler) processEvent(event *nostr.Event) {
	c.crawledMu.Lock()
	if c.crawled[event.ID] {
		c.crawledMu.Unlock()
		return
	}
	c.crawled[event.ID] = true
	c.crawledMu.Unlock()

	source := event.PubKey

	for _, tag := range event.Tags {
		if len(tag) >= 2 && tag[0] == "p" {
			target := tag[1]
			if target != "" && target != source && nostr.IsValidPublicKey(target) {
				c.repo.UpsertPubkey(source)
				c.repo.UpsertPubkey(target)
				c.repo.UpsertConnection(source, target)
			}
		}
	}
}

func (c *Crawler) statusReporter() {
	ticker := time.NewTicker(5 * time.Minute)
	checkpointTicker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	defer checkpointTicker.Stop()

	for {
		select {
		case <-ticker.C:
			c.crawledMu.Lock()
			crawledCount := len(c.crawled)
			c.crawledMu.Unlock()

			c.processedMu.Lock()
			processedCount := len(c.processed)
			c.processedMu.Unlock()

			total, banned := c.relayHealth.GetStats()
			log.Printf("[CRAWLER] Status: crawled=%d, processed=%d, relays(failed=%d, banned=%d)",
				crawledCount, processedCount, total, banned)
		case <-checkpointTicker.C:
			// Periodically checkpoint WAL to prevent it from growing too large
			log.Println("[CRAWLER] Running periodic WAL checkpoint...")
			c.repo.Checkpoint()
		case <-c.ctx.Done():
			return
		}
	}
}

// Helper to check if relay URL contains certain patterns
func containsAny(s string, substrs []string) bool {
	for _, substr := range substrs {
		if strings.Contains(s, substr) {
			return true
		}
	}
	return false
}
