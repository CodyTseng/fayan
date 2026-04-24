package config

import (
	"log"
	"os"
	"time"

	"github.com/goccy/go-yaml"
)

// SearchConfig represents the user search configuration
type SearchConfig struct {
	Enabled       bool `yaml:"enabled"`        // Whether search feature is enabled (default: false)
	TopPercentile int  `yaml:"top_percentile"` // Only index users in top X% reputation (default: 50)
}

// RankingConfig represents the ranking weight configuration
type RankingConfig struct {
	TrustRankWeight float64 `yaml:"trustrank_weight"` // Weight for TrustRank score (default: 0.7)
	PageRankWeight  float64 `yaml:"pagerank_weight"`  // Weight for PageRank score (default: 0.3)
}

// CrawlerConfig represents the crawler performance configuration
type CrawlerConfig struct {
	BatchSize            int `yaml:"batch_size"`             // Pubkeys per batch (default: 500)
	RequestIntervalMs    int `yaml:"request_interval_ms"`    // Milliseconds between requests per relay (default: 500)
	NumContactProcessors int `yaml:"num_contact_processors"` // Number of contact event processors (default: 4)
	NumProfileProcessors int `yaml:"num_profile_processors"` // Number of profile event processors (default: 4)
}

// VouchConfig enables the /vouch and /report API endpoints.
// Submissions are authenticated via NIP-98.
type VouchConfig struct {
	Enabled bool `yaml:"enabled"` // Default: false. When false, the endpoints return 404.
	// Weight of a vouch edge relative to a follow edge (1.0). Must be in (0, 1].
	// Lower values make vouches contribute less flow than follows. Default: 0.5.
	Weight float64 `yaml:"weight"`
}

// Config represents the application configuration
type Config struct {
	Relays           []string      `yaml:"relays"`
	SeedPubkeys      []string      `yaml:"seed_pubkeys"`
	Database         string        `yaml:"database"`
	PageRankInterval int           `yaml:"pagerank_interval"`
	Port             string        `yaml:"port"`
	Search           SearchConfig  `yaml:"search"`
	Ranking          RankingConfig `yaml:"ranking"`
	Crawler          CrawlerConfig `yaml:"crawler"`
	Vouch            VouchConfig   `yaml:"vouch"`
}

// Load reads and parses the configuration file
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	cfg := Config{
		Search: SearchConfig{
			Enabled:       false,
			TopPercentile: 50,
		},
		Ranking: RankingConfig{
			TrustRankWeight: 0.7,
			PageRankWeight:  0.3,
		},
		Crawler: CrawlerConfig{
			BatchSize:            500,
			RequestIntervalMs:    500,
			NumContactProcessors: 4,
			NumProfileProcessors: 4,
		},
		Vouch: VouchConfig{
			Enabled: false,
			Weight:  0.5,
		},
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	log.Printf("[CONFIG] Loaded configuration from %s", path)
	log.Printf("[CONFIG] - Relays: %d", len(cfg.Relays))
	log.Printf("[CONFIG] - Seed pubkeys: %d", len(cfg.SeedPubkeys))
	log.Printf("[CONFIG] - Database: %s", cfg.Database)
	log.Printf("[CONFIG] - PageRank interval: %d minutes", cfg.PageRankInterval)
	log.Printf("[CONFIG] - Port: %s", cfg.Port)
	log.Printf("[CONFIG] - Search enabled: %t", cfg.Search.Enabled)
	if cfg.Search.Enabled {
		log.Printf("[CONFIG] - Search top percentile: %d%%", cfg.Search.TopPercentile)
	}
	log.Printf("[CONFIG] - Ranking weights: TrustRank=%.2f, PageRank=%.2f", cfg.Ranking.TrustRankWeight, cfg.Ranking.PageRankWeight)
	log.Printf("[CONFIG] - Crawler: batch_size=%d, request_interval=%dms, contact_processors=%d, profile_processors=%d",
		cfg.Crawler.BatchSize, cfg.Crawler.RequestIntervalMs, cfg.Crawler.NumContactProcessors, cfg.Crawler.NumProfileProcessors)
	log.Printf("[CONFIG] - Vouch endpoints enabled: %t (weight=%.2f)", cfg.Vouch.Enabled, cfg.Vouch.Weight)

	return &cfg, nil
}

// GetPageRankInterval returns the PageRank calculation interval as a time.Duration
func (c *Config) GetPageRankInterval() time.Duration {
	return time.Duration(c.PageRankInterval) * time.Minute
}
