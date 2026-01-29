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

// Config represents the application configuration
type Config struct {
	Relays           []string      `yaml:"relays"`
	SeedPubkeys      []string      `yaml:"seed_pubkeys"`
	Database         string        `yaml:"database"`
	PageRankInterval int           `yaml:"pagerank_interval"`
	Port             string        `yaml:"port"`
	Search           SearchConfig  `yaml:"search"`
	Ranking          RankingConfig `yaml:"ranking"`
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

	return &cfg, nil
}

// GetPageRankInterval returns the PageRank calculation interval as a time.Duration
func (c *Config) GetPageRankInterval() time.Duration {
	return time.Duration(c.PageRankInterval) * time.Minute
}
