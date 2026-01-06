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

// Config represents the application configuration
type Config struct {
	Relays           []string     `yaml:"relays"`
	SeedPubkeys      []string     `yaml:"seed_pubkeys"`
	Database         string       `yaml:"database"`
	PageRankInterval int          `yaml:"pagerank_interval"`
	Port             string       `yaml:"port"`
	Search           SearchConfig `yaml:"search"`
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

	return &cfg, nil
}

// GetPageRankInterval returns the PageRank calculation interval as a time.Duration
func (c *Config) GetPageRankInterval() time.Duration {
	return time.Duration(c.PageRankInterval) * time.Minute
}
