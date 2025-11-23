package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"fayan/calculator"
	"fayan/config"
	"fayan/crawler"
	"fayan/database"
)

func main() {
	fmt.Println("\n" +
		"███████╗  █████╗  ██╗   ██╗  █████╗  ███╗   ██╗\n" +
		"██╔════╝ ██╔══██╗ ╚██╗ ██╔╝ ██╔══██╗ ████╗  ██║\n" +
		"█████╗   ███████║  ╚████╔╝  ███████║ ██╔██╗ ██║\n" +
		"██╔══╝   ██╔══██║   ╚██╔╝   ██╔══██║ ██║╚██╗██║\n" +
		"██║      ██║  ██║    ██║    ██║  ██║ ██║ ╚████║\n" +
		"╚═╝      ╚═╝  ╚═╝    ╚═╝    ╚═╝  ╚═╝ ╚═╝  ╚═══╝")
	fmt.Println("   PageRank-based Reputation System for Nostr")
	fmt.Println("   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	// 1. Load Configuration
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("[CONFIG] Failed to load configuration: %v", err)
	}

	// 2. Initialize Database
	log.Println("[DATABASE] Initializing...")
	db, err := database.Initialize(cfg.Database)
	if err != nil {
		log.Fatalf("[DATABASE] Failed to initialize: %v", err)
	}
	defer db.Close()
	log.Println("[DATABASE] Ready")

	// 3. Perform an initial PageRank calculation immediately
	log.Println("[PAGERANK] Performing initial calculation...")
	if err := calculator.Calculate(db); err != nil {
		log.Printf("[PAGERANK] Initial calculation failed: %v", err)
	} else {
		log.Println("[PAGERANK] Initial calculation completed")
	}

	// 4. Start the Nostr Crawler in a background goroutine
	c := crawler.NewCrawler(db, cfg.Relays, cfg.SeedPubkeys)
	c.Start()

	// 5. Periodically Calculate PageRank
	ticker := time.NewTicker(cfg.GetPageRankInterval())
	defer ticker.Stop()

	// 6. Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	log.Println("\n[MAIN] Crawler running in background. Press Ctrl+C to exit.")

	for {
		select {
		case <-ticker.C:
			log.Println("[PAGERANK] Starting periodic calculation...")
			if err := calculator.Calculate(db); err != nil {
				log.Printf("[PAGERANK] Calculation failed: %v", err)
			} else {
				log.Println("[PAGERANK] Calculation completed")
			}
		case <-sigChan:
			log.Println("\n[MAIN] Shutdown signal received")
			c.Stop()
			log.Println("[MAIN] Exiting...")
			return
		}
	}
}
