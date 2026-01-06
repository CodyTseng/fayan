package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"fayan/config"
	"fayan/internal/crawler"
	"fayan/internal/ranking"
	"fayan/internal/repository"
)

func main() {
	fmt.Println("\n" +
		"███████╗  █████╗  ██╗   ██╗  █████╗  ███╗   ██╗\n" +
		"██╔════╝ ██╔══██╗ ╚██╗ ██╔╝ ██╔══██╗ ████╗  ██║\n" +
		"█████╗   ███████║  ╚████╔╝  ███████║ ██╔██╗ ██║\n" +
		"██╔══╝   ██╔══██║   ╚██╔╝   ██╔══██║ ██║╚██╗██║\n" +
		"██║      ██║  ██║    ██║    ██║  ██║ ██║ ╚████║\n" +
		"╚═╝      ╚═╝  ╚═╝    ╚═╝    ╚═╝  ╚═╝ ╚═╝  ╚═══╝")
	fmt.Println("         Reputation System for Nostr")
	fmt.Println("        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	// 1. Load Configuration
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("[CONFIG] Failed to load configuration: %v", err)
	}

	// 2. Initialize Repository in read-write mode
	log.Println("[DATABASE] Initializing...")
	repo, err := repository.New(cfg.Database, repository.ModeReadWrite)
	if err != nil {
		log.Fatalf("[DATABASE] Failed to initialize: %v", err)
	}
	defer repo.Close()
	log.Println("[DATABASE] Ready (read-write mode)")

	// 3. Create ranking calculator
	calculator := ranking.NewCalculator(repo, cfg.SeedPubkeys)

	// 4. Perform an initial rank calculation
	log.Println("[RANK] Performing initial calculation...")
	if err := calculator.Calculate(); err != nil {
		log.Printf("[RANK] Initial calculation failed: %v", err)
	} else {
		log.Println("[RANK] Initial calculation completed")
	}

	// 5. Start the Nostr Crawler
	c := crawler.NewCrawler(repo, cfg.Relays, cfg.SeedPubkeys, &cfg.Search)
	c.Start()

	// 6. Periodically Calculate Ranks
	ticker := time.NewTicker(cfg.GetPageRankInterval())
	defer ticker.Stop()

	// 7. Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	log.Println("\n[MAIN] Crawler running in background. Press Ctrl+C to exit.")

	for {
		select {
		case <-ticker.C:
			c.Pause()
			log.Println("[RANK] Starting periodic calculation...")
			if err := calculator.Calculate(); err != nil {
				log.Printf("[RANK] Calculation failed: %v", err)
			} else {
				log.Println("[RANK] Calculation completed")
			}
			c.Resume()
		case <-sigChan:
			log.Println("\n[MAIN] Shutdown signal received")
			c.Stop()
			log.Println("[MAIN] Exiting...")
			return
		}
	}
}
