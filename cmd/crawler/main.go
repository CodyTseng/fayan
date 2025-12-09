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
	fmt.Println("         Reputation System for Nostr")
	fmt.Println("        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	// 1. Load Configuration
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("[CONFIG] Failed to load configuration: %v", err)
	}

	// 2. Initialize Database in read-write mode (optimized for crawler writes)
	log.Println("[DATABASE] Initializing...")
	db, err := database.Initialize(cfg.Database, database.ModeReadWrite)
	if err != nil {
		log.Fatalf("[DATABASE] Failed to initialize: %v", err)
	}
	defer db.Close()
	log.Println("[DATABASE] Ready (read-write mode)")

	// 3. Perform an initial rank calculation immediately
	log.Println("[RANK] Performing initial calculation...")
	if err := calculator.Calculate(db, cfg.SeedPubkeys); err != nil {
		log.Printf("[RANK] Initial calculation failed: %v", err)
	} else {
		log.Println("[RANK] Initial calculation completed")
	}

	// 4. Start the Nostr Crawler in a background goroutine
	c := crawler.NewCrawler(db, cfg.Relays, cfg.SeedPubkeys)
	c.Start()

	// 5. Periodically Calculate Ranks
	ticker := time.NewTicker(cfg.GetPageRankInterval())
	defer ticker.Stop()

	// 6. Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	log.Println("\n[MAIN] Crawler running in background. Press Ctrl+C to exit.")

	for {
		select {
		case <-ticker.C:
			log.Println("[RANK] Starting periodic calculation...")
			if err := calculator.Calculate(db, cfg.SeedPubkeys); err != nil {
				log.Printf("[RANK] Calculation failed: %v", err)
			} else {
				log.Println("[RANK] Calculation completed")
			}
		case <-sigChan:
			log.Println("\n[MAIN] Shutdown signal received")
			c.Stop()
			log.Println("[MAIN] Exiting...")
			return
		}
	}
}
