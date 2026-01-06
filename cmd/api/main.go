package main

import (
	"log"
	"net/http"
	"time"

	"fayan/config"
	"fayan/internal/api/handler"
	"fayan/internal/api/middleware"
	"fayan/internal/cache"
	"fayan/internal/repository"
)

func main() {
	// Load configuration
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Initialize repository in read-only mode
	repo, err := repository.New(cfg.Database, repository.ModeReadOnly)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer repo.Close()

	log.Println("[API] Database initialized successfully (read-only mode)")

	// Initialize cache
	apiCache := cache.New(10*time.Minute, 10*time.Minute)

	// Initialize handler with search config
	h := handler.New(repo, apiCache, &cfg.Search)

	// Setup HTTP routes
	http.HandleFunc("/health", middleware.CORS(h.Health))
	http.HandleFunc("/users", middleware.CORS(h.Users))
	http.HandleFunc("/users/", middleware.CORS(h.User))
	http.HandleFunc("/search", middleware.CORS(h.Search))
	http.HandleFunc("/", middleware.CORS(h.User)) // deprecated

	// Start server
	log.Printf("[API] Starting API server on port %s", cfg.Port)
	if err := http.ListenAndServe(cfg.Port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
