package main

import (
	"embed"
	"io"
	"io/fs"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"fayan/config"
	"fayan/internal/api/handler"
	"fayan/internal/api/middleware"
	"fayan/internal/cache"
	"fayan/internal/repository"
)

//go:embed static/*
var staticFiles embed.FS

func main() {
	// Load configuration
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	repo, err := repository.New(cfg.Database)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer repo.Close()

	log.Println("[API] Database initialized successfully")

	// Initialize cache
	apiCache := cache.New(10*time.Minute, 10*time.Minute)

	// Initialize handler with search config and seed pubkeys (seeds always
	// qualify for vouch/report submissions regardless of trust_score).
	h := handler.New(repo, apiCache, &cfg.Search, cfg.SeedPubkeys)

	// Setup static file system
	staticFS, err := fs.Sub(staticFiles, "static")
	if err != nil {
		log.Printf("[API] Warning: Failed to load static files: %v", err)
	}

	// serveStaticFile tries to serve a static file, returns true if successful
	serveStaticFile := func(w http.ResponseWriter, r *http.Request, path string, cacheMaxAge int) bool {
		if staticFS == nil {
			return false
		}

		f, err := staticFS.Open(path)
		if err != nil {
			return false
		}
		defer f.Close()

		stat, err := f.Stat()
		if err != nil || stat.IsDir() {
			return false
		}

		// Set cache headers
		if cacheMaxAge > 0 {
			w.Header().Set("Cache-Control", "public, max-age="+strconv.Itoa(cacheMaxAge))
		}

		http.ServeContent(w, r, stat.Name(), stat.ModTime(), f.(io.ReadSeeker))
		return true
	}

	// serveIndexHTML serves the SPA index.html (no cache for SPA entry point)
	serveIndexHTML := func(w http.ResponseWriter, r *http.Request) {
		if !serveStaticFile(w, r, "index.html", 0) {
			http.NotFound(w, r)
		}
	}

	// Setup HTTP routes
	http.HandleFunc("/health", middleware.CORS(h.Health))
	http.HandleFunc("/users", middleware.CORS(h.Users))
	http.HandleFunc("/users/", middleware.CORS(h.User))
	http.HandleFunc("/search", middleware.CORS(h.Search))

	// Event ingestion endpoint. Accepts signed Nostr events (kind 3 / 1984 /
	// 10040) as an immediate push complement to the crawler. When vouch.weight
	// <= 0 the feature is disabled: no route is registered and requests fall
	// through to the SPA catch-all handler below.
	if cfg.Vouch.Enabled() {
		http.HandleFunc("/event", middleware.CORS(h.PostEvent))
		log.Printf("[API] Event ingestion endpoint enabled (weight=%.2f)", cfg.Vouch.Weight)
	}

	// Serve static assets (js, css, images, etc.) with long cache (1 year for hashed assets)
	http.HandleFunc("/assets/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		if !serveStaticFile(w, r, path, 31536000) {
			http.NotFound(w, r)
		}
	})

	// Serve favicon and other root static files (1 day cache)
	http.HandleFunc("/favicon.svg", func(w http.ResponseWriter, r *http.Request) {
		serveStaticFile(w, r, "favicon.svg", 86400)
	})

	// Handle root and SPA routes
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")

		// Root path - serve index.html
		if path == "" {
			serveIndexHTML(w, r)
			return
		}

		// Known SPA routes - serve index.html
		if path == "docs" {
			serveIndexHTML(w, r)
			return
		}

		// Try to serve as static file first (1 hour cache)
		if serveStaticFile(w, r, path, 3600) {
			return
		}

		// Fall back to User handler (deprecated /{pubkey} route)
		middleware.CORS(h.User)(w, r)
	})

	// Start server
	log.Printf("[API] Starting API server on port %s", cfg.Port)
	if err := http.ListenAndServe(cfg.Port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
