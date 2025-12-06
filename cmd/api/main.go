package main

import (
	"database/sql"
	"encoding/json"
	"fayan/config"
	"fayan/database"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip19"
)

var (
	db  *sql.DB
	cfg *config.Config
)

// Cache structures
type userCacheEntry struct {
	user      *database.UserInfo
	timestamp time.Time
}

type totalUsersCacheEntry struct {
	count     int
	timestamp time.Time
}

var (
	userCache       = make(map[string]*userCacheEntry)
	userCacheMutex  sync.RWMutex
	totalUsersCache *totalUsersCacheEntry
	totalUsersMutex sync.RWMutex
	userCacheTTL    = 10 * time.Minute
	totalUsersTTL   = 10 * time.Minute
)

// HealthResponse represents the health check response
type HealthResponse struct {
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
	Database  string    `json:"database"`
}

// UserResponse represents the user query response
type UserResponse struct {
	Pubkey     string `json:"pubkey"`
	Rank       *int   `json:"rank,omitempty"` // Nullable
	Percentile int    `json:"percentile"`
	Followers  int    `json:"followers"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}

// corsMiddleware adds CORS headers to allow cross-origin requests
func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Set CORS headers
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		// Handle preflight requests
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Call the next handler
		next(w, r)
	}
}

func main() {
	// Load configuration
	var err error
	cfg, err = config.Load("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Initialize database
	db, err = database.Initialize(cfg.Database)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	log.Println("[API] Database initialized successfully")

	// Start cache cleanup goroutine
	go cacheCleanupRoutine()

	// Setup HTTP routes
	http.HandleFunc("/health", corsMiddleware(healthHandler)) // Handles /health
	http.HandleFunc("/", corsMiddleware(userHandler))         // Handles /{pubkeyOrNpub}

	// Start server
	log.Printf("[API] Starting API server on port %s", cfg.Port)
	if err := http.ListenAndServe(cfg.Port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

// healthHandler handles GET /health requests
func healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// Check database connectivity
	if err := db.Ping(); err != nil {
		writeError(w, http.StatusServiceUnavailable, "Database connection failed")
		return
	}

	response := HealthResponse{
		Status:    "ok",
		Timestamp: time.Now().UTC(),
		Database:  "connected",
	}

	writeJSON(w, http.StatusOK, response)
}

// getCachedUser retrieves user from cache or database
func getCachedUser(pubkey string) (*database.UserInfo, error) {
	// Check cache first
	userCacheMutex.RLock()
	if entry, exists := userCache[pubkey]; exists {
		if time.Since(entry.timestamp) < userCacheTTL {
			userCacheMutex.RUnlock()
			return entry.user, nil
		}
	}
	userCacheMutex.RUnlock()

	// Cache miss or expired, query from database
	user, err := database.GetUserByPubkey(db, pubkey)
	if err != nil || user == nil {
		return user, err
	}

	// Update cache
	userCacheMutex.Lock()
	userCache[pubkey] = &userCacheEntry{
		user:      user,
		timestamp: time.Now(),
	}
	userCacheMutex.Unlock()

	return user, nil
}

// getCachedTotalUsers retrieves total users from cache or database
func getCachedTotalUsers() (int, error) {
	// Check cache first
	totalUsersMutex.RLock()
	if totalUsersCache != nil && time.Since(totalUsersCache.timestamp) < totalUsersTTL {
		count := totalUsersCache.count
		totalUsersMutex.RUnlock()
		return count, nil
	}
	totalUsersMutex.RUnlock()

	// Cache miss or expired, query from database
	count, err := database.GetTotalUsers(db)
	if err != nil {
		return 0, err
	}

	// Update cache
	totalUsersMutex.Lock()
	totalUsersCache = &totalUsersCacheEntry{
		count:     count,
		timestamp: time.Now(),
	}
	totalUsersMutex.Unlock()

	return count, nil
}

// cacheCleanupRoutine periodically removes expired cache entries
func cacheCleanupRoutine() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		cleanupUserCache()
		cleanupTotalUsersCache()
	}
}

// cleanupUserCache removes expired user cache entries
func cleanupUserCache() {
	userCacheMutex.Lock()
	defer userCacheMutex.Unlock()

	now := time.Now()

	for pubkey, entry := range userCache {
		if now.Sub(entry.timestamp) > userCacheTTL {
			delete(userCache, pubkey)
		}
	}
}

// cleanupTotalUsersCache clears total users cache if expired
func cleanupTotalUsersCache() {
	totalUsersMutex.Lock()
	defer totalUsersMutex.Unlock()

	if totalUsersCache != nil && time.Since(totalUsersCache.timestamp) > totalUsersTTL {
		totalUsersCache = nil
	}
}

// userHandler handles GET /{pubkeyOrNpub} requests
func userHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// Extract pubkey from URL path
	// URL format: /{pubkeyOrNpub}
	path := strings.TrimPrefix(r.URL.Path, "/")
	pubkeyOrNpub := strings.TrimSpace(path)

	if pubkeyOrNpub == "" {
		writeError(w, http.StatusBadRequest, "Pubkey or npub is required in the URL path")
		return
	}

	pubkey := pubkeyOrNpub
	if strings.HasPrefix(pubkeyOrNpub, "npub") {
		prefix, value, err := nip19.Decode(pubkeyOrNpub)
		if err != nil || prefix != "npub" {
			writeError(w, http.StatusBadRequest, "Invalid npub format")
			return
		}
		// Type assert the value to string
		pubkeyStr, ok := value.(string)
		if !ok {
			writeError(w, http.StatusBadRequest, "Invalid npub value type")
			return
		}
		pubkey = pubkeyStr
	} else if !nostr.IsValidPublicKey(pubkey) {
		writeError(w, http.StatusBadRequest, "Invalid pubkey format (expected 64 hex characters)")
		return
	}

	// Query user from database with cache
	user, err := getCachedUser(pubkey)
	if err != nil {
		log.Printf("[API] Error querying user %s: %v", pubkey, err)
		writeError(w, http.StatusInternalServerError, "Failed to query user")
		return
	}

	if user == nil {
		writeError(w, http.StatusNotFound, "User not found")
		return
	}

	// Get total users for percentile calculation with cache
	totalUsers, err := getCachedTotalUsers()
	if err != nil {
		log.Printf("[API] Error getting total users: %v", err)
		writeError(w, http.StatusInternalServerError, "Failed to calculate percentile")
		return
	}

	// Calculate percentile (what percentage of users this user ranks better than)
	// rank 1 is the best, so percentile = (totalUsers - rank) / totalUsers * 100
	percentile := 0
	if user.Rank != nil && totalUsers > 0 {
		percentile = int(float64(totalUsers-*user.Rank) / float64(totalUsers) * 100)
	}

	response := UserResponse{
		Pubkey:     user.Pubkey,
		Rank:       user.Rank,
		Percentile: percentile,
		Followers:  user.Followers,
	}

	writeJSON(w, http.StatusOK, response)
}

// writeJSON writes a JSON response
func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("[API] Error encoding JSON: %v", err)
	}
}

// writeError writes an error response
func writeError(w http.ResponseWriter, status int, message string) {
	response := ErrorResponse{
		Error:   http.StatusText(status),
		Message: message,
	}
	writeJSON(w, status, response)
}
