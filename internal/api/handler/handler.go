package handler

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"fayan/internal/cache"
	"fayan/internal/repository"

	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip19"
)

// Handler contains dependencies for HTTP handlers
type Handler struct {
	repo  *repository.Repository
	cache *cache.Cache
}

// New creates a new Handler instance
func New(repo *repository.Repository, cache *cache.Cache) *Handler {
	return &Handler{
		repo:  repo,
		cache: cache,
	}
}

// HealthResponse represents the health check response
type HealthResponse struct {
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
	Database  string    `json:"database"`
}

// UserResponse represents the user query response
type UserResponse struct {
	Pubkey     string `json:"pubkey"`
	Rank       *int   `json:"rank,omitempty"`
	Percentile int    `json:"percentile"`
	Followers  int    `json:"followers"`
	Following  int    `json:"following"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}

// Health handles GET /health requests
func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if err := h.repo.Ping(); err != nil {
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

// User handles GET /{pubkeyOrNpub} requests
func (h *Handler) User(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

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

	// Get user with cache
	user, err := h.cache.GetUser(pubkey, func() (interface{}, error) {
		return h.repo.GetUserByPubkey(pubkey)
	})
	if err != nil {
		log.Printf("[API] Error querying user %s: %v", pubkey, err)
		writeError(w, http.StatusInternalServerError, "Failed to query user")
		return
	}

	if user == nil {
		writeError(w, http.StatusNotFound, "User not found")
		return
	}

	// Get total users with cache
	totalUsers, err := h.cache.GetTotalUsers(func() (int, error) {
		return h.repo.GetTotalUsers()
	})
	if err != nil {
		log.Printf("[API] Error getting total users: %v", err)
		writeError(w, http.StatusInternalServerError, "Failed to calculate percentile")
		return
	}

	percentile := 0
	if user.Rank != nil && totalUsers > 0 {
		percentile = int(float64(totalUsers-*user.Rank) / float64(totalUsers) * 100)
	}

	response := UserResponse{
		Pubkey:     user.Pubkey,
		Rank:       user.Rank,
		Percentile: percentile,
		Followers:  user.Followers,
		Following:  user.Following,
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
