package handler

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"fayan/config"
	"fayan/internal/cache"
	"fayan/internal/models"
	"fayan/internal/repository"

	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip19"
)

// Handler contains dependencies for HTTP handlers
type Handler struct {
	repo         *repository.Repository
	cache        *cache.Cache
	searchConfig *config.SearchConfig
}

// New creates a new Handler instance
func New(repo *repository.Repository, cache *cache.Cache, searchConfig *config.SearchConfig) *Handler {
	return &Handler{
		repo:         repo,
		cache:        cache,
		searchConfig: searchConfig,
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

// UsersRequest represents the batch user query request
type UsersRequest struct {
	Pubkeys []string `json:"pubkeys"`
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

// normalizePubkey converts npub to hex pubkey and validates the format
// Returns the normalized pubkey and true if valid, or empty string and false if invalid
func normalizePubkey(pubkeyOrNpub string) (string, bool) {
	pubkeyOrNpub = strings.TrimSpace(pubkeyOrNpub)
	if pubkeyOrNpub == "" {
		return "", false
	}

	if strings.HasPrefix(pubkeyOrNpub, "npub") {
		prefix, value, err := nip19.Decode(pubkeyOrNpub)
		if err != nil || prefix != "npub" {
			return "", false
		}
		pubkeyStr, ok := value.(string)
		if !ok {
			return "", false
		}
		return pubkeyStr, true
	}

	if !nostr.IsValidPublicKey(pubkeyOrNpub) {
		return "", false
	}
	return pubkeyOrNpub, true
}

// buildUserResponse creates a UserResponse from a user model
func buildUserResponse(user *models.UserInfo, totalUsers int) *UserResponse {
	percentile := 0
	if user.Rank != nil && totalUsers > 0 {
		percentile = int(float64(totalUsers-*user.Rank) / float64(totalUsers) * 100)
	}

	return &UserResponse{
		Pubkey:     user.Pubkey,
		Rank:       user.Rank,
		Percentile: percentile,
		Followers:  user.Followers,
		Following:  user.Following,
	}
}

// User handles GET /{pubkeyOrNpub} or GET /users/{pubkeyOrNpub} requests
func (h *Handler) User(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	var pubkeyOrNpub string
	if strings.HasPrefix(r.URL.Path, "/users/") {
		pubkeyOrNpub = strings.TrimPrefix(r.URL.Path, "/users/")
	} else {
		pubkeyOrNpub = strings.TrimPrefix(r.URL.Path, "/")
	}

	if pubkeyOrNpub == "" {
		writeError(w, http.StatusBadRequest, "Pubkey or npub is required in the URL path")
		return
	}

	pubkey, valid := normalizePubkey(pubkeyOrNpub)
	if !valid {
		writeError(w, http.StatusBadRequest, "Invalid pubkey or npub format")
		return
	}

	users, totalUsers, err := h.getUsersWithTotalCount([]string{pubkey})
	if err != nil {
		log.Printf("[API] Error querying user %s: %v", pubkey, err)
		writeError(w, http.StatusInternalServerError, "Failed to query user")
		return
	}

	user, found := users[pubkey]
	if !found || user == nil {
		writeError(w, http.StatusNotFound, "User not found")
		return
	}

	writeJSON(w, http.StatusOK, buildUserResponse(user, totalUsers))
}

// Users handles POST /users requests for batch user queries
func (h *Handler) Users(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	var req UsersRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid JSON body")
		return
	}

	if len(req.Pubkeys) == 0 {
		writeError(w, http.StatusBadRequest, "At least one pubkey is required")
		return
	}

	if len(req.Pubkeys) > 100 {
		writeError(w, http.StatusBadRequest, "Maximum 100 pubkeys allowed per request")
		return
	}

	// Normalize pubkeys (convert npub to hex)
	normalizedPubkeys := make([]string, 0, len(req.Pubkeys))
	requestPubkeyMapping := make(map[string]string) // original -> normalized
	for _, pubkeyOrNpub := range req.Pubkeys {
		pubkey, valid := normalizePubkey(pubkeyOrNpub)
		if !valid {
			continue
		}
		normalizedPubkeys = append(normalizedPubkeys, pubkey)
		requestPubkeyMapping[pubkeyOrNpub] = pubkey
	}

	if len(normalizedPubkeys) == 0 {
		writeError(w, http.StatusBadRequest, "No valid pubkeys provided")
		return
	}

	users, totalUsers, err := h.getUsersWithTotalCount(normalizedPubkeys)
	if err != nil {
		log.Printf("[API] Error querying users: %v", err)
		writeError(w, http.StatusInternalServerError, "Failed to query users")
		return
	}

	// Build response
	response := make(map[string]*UserResponse)
	for _, pubkeyOrNpub := range req.Pubkeys {
		normalizedPubkey, exists := requestPubkeyMapping[pubkeyOrNpub]
		if !exists {
			continue
		}
		user, found := users[normalizedPubkey]
		if !found || user == nil {
			continue
		}
		response[pubkeyOrNpub] = buildUserResponse(user, totalUsers)
	}

	writeJSON(w, http.StatusOK, response)
}

// getUsersWithTotalCount fetches users by pubkeys and the total user count
func (h *Handler) getUsersWithTotalCount(pubkeys []string) (map[string]*models.UserInfo, int, error) {
	users, err := h.cache.GetUsers(pubkeys, func(pks []string) (map[string]*models.UserInfo, error) {
		return h.repo.GetUsersByPubkeys(pks)
	})
	if err != nil {
		return nil, 0, err
	}

	totalUsers, err := h.cache.GetTotalUsers(func() (int, error) {
		return h.repo.GetTotalUsers()
	})
	if err != nil {
		return nil, 0, err
	}

	return users, totalUsers, nil
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

// SearchResponse represents a single search result with the raw event
type SearchUserResponse struct {
	Event      json.RawMessage `json:"event"`
	Pubkey     string          `json:"pubkey"`
	Rank       *int            `json:"rank,omitempty"`
	Percentile int             `json:"percentile"`
	Followers  int             `json:"followers"`
	Following  int             `json:"following"`
}

// buildUserResponse creates a UserResponse from a user model
func buildSearchUserResponse(user *models.UserProfile, totalUsers int) *SearchUserResponse {
	percentile := 0
	if user.Rank != nil && totalUsers > 0 {
		percentile = int(float64(totalUsers-*user.Rank) / float64(totalUsers) * 100)
	}

	return &SearchUserResponse{
		Event:      json.RawMessage(user.Event),
		Pubkey:     user.Pubkey,
		Rank:       user.Rank,
		Percentile: percentile,
		Followers:  user.Followers,
		Following:  user.Following,
	}
}

// Search handles GET /search?q=query&limit=20 requests for user search
func (h *Handler) Search(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// Check if search is enabled
	if h.searchConfig == nil || !h.searchConfig.Enabled {
		writeError(w, http.StatusServiceUnavailable, "Search feature is disabled")
		return
	}

	// Get query parameter
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if query == "" {
		writeError(w, http.StatusBadRequest, "Query parameter 'q' is required")
		return
	}

	// Get limit parameter (default 20, max 100)
	limit := 20
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if parsedLimit, err := strconv.Atoi(limitStr); err == nil && parsedLimit > 0 {
			limit = parsedLimit
			if limit > 100 {
				limit = 100
			}
		}
	}

	// Search users
	users, err := h.repo.SearchUsers(query, limit)
	if err != nil {
		log.Printf("[API] Error searching users: %v", err)
		writeError(w, http.StatusInternalServerError, "Failed to search users")
		return
	}

	// Get total users for percentile calculation
	totalUsers, err := h.cache.GetTotalUsers(func() (int, error) {
		return h.repo.GetTotalUsers()
	})
	if err != nil {
		log.Printf("[API] Error getting total users: %v", err)
		totalUsers = 0
	}

	// Build response with raw events
	response := make([]*SearchUserResponse, 0, len(users))
	for _, user := range users {
		response = append(response, buildSearchUserResponse(user, totalUsers))
	}

	writeJSON(w, http.StatusOK, response)
}
