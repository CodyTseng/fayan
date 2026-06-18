package handler

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/nbd-wtf/go-nostr"
)

// PostEvent handles POST /event. It accepts a single signed Nostr event, an
// immediate push complement to the crawler's relay subscriptions. The same
// event can (and should) also be published to public relays — Fayan is just one
// of many aggregators. Supported kinds: 3 (contacts), 1984 (reports), 10040
// (vouch sets); other kinds are rejected.
//
// As an open write endpoint it keeps the anti-inflation admission rule: the
// author must be a seed or have earned TrustRank > 0, otherwise the event is
// silently dropped with 200 so the client cannot probe admission. (The crawler
// path does not filter this way — it aggregates public events as-is, and the
// ranking stage already discounts untrusted sources.)
func (h *Handler) PostEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	var ev nostr.Event
	if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid event JSON")
		return
	}

	ok, err := ev.CheckSignature()
	if err != nil || !ok {
		writeError(w, http.StatusUnauthorized, "Invalid event signature")
		return
	}

	// Silent-ignore admission rule (see doc comment).
	if !h.authorQualifies(ev.PubKey) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}

	handled, err := h.ingester.Apply(&ev)
	if err != nil {
		log.Printf("[API] Error ingesting event (kind=%d pubkey=%s): %v", ev.Kind, ev.PubKey, err)
		writeError(w, http.StatusInternalServerError, "Failed to ingest event")
		return
	}
	if !handled {
		writeError(w, http.StatusBadRequest, "Unsupported event kind")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// authorQualifies returns true if the author is an explicit seed or has a
// positive last-computed TrustRank score.
func (h *Handler) authorQualifies(pubkey string) bool {
	if _, ok := h.seedSet[pubkey]; ok {
		return true
	}
	score, err := h.repo.GetTrustScore(pubkey)
	if err != nil {
		log.Printf("[API] Error reading trust_score for %s: %v", pubkey, err)
		return false
	}
	return score > 0
}
