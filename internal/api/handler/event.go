package handler

import (
	"encoding/json"
	"io"
	"log"
	"net/http"

	"github.com/nbd-wtf/go-nostr"
)

// Async ingest tuning for POST /event.
const (
	maxEventBytes   = 1 << 20 // 1 MiB cap on a request body; Nostr events are small
	ingestQueueSize = 1024    // buffered events awaiting background processing
	ingestWorkers   = 4       // background workers draining the queue
)

// PostEvent handles POST /event as a fire-and-forget intake: it reads the body,
// queues the event, and returns 202 immediately and unconditionally. Signature
// verification, the anti-inflation admission check, and persistence all happen
// on a background worker (processEvent) — the client learns nothing about the
// outcome. This is just a push accelerator: the same event is published to
// public relays, and the crawler ingests it from there regardless, so a full
// queue can simply drop the event.
func (h *Handler) PostEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// Decode synchronously — the body is gone once we return — then hand off.
	var ev nostr.Event
	if err := json.NewDecoder(io.LimitReader(r.Body, maxEventBytes)).Decode(&ev); err == nil {
		select {
		case h.ingestCh <- &ev:
		default:
			// Queue full: drop. The crawler will pick the event up from relays.
			log.Printf("[API] Ingest queue full, dropped event kind=%d", ev.Kind)
		}
	}

	writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

// ingestWorker drains the async ingest queue, processing one event at a time.
func (h *Handler) ingestWorker() {
	for ev := range h.ingestCh {
		h.processEvent(ev)
	}
}

// processEvent verifies, admits, and persists a single queued event off the
// request path. Every failure is silently dropped (logged at most) — there is
// no caller to report back to.
func (h *Handler) processEvent(ev *nostr.Event) {
	if ok, err := ev.CheckSignature(); err != nil || !ok {
		return
	}
	// Anti-inflation admission: only seeds or pubkeys with positive TrustRank
	// contribute, so an untrusted author cannot inflate the graph by pushing.
	if !h.authorQualifies(ev.PubKey) {
		return
	}
	if _, err := h.ingester.Apply(ev); err != nil {
		log.Printf("[API] Error ingesting event (kind=%d pubkey=%s): %v", ev.Kind, ev.PubKey, err)
	}
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
