package handler

import (
	"encoding/json"
	"log"
	"net/http"
)

// relationRequest is the body shape for POST /vouch and POST /report.
type relationRequest struct {
	Target string `json:"target"`
}

// Vouch handles POST /vouch. Expects a NIP-98 authenticated request.
// authorPubkey is injected by the NIP98Auth middleware.
func (h *Handler) Vouch(w http.ResponseWriter, r *http.Request, authorPubkey string) {
	h.handleRelation(w, r, authorPubkey, relationKindVouch)
}

// Report handles POST /report. Expects a NIP-98 authenticated request.
func (h *Handler) Report(w http.ResponseWriter, r *http.Request, authorPubkey string) {
	h.handleRelation(w, r, authorPubkey, relationKindReport)
}

type relationKind int

const (
	relationKindVouch relationKind = iota
	relationKindReport
)

func (h *Handler) handleRelation(w http.ResponseWriter, r *http.Request, authorPubkey string, kind relationKind) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	var req relationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid JSON body")
		return
	}
	target, valid := normalizePubkey(req.Target)
	if !valid {
		writeError(w, http.StatusBadRequest, "Invalid target pubkey")
		return
	}
	if target == authorPubkey {
		writeError(w, http.StatusBadRequest, "Cannot vouch for or report yourself")
		return
	}

	// Silent-ignore admission rule: author must be a seed or have earned
	// TrustRank > 0 in the last ranking round. Respond 200 regardless so
	// the client cannot distinguish "not admitted" from "successfully stored".
	if !h.authorQualifies(authorPubkey) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}

	var err error
	switch kind {
	case relationKindVouch:
		err = h.repo.SetVouch(authorPubkey, target)
	case relationKindReport:
		err = h.repo.SetReport(authorPubkey, target)
	}
	if err != nil {
		log.Printf("[API] Error setting relation (author=%s target=%s kind=%d): %v", authorPubkey, target, kind, err)
		writeError(w, http.StatusInternalServerError, "Failed to store relation")
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
