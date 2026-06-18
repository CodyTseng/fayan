// Package ingest turns signed Nostr events into reputation-graph data. The same
// logic backs both the crawler (events pulled from relays) and the API's
// POST /event endpoint (events pushed by clients), so the two paths stay in sync.
package ingest

import (
	"fayan/internal/repository"

	"github.com/nbd-wtf/go-nostr"
)

// Supported event kinds.
const (
	KindContacts = 3     // NIP-02 contact list → follow edges
	KindReport   = 1984  // NIP-56 report → report edge (profile-level only)
	KindVouchSet = 30000 // NIP-51 follow set; the vouch set is the one tagged d=VouchSetIdentifier
)

// VouchSetIdentifier is the NIP-51 `d` tag value identifying the follow set
// used as a vouch set. A kind:30000 with any other `d` is a regular follow set
// and is ignored. The value is intentionally generic (no project prefix) so it
// can serve as a shared convention if one emerges.
const VouchSetIdentifier = "vouch"

// acceptedReportTypes are the NIP-56 report types that affect reputation. Fayan
// is a spam-detection system, so only these two carry weight; other types
// (nudity, profanity, …) are ignored.
var acceptedReportTypes = map[string]bool{
	"spam":          true,
	"impersonation": true,
}

// Ingester applies signature-verified Nostr events to the repository.
type Ingester struct {
	repo *repository.Repository
}

// New creates an Ingester backed by repo.
func New(repo *repository.Repository) *Ingester {
	return &Ingester{repo: repo}
}

// Apply dispatches a verified event by kind, reporting whether the kind is one
// Fayan ingests. The caller is responsible for verifying ev's signature first.
func (in *Ingester) Apply(ev *nostr.Event) (handled bool, err error) {
	switch ev.Kind {
	case KindContacts:
		return true, in.ApplyContacts(ev)
	case KindReport:
		_, err := in.ApplyReport(ev)
		return true, err
	case KindVouchSet:
		if !IsVouchSet(ev) {
			return false, nil // some other follow set — not ours
		}
		return true, in.ApplyVouchSet(ev)
	}
	return false, nil
}

// IsVouchSet reports whether ev is the NIP-51 follow set (kind:30000) Fayan
// uses as a vouch set, i.e. tagged with VouchSetIdentifier.
func IsVouchSet(ev *nostr.Event) bool {
	return ev.Kind == KindVouchSet && dTagValue(ev.Tags) == VouchSetIdentifier
}

// dTagValue returns the value of the first `d` tag, or "" if absent.
func dTagValue(tags nostr.Tags) string {
	for _, tag := range tags {
		if len(tag) >= 2 && tag[0] == "d" {
			return tag[1]
		}
	}
	return ""
}

// ApplyContacts parses a kind:3 event into follow connections and persists them.
func (in *Ingester) ApplyContacts(ev *nostr.Event) error {
	if ev.Kind != KindContacts {
		return nil
	}
	pubkeys, connections := ParseContacts(ev)
	return in.repo.BatchUpsertPubkeysAndConnections(pubkeys, connections)
}

// ParseContacts extracts the author plus followed pubkeys and the follow edges
// from a kind:3 event. Exported so the crawler shares one parser with the API.
func ParseContacts(ev *nostr.Event) ([]string, []repository.Connection) {
	pubkeySet := make(map[string]bool)
	pubkeySet[ev.PubKey] = true

	var connections []repository.Connection
	for _, tag := range ev.Tags {
		if len(tag) >= 2 && tag[0] == "p" {
			target := tag[1]
			if !nostr.IsValidPublicKey(target) || target == ev.PubKey {
				continue
			}
			pubkeySet[target] = true
			connections = append(connections, repository.Connection{Source: ev.PubKey, Target: target})
		}
	}

	pubkeys := make([]string, 0, len(pubkeySet))
	for pk := range pubkeySet {
		pubkeys = append(pubkeys, pk)
	}
	return pubkeys, connections
}

// ApplyReport stores a profile-level spam/impersonation report as a report edge.
// It returns false (without error) when the event is not an accepted profile
// report: it targets a specific event (has an e tag), lacks a usable p target,
// or carries a report type Fayan does not weigh.
func (in *Ingester) ApplyReport(ev *nostr.Event) (bool, error) {
	if ev.Kind != KindReport {
		return false, nil
	}
	target, ok := profileReportTarget(ev)
	if !ok {
		return false, nil
	}
	return true, in.repo.UpsertReport(ev.PubKey, target, ev.CreatedAt.Time())
}

// profileReportTarget returns the reported pubkey when ev is a NIP-56 report
// that targets a profile (not a specific event) with an accepted report type.
// Any e tag disqualifies the event — that is a note-level report, out of scope.
func profileReportTarget(ev *nostr.Event) (string, bool) {
	target := ""
	for _, tag := range ev.Tags {
		if len(tag) == 0 {
			continue
		}
		switch tag[0] {
		case "e":
			return "", false
		case "p":
			if target != "" || len(tag) < 2 {
				continue
			}
			pk := tag[1]
			if !nostr.IsValidPublicKey(pk) || pk == ev.PubKey {
				continue
			}
			// NIP-56 carries the report type in the p tag's third element.
			if len(tag) < 3 || !acceptedReportTypes[tag[2]] {
				continue
			}
			target = pk
		}
	}
	return target, target != ""
}

// ApplyVouchSet refreshes the author's vouch edges from the pubkeys listed in
// their vouch set (the NIP-51 kind:30000 follow set tagged with
// VouchSetIdentifier). A no-op for any other event.
func (in *Ingester) ApplyVouchSet(ev *nostr.Event) error {
	if !IsVouchSet(ev) {
		return nil
	}
	targets := ParseVouchTargets(ev)
	return in.repo.UpsertVouches(ev.PubKey, targets)
}

// ParseVouchTargets extracts the valid, de-duplicated pubkeys an author vouches
// for from a follow set's p tags (excluding the author themselves).
func ParseVouchTargets(ev *nostr.Event) []string {
	seen := make(map[string]bool)
	var targets []string
	for _, tag := range ev.Tags {
		if len(tag) >= 2 && tag[0] == "p" {
			pk := tag[1]
			if !nostr.IsValidPublicKey(pk) || pk == ev.PubKey || seen[pk] {
				continue
			}
			seen[pk] = true
			targets = append(targets, pk)
		}
	}
	return targets
}
