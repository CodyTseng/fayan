package ingest

import (
	"testing"

	"github.com/nbd-wtf/go-nostr"
)

// mustPubkey returns a real, curve-valid x-only pubkey (IsValidPublicKey parses
// the secp256k1 point, so arbitrary hex will not do).
func mustPubkey(t *testing.T) string {
	t.Helper()
	pk, err := nostr.GetPublicKey(nostr.GeneratePrivateKey())
	if err != nil {
		t.Fatalf("derive pubkey: %v", err)
	}
	return pk
}

func TestParseContacts(t *testing.T) {
	author, bob, carol := mustPubkey(t), mustPubkey(t), mustPubkey(t)
	ev := &nostr.Event{
		PubKey: author,
		Kind:   KindContacts,
		Tags: nostr.Tags{
			{"p", bob},
			{"p", carol},
			{"p", author},     // self — excluded
			{"p", "not-hex"},  // invalid — excluded
			{"e", "whatever"}, // non-p — ignored
		},
	}
	pubkeys, conns := ParseContacts(ev)
	if len(conns) != 2 {
		t.Fatalf("expected 2 connections, got %d", len(conns))
	}
	// author + bob + carol = 3 distinct pubkeys
	if len(pubkeys) != 3 {
		t.Fatalf("expected 3 pubkeys, got %d", len(pubkeys))
	}
}

func TestParseVouchTargets(t *testing.T) {
	author, bob, carol := mustPubkey(t), mustPubkey(t), mustPubkey(t)
	ev := &nostr.Event{
		PubKey: author,
		Kind:   KindVouchSet,
		Tags: nostr.Tags{
			{"d", VouchSetIdentifier},
			{"p", bob},
			{"p", bob}, // duplicate — collapsed
			{"p", carol},
			{"p", author}, // self — excluded
		},
	}
	targets := ParseVouchTargets(ev)
	if len(targets) != 2 {
		t.Fatalf("expected 2 deduped targets, got %d: %v", len(targets), targets)
	}
}

func TestIsVouchSet(t *testing.T) {
	author := mustPubkey(t)
	cases := []struct {
		name string
		kind int
		d    string
		want bool
	}{
		{"vouch set", KindVouchSet, VouchSetIdentifier, true},
		{"other follow set", KindVouchSet, "friends", false},
		{"follow set without d", KindVouchSet, "", false},
		{"wrong kind", KindContacts, VouchSetIdentifier, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev := &nostr.Event{PubKey: author, Kind: tc.kind}
			if tc.d != "" {
				ev.Tags = nostr.Tags{{"d", tc.d}}
			}
			if got := IsVouchSet(ev); got != tc.want {
				t.Fatalf("IsVouchSet = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestProfileReportTarget(t *testing.T) {
	author, bob := mustPubkey(t), mustPubkey(t)
	cases := []struct {
		name   string
		tags   nostr.Tags
		want   string
		wantOK bool
	}{
		{"spam profile report", nostr.Tags{{"p", bob, "spam"}}, bob, true},
		{"impersonation profile report", nostr.Tags{{"p", bob, "impersonation"}}, bob, true},
		{"unweighted type (nudity)", nostr.Tags{{"p", bob, "nudity"}}, "", false},
		{"missing report type", nostr.Tags{{"p", bob}}, "", false},
		{"event-level report (has e tag)", nostr.Tags{{"e", "evt", "spam"}, {"p", bob, "spam"}}, "", false},
		{"self report", nostr.Tags{{"p", author, "spam"}}, "", false},
		{"no p tag", nostr.Tags{{"e", "evt", "spam"}}, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev := &nostr.Event{PubKey: author, Kind: KindReport, Tags: tc.tags}
			got, ok := profileReportTarget(ev)
			if ok != tc.wantOK || got != tc.want {
				t.Fatalf("got (%q, %v), want (%q, %v)", got, ok, tc.want, tc.wantOK)
			}
		})
	}
}
