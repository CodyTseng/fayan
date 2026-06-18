package repository

import (
	"path/filepath"
	"testing"
	"time"

	"fayan/internal/models"
)

func newTestRepo(t *testing.T) *Repository {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	repo, err := New(path)
	if err != nil {
		t.Fatalf("failed to open repo: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })
	return repo
}

// seedPubkey inserts a pubkey with the given trust_score so tests can simulate
// an account that has earned reputation in a previous ranking round.
func seedPubkey(t *testing.T, repo *Repository, pubkey string, trustScore float64) {
	t.Helper()
	now := time.Now().UTC()
	if _, err := repo.db.Exec(
		`INSERT INTO pubkeys (pubkey, trust_score, created_at, updated_at) VALUES (?, ?, ?, ?)
		 ON CONFLICT(pubkey) DO UPDATE SET trust_score = excluded.trust_score;`,
		pubkey, trustScore, now, now,
	); err != nil {
		t.Fatalf("failed to seed pubkey: %v", err)
	}
}

func countRows(t *testing.T, repo *Repository, table, source, target string) int {
	t.Helper()
	var n int
	q := "SELECT COUNT(*) FROM " + table + " WHERE source_pubkey = ? AND target_pubkey = ?;"
	if err := repo.db.QueryRow(q, source, target).Scan(&n); err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	return n
}

var (
	t1 = time.Unix(1_700_000_000, 0).UTC()
	t2 = time.Unix(1_700_000_100, 0).UTC()
)

func TestUpsertVouches_NewInsert(t *testing.T) {
	repo := newTestRepo(t)
	if err := repo.UpsertVouches("alice", []string{"bob"}); err != nil {
		t.Fatalf("UpsertVouches failed: %v", err)
	}
	if countRows(t, repo, "vouches", "alice", "bob") != 1 {
		t.Fatalf("expected one vouch row")
	}
}

// TestUpsertVouches_DoesNotDelete verifies vouches follow the follow-edge
// lifecycle: a target dropped from a later set is NOT actively removed — it
// lingers (to be aged out by the staleness window at ranking time).
func TestUpsertVouches_DoesNotDelete(t *testing.T) {
	repo := newTestRepo(t)
	if err := repo.UpsertVouches("alice", []string{"bob", "charlie"}); err != nil {
		t.Fatal(err)
	}
	// A later set without bob must not delete bob's edge.
	if err := repo.UpsertVouches("alice", []string{"charlie", "dave"}); err != nil {
		t.Fatal(err)
	}
	if countRows(t, repo, "vouches", "alice", "bob") != 1 {
		t.Fatalf("expected bob to linger (not actively deleted)")
	}
	if countRows(t, repo, "vouches", "alice", "charlie") != 1 {
		t.Fatalf("expected charlie to remain")
	}
	if countRows(t, repo, "vouches", "alice", "dave") != 1 {
		t.Fatalf("expected dave to be added")
	}
}

func TestUpsertVouches_EmptyNoop(t *testing.T) {
	repo := newTestRepo(t)
	if err := repo.UpsertVouches("alice", []string{"bob"}); err != nil {
		t.Fatal(err)
	}
	// An empty set is a no-op: nothing is refreshed, nothing is deleted.
	if err := repo.UpsertVouches("alice", nil); err != nil {
		t.Fatal(err)
	}
	if countRows(t, repo, "vouches", "alice", "bob") != 1 {
		t.Fatalf("expected bob to remain after empty set (no active delete)")
	}
}

func TestUpsertVouches_UpsertsTargetPubkey(t *testing.T) {
	repo := newTestRepo(t)
	if err := repo.UpsertVouches("alice", []string{"brand-new-target"}); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := repo.db.QueryRow("SELECT COUNT(*) FROM pubkeys WHERE pubkey = ?;", "brand-new-target").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected target pubkey to be upserted into pubkeys table")
	}
}

func TestUpsertReport_NewAndIdempotent(t *testing.T) {
	repo := newTestRepo(t)
	if err := repo.UpsertReport("alice", "bob", t1); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpsertReport("alice", "bob", t2); err != nil {
		t.Fatal(err)
	}
	if countRows(t, repo, "reports", "alice", "bob") != 1 {
		t.Fatalf("expected exactly one report row after re-report")
	}
}

func TestVouchAndReportCoexist(t *testing.T) {
	repo := newTestRepo(t)
	// No mutual exclusion at write time: both rows persist independently. The
	// vouch adds flow and the report subtracts it at ranking time.
	if err := repo.UpsertVouches("alice", []string{"bob"}); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpsertReport("alice", "bob", t1); err != nil {
		t.Fatal(err)
	}
	if countRows(t, repo, "vouches", "alice", "bob") != 1 {
		t.Fatalf("expected vouch to persist alongside report")
	}
	if countRows(t, repo, "reports", "alice", "bob") != 1 {
		t.Fatalf("expected report to persist alongside vouch")
	}
}

func TestGetTrustScore_UnknownPubkey(t *testing.T) {
	repo := newTestRepo(t)
	score, err := repo.GetTrustScore("who-dis")
	if err != nil {
		t.Fatal(err)
	}
	if score != 0 {
		t.Fatalf("unknown pubkey should return 0 trust, got %v", score)
	}
}

func TestGetTrustScore_KnownPubkey(t *testing.T) {
	repo := newTestRepo(t)
	seedPubkey(t, repo, "trusted", 0.42)
	score, err := repo.GetTrustScore("trusted")
	if err != nil {
		t.Fatal(err)
	}
	if score != 0.42 {
		t.Fatalf("expected 0.42, got %v", score)
	}
}

func TestStreamVouches(t *testing.T) {
	repo := newTestRepo(t)
	if err := repo.UpsertVouches("alice", []string{"bob", "charlie"}); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpsertVouches("dave", []string{"bob"}); err != nil {
		t.Fatal(err)
	}

	var got []models.Vouch
	if err := repo.StreamVouches(func(v models.Vouch) error {
		got = append(got, v)
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 vouches, got %d", len(got))
	}
}

// TestStreamVouches_StaleFiltered verifies the staleness window: edges last
// seen before the cutoff are excluded, the same way stale follow edges are.
func TestStreamVouches_StaleFiltered(t *testing.T) {
	repo := newTestRepo(t)
	if err := repo.UpsertVouches("alice", []string{"bob"}); err != nil {
		t.Fatal(err)
	}

	count := func(after *time.Time) int {
		n := 0
		if err := repo.StreamVouches(func(models.Vouch) error { n++; return nil }, after); err != nil {
			t.Fatal(err)
		}
		return n
	}

	past := time.Now().UTC().Add(-time.Hour)
	if count(&past) != 1 {
		t.Fatalf("expected the fresh vouch to pass a past cutoff")
	}
	future := time.Now().UTC().Add(time.Hour)
	if count(&future) != 0 {
		t.Fatalf("expected the vouch to be filtered out by a future cutoff")
	}
}

func TestGetPubkeysWithPositiveTrust(t *testing.T) {
	repo := newTestRepo(t)
	seedPubkey(t, repo, "high", 0.5)
	seedPubkey(t, repo, "zero", 0)
	seedPubkey(t, repo, "neg", -0.1) // should be excluded

	set, err := repo.GetPubkeysWithPositiveTrust()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := set["high"]; !ok {
		t.Fatalf("expected 'high' in set")
	}
	if _, ok := set["zero"]; ok {
		t.Fatalf("did not expect 'zero' in set")
	}
	if _, ok := set["neg"]; ok {
		t.Fatalf("did not expect 'neg' in set")
	}
}

func TestGetTrustWeightedReports(t *testing.T) {
	repo := newTestRepo(t)
	seedPubkey(t, repo, "r1", 0.3)
	seedPubkey(t, repo, "r2", 0.7)
	seedPubkey(t, repo, "r3", 0) // untrusted; should be excluded

	if err := repo.UpsertReport("r1", "target", t1); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpsertReport("r2", "target", t1); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpsertReport("r3", "target", t1); err != nil {
		t.Fatal(err)
	}

	reports, err := repo.GetTrustWeightedReports()
	if err != nil {
		t.Fatal(err)
	}
	agg, ok := reports["target"]
	if !ok {
		t.Fatalf("expected 'target' in aggregates")
	}
	if agg.NumReporters != 2 {
		t.Fatalf("expected 2 trusted reporters, got %d", agg.NumReporters)
	}
	expected := 0.3 + 0.7
	if absDiff(agg.TotalReporterTrust, expected) > 1e-9 {
		t.Fatalf("expected trust sum %v, got %v", expected, agg.TotalReporterTrust)
	}
}

func absDiff(a, b float64) float64 {
	if a > b {
		return a - b
	}
	return b - a
}
