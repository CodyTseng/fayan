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

func TestSetVouch_NewInsert(t *testing.T) {
	repo := newTestRepo(t)
	if err := repo.SetVouch("alice", "bob"); err != nil {
		t.Fatalf("SetVouch failed: %v", err)
	}
	if countRows(t, repo, "vouches", "alice", "bob") != 1 {
		t.Fatalf("expected one vouch row")
	}
}

func TestSetVouch_Idempotent(t *testing.T) {
	repo := newTestRepo(t)
	if err := repo.SetVouch("alice", "bob"); err != nil {
		t.Fatal(err)
	}
	if err := repo.SetVouch("alice", "bob"); err != nil {
		t.Fatal(err)
	}
	if countRows(t, repo, "vouches", "alice", "bob") != 1 {
		t.Fatalf("expected exactly one vouch after duplicate SetVouch")
	}
}

func TestSetVouch_MutualExclusion_DeletesReport(t *testing.T) {
	repo := newTestRepo(t)
	if err := repo.SetReport("alice", "bob"); err != nil {
		t.Fatal(err)
	}
	if countRows(t, repo, "reports", "alice", "bob") != 1 {
		t.Fatalf("expected report pre-existing")
	}

	if err := repo.SetVouch("alice", "bob"); err != nil {
		t.Fatal(err)
	}
	if countRows(t, repo, "reports", "alice", "bob") != 0 {
		t.Fatalf("expected prior report to be deleted by SetVouch")
	}
	if countRows(t, repo, "vouches", "alice", "bob") != 1 {
		t.Fatalf("expected vouch to exist")
	}
}

func TestSetReport_MutualExclusion_DeletesVouch(t *testing.T) {
	repo := newTestRepo(t)
	if err := repo.SetVouch("alice", "bob"); err != nil {
		t.Fatal(err)
	}
	if err := repo.SetReport("alice", "bob"); err != nil {
		t.Fatal(err)
	}
	if countRows(t, repo, "vouches", "alice", "bob") != 0 {
		t.Fatalf("expected prior vouch to be deleted by SetReport")
	}
	if countRows(t, repo, "reports", "alice", "bob") != 1 {
		t.Fatalf("expected report to exist")
	}
}

func TestSetVouch_UpsertsTargetPubkey(t *testing.T) {
	repo := newTestRepo(t)
	if err := repo.SetVouch("alice", "brand-new-target"); err != nil {
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
	if err := repo.SetVouch("alice", "bob"); err != nil {
		t.Fatal(err)
	}
	if err := repo.SetVouch("alice", "charlie"); err != nil {
		t.Fatal(err)
	}
	if err := repo.SetVouch("dave", "bob"); err != nil {
		t.Fatal(err)
	}

	var got []models.Vouch
	if err := repo.StreamVouches(func(v models.Vouch) error {
		got = append(got, v)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 vouches, got %d", len(got))
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

	if err := repo.SetReport("r1", "target"); err != nil {
		t.Fatal(err)
	}
	if err := repo.SetReport("r2", "target"); err != nil {
		t.Fatal(err)
	}
	if err := repo.SetReport("r3", "target"); err != nil {
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
