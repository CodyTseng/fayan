package ranking

import (
	"path/filepath"
	"testing"
	"time"

	"fayan/internal/repository"
)

func newTestRepo(t *testing.T) *repository.Repository {
	t.Helper()
	dir := t.TempDir()
	repo, err := repository.New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open repo: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })
	return repo
}

func insertFollow(t *testing.T, repo *repository.Repository, source, target string) {
	t.Helper()
	err := repo.BatchUpsertPubkeysAndConnections(
		[]string{source, target},
		[]repository.Connection{{Source: source, Target: target}},
	)
	if err != nil {
		t.Fatalf("insert follow %s->%s: %v", source, target, err)
	}
}

func getUser(t *testing.T, repo *repository.Repository, pubkey string) (rank *int, followers, following int, score float64) {
	t.Helper()
	info, err := repo.GetUserByPubkey(pubkey)
	if err != nil {
		t.Fatalf("get user %s: %v", pubkey, err)
	}
	if info == nil {
		return nil, 0, 0, 0
	}
	return info.Rank, info.Followers, info.Following, info.Score
}

// TestVouchPromotesUnfollowedUser verifies the core value proposition: after
// two ranking cycles (first establishes seed trust, second admits vouch edge),
// a newbie with no followers but one vouch from a seed receives a rank.
func TestVouchPromotesUnfollowedUser(t *testing.T) {
	repo := newTestRepo(t)

	seeds := []string{"seed1", "seed2", "seed3"}
	// Make seeds mutually follow so they have outgoing edges; TrustRank
	// requires at least some graph structure to propagate.
	insertFollow(t, repo, "seed1", "seed2")
	insertFollow(t, repo, "seed2", "seed3")
	insertFollow(t, repo, "seed3", "seed1")

	calc := NewCalculator(repo, seeds, 0.7, 0.3, 0.5)

	// First pass: seeds acquire trust_score > 0.
	if err := calc.Calculate(); err != nil {
		t.Fatal(err)
	}

	// Seed vouches for a newbie nobody follows.
	if err := repo.UpsertVouches("seed1", []string{"newbie"}); err != nil {
		t.Fatal(err)
	}

	// Second pass: vouch edge admitted because seed1 has trust_score > 0.
	if err := calc.Calculate(); err != nil {
		t.Fatal(err)
	}

	rank, followers, _, score := getUser(t, repo, "newbie")
	if rank == nil {
		t.Fatalf("newbie should have a rank after vouch")
	}
	if followers != 1 {
		t.Fatalf("newbie should have 1 follower (the vouch edge), got %d", followers)
	}
	if score <= 0 {
		t.Fatalf("newbie score should be positive, got %v", score)
	}
}

// TestVouchWeightShrinksContribution verifies that a lower vouchWeight
// reduces the score a vouch-only edge contributes, relative to a full-weight
// (1.0) follow edge.
func TestVouchWeightShrinksContribution(t *testing.T) {
	repo := newTestRepo(t)

	seeds := []string{"seed1", "seed2", "seed3"}
	insertFollow(t, repo, "seed1", "seed2")
	insertFollow(t, repo, "seed2", "seed3")
	insertFollow(t, repo, "seed3", "seed1")

	// Bootstrap seed trust.
	calcHigh := NewCalculator(repo, seeds, 0.7, 0.3, 1.0)
	if err := calcHigh.Calculate(); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpsertVouches("seed1", []string{"newbie"}); err != nil {
		t.Fatal(err)
	}

	if err := calcHigh.Calculate(); err != nil {
		t.Fatal(err)
	}
	_, _, _, scoreAtWeight1 := getUser(t, repo, "newbie")

	// Run the same graph again with vouchWeight=0.25.
	calcLow := NewCalculator(repo, seeds, 0.7, 0.3, 0.25)
	if err := calcLow.Calculate(); err != nil {
		t.Fatal(err)
	}
	_, _, _, scoreAtWeight025 := getUser(t, repo, "newbie")

	if !(scoreAtWeight025 < scoreAtWeight1) {
		t.Fatalf("expected lower vouch weight to produce lower score, got %.6g (w=0.25) vs %.6g (w=1.0)",
			scoreAtWeight025, scoreAtWeight1)
	}
	if scoreAtWeight025 <= 0 {
		t.Fatalf("score at w=0.25 should still be positive, got %v", scoreAtWeight025)
	}
}

// TestVouchAndFollowDedupe verifies A following AND vouching for B only
// produces one edge (A's following count = 1, not 2).
func TestVouchAndFollowDedupe(t *testing.T) {
	repo := newTestRepo(t)

	insertFollow(t, repo, "a", "b")
	if err := repo.UpsertVouches("a", []string{"b"}); err != nil {
		t.Fatal(err)
	}
	// Give A trust so vouch edge would be admitted.
	repo.BatchUpdatePubkeys([]repository.PubkeyUpdate{{
		Pubkey:     "a",
		Score:      0.5,
		Rank:       1,
		TrustScore: 0.5,
		PageScore:  0.5,
		Followers:  0,
		Following:  1,
	}})

	calc := NewCalculator(repo, []string{"a"}, 0.7, 0.3, 0.5)
	if err := calc.Calculate(); err != nil {
		t.Fatal(err)
	}

	_, _, following, _ := getUser(t, repo, "a")
	if following != 1 {
		t.Fatalf("A should have following=1 after dedup, got %d", following)
	}
	_, followers, _, _ := getUser(t, repo, "b")
	if followers != 1 {
		t.Fatalf("B should have followers=1 after dedup, got %d", followers)
	}
}

// TestReportDecaysScore verifies a well-connected pubkey loses score when
// reported by multiple trusted accounts.
func TestReportDecaysScore(t *testing.T) {
	repo := newTestRepo(t)

	// Build a graph where X has several followers (seeds), so X's score starts high.
	seeds := []string{"seed1", "seed2", "seed3"}
	insertFollow(t, repo, "seed1", "seed2")
	insertFollow(t, repo, "seed2", "seed3")
	insertFollow(t, repo, "seed3", "seed1")
	insertFollow(t, repo, "seed1", "x")
	insertFollow(t, repo, "seed2", "x")
	insertFollow(t, repo, "seed3", "x")

	calc := NewCalculator(repo, seeds, 0.7, 0.3, 0.5)
	if err := calc.Calculate(); err != nil {
		t.Fatal(err)
	}

	_, _, _, scoreBefore := getUser(t, repo, "x")
	if scoreBefore <= 0 {
		t.Fatalf("setup should give X a positive score, got %v", scoreBefore)
	}

	// All three seeds report X.
	for _, s := range seeds {
		if err := repo.UpsertReport(s, "x", time.Now()); err != nil {
			t.Fatal(err)
		}
	}

	if err := calc.Calculate(); err != nil {
		t.Fatal(err)
	}

	_, _, _, scoreAfter := getUser(t, repo, "x")
	if scoreAfter >= scoreBefore {
		t.Fatalf("score should decay after reports: before=%v after=%v", scoreBefore, scoreAfter)
	}
	// All followers also report → R == F → penalty = 0.5 → score halves.
	// Allow a small tolerance for rounding.
	if scoreAfter > scoreBefore*0.55 {
		t.Fatalf("expected penalty near 0.5 (R==F), score dropped only from %v to %v", scoreBefore, scoreAfter)
	}
}

// TestReportWithNoTrustIgnored verifies reports from untrusted accounts have
// no effect on the reported user's score.
func TestReportWithNoTrustIgnored(t *testing.T) {
	repo := newTestRepo(t)

	seeds := []string{"seed1", "seed2"}
	insertFollow(t, repo, "seed1", "seed2")
	insertFollow(t, repo, "seed2", "seed1")
	insertFollow(t, repo, "seed1", "target")
	insertFollow(t, repo, "seed2", "target")

	// Add an untrusted account (no inbound edges, no trust).
	if _, err := repo.DB().Exec(
		`INSERT INTO pubkeys (pubkey, trust_score, created_at, updated_at) VALUES (?, 0, ?, ?);`,
		"troll", time.Now(), time.Now(),
	); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpsertReport("troll", "target", time.Now()); err != nil {
		t.Fatal(err)
	}

	calc := NewCalculator(repo, seeds, 0.7, 0.3, 0.5)
	if err := calc.Calculate(); err != nil {
		t.Fatal(err)
	}
	_, _, _, scoreWithTrollReport := getUser(t, repo, "target")

	// Remove the troll report, recompute.
	if _, err := repo.DB().Exec(
		`DELETE FROM reports WHERE source_pubkey = ? AND target_pubkey = ?;`,
		"troll", "target",
	); err != nil {
		t.Fatal(err)
	}
	if err := calc.Calculate(); err != nil {
		t.Fatal(err)
	}
	_, _, _, scoreWithoutReport := getUser(t, repo, "target")

	if absDiff(scoreWithTrollReport, scoreWithoutReport) > 1e-9 {
		t.Fatalf("untrusted report should not change score: with=%v without=%v", scoreWithTrollReport, scoreWithoutReport)
	}
}

func absDiff(a, b float64) float64 {
	if a > b {
		return a - b
	}
	return b - a
}
