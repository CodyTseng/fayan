package repository

import (
	"database/sql"
	"fmt"
	"time"

	"fayan/internal/models"
)

// UpsertVouches refreshes the vouch edges from source for the pubkeys in the
// latest kind:30000 set, mirroring how kind:3 contacts are stored: each edge is
// upserted with last_seen = now and never actively deleted. A vouch dropped
// from the set simply stops being refreshed and ages out of the ranking graph
// via the same staleness window as follows (see StreamVouches). Targets are
// upserted into pubkeys so ranking can cover brand-new accounts.
func (r *Repository) UpsertVouches(source string, targets []string) error {
	if len(targets) == 0 {
		return nil
	}

	r.writeMu.Lock()
	defer r.writeMu.Unlock()

	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC()

	pkStmt, err := tx.Prepare(`INSERT INTO pubkeys (pubkey, created_at, updated_at) VALUES (?, ?, ?) ON CONFLICT(pubkey) DO NOTHING;`)
	if err != nil {
		return fmt.Errorf("failed to prepare pubkey statement: %w", err)
	}
	defer pkStmt.Close()

	vStmt, err := tx.Prepare(`REPLACE INTO vouches (source_pubkey, target_pubkey, last_seen) VALUES (?, ?, ?);`)
	if err != nil {
		return fmt.Errorf("failed to prepare vouch statement: %w", err)
	}
	defer vStmt.Close()

	for _, target := range targets {
		if _, err := pkStmt.Exec(target, now, now); err != nil {
			return fmt.Errorf("failed to upsert target pubkey %s: %w", target, err)
		}
		if _, err := vStmt.Exec(source, target, now); err != nil {
			return fmt.Errorf("failed to insert vouch %s -> %s: %w", source, target, err)
		}
	}

	return tx.Commit()
}

// UpsertReport records source→target as a report edge. Reports are additive
// (kind:1984 events are not replaceable); re-reporting the same target just
// refreshes the timestamp. The target is upserted into pubkeys so ranking can
// cover brand-new accounts. No vouch is touched — the vouch-beats-report
// precedence is resolved at ranking time (see GetTrustWeightedReports).
func (r *Repository) UpsertReport(source, target string, createdAt time.Time) error {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()

	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	now := createdAt.UTC()
	if _, err := tx.Exec(
		`INSERT INTO pubkeys (pubkey, created_at, updated_at) VALUES (?, ?, ?) ON CONFLICT(pubkey) DO NOTHING;`,
		target, now, now,
	); err != nil {
		return fmt.Errorf("failed to upsert target pubkey: %w", err)
	}

	if _, err := tx.Exec(
		`INSERT OR REPLACE INTO reports (source_pubkey, target_pubkey, created_at) VALUES (?, ?, ?);`,
		source, target, now,
	); err != nil {
		return fmt.Errorf("failed to insert report: %w", err)
	}

	return tx.Commit()
}

// GetTrustScore returns the trust_score for a pubkey. Returns 0 if the pubkey
// is not present in the pubkeys table.
func (r *Repository) GetTrustScore(pubkey string) (float64, error) {
	var score float64
	err := r.db.QueryRow("SELECT COALESCE(trust_score, 0) FROM pubkeys WHERE pubkey = ?;", pubkey).Scan(&score)
	if err != nil {
		// sql.ErrNoRows → pubkey not known → trust is zero.
		return 0, nil
	}
	return score, nil
}

// StreamVouches streams vouch edges. When afterTime is non-nil, only edges
// refreshed at or after it are returned — the same staleness window that ages
// out follow edges, so a vouch dropped from a set eventually stops counting.
func (r *Repository) StreamVouches(callback func(models.Vouch) error, afterTime *time.Time) error {
	var rows *sql.Rows
	var err error
	if afterTime != nil {
		rows, err = r.db.Query("SELECT source_pubkey, target_pubkey FROM vouches WHERE last_seen >= ?;", afterTime)
	} else {
		rows, err = r.db.Query("SELECT source_pubkey, target_pubkey FROM vouches;")
	}
	if err != nil {
		return fmt.Errorf("failed to query vouches: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var v models.Vouch
		if err := rows.Scan(&v.Source, &v.Target); err != nil {
			return fmt.Errorf("failed to scan vouch: %w", err)
		}
		if err := callback(v); err != nil {
			return fmt.Errorf("callback error: %w", err)
		}
	}
	return rows.Err()
}

// GetPubkeysWithPositiveTrust returns the set of pubkeys whose last-computed
// trust_score is > 0. Used as the vouch-edge admission filter in ranking.
func (r *Repository) GetPubkeysWithPositiveTrust() (map[string]struct{}, error) {
	rows, err := r.db.Query("SELECT pubkey FROM pubkeys WHERE trust_score > 0;")
	if err != nil {
		return nil, fmt.Errorf("failed to query pubkeys with positive trust: %w", err)
	}
	defer rows.Close()

	result := make(map[string]struct{})
	for rows.Next() {
		var pk string
		if err := rows.Scan(&pk); err != nil {
			return nil, fmt.Errorf("failed to scan pubkey: %w", err)
		}
		result[pk] = struct{}{}
	}
	return result, rows.Err()
}

// GetTrustWeightedReports aggregates reports per target, weighting each report
// by the reporter's trust_score. Reporters with trust_score ≤ 0 are excluded —
// the same admission rule that gates vouch edges. A source that both vouches
// for and reports the same target is not specially handled: the vouch adds
// flow and the report subtracts it, which roughly cancels out on its own.
func (r *Repository) GetTrustWeightedReports() (map[string]models.ReportAggregate, error) {
	query := `
		SELECT r.target_pubkey, COUNT(*), COALESCE(SUM(p.trust_score), 0)
		FROM reports r
		JOIN pubkeys p ON p.pubkey = r.source_pubkey
		WHERE p.trust_score > 0
		GROUP BY r.target_pubkey;
	`
	rows, err := r.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query weighted reports: %w", err)
	}
	defer rows.Close()

	result := make(map[string]models.ReportAggregate)
	for rows.Next() {
		var target string
		var agg models.ReportAggregate
		if err := rows.Scan(&target, &agg.NumReporters, &agg.TotalReporterTrust); err != nil {
			return nil, fmt.Errorf("failed to scan report aggregate: %w", err)
		}
		result[target] = agg
	}
	return result, rows.Err()
}
