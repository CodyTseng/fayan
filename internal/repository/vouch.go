package repository

import (
	"fmt"
	"time"

	"fayan/internal/models"
)

// SetVouch records source→target as a vouch relationship and atomically removes
// any existing report from source to the same target (mutual-exclusion toggle).
// The target pubkey row is upserted so ranking can cover brand-new targets.
func (r *Repository) SetVouch(source, target string) error {
	return r.setRelation(source, target, "vouches", "reports")
}

// SetReport records source→target as a report and atomically removes any
// existing vouch from source to the same target.
func (r *Repository) SetReport(source, target string) error {
	return r.setRelation(source, target, "reports", "vouches")
}

func (r *Repository) setRelation(source, target, insertTable, deleteTable string) error {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()

	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC()

	if _, err := tx.Exec(
		`INSERT INTO pubkeys (pubkey, created_at, updated_at) VALUES (?, ?, ?) ON CONFLICT(pubkey) DO NOTHING;`,
		target, now, now,
	); err != nil {
		return fmt.Errorf("failed to upsert target pubkey: %w", err)
	}

	if _, err := tx.Exec(
		fmt.Sprintf(`DELETE FROM %s WHERE source_pubkey = ? AND target_pubkey = ?;`, deleteTable),
		source, target,
	); err != nil {
		return fmt.Errorf("failed to delete opposite relation: %w", err)
	}

	if _, err := tx.Exec(
		fmt.Sprintf(`INSERT OR REPLACE INTO %s (source_pubkey, target_pubkey, created_at) VALUES (?, ?, ?);`, insertTable),
		source, target, now,
	); err != nil {
		return fmt.Errorf("failed to insert relation: %w", err)
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

// StreamVouches streams all vouch edges. Shape mirrors StreamConnections.
func (r *Repository) StreamVouches(callback func(models.Vouch) error) error {
	rows, err := r.db.Query("SELECT source_pubkey, target_pubkey FROM vouches;")
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
// the same admission rule that gates vouch edges.
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
