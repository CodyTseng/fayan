package repository

import (
	"database/sql"
	"fmt"
	"strings"

	"fayan/internal/models"
)

// UpsertUserProfile adds or updates a user's profile information for search.
func (r *Repository) UpsertUserProfile(pubkey, name, displayName, nip05, event string) error {
	// FTS5 doesn't support UPSERT, so we delete first then insert
	if _, err := r.db.Exec("DELETE FROM user_profiles WHERE pubkey = ?", pubkey); err != nil {
		return err
	}

	query := `INSERT INTO user_profiles (pubkey, name, display_name, nip05, event) VALUES (?, ?, ?, ?, ?);`
	_, err := r.db.Exec(query, pubkey, name, displayName, nip05, event)
	return err
}

// SearchUsers searches for users by name, display_name or nip05 using FTS5.
// Results are sorted by a combination of relevance and reputation score.
// limit specifies the maximum number of results to return.
func (r *Repository) SearchUsers(query string, limit int) ([]*models.UserProfile, error) {
	if query == "" || limit <= 0 {
		return []*models.UserProfile{}, nil
	}

	// Count characters (not bytes) for proper Unicode handling
	charCount := len([]rune(query))

	var rows *sql.Rows
	var err error

	if charCount < 3 {
		// For queries less than 3 characters, use prefix search with LIKE
		searchPattern := query + "%"
		sqlQuery := `
			SELECT 
				up.event,
				p.pubkey,
				p.rank,
				p.score,
				p.followers,
				p.following
			FROM user_profiles up
			JOIN pubkeys p ON up.pubkey = p.pubkey
			WHERE up.name LIKE ? OR up.display_name LIKE ? OR up.nip05 LIKE ?
			ORDER BY p.score DESC
			LIMIT ?;
		`
		rows, err = r.db.Query(sqlQuery, searchPattern, searchPattern, searchPattern, limit)
	} else {
		// For queries with 3+ characters, use FTS5 trigram search
		searchTerm := escapeFTS5Query(query)
		sqlQuery := `
			SELECT
				up.event,
				p.pubkey,
				p.rank,
				p.score,
				p.followers,
				p.following
			FROM user_profiles up
			JOIN pubkeys p ON up.pubkey = p.pubkey
			WHERE user_profiles MATCH ?
			ORDER BY (bm25(user_profiles) * 0.3 + p.score * 0.7) DESC, p.score DESC
			LIMIT ?;
		`
		rows, err = r.db.Query(sqlQuery, searchTerm, limit)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to search users: %w", err)
	}
	defer rows.Close()

	var users []*models.UserProfile
	for rows.Next() {
		var user models.UserProfile
		var rank sql.NullInt64

		if err := rows.Scan(&user.Event, &user.Pubkey, &rank, &user.Score, &user.Followers, &user.Following); err != nil {
			return nil, fmt.Errorf("failed to scan user row: %w", err)
		}

		if rank.Valid {
			rankValue := int(rank.Int64)
			user.Rank = &rankValue
		}

		users = append(users, &user)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating user rows: %w", err)
	}

	return users, nil
}

// escapeFTS5Query escapes special characters in FTS5 query for trigram
func escapeFTS5Query(query string) string {
	// Escape double quotes by doubling them
	escaped := strings.ReplaceAll(query, `"`, `""`)
	// Wrap in quotes for exact phrase matching
	return `"` + escaped + `"`
}

// GetRankCutoff returns the rank value at the given percentile.
// For example, if percentile is 50 and there are 1000 users, it returns the rank at position 500.
// The total users count is cached for 5 minutes to reduce database load.
func (r *Repository) GetRankCutoff(percentile int) (int, error) {
	if percentile <= 0 || percentile > 100 {
		return 0, fmt.Errorf("percentile must be between 1 and 100")
	}

	// Get total count of users (with cache)
	totalUsers, err := r.GetTotalUsersCached()
	if err != nil {
		return 0, err
	}

	if totalUsers == 0 {
		return 0, nil
	}

	// Calculate the cutoff rank (top X%)
	cutoffRank := (totalUsers * percentile) / 100
	if cutoffRank == 0 {
		cutoffRank = 1
	}

	return cutoffRank, nil
}

// IsUserInTopPercentile checks if a user's rank is within the top percentile.
func (r *Repository) IsUserInTopPercentile(pubkey string, percentile int) (bool, error) {
	cutoff, err := r.GetRankCutoff(percentile)
	if err != nil {
		return false, err
	}

	if cutoff == 0 {
		// No ranked users yet, allow all
		return true, nil
	}

	var rank sql.NullInt64
	err = r.db.QueryRow("SELECT rank FROM pubkeys WHERE pubkey = ?;", pubkey).Scan(&rank)
	if err == sql.ErrNoRows {
		// User not found, don't allow
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to get user rank: %w", err)
	}

	if !rank.Valid {
		// User has no rank yet, don't allow
		return false, nil
	}

	return int(rank.Int64) <= cutoff, nil
}
