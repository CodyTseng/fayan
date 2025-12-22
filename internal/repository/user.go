package repository

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"fayan/internal/models"
)

// UpsertPubkey adds a pubkey to the pubkeys table if it doesn't exist.
func (r *Repository) UpsertPubkey(pubkey string) error {
	now := time.Now().UTC()
	query := `
		INSERT INTO pubkeys (pubkey, created_at, updated_at)
		VALUES (?, ?, ?)
		ON CONFLICT(pubkey) DO NOTHING;
	`
	_, err := r.db.Exec(query, pubkey, now, now)
	return err
}

// UpdatePubkey updates the trustrank score, pagerank score, ranks, followers, and following for a given pubkey.
func (r *Repository) UpdatePubkey(pubkey string, score float64, rank int, trustScore float64, pageScore float64, followers int, following int32) error {
	now := time.Now().UTC()
	query := `
		UPDATE pubkeys
		SET score = ?, rank = ?, trust_score = ?, page_score = ?, followers = ?, following = ?, updated_at = ?
		WHERE pubkey = ?;
	`
	_, err := r.db.Exec(query, score, rank, trustScore, pageScore, followers, following, now, pubkey)
	if err != nil {
		return fmt.Errorf("failed to update score for pubkey %s: %w", pubkey, err)
	}
	return nil
}

// PubkeyUpdate represents an update to a pubkey's scores
type PubkeyUpdate struct {
	Pubkey     string
	Score      float64
	Rank       int
	TrustScore float64
	PageScore  float64
	Followers  int
	Following  int32
}

// BatchUpdatePubkeys updates multiple pubkeys in a single transaction.
// This is more efficient and reduces WAL growth.
func (r *Repository) BatchUpdatePubkeys(updates []PubkeyUpdate) error {
	if len(updates) == 0 {
		return nil
	}

	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		UPDATE pubkeys
		SET score = ?, rank = ?, trust_score = ?, page_score = ?, followers = ?, following = ?, updated_at = ?
		WHERE pubkey = ?;
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	now := time.Now().UTC()
	for _, u := range updates {
		_, err := stmt.Exec(u.Score, u.Rank, u.TrustScore, u.PageScore, u.Followers, u.Following, now, u.Pubkey)
		if err != nil {
			return fmt.Errorf("failed to update pubkey %s: %w", u.Pubkey, err)
		}
	}

	return tx.Commit()
}

// GetUserByPubkey retrieves user information by public key.
func (r *Repository) GetUserByPubkey(pubkey string) (*models.UserInfo, error) {
	query := `
		SELECT pubkey, rank, score, followers, following
		FROM pubkeys
		WHERE pubkey = ?;
	`
	var user models.UserInfo
	var rank sql.NullInt64
	err := r.db.QueryRow(query, pubkey).Scan(
		&user.Pubkey,
		&rank,
		&user.Score,
		&user.Followers,
		&user.Following,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get user by pubkey: %w", err)
	}

	if rank.Valid {
		rankValue := int(rank.Int64)
		user.Rank = &rankValue
	}

	return &user, nil
}

// GetUsersByPubkeys retrieves multiple users by their public keys.
func (r *Repository) GetUsersByPubkeys(pubkeys []string) (map[string]*models.UserInfo, error) {
	if len(pubkeys) == 0 {
		return make(map[string]*models.UserInfo), nil
	}

	// Build placeholders for IN clause
	placeholders := make([]string, len(pubkeys))
	args := make([]interface{}, len(pubkeys))
	for i, pk := range pubkeys {
		placeholders[i] = "?"
		args[i] = pk
	}

	query := fmt.Sprintf(`
		SELECT pubkey, rank, score, followers, following
		FROM pubkeys
		WHERE pubkey IN (%s);
	`, strings.Join(placeholders, ","))

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to get users by pubkeys: %w", err)
	}
	defer rows.Close()

	users := make(map[string]*models.UserInfo)
	for rows.Next() {
		var user models.UserInfo
		var rank sql.NullInt64
		if err := rows.Scan(&user.Pubkey, &rank, &user.Score, &user.Followers, &user.Following); err != nil {
			return nil, fmt.Errorf("failed to scan user row: %w", err)
		}
		if rank.Valid {
			rankValue := int(rank.Int64)
			user.Rank = &rankValue
		}
		users[user.Pubkey] = &user
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating user rows: %w", err)
	}

	return users, nil
}

// GetTotalUsers returns the total number of users in the database.
func (r *Repository) GetTotalUsers() (int, error) {
	var count int
	query := `SELECT COUNT(*) FROM pubkeys;`
	err := r.db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get total users: %w", err)
	}
	return count, nil
}

// RandomPubkeys retrieves a random sample of pubkeys from the database.
func (r *Repository) RandomPubkeys(limit int) ([]string, error) {
	var count int64
	err := r.db.QueryRow("SELECT COUNT(1) FROM pubkeys;").Scan(&count)
	if err != nil {
		return nil, fmt.Errorf("failed to count pubkeys: %w", err)
	}

	if count == 0 {
		return []string{}, nil
	}

	var pubkeys []string
	for len(pubkeys) < limit {
		offset := rand.Int63n(count)
		var pubkey string
		err := r.db.QueryRow("SELECT pubkey FROM pubkeys LIMIT 1 OFFSET ?;", offset).Scan(&pubkey)
		if err == nil {
			pubkeys = append(pubkeys, pubkey)
		}
	}

	return pubkeys, nil
}
