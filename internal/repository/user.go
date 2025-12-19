package repository

import (
	"database/sql"
	"fmt"
	"math/rand"
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
