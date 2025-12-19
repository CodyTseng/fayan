package repository

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"fayan/internal/models"
)

// UpsertConnection inserts or replaces a connection between two pubkeys.
func (r *Repository) UpsertConnection(source, target string) error {
	now := time.Now().UTC()
	query := `
		REPLACE INTO connections (source_pubkey, target_pubkey, last_seen)
		VALUES (?, ?, ?);
	`
	_, err := r.db.Exec(query, source, target, now)
	if err != nil {
		log.Printf("Error upserting connection %s -> %s: %v", source, target, err)
	}
	return err
}

// StreamConnections streams connections from the database using a callback function.
func (r *Repository) StreamConnections(callback func(models.Connection) error) error {
	rows, err := r.db.Query("SELECT source_pubkey, target_pubkey FROM connections;")
	if err != nil {
		return fmt.Errorf("failed to query connections: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var conn models.Connection
		if err := rows.Scan(&conn.Source, &conn.Target); err != nil {
			return fmt.Errorf("failed to scan connection: %w", err)
		}

		if err := callback(conn); err != nil {
			return fmt.Errorf("callback error: %w", err)
		}
	}

	return rows.Err()
}

// StreamConnectionsInTx streams connections from the database within a read-only transaction.
func (r *Repository) StreamConnectionsInTx(callback func(models.Connection) error, afterTime *time.Time) error {
	tx, err := r.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var rows *sql.Rows
	if afterTime != nil {
		rows, err = tx.Query("SELECT source_pubkey, target_pubkey FROM connections WHERE last_seen >= ?;", afterTime)
	} else {
		rows, err = tx.Query("SELECT source_pubkey, target_pubkey FROM connections;")
	}

	if err != nil {
		return fmt.Errorf("failed to query connections: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var conn models.Connection
		if err := rows.Scan(&conn.Source, &conn.Target); err != nil {
			return fmt.Errorf("failed to scan connection: %w", err)
		}

		if err := callback(conn); err != nil {
			return fmt.Errorf("callback error: %w", err)
		}
	}

	return rows.Err()
}
