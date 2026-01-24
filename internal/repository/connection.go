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
	// Serialize write operations to avoid SQLite lock contention
	r.writeMu.Lock()
	defer r.writeMu.Unlock()

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

// Connection represents a follow relationship
type Connection struct {
	Source string
	Target string
}

// BatchUpsertPubkeysAndConnections inserts pubkeys and connections in a single transaction.
// This dramatically reduces lock contention compared to individual writes.
func (r *Repository) BatchUpsertPubkeysAndConnections(pubkeys []string, connections []Connection) error {
	if len(pubkeys) == 0 && len(connections) == 0 {
		return nil
	}

	// Serialize write operations to avoid SQLite lock contention
	r.writeMu.Lock()
	defer r.writeMu.Unlock()

	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC()

	// Batch insert pubkeys
	if len(pubkeys) > 0 {
		pubkeyStmt, err := tx.Prepare(`
			INSERT INTO pubkeys (pubkey, created_at, updated_at)
			VALUES (?, ?, ?)
			ON CONFLICT(pubkey) DO NOTHING;
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare pubkey statement: %w", err)
		}
		defer pubkeyStmt.Close()

		for _, pk := range pubkeys {
			if _, err := pubkeyStmt.Exec(pk, now, now); err != nil {
				return fmt.Errorf("failed to insert pubkey %s: %w", pk, err)
			}
		}
	}

	// Batch insert connections
	if len(connections) > 0 {
		connStmt, err := tx.Prepare(`
			REPLACE INTO connections (source_pubkey, target_pubkey, last_seen)
			VALUES (?, ?, ?);
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare connection statement: %w", err)
		}
		defer connStmt.Close()

		for _, conn := range connections {
			if _, err := connStmt.Exec(conn.Source, conn.Target, now); err != nil {
				return fmt.Errorf("failed to insert connection %s -> %s: %w", conn.Source, conn.Target, err)
			}
		}
	}

	return tx.Commit()
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
