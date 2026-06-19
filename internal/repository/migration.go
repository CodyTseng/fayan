package repository

import (
	"database/sql"
	"fmt"
	"log"
	"time"
)

// Migration represents a database schema migration
type Migration struct {
	Version int
	Name    string
	Up      func(db *sql.DB) error
}

// migrations contains all database migrations in order
var migrations = []Migration{
	{
		Version: 1,
		Name:    "initial_schema",
		Up: func(db *sql.DB) error {
			pubkeysTable := `
			CREATE TABLE IF NOT EXISTS pubkeys (
				pubkey TEXT PRIMARY KEY,
				score REAL DEFAULT 0.0,
				rank INTEGER,
				trust_score REAL DEFAULT 0.0,
				page_score REAL DEFAULT 0.0,
				followers INTEGER DEFAULT 0,
				following INTEGER DEFAULT 0,
				created_at TIMESTAMP NOT NULL,
				updated_at TIMESTAMP NOT NULL
			);`

			connectionsTable := `
			CREATE TABLE IF NOT EXISTS connections (
				source_pubkey TEXT NOT NULL,
				target_pubkey TEXT NOT NULL,
				last_seen TIMESTAMP NOT NULL,
				PRIMARY KEY(source_pubkey, target_pubkey)
			);`

			userProfilesTable := `
			CREATE VIRTUAL TABLE IF NOT EXISTS user_profiles USING fts5(
				pubkey UNINDEXED,
				name,
				display_name,
				nip05,
				event UNINDEXED,
				tokenize='trigram'
			);`

			if _, err := db.Exec(pubkeysTable); err != nil {
				return fmt.Errorf("failed to create pubkeys table: %w", err)
			}

			if _, err := db.Exec(connectionsTable); err != nil {
				return fmt.Errorf("failed to create connections table: %w", err)
			}

			if _, err := db.Exec(userProfilesTable); err != nil {
				return fmt.Errorf("failed to create user_profiles table: %w", err)
			}

			if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_connections_target ON connections(target_pubkey);"); err != nil {
				return fmt.Errorf("failed to create idx_connections_target: %w", err)
			}

			return nil
		},
	},
	{
		Version: 2,
		Name:    "add_last_crawled_at",
		Up: func(db *sql.DB) error {
			if _, err := db.Exec("ALTER TABLE pubkeys ADD COLUMN last_crawled_at TIMESTAMP;"); err != nil {
				return fmt.Errorf("failed to add last_crawled_at column: %w", err)
			}

			if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_pubkeys_last_crawled ON pubkeys(last_crawled_at);"); err != nil {
				return fmt.Errorf("failed to create idx_pubkeys_last_crawled: %w", err)
			}

			return nil
		},
	},
	{
		Version: 3,
		Name:    "add_connections_last_seen_index",
		Up: func(db *sql.DB) error {
			if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_connections_last_seen ON connections(last_seen);"); err != nil {
				return fmt.Errorf("failed to create idx_connections_last_seen: %w", err)
			}
			return nil
		},
	},
	{
		Version: 4,
		Name:    "add_vouches_and_reports",
		Up: func(db *sql.DB) error {
			// Vouches share the follow-edge lifecycle: refreshed on each
			// kind:30000 set, never actively deleted, aged out by a staleness
			// window — hence last_seen (cf. connections), not created_at.
			vouchesTable := `
			CREATE TABLE IF NOT EXISTS vouches (
				source_pubkey TEXT NOT NULL,
				target_pubkey TEXT NOT NULL,
				last_seen     TIMESTAMP NOT NULL,
				PRIMARY KEY (source_pubkey, target_pubkey)
			);`

			reportsTable := `
			CREATE TABLE IF NOT EXISTS reports (
				source_pubkey TEXT NOT NULL,
				target_pubkey TEXT NOT NULL,
				created_at    TIMESTAMP NOT NULL,
				PRIMARY KEY (source_pubkey, target_pubkey)
			);`

			if _, err := db.Exec(vouchesTable); err != nil {
				return fmt.Errorf("failed to create vouches table: %w", err)
			}
			if _, err := db.Exec(reportsTable); err != nil {
				return fmt.Errorf("failed to create reports table: %w", err)
			}
			if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_vouches_target ON vouches(target_pubkey);"); err != nil {
				return fmt.Errorf("failed to create idx_vouches_target: %w", err)
			}
			if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_vouches_last_seen ON vouches(last_seen);"); err != nil {
				return fmt.Errorf("failed to create idx_vouches_last_seen: %w", err)
			}
			if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_reports_target ON reports(target_pubkey);"); err != nil {
				return fmt.Errorf("failed to create idx_reports_target: %w", err)
			}
			return nil
		},
	},
}

// RunMigrations executes all pending database migrations
func (r *Repository) RunMigrations() error {
	// Create migrations table if it doesn't exist
	migrationsTable := `
	CREATE TABLE IF NOT EXISTS migrations (
		version INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		applied_at TIMESTAMP NOT NULL
	);`

	if _, err := r.db.Exec(migrationsTable); err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	// Check if this is a legacy database (has pubkeys table but no migration records)
	if err := r.handleLegacyDatabase(); err != nil {
		return fmt.Errorf("failed to handle legacy database: %w", err)
	}

	// Execute pending migrations
	for _, m := range migrations {
		applied, err := r.isMigrationApplied(m.Version)
		if err != nil {
			return fmt.Errorf("failed to check migration %d: %w", m.Version, err)
		}

		if applied {
			continue
		}

		log.Printf("[MIGRATION] Running migration %d: %s", m.Version, m.Name)

		if err := m.Up(r.db); err != nil {
			return fmt.Errorf("migration %d (%s) failed: %w", m.Version, m.Name, err)
		}

		if err := r.recordMigration(m.Version, m.Name); err != nil {
			return fmt.Errorf("failed to record migration %d: %w", m.Version, err)
		}

		log.Printf("[MIGRATION] Completed migration %d: %s", m.Version, m.Name)
	}

	return nil
}

// handleLegacyDatabase detects and handles databases created before the migration system
func (r *Repository) handleLegacyDatabase() error {
	// Check if pubkeys table exists
	var tableName string
	err := r.db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='pubkeys';").Scan(&tableName)
	if err == sql.ErrNoRows {
		// Fresh database, no legacy handling needed
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to check for pubkeys table: %w", err)
	}

	// Check if any migrations have been recorded
	var count int
	err = r.db.QueryRow("SELECT COUNT(*) FROM migrations;").Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to count migrations: %w", err)
	}

	if count > 0 {
		// Migrations already recorded, not a legacy database
		return nil
	}

	// This is a legacy database - mark migration 1 as already applied
	log.Printf("[MIGRATION] Detected legacy database, marking initial schema as applied")
	if err := r.recordMigration(1, "initial_schema"); err != nil {
		return fmt.Errorf("failed to record legacy migration: %w", err)
	}

	return nil
}

// isMigrationApplied checks if a migration version has been applied
func (r *Repository) isMigrationApplied(version int) (bool, error) {
	var count int
	err := r.db.QueryRow("SELECT COUNT(*) FROM migrations WHERE version = ?;", version).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// recordMigration records a migration as applied
func (r *Repository) recordMigration(version int, name string) error {
	_, err := r.db.Exec(
		"INSERT INTO migrations (version, name, applied_at) VALUES (?, ?, ?);",
		version, name, time.Now().UTC(),
	)
	return err
}
