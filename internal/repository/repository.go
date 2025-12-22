package repository

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// DBMode specifies the database access mode
type DBMode int

const (
	// ModeReadWrite is for crawler - optimized for writes
	ModeReadWrite DBMode = iota
	// ModeReadOnly is for API - optimized for reads
	ModeReadOnly
)

// Repository handles all database operations
type Repository struct {
	db *sql.DB
}

// New creates a new Repository instance
func New(dataSourceName string, mode DBMode) (*Repository, error) {
	db, err := sql.Open("sqlite3", dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("could not open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("could not connect to database: %w", err)
	}

	// Configure connection pool based on mode
	if mode == ModeReadOnly {
		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(5)
	} else {
		db.SetMaxOpenConns(4)
		db.SetMaxIdleConns(2)
	}
	db.SetConnMaxLifetime(time.Hour)

	// Set additional PRAGMAs for better concurrency
	pragmas := []string{
		"PRAGMA journal_mode = WAL;",
		"PRAGMA synchronous = NORMAL;",
		"PRAGMA temp_store = MEMORY;",
		"PRAGMA mmap_size = 1073741824;",
		"PRAGMA cache_size = -64000;",
		"PRAGMA wal_autocheckpoint = 1000;",
		"PRAGMA journal_size_limit = 104857600;",
		"PRAGMA busy_timeout = 30000;",
	}

	if mode == ModeReadOnly {
		pragmas = append(pragmas, "PRAGMA query_only = ON;")
	}

	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			log.Printf("Warning: failed to set pragma: %v", err)
		}
	}

	// Only create tables in read-write mode
	if mode == ModeReadWrite {
		if err := createTables(db); err != nil {
			return nil, fmt.Errorf("could not create tables: %w", err)
		}
	}

	return &Repository{db: db}, nil
}

// DB returns the underlying database connection (for backward compatibility)
func (r *Repository) DB() *sql.DB {
	return r.db
}

// Close closes the database connection
func (r *Repository) Close() error {
	return r.db.Close()
}

// Checkpoint performs a WAL checkpoint to reduce WAL file size.
// This should be called periodically, especially after batch operations.
func (r *Repository) Checkpoint() error {
	_, err := r.db.Exec("PRAGMA wal_checkpoint(TRUNCATE);")
	if err != nil {
		log.Printf("Warning: failed to checkpoint WAL: %v", err)
	}
	return err
}

// Ping checks the database connectivity
func (r *Repository) Ping() error {
	return r.db.Ping()
}

// BeginTransaction starts a new transaction for batch operations
func (r *Repository) BeginTransaction() (*sql.Tx, error) {
	return r.db.Begin()
}

// createTables defines and executes the SQL statements to create the necessary tables.
func createTables(db *sql.DB) error {
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

	if _, err := db.Exec(pubkeysTable); err != nil {
		return err
	}

	if _, err := db.Exec(connectionsTable); err != nil {
		return err
	}

	// Create indexes for faster queries
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_connections_target ON connections(target_pubkey);"); err != nil {
		return err
	}

	// Add columns if they don't exist (for existing databases)
	db.Exec("ALTER TABLE pubkeys ADD COLUMN trust_score REAL DEFAULT 0.0;")
	db.Exec("ALTER TABLE pubkeys ADD COLUMN page_score REAL DEFAULT 0.0;")

	return nil
}
