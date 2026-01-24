package repository

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
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

// totalUsersCache caches the count of users
type totalUsersCache struct {
	count  int
	expiry time.Time
	mu     sync.RWMutex
	ttl    time.Duration
}

// Repository handles all database operations
type Repository struct {
	db              *sql.DB
	totalUsersCache *totalUsersCache
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
	// For write mode: 4 contact processors + 2 profile processors + 1 ranking = 7 concurrent writers
	// Set to 8 to ensure no connection starvation
	if mode == ModeReadOnly {
		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(5)
	} else {
		db.SetMaxOpenConns(8)
		db.SetMaxIdleConns(4)
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

	repo := &Repository{
		db: db,
		totalUsersCache: &totalUsersCache{
			ttl: 5 * time.Minute,
		},
	}

	// Run migrations in read-write mode
	if mode == ModeReadWrite {
		if err := repo.RunMigrations(); err != nil {
			return nil, fmt.Errorf("could not run migrations: %w", err)
		}
	}

	return repo, nil
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

