package repository

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
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
	writeMu         sync.Mutex // Serializes all write operations for SQLite
}

// New creates a new Repository instance.
// WAL mode allows concurrent readers alongside a single writer; writeMu
// serializes writers within this process, SQLite's own locks coordinate
// across processes.
func New(dataSourceName string) (*Repository, error) {
	// Build DSN with parameters that apply to ALL connections in the pool.
	// DSN parameters are applied when each connection is created, while
	// PRAGMA statements would only affect whichever connection ran them.
	dsnParams := []string{
		"_journal_mode=WAL",
		"_synchronous=NORMAL",
		"_busy_timeout=30000",
		"_cache_size=-64000",
		"_txlock=immediate", // Acquire write lock at BEGIN, not at first write
	}

	separator := "?"
	if strings.Contains(dataSourceName, "?") {
		separator = "&"
	}
	fullDSN := dataSourceName + separator + strings.Join(dsnParams, "&")

	db, err := sql.Open("sqlite3", fullDSN)
	if err != nil {
		return nil, fmt.Errorf("could not open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("could not connect to database: %w", err)
	}

	// Connection pool: multiple connections so reads can run concurrently
	// under WAL. Writes serialize on writeMu, so extra connections do not
	// cause write contention.
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	additionalPragmas := []string{
		"PRAGMA temp_store = MEMORY;",
		"PRAGMA mmap_size = 1073741824;",
		"PRAGMA wal_autocheckpoint = 1000;",
		"PRAGMA journal_size_limit = 104857600;",
	}

	for _, pragma := range additionalPragmas {
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

	if err := repo.RunMigrations(); err != nil {
		return nil, fmt.Errorf("could not run migrations: %w", err)
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

