package database

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type Connection struct {
	Source string
	Target string
}

// UserInfo represents a user's complete information.
type UserInfo struct {
	Pubkey    string  `json:"pubkey"`
	Rank      *int    `json:"rank,omitempty"` // Nullable
	Score     float64 `json:"score"`
	Followers int     `json:"followers"`
}

// Initialize opens the database connection and ensures the schema is up to date.
func Initialize(dataSourceName string) (*sql.DB, error) {
	// Enable WAL mode and busy timeout in the connection string
	// WAL mode allows readers and writers to work concurrently
	if !containsQueryParams(dataSourceName) {
		dataSourceName += "?_journal_mode=WAL&_busy_timeout=5000"
	}

	db, err := sql.Open("sqlite3", dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("could not open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("could not connect to database: %w", err)
	}

	// Configure connection pool to reduce contention
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	// Set additional PRAGMAs for better concurrency
	pragmas := []string{
		"PRAGMA synchronous = NORMAL;",    // Faster writes, still safe in WAL mode
		"PRAGMA temp_store = MEMORY;",     // Use memory for temp tables
		"PRAGMA mmap_size = 30000000000;", // Memory-mapped I/O for better performance
		"PRAGMA cache_size = -64000;",     // 64MB cache
	}

	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			log.Printf("Warning: failed to set pragma: %v", err)
		}
	}

	if err := createTables(db); err != nil {
		return nil, fmt.Errorf("could not create tables: %w", err)
	}

	return db, nil
}

// containsQueryParams checks if the data source name already contains query parameters
func containsQueryParams(dsn string) bool {
	return len(dsn) > 0 && (dsn[len(dsn)-1] == '?' || containsChar(dsn, '?'))
}

func containsChar(s string, c rune) bool {
	for _, ch := range s {
		if ch == c {
			return true
		}
	}
	return false
}

// createTables defines and executes the SQL statements to create the necessary tables.
func createTables(db *sql.DB) error {
	pubkeysTable := `
	CREATE TABLE IF NOT EXISTS pubkeys (
		pubkey TEXT PRIMARY KEY,
		score REAL DEFAULT 0.0,
		rank INTEGER,
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

	_, err := db.Exec(pubkeysTable)
	if err != nil {
		return err
	}

	_, err = db.Exec(connectionsTable)
	if err != nil {
		return err
	}

	// Create indexes for faster queries
	_, err = db.Exec("CREATE INDEX IF NOT EXISTS idx_connections_target ON connections(target_pubkey);")
	if err != nil {
		return err
	}

	return nil
}

// UpsertPubkey adds a pubkey to the pubkeys table if it doesn't exist.
func UpsertPubkey(db *sql.DB, pubkey string) error {
	now := time.Now().UTC()
	query := `
		INSERT INTO pubkeys (pubkey, created_at, updated_at)
		VALUES (?, ?, ?)
		ON CONFLICT(pubkey) DO NOTHING;
	`
	_, err := db.Exec(query, pubkey, now, now)
	return err
}

// UpdatePubkey updates the score, rank, followers, and following for a given pubkey.
func UpdatePubkey(db *sql.DB, pubkey string, score float64, rank int, followers int, following int32) error {
	now := time.Now().UTC()
	query := `
		UPDATE pubkeys
		SET score = ?, rank = ?, followers = ?, following = ?, updated_at = ?
		WHERE pubkey = ?;
	`
	_, err := db.Exec(query, score, rank, followers, following, now, pubkey)
	if err != nil {
		return fmt.Errorf("failed to update score for pubkey %s: %w", pubkey, err)
	}
	return err
}

// UpsertConnection inserts or replaces a connection between two pubkeys.
func UpsertConnection(db *sql.DB, source, target string) error {
	now := time.Now().UTC()
	query := `
		REPLACE INTO connections (source_pubkey, target_pubkey, last_seen)
		VALUES (?, ?, ?);
	`
	_, err := db.Exec(query, source, target, now)
	if err != nil {
		log.Printf("Error upserting connection %s -> %s: %v", source, target, err)
	}
	return err
}

// StreamConnections streams connections from the database using a callback function.
// This approach avoids loading all connections into memory and reduces database lock time.
func StreamConnections(db *sql.DB, callback func(Connection) error) error {
	rows, err := db.Query("SELECT source_pubkey, target_pubkey FROM connections;")
	if err != nil {
		return fmt.Errorf("failed to query connections: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var conn Connection
		if err := rows.Scan(&conn.Source, &conn.Target); err != nil {
			return fmt.Errorf("failed to scan connection: %w", err)
		}

		if err := callback(conn); err != nil {
			return fmt.Errorf("callback error: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating connections: %w", err)
	}

	return nil
}

// StreamConnectionsInTx streams connections from the database within a transaction.
// This allows using a read-only transaction to avoid blocking writes.
// If afterTime is provided, only connections with last_seen after that time will be streamed.
func StreamConnectionsInTx(tx *sql.Tx, callback func(Connection) error, afterTime *time.Time) error {
	var rows *sql.Rows
	var err error

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
		var conn Connection
		if err := rows.Scan(&conn.Source, &conn.Target); err != nil {
			return fmt.Errorf("failed to scan connection: %w", err)
		}

		if err := callback(conn); err != nil {
			return fmt.Errorf("callback error: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating connections: %w", err)
	}

	return nil
}

// RandomPubkeys retrieves a random sample of pubkeys from the database.
func RandomPubkeys(db *sql.DB, limit int) ([]string, error) {
	// First get the total count
	var count int64
	err := db.QueryRow("SELECT COUNT(*) FROM pubkeys;").Scan(&count)
	if err != nil {
		return nil, fmt.Errorf("failed to count pubkeys: %w", err)
	}

	if count == 0 {
		return []string{}, nil
	}

	// For large tables, use a more efficient random sampling
	// Generate random offsets
	var pubkeys []string
	for len(pubkeys) < limit {
		offset := rand.Int63n(count)
		var pubkey string
		err := db.QueryRow("SELECT pubkey FROM pubkeys LIMIT 1 OFFSET ?;", offset).Scan(&pubkey)
		if err == nil {
			pubkeys = append(pubkeys, pubkey)
		}
	}

	return pubkeys, nil
}

// GetUserByPubkey retrieves user information by public key.
func GetUserByPubkey(db *sql.DB, pubkey string) (*UserInfo, error) {
	query := `
		SELECT pubkey, rank, score, followers
		FROM pubkeys
		WHERE pubkey = ?;
	`
	var user UserInfo
	var rank sql.NullInt64
	err := db.QueryRow(query, pubkey).Scan(
		&user.Pubkey,
		&rank,
		&user.Score,
		&user.Followers,
	)
	if err == sql.ErrNoRows {
		return nil, nil // User not found
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get user by pubkey: %w", err)
	}

	// Convert sql.NullInt64 to *int
	if rank.Valid {
		rankValue := int(rank.Int64)
		user.Rank = &rankValue
	}

	return &user, nil
}

// GetTotalUsers returns the total number of users in the database.
func GetTotalUsers(db *sql.DB) (int, error) {
	var count int
	query := `SELECT COUNT(*) FROM pubkeys;`
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get total users: %w", err)
	}
	return count, nil
}
