package models

// UserInfo represents a user's complete information.
type UserInfo struct {
	Pubkey    string  `json:"pubkey"`
	Score     float64 `json:"score"`
	Rank      *int    `json:"rank,omitempty"`
	Followers int     `json:"followers"`
	Following int     `json:"following"`
}

// UserProfile represents a user's profile metadata for search.
type UserProfile struct {
	Event     string  `json:"event"`
	Pubkey    string  `json:"pubkey"`
	Score     float64 `json:"score"`
	Rank      *int    `json:"rank,omitempty"`
	Followers int     `json:"followers"`
	Following int     `json:"following"`
}

// Connection represents a follow relationship between two users.
type Connection struct {
	Source string
	Target string
}

// Vouch represents a user's explicit endorsement of another user,
// authenticated via NIP-98 and submitted through the API.
type Vouch struct {
	Source string
	Target string
}

// ReportAggregate summarises reports against a single target pubkey.
// Only reporters with trust_score > 0 contribute.
type ReportAggregate struct {
	NumReporters       int
	TotalReporterTrust float64
}
