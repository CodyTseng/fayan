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
