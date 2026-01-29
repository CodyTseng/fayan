# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Fayan (法眼) is a TrustRank & PageRank based reputation system for the Nostr network. It crawls the Nostr social graph to identify spam accounts by analyzing follow relationships.

## Build and Run Commands

```bash
# Build binaries
go build -o fayan-crawler ./cmd/crawler/main.go
go build -o fayan-api ./cmd/api/main.go

# Run crawler (fetches and processes Nostr events)
./fayan-crawler

# Run API server (default port :9090)
./fayan-api

# Docker Compose (runs both services)
docker compose up --build
```

## Architecture

### Two-Binary System
- **Crawler** (`cmd/crawler/main.go`): Crawls the Nostr network, stores follow relationships in SQLite, periodically calculates PageRank/TrustRank scores
- **API** (`cmd/api/main.go`): Read-only HTTP server that queries reputation data

### Database Access Modes
The repository supports two modes via `repository.New(path, mode)`:
- `ModeReadWrite`: For crawler - writes connections and scores
- `ModeReadOnly`: For API - optimized for concurrent reads with `PRAGMA query_only = ON`

### Key Packages
- `internal/crawler/`: Network crawler using go-nostr library
  - `crawler.go`: Main crawl loop with pause/resume support, processes kind:3 (contacts) and kind:0 (profiles) events
  - `pool_manager.go`: Manages Nostr relay WebSocket connections
  - `relay_health.go`: Tracks relay failures, bans unreliable relays
- `internal/ranking/`: PageRank and TrustRank algorithms
  - Combines scores: `trustrank_weight*trustScore + pagerank_weight*pageScore` (configurable)
  - Uses seed pubkeys from config as trust anchors for TrustRank
- `internal/repository/`: SQLite storage layer with FTS5 for user search
- `internal/api/handler/`: HTTP handlers for `/users`, `/search` endpoints

### Crawl Flow
1. Fetch kind:10002 (relay list) events from bootstrap relays
2. Calculate target relays for each pubkey (user's write relays + fallbacks)
3. Fetch kind:3 (contacts) and optionally kind:0 (profiles) from target relays
4. Process follow relationships into `connections` table
5. Periodically run PageRank/TrustRank calculations

### Search Feature (when `search.enabled: true`)
When enabled, the crawler also fetches and processes kind:0 (profile) events:
1. `profileProcessor` goroutines receive kind:0 events from `profilesChan`
2. `processKind0Event` parses metadata (name, display_name, nip05) via go-nostr SDK
3. Checks if user is in top percentile (`search.top_percentile`) before storing
4. Stores qualifying profiles in `user_profiles` FTS5 table with trigram tokenizer (supports CJK)
5. Search queries use FTS5 for 3+ char queries, LIKE prefix matching for shorter queries
6. Results ranked by: `bm25(relevance) * 0.3 + reputation_score * 0.7`

### Configuration
Copy `config.example.yaml` to `config.yaml` (and `docker-compose.example.yml` to `docker-compose.yml` for Docker). Config options:
- `relays`: Bootstrap relays for initial queries
- `seed_pubkeys`: Trusted accounts for TrustRank algorithm
- `pagerank_interval`: Minutes between score recalculations
- `search.enabled`: Toggle user search feature (requires FTS5)
- `ranking.trustrank_weight`: Weight for TrustRank score (default: 0.7)
- `ranking.pagerank_weight`: Weight for PageRank score (default: 0.3)
