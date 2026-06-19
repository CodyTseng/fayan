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

### Database Access

`repository.New(path)` opens a single read-write handle. WAL mode allows multiple concurrent readers alongside a single writer; within a process `writeMu` serializes writers, and SQLite's own locks coordinate across the crawler and API processes. The connection pool is sized for 10 concurrent readers.

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
- `crawler.batch_size`: Pubkeys per batch (default: 500)
- `crawler.request_interval_ms`: Milliseconds between requests per relay (default: 500)
- `crawler.num_contact_processors`: Number of contact event processors (default: 4)
- `crawler.num_profile_processors`: Number of profile event processors (default: 4)
- `vouch.weight`: Enables the feature (0 = disabled, default) and sets the weight of a vouch edge relative to a follow edge (1.0). Typical enabled value: `0.5`.

### Vouch & Report via Nostr Events (when `vouch.weight > 0`)

Vouches and reports are plain signed Nostr events, not a private API. They flow in two ways (both verify the event signature before storing):

1. **Crawler ingestion (pull)** — one subscription per relay carries two filters (a REQ OR's multiple filters): filter 1 is kind:3 (+kind:0 when search is on); filter 2 is the kind:30000 vouch set constrained by `#d` (scoped to that filter, so it doesn't affect 3/0). kind:1984 reports are fetched in a separate subscription, capped at the newest 50, so an append-only flood can't crowd out the replaceable events.
2. **`POST /event` (push)** — accepts a single signed event (kind 3 / 1984 / 30000). It is fire-and-forget: the event is queued and the request returns 202 immediately and unconditionally. Background workers then verify the signature, apply the anti-inflation admission rule (events from pubkeys with no TrustRank and not in `seed_pubkeys` are dropped), and persist. A full queue simply drops the event — the crawler ingests it from relays anyway. (The crawler path does not apply the admission rule — ranking already discounts untrusted sources.)

Shared parsing/storage lives in `internal/ingest` so both paths behave identically.

- **Vouch** = membership in the author's vouch set: a **NIP-51 follow set (kind:30000)** tagged `d=vouch` (regular follow sets with any other `d` are ignored; the identifier is deliberately generic so it can become a shared convention). Its `p` tags list the vouched pubkeys. Registers source→target as a vouch edge (weight `vouch.weight`, deduped against a follow from the same source). Vouches follow the **same lifecycle as follow edges** (`vouches.last_seen`, not active deletion): each set refreshes its edges' `last_seen`; a pubkey dropped from the set is not deleted but stops being refreshed and ages out via the same staleness window as follows (`StreamVouches` filters on the ranking cutoff). So revoking a vouch takes effect after the window, exactly like unfollowing.
- **Report** = a **kind:1984** (NIP-56) event targeting a **profile** (`p` tag, no `e` tag) with report type `spam` or `impersonation` (other types ignored). Applies a trust-weighted penalty to the target's final score: `final = raw * (1 - R/(R+F))` where R is the sum of reporter trust_scores and F is the sum of follower/voucher trust_scores.

No mutual exclusion — `vouches` and `reports` rows coexist independently. A source that both vouches for and reports the same target is not specially handled: the vouch adds flow and the report subtracts it at ranking time, which roughly cancels out. Stored in the `vouches` and `reports` tables (schema from migration v4).
