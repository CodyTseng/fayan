# Fayan (法眼)

> [!WARNING]
> This project is currently under active development.

**Fayan** (法眼), meaning "Dharma Eye" in Chinese, represents the ability to perceive truth and distinguish authenticity from deception. This project uses TrustRank and PageRank algorithms to identify spam accounts in the Nostr network.

A TrustRank & PageRank based reputation system for Nostr.

## Features

- **Outbox Model Support** - Accurately retrieves users' latest follow lists by querying their preferred relays
- **Low Memory Footprint** - Optimized for minimal resource consumption
- **SQLite Storage** - Lightweight and portable database backend

## API

| Method | Path                    | Description          |
| ------ | ----------------------- | -------------------- |
| GET    | `/users/{pubkeyOrNpub}` | Query single user    |
| POST   | `/users`                | Batch query users    |
| GET    | `/search?q=...`         | Search users by name |

> Offical server: `https://fayan.jumble.social/`

### Query Single User

**GET** `/users/{pubkeyOrNpub}`

Supports both hex pubkey and npub format.

**Response Fields:**

| Field      | Type   | Description            |
| ---------- | ------ | ---------------------- |
| pubkey     | string | User public key (hex)  |
| rank       | int    | Rank (lower is better) |
| percentile | int    | Percentile (0-100)     |
| followers  | int    | Number of followers    |
| following  | int    | Number of following    |

**Response Example:**

```json
{
  "pubkey": "hex_pubkey",
  "rank": 123,
  "percentile": 95,
  "followers": 1000,
  "following": 50
}
```

### Batch Query Users

**POST** `/users`

Request body:

```json
{
	"pubkeys": ["pubkey1", "npub1...", ...]
}
```

Up to 100 pubkeys per request.

**Response Structure:**

| Key         | Value Type   | Description      |
| ----------- | ------------ | ---------------- |
| pubkey/npub | UserResponse | User info object |

**Response Example:**

```json
{
  "pubkey1": {
    "pubkey": "hex_pubkey1",
    "rank": 1,
    "percentile": 100,
    "followers": 5000,
    "following": 10
  },
  "npub1...": {
    "pubkey": "hex_pubkey2",
    "rank": 123,
    "percentile": 95,
    "followers": 1000,
    "following": 50
  }
}
```

### Error Response Format

All endpoints return the following format on error:

```json
{
  "error": "Bad Request",
  "message": "Detailed error message"
}
```

### Search Users

**GET** `/search?q={query}&limit={limit}`

Search for users by name, display name, or NIP-05 identifier. Results are sorted by a combination of relevance and reputation score.

> **Note:** This feature is disabled by default. Enable it in `config.yaml` by setting `search.enabled: true`.

**Query Parameters:**

| Parameter | Type   | Required | Default | Description                     |
| --------- | ------ | -------- | ------- | ------------------------------- |
| q         | string | Yes      | -       | Search query                    |
| limit     | int    | No       | 20      | Max results to return (max 100) |

**Response Fields:**

| Field      | Type   | Description            |
| ---------- | ------ | ---------------------- |
| event      | object | Raw Nostr kind 0 event |
| pubkey     | string | User public key (hex)  |
| rank       | int    | Rank (lower is better) |
| percentile | int    | Percentile (0-100)     |
| followers  | int    | Number of followers    |
| following  | int    | Number of following    |

**Response Example:**

```json
[
  {
    "event": {
      "id": "...",
      "pubkey": "3efdaebb1d8923ebd99c9e7ace3b4194ab45512e2be79c1b7d68d9243e0d2681",
      "created_at": 1704067200,
      "kind": 0,
      "tags": [],
      "content": "{\"name\":\"xxx\",\"about\":\"...\",\"picture\":\"...\"}",
      "sig": "..."
    },
    "pubkey": "3efdaebb1d8923ebd99c9e7ace3b4194ab45512e2be79c1b7d68d9243e0d2681",
    "rank": 1,
    "percentile": 100,
    "followers": 1000,
    "following": 10
  }
]
```

## How to Run

### Local Build & Run

```sh
# Build both crawler and API binaries
go build -o fayan-crawler ./cmd/crawler/main.go
go build -o fayan-api ./cmd/api/main.go

# Run crawler
./fayan-crawler

# Run API server in another terminal
./fayan-api
```

Default API port is 9090 (configurable in `config.yaml`).

### Docker Compose (Recommended)

```sh
docker compose up --build
```

This will start both crawler and API services. API is mapped to local port 9090 by default.

> Note: Recommend changing the volume paths in `docker-compose.yaml` for persistent data storage.

## Configuration

The application is configured through the `config.yaml` file.

### Search Configuration

The user search feature is disabled by default. To enable it, add the following to your `config.yaml`:

```yaml
search:
  enabled: true # Enable user search feature
  top_percentile: 50 # Only index users in top X% reputation (default: 50)
```

The `top_percentile` setting controls which users' profiles are stored in the search index. Lower values mean stricter filtering, which reduces database size but may exclude more users from search results.

## License

MIT License
