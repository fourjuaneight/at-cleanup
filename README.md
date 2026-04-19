# bsky-repo-cleanup

HTTP service for cleaning up old Bluesky posts, reposts, and likes from your account.

## What it does

Connects to your Bluesky PDS, scans your repo for records matching your criteria (age, type), and deletes them in batches — respecting rate limits (4,000/hour, 30,000/day). Jobs run in the background and persist across restarts via Postgres.

## Running

```bash
# Start (Postgres + service)
docker compose up -d
```

The API is available at `http://localhost:8080`.

## API

### Start a cleanup job

```bash
POST /repo/cleanup
Content-Type: application/json

{
  "identifier": "you.bsky.social",      # handle, DID, or email
  "app_password": "xxxx-xxxx-xxxx-xxxx",
  "cleanup_types": ["post", "repost", "like", "post_with_media"],
  "delete_until_days_ago": 30,
  "actually_delete_stuff": false         # set true to actually delete
}
```

`actually_delete_stuff: false` is a dry run — returns the count of records that would be deleted without touching anything.

### Check job status

```bash
GET /repo/cleanup?job_id=<id>
GET /repo/cleanup?did=<did>      # list all jobs for a DID
```

### Cancel a job

```bash
DELETE /repo/cleanup?job_id=<id>
```

### Aggregate stats

```bash
GET /repo/cleanup/stats
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `LISTEN_ADDRESS` | `:8080` | HTTP server bind address |
| `DATABASE_URL` | `postgres://bsky:bsky@postgres:5432/bsky?sslmode=disable` | Postgres connection string |
| `TOKEN_ENCRYPTION_KEY` | | Hex-encoded 32-byte AES-256 key for encrypting refresh tokens at rest. Generate with `openssl rand -hex 32`. |
| `MAGIC_HEADER_VAL` | | Optional rate-limit bypass header for the Bluesky PDS |
| `DEBUG` | `false` | Enable debug logging |

## Project structure

```
cmd/search/         # service entrypoint
pkg/
  endpoints/        # HTTP handlers (repocleanup.go)
  store/            # Postgres client + cleanup job queries
telemetry/          # structured JSON logging
version/            # build version info
Dockerfile          # multi-stage build (Go 1.24 / Alpine 3.21)
docker-compose.yml  # Postgres + service
env/
  search.env.example
```

The schema (`repo_cleanup_jobs` table) is created automatically on first startup — no separate migration step needed.

## Dependencies

- [echo](https://github.com/labstack/echo) — HTTP framework
- [pgx](https://github.com/jackc/pgx) — Postgres driver
- [indigo](https://github.com/bluesky-social/indigo) — ATProto client
