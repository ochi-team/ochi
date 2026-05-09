# Query Test Strategy

## Goals

- Test that LOQL queries return the correct log lines against a known dataset.
- Support multiple tenants and streams without coupling.
- Let the corpus and the query suite grow independently: adding a log to the corpus should not require touching every query fixture; adding a new query should not require touching the corpus.

---

## Core Model

Two artifacts drive every test:

1. **Corpus** — a fixed set of log entries with stable IDs, ingested once before all queries run.
2. **Query fixtures** — one file per query, declaring which corpus IDs must appear in the result.

The test runner materializes the corpus into a fresh server, runs every query fixture, and diffs the returned IDs against the declared sets. A mismatch is a failure.

When you add a new log to the corpus, only the query fixtures whose predicates happen to match it need updating. The runner tells you exactly which fixtures are now broken and what the diff is — you do not need to touch the rest.

---

## Corpus Format

File name is lexicographically sorted in order to let the test runner ingest entries in a deterministic order of multiple batches.

Location: `testdata/corpora/ingest_<ingest_order>.json`

```json
[
  {
    "tenant": "team-a",
    "offsetMin": -30,
    "stream": { "env": "prod", "service": "web" },
    "message": "GET /api/health 200 3ms",
    "fields": { "id": "web-001", "status": "200", "path": "/api/health", "latency_ms": "3" }
  },
  {
    "tenant": "team-a",
    "offsetMin": -20,
    "stream": { "env": "prod", "service": "web" },
    "message": "POST /api/orders 500 timeout",
    "fields": { "id": "web-002" , "status": "500", "path": "/api/orders", "latency_ms": "4820" }
  }
]
```

### Field semantics

| Field       | Type              | Meaning                                                    |
|-------------|-------------------|------------------------------------------------------------|
| `fields.id`        | string            | Stable, unique identifier used in query fixtures           |
| `tenant`    | string            | Maps to `X-Scope-OrgID` header; isolates multi-tenant data |
| `offsetMin` | int               | Minutes relative to test epoch    |
| `stream`    | map(string => string) | Loki stream labels / LOQL tag filters                      |
| `message`   | string            | The bare log line (the empty-key field in the protocol, or shortly a `message`)    |
| `fields`    | map(string => string) | Structured fields attached to the log entry                |

### Time handling

---

## Query Fixture Format

Location: `testdata/query_<ingest_order>_<query_name>.toml`

```toml
# Human-readable description
description = "500 errors on the web service in prod"

tenant = "team-a"

query = "[-60m,now] {env=prod AND service=web} status=500"

# Corpus IDs that MUST appear in the result.
match = ["web-002"]
```

