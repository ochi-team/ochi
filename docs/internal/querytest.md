# Query Test Strategy

## Goals

- Test that LOQL queries return the correct log lines against a known dataset.
- Support multiple tenants and streams without coupling.
- Let the corpus and the query suite grow independently: adding a log to the corpus should not require touching every query fixture; adding a new query should not require touching the corpus.

---

## Core Model

Two artifacts drive every test:

1. **Corpus** — a fixed set of log entries with stable IDs, ingested once before all queries run.
2. **Query fixtures** — query file, declaring which corpus IDs must appear in the results.

The test runner materializes the corpus into a fresh server, runs every query fixture, and diffs the returned IDs against the declared sets. A mismatch is a failure.

When you add a new log to the corpus, only the query fixtures whose predicates happen to match it need updating. The runner tells you exactly which fixtures are now broken and what the diff is — you do not need to touch the rest.

---

## Corpus Format

File names are lexicographically sorted in order to let the test runner ingest entries in a deterministic order of multiple batches.

Location: `corpora/<corpus_name>/ingest.json`, example of corpus_name `0001_and_eq`

```json
{
  "tenant": "team-a",
  "stream": { "env": "prod", "service": "web" },
  "logs": [
    {
        "offsetMin": -30,
        "message": "GET /api/health 200 3ms",
        "fields": { "id": "web-001", "status": "200", "path": "/api/health", "latency_ms": "3" }
    },
    {
        "offsetMin": -20,
        "message": "POST /api/orders 500 timeout",
        "fields": { "id": "web-002" , "status": "500", "path": "/api/orders", "latency_ms": "4820" }
    }
  ]
}
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

Location: `<corpus_name>/queries.json`

```json
[
    {
        "description": "Find all 500 errors in the last hour",
        "tenant": "team-a",
        "query": "[-60m,now] {env=prod AND service=web} status=500",
        "match": ["web-002"]
    },
    ...
]
```

