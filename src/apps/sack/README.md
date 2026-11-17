# Sack: ingest/query continuous soak testing

## Run

```sh
zig build
# Terminal 1: isolate the server's .ochi directory from repository data.
mkdir /tmp/ochi-sack-server
cd /tmp/ochi-sack-server
/absolute/path/to/ochi/zig-out/bin/ochi
```

From the repository in another terminal, after the server is ready:

TODO: run the server from sack process
TODO: run-dir must be generated from the sack process, no flag required

```sh
curl --fail http://127.0.0.1:9014/ingest/loki/ready
zig build sack -- --url http://127.0.0.1:9014 --run-dir /tmp/sack-run-001
# Or: ./zig-out/bin/sack --url http://127.0.0.1:9014 --run-dir /tmp/sack-run-001
```

Both flags are required. `--url` must be a plain HTTP. `--run-dir` must
not exist; its parent must already exist. Sack exclusively creates and owns that
directory, never reuses it, and preserves artifacts on success and failure.
Stop the server yourself with SIGTERM or Ctrl-C. Use fresh server storage and a
new run directory for another smoke run.

## Artifacts

- `ingest.request.json`, `query.request.json`: exact request bodies.
- `expected.json`: complete expected query response.
- `ingest.response.json`, `query.response.json`: received response bodies.
- `ingest.status.txt`, `query.status.txt`: numeric HTTP statuses.

Both requests and expectations are saved before ingestion. A rejected ingest
prevents any query. Artifacts are diagnostics, not yet a replay/fixture format;
old timestamps may fall outside Ochi's default 30-day retention.

## Tests

```sh
zig build test-sack
```
