### Quality plan.

We start with tests that catch the most regressions per engineer hour.
Add higher cost layers only after lower layers are stable and fast.

All the tests must be able to run locally by engineers, with clear instructions and minimal setup.

### Gates and Quality Bar
- Pre push git hooks: Tier 0, 1.
- PRs: Tier 0, 1, 2, 3
- Nightly: Tier 0, 1, 2, 3, 4
- Dedicated machine runs: Tier 5, 6 (property/fuzz, chaos)
- Release candidate: Tier 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

#### Tier 0: Static Checks
Support code style and dangerous statements for the codebase.
Includes basic static analysis: fmt, lint, license checks, etc.
TBD:
- [ ] linter
- [x] formatter
- [ ] license checks
- [ ] custom style constrains (print, Self, constCast, undefined, large files in git, TODO comments, etc.)

#### Tier 1: Unit Tests
Validate core logic in isolation.
Deterministic, no sleeps, no network, no shared global state.
TBD:
- [ ] Get rid of background jobs in unit tests (store, recorders)
- [ ] coverage reporting (only local, for understanding of the engineers how high the unit test quality)
- [ ] Redesign configuration to fit the unit testing
- [ ] Cover missing components

#### Tier 2: e2e tests
Validate user workflows and correctness over realistic operations,
e.g. insert/query sequences, idempotent retries, pagination consistency, snapshot/restore.
TBD:
- [ ] scenarios are defined: different querie, inserts
- [ ] multitenancy tests
- [ ] pagination tests
- [ ] tagging filtering
- [ ] fields filtering
- [ ] error codes
- [ ] side effects: observability signals, metrics, logs

#### Tier 3: Integration Tests
Validate component interactions using real process boundaries,
e.g. replication - storage, auth - permissions, new nodes introduction (scaling.
TBD:
- [ ] replication of the app
- [ ] new nodes introduction, scaling scenario
- [ ] auth proxy
- [ ] ingestors integration

#### Tier 4: installation tests
Prove fresh install works.
Run installation script in clean VM/container, start DB, write basic dataset, read it back, restart process, verify persistence.
TBD:
- [ ] installer
- [ ] app image
- [ ] test harness for installation tests

#### Tier 5: Property-Based + Fuzz Testing
Discover edge cases.
TBD:
- [ ] determined seeds
- [ ] crash repro output
- [ ] property tests on operation sequences
- [ ] fuzz tests

#### Tier 6: Fault Injection & Chaos (process/network/storage faults)
Validate safety + liveness under failures.
Key assertions - committed data remains correct, bounded recovery time.
TBD:
- [ ] fault injection hooks
- [ ] IO, disk writes, network, clock skew fault scenarios
- [ ] disk full
- [ ] fsync error scenarios
- [ ] OOM
- [ ] node crash/restart
- [ ] packet loss/reorder

#### Tier 7: Version Compatibility
Prove mixed-version clusters and clients work safely.
TBD:
- [ ] server skew tests
- [ ] client skew tests

#### Tier 8: Upgrade/Downgrade & Recovery Tests
Ensure real upgrade paths are safe.
- Scenarios: in-place upgrade, rolling upgrade, failed halfway upgrade, rollback, snapshot compatibility, schema/data migration.
- Preparation: migration tooling, backup/restore automation, old-version fixtures.
Assertions: no data loss, no prolonged unavailability, same query results before/after.
TBD:
- [] ugprade downgrade tooling
- [ ] upgrade/downgrade scenarios
- [ ] version upgrades to a single node, no data loss, no unavailability
- [ ] same query results

#### Tier 9: Deterministic Simulation Testing (DST)
TBD:
- [ ] deterministic abstractions for clock/network/disk
- [ ] event scheduler
- [ ] seeded PRNG everywhere
- [ ] replay tooling by seed + commit hash

#### Tier 10: Performance
Prove stability under sustained load.
Includes: throughput/latency benchmarks, soak tests (24h+), memory/FD leak checks, compaction pressure
- TBD:
- [ ] soak test harness (e.g. k6 configuration or something)
- [ ] perf lab setup
- [ ] benchmarks for key workloads
- [ ] leak detection tooling
- [ ] key scenarios performance targets, assert against degradation (mem/cpu usage, latency, throughput)

