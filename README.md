<p align=center>
  <img src="logo.png" />
</p>

Ochi is a cost-effective, Loki compatible database for logs.

### Code style
- a good inner function comment describes why, not what
- a good outer function comment (doc string) describes what
- explicit is better than implicit, therefore default options are not the best
- tests must either cover data or effects, not both
- to produce effect it must take a data to produce a pre determined effect, pure functions are welcome

### Roadmap

* 0.1
- [x] store is able to persist the data, simple query API is functionaning
- [x] grafana datasource available

* 0.2
- [ ] home made query language

* 0.3
- [ ] support Loki ingestion protocol (snappy, zstd, protobuf encoding, etc.)

* 0.4
- [ ] Logging for Logging, Ochi starts emitting logs to itself in order to let the team dog food Ochi

* 0.5
- [ ] yaml configuration is supported

#### Goals
- support majority ingestion protocols (loki, fluentd, syslog, etc)
- home made UI
- cost analysis built in
- GDPR compliance
- support ARM64 and x86_64 as Tier 1
- support POSIX systems, but Linux remains Tier 1
- better core over features
- documentation must be able to answer 99% questions, if it doesn't - write a doc and share as an answer

#### Non goals
- support every OS
- being "feature complete"

### tiny package movements
- extract structs from store/inmem/block_header.zig
- move data.zig to data/Data.zig
- extract components from BlockData
- separate data and data/MemTable packages
- separate index and index/Memtable packages
- test data and effects separately
- setup coverage reports


TODO: setup spellchecker for docs
TODO: setup changelog in the docs
TODO: extract website to its repo, leave only related content: docs, changelog, api reference, etc.
