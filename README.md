<p align=center>
  <img src="logo.png" />
</p>

Ochi is a cost-effective, Loki compatible database for logs.

#### Why?

The market needs an alternatives that is more efficient in compute to store logs

### Roadmap

* 0.1
- [x] store is able to persist the data, simple query API is functionaning
- [x] grafana datasource available
- [ ] home made query language
- [ ] Logging for Logging, Ochi starts emitting logs to itself in order to let the team dog food Ochi
- [ ] support Loki ingestion protocol (snappy, zstd, protobuf encoding, etc.)
- [ ] installation script
- [ ] compose file with configured datasource to grafana
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

## Contributors

Ochi is being developed by a small team and we don't open the public road besides the one above, because too many things are uncertain.

Therefore it's important to open an issue in advance to request a feature.

Bugs can be fixed without an issue, but you are welcome file one.

Optimization improvements are welcome, but must be accompanied by a benchmark that shows the improvement.

### AI Policy

TLDR No intelligence involved is better than LLM.

If you feel github is too long to communicate you can address us in [Discord](https://discord.gg/AsCKpCNp5c).

1. We don't shame lack of knowledge, better to accept not knowing something than do disinformation due to hallucination.
2. Using LLMs you put your trust profile as a bet, we don't encourage, but you may.
3. Complete slop will be rejected with a very short comment of explanation and ban.
4. We can invest our time to help you with understanding. You can rely on LLM to learn, but don't rely on it to implement a solution, better to confirm it.

### Code style
- a good inner function comment describes why, not what
- a good outer function comment (doc string) describes what
- explicit is better than implicit
- therefore default options are not the best, fulfill them all is better
- tests must either cover data or effects, not both
- to produce effect it must take a data to produce a pre determined effect, pure functions are welcome

