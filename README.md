
<p align=center>
  <img alt="GitHub Tag" src="https://img.shields.io/github/v/tag/ochi-team/ochi">
  <img alt="GitHub License" src="https://img.shields.io/github/license/ochi-team/ochi">
  <img alt="GitHub top language" src="https://img.shields.io/github/languages/top/ochi-team/ochi">
</p>

<h3 align="center">Ochi is a cost-effective, Loki compatible, database for logs.</h3>

<p align="center">
  <img src="logo.png" alt="Ochi Logo" />
</p>

<p align="center">
  <sub><i>Ochi (pronounced oh-chee) translates to "Eyes" in various Slavic languages.</i></sub>
</p>

## The reasoning

The market is in desperate need of a more compute-efficient alternative to storing logs. In a world where computing is getting more and more expensive, everyone is on the lookout for cheaper-to-run solutions.

## Roadmap

* 0.2
- [x] store persists the data, working simple API queries
- [x] Grafana datasource available
- [ ] home made query language
- [ ] Ochi starts emitting logs to itself, [dogfooding](https://en.wikipedia.org/wiki/Eating_your_own_dog_food) Ochi
- [ ] support Loki ingestion protocol (snappy, zstd, protobuf encoding, etc.)
- [ ] install script
- [ ] compose file with configured datasource to grafana
- [ ] yaml configuration support

## Goals
- support majority ingestion protocols (loki, fluentd, syslog, etc)
- home made UI
- cost analysis built in
- GDPR compliance
- support ARM64 and x86_64 as Tier 1
- support POSIX systems, but Linux remains Tier 1
- better core over features
- documentation to answer 99% of questions

## Non goals
- support every OS
- "feature completeness"

## Contributors

Ochi is being actively developed by a small team of engineers.

Features are requested by opening an issue, though currently there is a very small chance they will be taken into consideration, due to the ongoing development of the project.

Bugs can be fixed without opening an issue, but you are welcome file one.

Optimization improvements are welcome, but must be accompanied by a benchmark.

Before submitting a PR, make sure the tests pass:
```bash
zig build test
```

For all questions, major changes, suggestions, notes, feel free to reach out to the team on [Discord](https://discord.gg/AsCKpCNp5c).

## AI Policy

1. We don't shame the lack of knowledge, it's better to accept not knowing, than spreading disinformation.
2. Using LLMs you put your trust profile on a line, we don't encourage it, but you may.
3. Complete slop will be rejected with a very short comment and a permanent ban.
4. We can invest our time helping you learn. Don't fully rely on LLMs to implement a solution.

## Code style
- a good inner function comment describes why, not what
- a good outer function comment (doc string) describes what
- explicit is better than implicit
- therefore default options are not the best
- tests must either cover data or effects, not both
- to produce effect it must take a data to produce a pre determined effect
- pure functions are welcome

## License

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
