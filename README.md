
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

## Introduction

We are creating a more efficient alternative for storing logs.

## Install

#### Download pre-built binary

Visit [release page](https://github.com/ochi-team/ochi/releases)

#### Build from source

- install [zig](https://ziglang.org/learn/getting-started/#managers) 0.16.0
- do `zig build -Doptimize=ReleaseSafe`

## Documentation

https://ochi.dev/docs/guides/installation/

## Roadmap

- [x] store persists the data, working simple API queries
- [x] Grafana datasource available
- [ ] home made query language
- [ ] Ochi starts emitting logs to itself, [dogfooding](https://en.wikipedia.org/wiki/Eating_your_own_dog_food) Ochi
- [ ] full support of Loki ingestion protocol (snappy, zstd, protobuf encoding, etc.)
- [ ] installation scripts
- [ ] docker-compose with configured datasource to grafana
- [ ] configuration support

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

## Contributing

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

We encourage to use AI in order to:
1. generate a test that is able to crash the system

## Code style
- a good inner function comment describes why, not what
- a good outer function comment (doc string) describes what
- explicit is better than implicit
- therefore default options are not the best
- tests must either cover data or effects, not both
- to produce effect it must take a data to produce a pre determined effect
- pure functions are welcome
limitations under the License.
