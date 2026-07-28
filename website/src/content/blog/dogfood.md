---
title: Ochi is the first user of Ochi
date: "2026-07-29"
description: "how it dogfoods itself"
author: "Denis"
---

### How did it start

I started Ochi because the current state of the logs is not OK.

The search experience, ingestion cost, everything is broken.

So the basic need is to give me 2 things:
- find whether one is able to ingest logs more efficient
- come up with a decent UX to find the necessary logs

While it's on the way I may start using the solution in order to understand better what I build and become my first users.

### Prepare your logs

To scaffold the point I want to log during development I scattered events I want to log.

It was dumb and simple:
```zig
std.debug.log("something happened event={s}", .{event});
```

It's even a structured log in a logfmt format.

Although, I gotta make a basic cleaning to bring a real logger.
It gives me a log level control, sampling baased on the level, adjusted buffered output, etc.

So I could have api like:
```zig
Logger.log(.err, "failed to run something", .{ .err = err });
```

### How does it use it

First idea I had to run 2 Ochi instances:
- one is a testing target I keep changing and observe the performance/quality diff
- another stores only the Ochi target's logs

It takes more resource to run it and contradicts the essentials - reduce the compute waste.

I could start a collector and the data flow as most users experience.
It's still suboptimal, but this experience matters.

So now sometimes I start ochi as:
```shell
zig build run | vector --config ingestorsconf/vector-loki-fmt-json.yaml
```

It makes stdout sending to vector via shell pipe, 
the prepared config makes it reading logfmt output and send to Ochi ingest JSON API.

The next step is to run webui and I could start reading the events and experiencing the query process.

### Future plans

The basic idea is to see quality and performance events. 
For instance, I could see what buffer size it requires on average for bloom filter tokens.

Using it a couple weeks highlights me a few things it lacks.

- Reading performance events is more convenient in a heatmap or a histogram to see values distribution picture, it requires adjusting UI and adding pipe functions to a query language
- Make it flushing to itself without a collector, so a logger could directly flush the buffers to its internal API instead of running a collector

Glad to hear other opinions.

###### Happy coding

Follow our journey.

[Github](https://github.com/ochi-team/ochi)
[Discord](https://discord.gg/AsCKpCNp5c)

