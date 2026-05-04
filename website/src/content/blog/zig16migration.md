---
title: Migrating Ochi to Zig version 0.16.0
date: "2026-05-4"
description: Bringing in improvements from outside
author: "Marko"
---

# Intro

The primary language we are using for building Ochi is relatively young, Zig is still under active pre v1 development. This has it's pros and cons, which affect codebases in some way. During the last 8 months, the Zig programming language team has been extensively working on one of the biggest changes so far, the Io interface. Set to finally release in version 0.16.0, this would also be the biggest change Zig codebases have to conform to.

The ultimate pro of switching to a newer version compiler is most definitely the improvements, that are in a sense downstreamed into your project. The biggest con is the migration itself, which can take long, in the case of Ochi ~10 days.

# The migration process

Having to migrate a ~17k lines of code project is not an easy task. The Zig team has provided very extensive [release notes](https://ziglang.org/download/0.16.0/release-notes.html), which are incredibly useful, and serve as a guide for this process.

This task becomes less hard if the project maintainers closely follow the language development, which in the case of Ochi, we do. We haven't really been shocked by all the new APIs, but have been anticipating them for quite some time.

Anyways, after going trough the release notes, the only thing really left is to simply call the new compiler, which will in return produce lots of errors, obviously failing in the meantime.

The new Io interface is very familiar to those who are aware of the Allocator interface, which is considered of the biggest "features" of the language. In short terms, all functions that allocate memory, need to have the Allocator as an argument:
```zig
pub fn init(allocator: std.mem.Allocator) !*Self {
    const s = try allocator.create(Self);

    s.* = .{};

    return s;
}
```

The truly groundbreaking feature of the language is that from now on, for all input or output related tasks, you need to pass in an Io instance:
```zig
pub fn createDir(io: Io, path: []const u8) void {
    fs.createDirAssert(io, path);
    fs.syncPathAndParentDir(io, path);
}
```
The biggest improvements this brings are code reusability, better platform support, and incredible asynchronous and concurrent programming features.

Back to the migration, this meant that we should just pass an Io instance to a lot of methods, and this incredibly boring task has consumed lots of time. In the end, we have added ~1500 instances of Io in 58 different files. In literal terms this has been done using regex.

Note that this may not have been the most efficient approach, since we could have used a super cool solution - [AST grep](https://ast-grep.github.io/).

Other notable tasks have been just replacing concurrent parts of our code with the new interface. Other than that, most changes have been 1:1, needing little to no effort in implementing.

# The measurable results

Here are the results of the performance improvements the release brings:

## Compilation

| Profile | Version | Real Time | User Time | Sys Time |
| :--- | :--- | :--- | :--- | :--- |
| **Debug** | 0.16 | 0m29.734s | 0m24.564s | 0m3.701s |
| **Debug** | 0.15 | 0m26.694s | 0m35.074s | 0m5.175s |
| **Release Fast** | 0.16 | 1m28.806s | 1m21.891s | 0m4.542s |
| **Release Fast** | 0.15 | 1m26.376s | 1m38.476s | 0m7.672s |

## Tests execution
| Version | Real Time | User Time | Sys Time |
| :--- | :--- | :--- | :--- |
| **0.16** | 0m12.168s | 0m8.742s | 1m1.837s |
| **0.15** | 0m34.038s | 0m19.796s | 0m5.491s |

## Binary size
| Version | App Binary (Release Fast) | App Binary (Debug) |
| :--- | :--- | :--- |
| **0.16** | 17,012,168 | 75,186,765 |
| **0.15** | 14,080,904 | 66,685,304 |


## TLDR:
The compilation speed has slightly decreased, the test execution times are x3 faster, and the binary size has went up a bit.

The test times are incredible, and are expected to fall down even further when we fully leverage all Io interface features.
