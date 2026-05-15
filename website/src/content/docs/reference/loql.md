---
title: "LOQL reference"
slug: docs/reference/loql
sidebar:
  order: 2
---

This page documents the LOQL - Logs Ochi Query Language.

### Overview

A query consist of the following expressions:
- time range, e.g.: `[-15m, now]`
- tags, e.g.: `{tag1=alpha AND tag2=beta}`
- fields, e.g.: `field1=x OR field2=x`


### Time range

Time range defines a time window to query logs from. It is defined as two timestamps in square brackets, separated by a comma. The first timestamp is the start time, and the second timestamp is the end time. Left side of the expression must be lower than the right.

- Timestamps can be specified in the following formats:

###### Timestamps
Is an absolute time in ISO8601 format, e.g. `2024-06-01T00:00:00Z` or even `20240601` is a valid value.

Timezone can be a UTC offset, e.g. `2024-06-01T00:00:00+08:00` or `2024-06-01T00:00:00-08:00`.
Time and date can be separated by a space, e.g. `2024-06-01 00:00:00`.

###### Relative time

Is a time relative to the current time, e.g. `-15m` means 15 minutes ago, the following units are supported:
- `s` for seconds
- `m` for minutes
- `h` for hours
- `d` for days

The positive values are supported as well, they don't require a sign, e.g. `15m` means 15 minutes in the future.

###### Now

Is a special value that represents the current time, e.g. `now` means the current time.

### Tags

Tags are key-value pairs that are associated with log lines. 
They define a stream.
Your collect specifies them during the ingestion time.
They are defined in curly braces, e.g. `{tag1=alpha AND tag2=beta}`. The expression inside the curly braces is a boolean expression that can contain the following:

- `AND` for logical AND
- `OR` for logical OR
- `()` for grouping

Eventually the query might be more complex, e.g. `{(tag1=alpha AND tag2=beta) OR (tag3=gamma AND tag4=delta)}`.

For comparison of the tag value, only exact match is supported, e.g. `tag1=alpha` means that the value of `tag1` must be exactly `alpha`.

### Fields

Fields are key-value pairs of the log lines.
The values you emit from the application level appear here by default (review your collector configuration).

Tags expression is a subset of the Fields expression, but fields are more flexible and support more operations.

Expressions are defined by the following:
- `AND` for logical AND
- `OR` for logical OR
- `()` for grouping

The following key value operators are possible:
- `=` for exact match, e.g. `field1=x` means that the value of `field1` must be exactly `x`.
- `!=` for not equal, e.g. `field1!=x` means that

