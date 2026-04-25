# Query language

This document describes the design to the query language for logs in Ochi.

The language is used in Grafana datasource and other UIs.

It does not belong to any Ochi API, neither HTTP nor anything else.
Ochi keeps transport API separate in order to being able to introduce different transports and have a different versioning.

Query Language and Ochi API will provide the designated versioning mapping and we do our best in order to keep them in sync.

#### Syntax

Sample:
```
[-5m,now] {tag1=alpha} (field=value OR another=*suffix) | fields(another, one_more) | order(time)
```

A query consist of 4 parts:
1. Time range: `[-5m, now]`
2. Stream or tag filter: `{tag1=alpha}`
3. Field filter: `(field=value OR another=*suffix)`
4. Functions: `fields(another, one_more) | order(time)`

### Time range

In the UIs (grafana, CLI, etc.) the time range MIGHT specified separately from the query.
This part usually can my be omitted and can be turned on some UIs into a config flags.
Read the designated UI documentation for more details.

Time range values support the following formats:
- `now` - current time, can be omitted using a short version `[-5m:]`
- `-5m` - 5 minutes ago, a duration format, supports `s`, `m`, `h`, `d` suffixes for seconds, minutes, hours and days respectively.
- `2024-01-01T00:00:00Z` - absolute time in ISO 8601 format.
- `2024-01-01T00:00:00+02:00` - absolute time with timezone offset in RFC 3339 format. `T` is a separator between date and time can be replaced with space for simplicity.

Left hand of the expression MUST be less than the right, so the minimum interval is `[-1m,]`, which matches log entries from 1 minute ago up to the current time, and the maximum interval is `[]`, which matches all log entries up to the infinite future.

The bounds are inclusive, so `[-5m, now]` matches log entries from 5 minutes ago up to and including the current time.

### Stream

Stream filter is used to select log streams based on their tags.
It is specified in curly braces `{}` and consists of one or more tag matchers separated by conditional clause statements.
AND is implicit between tag matchers, so `{tag1=alpha tag2=beta}` is the same as `{tag1=alpha AND tag2=beta}`.

Tags filter may be omitted, in which case all streams are selected.
However, it's highly discouraged to do so, as it may lead to performance issues leading to a selection of a huge amount of data.

TODO: attach a link to a doc to explain what is a stream.

#### Conditional clauses
- `AND` - matches if all conditions are satisfied. For example, `{tag1="alpha" AND tag2="beta"}` matches if both tag1 is "alpha" and tag2 is "beta".
It's implicit between tag matchers, so `{tag1="alpha" tag2="beta"}` is the same as `{tag1="alpha" AND tag2="beta"}`.
- `OR` - matches if at least one of the conditions is satisfied. For example, `{tag1="alpha" OR tag2="beta"}` matches if either.

Apart from `=` operator, the following operators are supported for tag matchers:
- `!=` - matches if the condition is not satisfied. For example, `{tag1="alpha" AND tag2!="beta"}` matches if tag1 is "alpha" and tag2 is not "beta".

`AND` and `OR` are not case sensitive, although we do so to make them more readable.

The conditional clauses can be combined in any way, for example `{tag1="alpha" OR (tag2="beta" AND tag3!="gamma")}` matches if tag1 is "alpha" or tag2 is "beta" and tag3 is not "gamma".

The conditional clauses are applied to the field filters as well.

`AND` executed first, then `OR`, and the parentheses can be used to change the order of evaluation.

### Field filter

Each filter expression has a filter by text or an expression. 
The following expressions are supported:
- wildcard expressions support `*` as a wildcard character, which matches any sequence of characters (including an empty sequence), e.g. `field=val*` matches if field is "value", "valve", "value42", but not "vague".
- `?` matches any single character, e.g. `field=val?e` matches if field is "value" or "valve", but not "valve1".

Apart from `=` and `!=` operators:
- `~` and `!~` - matches if the condition is satisfied or not satisfied respectively, where the value is a regular expression. For example, `star~"^alpha.*"` matches if tag1 starts with "alpha".
- numerical values support comparison operators `>`, `<`, `>=`, `<=`, for example `status>=500` matches if status is greater or equal to 500.

IMPORTANT: regex and wildcards are not supported for tags, tags must be determined and known in advance.

The matching rules are equal to the tags matching rules, but applied to the fields of the log entries.

Surrounding brackets `()` are used to group expressions and define the order of evaluation. For simple expressions it may be omitted, for example `field=value` is the same as `(field=value)`.

#### Line filter

A key field can be omitted, in which case the filter is applied to the `message`, the exact log line main key as it's defined in the collector.
TODO: attach a doc link to explain what is a message.

For example, `=*error*` matches if the message contains "error", and `!~"^debug.*"` matches if the message does not start with "debug".

To apply the filter rule to every field of the log entry, the `*` key can be used, for example `*="*error*"` matches if any field contains "error", and `*!~"^debug.*"` matches if any field does not start with "debug" (which might be rarely useful, but still supported and discouraged for use). TODO: display UI warning for usage `*!` syntax.

### Field functions

Field functions are executed during filtering of the fields, they are scan stage predicates, therefore allow to reduce the amount of data we process and transfer.

Examples:
- `len(x) > 100` - matches if the length of the message is greater than 100 characters, e.g. `len() > 50` matches if the message len is greater than 50 characters (no argument means the main message field).

#### Log pipelines

Log pipelines are functions applied to the end result.

They can:
- add new fields, e.g. `concat(field1, ":", field2)  @new_field` adds a new field `new_field` which is a concatenation of `field1`, `:` and `field2`.
- change the order of the fields, e.g. `order(time)` orders the log entries by time in ascending order, and `order(time desc)` orders the log entries by time in descending order.
- limit the number of log entries, e.g. `limit(100)` limits the result to 100 log entries.
- change the presentation format, e.g. `json` converts the log entries to JSON format.


Example of a complex query:
```
[-5m:] {service=checkout} (
  concat(app.name, order.service.name) @service_name AND
  status=>500 AND
  message~"*timeout*"
)
| count(hits) @count
| order(hits, desc)
| limit(100)
```
