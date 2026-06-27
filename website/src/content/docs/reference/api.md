---
title: "API reference"
slug: docs/reference/api
sidebar:
  order: 1
---

This page documents the HTTP API available in Ochi `0.2`.

### Base URL

- Local default: `http://127.0.0.1:9014`

### Authentication / tenancy

- Multi-tenant routing uses `X-Scope-OrgID` header.
- If the header is not provided, Ochi uses tenant `default`.

### `GET /ingest/loki/ready`

Readiness probe endpoint.

Used primarily by Grafana to confirm the datasource availability.

#### Request

- Body: none
- Headers: none required

#### Response

- `200 OK`
- Content-Type: text/plain
- Body: `ready`

### `POST /ingest/loki/api/v1/push`

Ingest logs using Loki JSON push format.

Read [Loki ingestion API](https://grafana.com/docs/loki/latest/reference/loki-http-api/) for more details.

#### Request headers

- `Content-Type: application/json` (required when header is set), only json encoding is supported for payloads
- `Content-Encoding: snappy`, only `snappy` is currently supported for compressed payloads.

#### Request body

```json
{
  "streams": [
    {
      "stream": {
        "tag1": "alpha",
        "tag2": "beta"
      },
      "values": [
        [
          "1715173665000000000", // timestamp in nanoseconds as a string, in a query becomes `_time`
          "same message", // log message, in a query becomes `_msg`
          {
            "field1": "x",
            "field2": "x"
          }
        ]
      ]
    }
  ]
}
```

Per log line in `values`:

- element `0`: timestamp in nanoseconds as a string
- element `1`: log message as a string
- element `2` (optional): structured metadata object (`key -> string value`)
The JSON object must be a valid JSON object with string keys and string values. The JSON object should not contain any nested object.

#### Response

- `200 OK` on successful ingestion
- Empty body

### `POST /query`

Query logs by time range plus exact-match tags and fields.

#### Query Language API

Requires a specified header:
- `Content-Type: application/loql`

#### Request body

```loql
[-15m, now] {tag1=alpha AND tag2=beta} field1=x OR field2=x
```

Full language reference: [LOQL](../loql)

#### Response

`200 OK` with JSON array of matching lines:

```json
{
    "line": [
        {
            "_time": 1715173665000000000,
            "field_1": "value 1"
        },
        {
            "_time": 1715173665000000000,
            "field_1": "value 2"
        }
    ]
}
```

where:
- `_time` - timestamp key in nanoseconds

- The log message is returned as a field with empty key (`"key": ""`).
- Query filters are exact matches.

### `POST /stream_ids`

Fetch all the available stream ids.

#### Request body

```json
{
    "since": "2h"
    "fromNs": 1780225324,
    "toNs": 1780225600,
    "from": "2025-12-19T16:39:57",
    "to": "2025-12-19T16:59:57",
}
```

The body needs to contain one of the following group of fields, the priorities are kept accordingly:
- since - duration since what
- fromNs/toNs - timestamps in nanoseconds to define the time range
- from/to - timestamps in ISO8601 format to define the time range

#### Response

`200 OK` with JSON of matching stream ids:

```json
{
    "streamIDs": [1715173665000000, 1715173665000001],
}
```

