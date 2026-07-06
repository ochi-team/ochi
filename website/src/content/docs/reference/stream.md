---
title: "Stream"
slug: docs/reference/stream
sidebar:
  order: 3
---

This page covers Stream concept.

### Overview

Stream is a data source identification.

Depending on the defined stream tags Ochi stores the indexed labels `tags`, it allows to speed up a query by skipping the search in the data segments that don't belong to the stream.

### Example

Consider the following topology:
- 2 applications, a HTTP service "api" and a background worker "worker"
- 2 regions, eu-west and ap-east

Eventually we have 4 applications:
- api eu-west
- api ap-east
- worker eu-west
- worker ap-east

If we want query logs only in a specific stream we have to:
1. setup stream on the ingestion client, in Loki API we can do:

```json
{
  "streams": [
    {
      "stream": {
        "service": "api",
        "region": "eu-west"
      },
      "values": [
        ...
      ]
    }
  ]
}

```

Read more in [api](../api).

Then in query we are able to provide tags, same filter but surrounded in curly brackets:

```
{service=api region=eu-west}
```

Read more in [query](../loql).

### Limitations

Although Ochi is a schemaless storage it still has to setup the limits.

One of them is a fields cardinality.

In order to keep the resource usage under control currently the max amount of unique field keys currently is 2048.
Logs producing large amount of field are ignored.

The limit is per stream, it's a sum of all the unique field keys across all the logs in your application.

Pay attention on logging large array fields as `field_0=1 field_1=2`.

Instead consider logging an array of values `fields=[1,2]`.

