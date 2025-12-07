## Goal

The eventual goal of this project is to create a logging database.

A database is no joke and contains many modules (future work items, list is not complete):
- ingestors: write path handlers to deliver logs to the storage
- logs processors: data preparation (parsing, groupping by a stream, etc.)
- data storage: API to the data itself, holds behined data, indexing, etc.
  * memtable: memory resident table to hold recent logs, prepared data to be flushed to disk
- read handlers: query path handlers to retrieve logs from the storage

## Roadmap 

We keep the roadmap as short as possible in order make the furure development clear, predictable and achievable.
Short list gives us easy commitment points to the community and ourselves.

#### Step 1: memtable

First component we implement is a memtable.
It gives a core foundation and starting point for the first integration and feedback loop.

#### Step 2: Loki http ingestor (snappy compression)

Second component is an ingestor for Loki http protocol.
It gives us a way to push logs into the system and test the memtable.

#### Step 3: Structured logging and grafana loki setup

The idea is to start experiencing usage of Seqor through Loki dashboard in order to validate the design and implementation and use Loki more often.
