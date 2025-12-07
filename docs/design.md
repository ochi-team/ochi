## Design choices

The document describes the made trade-offs and design choices behind Seqor.

### SS Table 

Many databases, including logging and timeseries, use WAL.

WAL is a great way to ensure data durability and integrity, but carries tons of disadvantages:
- complex implementation
- performance overhead
- operational overhead (WAL files management, recovery procedures, etc.)
- increased latency
- increased complexity of recovery 

WAL throughtput is limited by disk IOPS, which makes it a bottleneck for high-throughput systems.
Eventually for HDD it allows 10-100 fsync/s.

Instead of WAL, Seqor uses an approach based on Sorted String Table (SSTable) design.

SSTable is an immutable, sorted key-value map stored in memory.
During next second it flushes to disk as a single file (L0 of LSM tree).

