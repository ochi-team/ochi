# Memtable Design Documentation

## Overview

The memtable is an in-memory columnar storage. It organizes log lines by Stream ID (SID), stores them in a columnar format with type detection and compression, and builds indexes.

## Core Data Model

```

## Architecture Diagram

```
                                 +-------------+
                                 |  MemTable   |
                                 +------+------+
                                        |
                        addLines([]*Line)
                                        |
                                        v
                    +-----------------------------------+
                    | 1. Sort by SID + Timestamp        |
                    | 2. Sort fields within each line   |
                    | 3. Split into 2MB blocks per SID  |
                    +-----------------+-----------------+
                                      |
                                      v
                              +--------------+
                              | BlockWriter  |
                              +------+-------+
                                     |
            +------------------------+------------------------+
            |                        |                        |
            v       ------->         v      ------->          v
    +--------------+        +--------------+        +--------------+
    |    Block     |        | BlockHeader  |        |StreamWriter  |
    | (Columnar)   |        |  (Metadata)  |        |  (Storage)   |
    +--------------+        +--------------+        +--------------+
```

## Component Details

### 1. MemTable (memtable.zig)

**Purpose**: Top-level coordinator for ingesting log lines

**Process**:
1. Receives array of log lines
2. Sorts lines by (SID, timestamp) using `lineLessThan`
3. Sorts fields within each line alphabetically
4. Groups lines by SID and splits into 2MB blocks
5. Delegates to BlockWriter for persistence

```
Input: []*Line (unsorted, mixed SIDs)
  |
  v
Sort: lineLessThan(SID, timestamp)
  |
  v
Group by SID + split at 2MB boundaries
  |
  v
Output: Multiple blocks per SID -> BlockWriter
```

### 2. Block (block.zig)

**Purpose**: Convert row-oriented lines into columnar storage

**Transformation**:
```
Row format (Lines):                Columnar format (Block):
+---------------------+           +----------------------+
| Line 1:             |           | timestamps: [t1,t2,t3]
|   ts: t1            |           +----------------------+
|   level: "info"     |           | Columns:             |
|   msg: "start"      |    --->   |   level: ["info",    |
+---------------------+           |           "info",    |
| Line 2:             |           |           "info"]    |
|   ts: t2            |           |   msg: ["start",     |
|   level: "info"     |           |         "error",     |
|   msg: "error"      |           |         "done"]      |
+---------------------+           +----------------------+
| Line 3:             |           | Celled Columns:      |
|   ts: t3            |           |   level: ["info"]    |
|   level: "info"     |           |   (repeated 3x)      |
|   msg: "done"       |           +----------------------+
+---------------------+
```

**Column Separation**:
- **Celled Columns**: All values identical, store once (max 256 bytes/value)
- **Regular Columns**: Variable values, store all

### 3. Encoding System (encode.zig)

**Purpose**: Type detection and efficient encoding

#### ValuesEncoder Strategy

For each column, try encodings in order:

```
1. Dict (< 8 unique values, each < 256 bytes)
   |-> Store: value -> u8 index

2. Unsigned Integer (parseable as u64)
   |-> Detect width: uint8/uint16/uint32/uint64

3. Signed Integer (parseable as i64)
   |-> Store as: int64

4. Float (parseable as f64)
   |-> Store as: float64

5. IPv4 (format: "x.x.x.x")
   |-> Store as: u32

6. ISO8601 Timestamp
   |-> Store as: i64 nanoseconds

7. String (fallback)
   |-> Store as: raw bytes
```

#### Value Packing

After type encoding, values are packed with compression:

```
packValues([values]) -> encoded bytes
  |
  +-> Lengths: Detect cell optimization
  |   +- All same? -> [type:u8, value:1-8 bytes]
  |   +- Variable? -> [type:u8, len1, len2, len3, ...]
  |       |-> Compress with zstd if >= 128 bytes
  |
  +-> Values: Concatenate raw bytes
      |-> Compress with zstd if >= 128 bytes

Final format:
  [compressed lengths][compressed values]
```

**Compression Header**:
```
Plain (< 128 bytes):
  [kind:0x00][len:u8][data...]

Zstd (>= 128 bytes):
  [kind:0x01][len:leb128][compressed_data...]
```

### 4. Block Headers (block_header.zig)

**BlockHeader** - Per-block metadata (76 bytes):
```
+---------------------------------+
| SID (32 bytes)                  |  tenantID (16) + id (16)
+---------------------------------+
| size: u64 (8 bytes)             |  Estimated JSON size
+---------------------------------+
| len: u32 (4 bytes)              |  Number of log lines
+---------------------------------+
| TimestampsHeader (32 bytes)     |
|   - offset: u64                 |  Position in buffer
|   - size: u64                   |  Byte length
|   - min: u64                    |  Earliest timestamp
|   - max: u64                    |  Latest timestamp
+---------------------------------+
```

**ColumnHeader** - Per-column metadata:
```
+---------------------------------+
| key: []u8                       |  Column name
+---------------------------------+
| type: ColumnType (enum)         |  Detected type
+---------------------------------+
| min/max: u64                    |  Value range
+---------------------------------+
| offset: usize                   |  Position in bloom buffer
+---------------------------------+
| size: usize                     |  Encoded data length
+---------------------------------+
| dict: ColumnDict                |  Dictionary values (if dict type)
+---------------------------------+
```

**BlockHeader Creation Flow** (block_writer.zig:83, block_header.zig:16):
```
Block (in memory)
  |
  v
BlockHeader.init(block, sid)
  |-> Extracts: block.size(), block.len()
  |-> Creates empty TimestampsHeader (all zeros)
  |
  v
StreamWriter.writeBlock(block, &blockHeader)
  |-> Writes timestamps to buffer
  |-> Populates: timestampsHeader.offset, size, min, max
  |-> Encodes columns
  |
  v
BlockHeader (complete)
  |-> Used by BlockWriter for index
```

### 5. BlockWriter (block_writer.zig)

**Purpose**: Batch block metadata into index blocks

**Index Block Structure**:
```
+--------------------------------------+
| Index Block (16KB buffer)            |
| +----------------------------------+ |
| | BlockHeader 1 (76 bytes)         | |
| +----------------------------------+ |
| | BlockHeader 2 (76 bytes)         | |
| +----------------------------------+ |
| | BlockHeader 3 (76 bytes)         | |
| +----------------------------------+ |
|                                      |
| Flush at 128KB threshold             |
+--------------------------------------+
         |
         v
+--------------------------------------+
| Meta Index (4KB buffer)              |
| +----------------------------------+ |
| | IndexBlockHeader 1               | |
| +----------------------------------+ |
| | IndexBlockHeader 2               | |
| +----------------------------------+ |
+--------------------------------------+
```

**State Management**:
- Tracks per-SID statistics (min/max timestamp)
- Tracks global statistics across all blocks
- Flushes index block when approaching capacity

### 6. StreamWriter (stream_writer.zig)

**Purpose**: Write block data to storage buffers

**Storage Buffers**:
```
+------------------------------------+
| timestampsBuffer                   |  All timestamps
| [t1, t2, t3, ...] (debug format)   |
+------------------------------------+

+------------------------------------+
| messageBloomValuesBuf              |  Message field values
| [packed column data]               |
+------------------------------------+

+------------------------------------+
| bloomValuesList[colID]             |  Per-column values
| [packed column data for col0]      |
| [packed column data for col1]      |
| ...                                |
+------------------------------------+

+------------------------------------+
| indexBuffer                        |  Block indexes
+------------------------------------+

+------------------------------------+
| metaIndexBuf                       |  Index of indexes
+------------------------------------+
```

**Column ID Generation**:
- Assigns unique u64 ID to each column name
- Maps column ID to bloom buffer index
- Supports modulo wrapping for bounded memory

**Tokenization** (for full-text search):
- Extracts tokens from ASCII text: `[a-zA-Z0-9_]+`
- Hashes tokens using XxHash64
- Deduplicates using bitset + overflow buckets
- Prepares data for bloom filter construction

### 7. Decoding (decode.zig, unpack.zig)

**ValuesDecoder**: Reverse transformation from encoded bytes to strings

```
Encoded bytes -> decode(ColumnType) -> String representation
```

**Type-specific decoders**:
- `uint8/16/32/64` -> ASCII number string
- `int64` -> ASCII signed number
- `float64` -> ASCII float
- `ipv4` -> "x.x.x.x" format
- `timestampIso8601` -> ISO8601 string
- `dict` -> Lookup in dictionary array
- `string` -> Direct bytes

**Unpacker**: Decompress and unpack values

```
Packed bytes:
  |
  v
1. Read compression header
  |
  v
2. Decompress if zstd (or copy if plain)
  |
  v
3. Read value type header
  |
  v
4. Unpack based on type:
   - Cell type: Replicate single value N times
   - Block type: Read N values of fixed width
  |
  v
Output: []u64 or [][]u8
```

## Data Flow Example

```
Input:
  Line 1: ts=100, sid=A, level="info", msg="start"
  Line 2: ts=200, sid=B, level="info", msg="alert"
  Line 3: ts=150, sid=A, level="warn", msg="retry"

MemTable.addLines():
  Sort by (SID, timestamp):
    Line 1: ts=100, sid=A, level="info", msg="start"
    Line 3: ts=150, sid=A, level="warn", msg="retry"
    Line 2: ts=200, sid=B, level="info", msg="alert"

  Split by SID:
    Block A: [Line 1, Line 3]
    Block B: [Line 2]

Block A (columnar):
  timestamps: [100, 150]
  columns:
    level:  ["info", "warn"]  -> Dict encoding: [0, 1], dict=["info","warn"]
    msg:    ["start", "retry"] -> String encoding

BlockWriter.writeBlock(Block A):
  BlockHeader A: sid=A, len=2, min_ts=100, max_ts=150

  StreamWriter.writeBlock():
    timestampsBuffer += "{ 100, 150 }"

    For column "level":
      ValuesEncoder.encode() -> Dict type, min=0, max=1
      ValuesEncoder.packValues([0, 1]) -> compressed bytes
      bloomValuesBuf["level"] += packed bytes

    For column "msg":
      ValuesEncoder.encode() -> String type
      ValuesEncoder.packValues(["start", "retry"]) -> compressed bytes
      bloomValuesBuf["msg"] += packed bytes

  indexBlockBuf += BlockHeader A (76 bytes)

BlockWriter.finish():
  Flush index block -> StreamWriter.indexBuffer
  IndexBlockHeader -> metaIndexBuf
```


