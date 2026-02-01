<p align=center>
  <img src="logo.png" />
</p>

Seqor is a cost-effective, Loki compatible database for logs.

### Build
The build will automatically resolve dependencies listed in `build.zig.zon`.
```bash
zig build
```
This produces the `Seqor` executable in `zig-out/bin/`.

### Run
By default Seqor looks for `seqor.yaml` in the current directory.
```bash
zig build run
```
Specify a custom config file:

TODO: For some reason this doesn't work.
```bash
zig build run -- -c ./my-seqor.yaml
```

### Configuration
Example `seqor.yaml`:
```yaml
server:
  port: 9012
app:
  maxRequestSize: 4194304 # 4MB
```

## Dependency Resources
###### Where to look for zig dependencies
1. https://zigistry.dev/
2. https://ziglist.org/


Nice to have:
- Design landing page
- Drop a couple blog posts in there on:
    1. why static allocation is not the best 
    2. write logs effectively

### tiny package movements
- extract structs from store/inmem/block_header.zig
- move data.zig to data/Data.zig
- separate data and data/MemTable packages
- separate index and index/Memtable packages

###  tests todos

##### index

- index
- index table
- writer
- mem block
- mem table
- meta index
- table header
- meta index record

##### data

- block writer
- columns header index
- stream writer
- table header
- mem table
- data

