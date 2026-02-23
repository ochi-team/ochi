<p align=center>
  <img src="logo.png" />
</p>

Ochi is a cost-effective, Loki compatible database for logs.

### Build
The build will automatically resolve dependencies listed in `build.zig.zon`.
```bash
zig build
```
This produces the `Ochi` executable in `zig-out/bin/`.

### Run
By default Ochi looks for `ochi.yaml` in the current directory.
```bash
zig build run
```
Specify a custom config file:

TODO: For some reason this doesn't work.
```bash
zig build run -- -c ./my-ochi.yaml
```

### Configuration
Example `ochi.yaml`:
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
    3. key value store for a logging database
    4. storing index for logs
    5. object storage complexity

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

