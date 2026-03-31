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

### tiny package movements
- extract structs from store/inmem/block_header.zig
- move data.zig to data/Data.zig
- extract components from BlockData
- separate data and data/MemTable packages
- separate index and index/Memtable packages

