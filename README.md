<p align=center>
  <img src="logo.png" />
</p>

Ochi is a cost-effective, Loki compatible database for logs.

&#8203;

Check out our new [website](https://ochi.dev/) for more infos, what Ochi is.

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

1. [https://zigistry.dev/](https://zigistry.dev/)
2. [https://ziglist.org/](https://ziglist.org/)

Nice to have:

- Design landing page
- Drop a couple blog posts in there on:
   1. why static allocation is not the best
   2. what is the alternative to static allocations?
   3. chunked buffers as an alternative to ArrayList
   4. write logs effectively
   5. key value store for a logging database
   6. storing index for logs
   7. object storage complexity
   8. ARM matters
   9. opencost integration with Grafana
   10. why we must stay opensource forever
### tiny package movements

- extract structs from store/inmem/block_header.zig
- move data.zig to data/Data.zig
- separate data and data/MemTable packages
- separate index and index/Memtable packages
### tests todos

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