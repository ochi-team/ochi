### lldb tips

- `frame variable` to print all variables in the current frame
- `bt` to print the backtrace of the current thread
- `thread backtrace all [bt all]` to print the backtrace of all threads

- `breakpoint set --file [-f] <filename> --line [-l] <line>` to set a breakpoint at a specific line in a file
- `breakpoint set --name [-n] <function>` to set a breakpoint at the beginning of a function
- `breakpoint list [br l]` to list all breakpoints

### tracy

##### Install from source on fedora

- install dependencies
```
sudo dnf install \
  gcc-c++ \
  make \
  cmake \
  git \
  xz-devel \
  zlib-ng-compat-devel \
  mesa-libGL-devel mesa-libEGL-devel libglvnd-devel bzip2-devel \
  boost-devel
```
- clone  `git clone https://github.com/wolfpld/tracy.git`
- checkout the client version (validate in build.zig), e.g. `git checkout v0.13.1`
- set cpm cache dir `export CPM_SOURCE_CACHE=~/.cache/cpm`
- prepare build
```
cmake -B build -S profiler -DCMAKE_BUILD_TYPE=Release -DNO_FILESELECTOR=ON -DLEGACY=ON -DGLFW_BUILD_WAYLAND=OFF -DGLFW_BUILD_X11=ON -DCMAKE_CXX_FLAGS="-DTRACY_NO_FILESELECTOR"
```
- build `cmake --build build`

### profile

trace futex/sleep/fsync/epoll/readv/writev

```sh
strace -f -ttT -c -o trace.log ./zig-out/bin/ochi
```

perf sched (threads context switch)

```sh
perf sched record -g -- ./zig-out/bin/ochi
```

perf lock contention

```sh
perf lock record -- ./zig-out/bin/ochi
```

basic performance stats

```sh
perf stat -d -d -d -- ./zig-out/bin/ochi
```

and 

```sh
time -v ./zig-out/bin/ochi
```

cache perf

```sh
sudo perf stat -e cycles,instructions,cache-references,cache-misses,\
branches,branch-misses,dTLB-loads,dTLB-load-misses \
-- ./zig-out/bin/ochi
```

