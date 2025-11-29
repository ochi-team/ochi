# Agent Guidelines for Seqor

## Build/Test Commands
- **Build**: `zig build`
- **Run**: `zig build run`
- **Test all**: `zig build test`
- **Test single**: `zig build test -Dtest-filter="SIGTERM"` (use substring from test name)

## Code Style
- **Language**: Zig, follows standard Zig conventions
- **Imports**: Group by std library, external deps (datetime, httpz, snappy), then local modules
- **Naming**: PascalCase for types/structs, camelCase for variables/fields and functions, but if a function returns a type - PascalCase
- **Error handling**: Use `try` for error propagation, explicit error types where needed
- **Comments**: TODO comments for future work, doc comments (///) for public API
- **Memory**: Use passed allocator, defer cleanup with deinit/destroy/free

### Current zig version

@intCast, @bitCast take a single argument:

```zig
test "integer cast panic" {
    var a: u16 = 0xabcd; // runtime-known
    _ = &a;
    const b: u8 = @intCast(a);
    _ = b;
}
```

std.ArrayList:

```zig
const std = @import("std");
test "initCapacity" {
    const a = testing.allocator;
    {
        var list = try ArrayList(i8).initCapacity(a, 200);
        defer list.deinit(a);
        try testing.expect(list.items.len == 0);
        try testing.expect(list.capacity >= 200);
    }
}
```

Use slices indexing and length properties instead of pointer arithmetic:

```zig
test "slice indexing" {
    const arr: [5]u8 = [_]u8{1, 2, 3, 4, 5};
    // GOOD
    for (arr) |value, i| {
        try testing.expect(value == arr[index]);
        arr[i] += 1;
    }
    // BAD
    for (arr) |*ptr, index| {
        try testing.expect(*ptr == arr[index]);
        ptr.* += 1;
    }
}
```

Use zigdoc to validate the API of the used Zig version, e.g.:
- zigdoc std.ArrayList
- zigdoc std.mem.Allocator
- zigdoc std.http.Server

## Project Context
- Loki-compatible log database written in Zig
- HTTP server using httpz library
- Main entry: `src/main.zig`, server logic: `src/server.zig`
- Signal handling for graceful shutdown (SIGTERM)


