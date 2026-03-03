# Agent Guidelines for Ochi

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
var a: u16 = 0xabcd; // runtime-known
_ = &a;
const b: u8 = @intCast(a);
_ = b;
```

std.ArrayList:

```zig
var list = try ArrayList(i8).initCapacity(a, 200); // OR ArrayList(i8).empty
defer list.deinit(a);
```

Use slices indexing and length properties instead of pointer arithmetic:

```zig
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
```

### Test

Good test has multiple cases
```zig
test "readNames" {
    const Case = struct {
        content: []const u8,
        expected: []const []const u8,
        expectedErr: ?anyerror = null,
    };

    const alloc = testing.allocator;
    const cases = [_]Case{
        .{
            .content = "[\"table-a\",\"table-b\"]",
            .expected = &.{ "table-a", "table-b" },
        },
        .{
            .content = "not-json",
            .expected = &.{},
            .expectedErr = error.SyntaxError,
        },
    };

    for (cases) |case| {
        // details here
    }
}
```

And has proper assertions comparing all the fields at once:

```zig
// GOOD
const h = Self.decode(encodeBuf[0..offset]);
try std.testing.expectEqualDeep(case.header, h);
// BAD
const h = Self.decode(encodeBuf[0..offset]);
try std.testing.expectEqual(case.header.sid, h.sid);
try std.testing.expectEqual(case.header.minTs, h.minTs);
try std.testing.expectEqual(case.header.maxTs, h.maxTs);
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


