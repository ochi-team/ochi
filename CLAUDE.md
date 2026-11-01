# Agent Guidelines for Ochi

## Logging database server
Default port is 9014

## Build/Test Commands
- **Build**: `zig build`
- **Run**: `zig build run`
- **Test all**: `zig build test`
- **Test single**: `zig build test -Dtest-filter="SIGTERM"` (use substring from test name)

## Code Style
- **Naming**: PascalCase for types/structs, camelCase for variables/fields and functions, but if a function returns a type - PascalCase
- **Comments**: TODO comments for future work, doc comments (///) for public API

## Imports

Never use in-place imports:

```zig
// BAD
var sink: @import("../store/query/Sink.zig") = .{};
defer sink.deinit(a);

// GOOD
const Sink = @import("../store/query/Sink.zig");
...
var sink: Sink = .{};
defer sink.deinit(a);
```

### Test

Good test has multiple cases
```zig
// GOOD
test "bitsetIsEmpty" {
    const f = struct {
        fn f(alloc: Allocator, len: usize, set: []const usize, expectedEmpty: bool) !void {
            var bitset: std.bit_set.DynamicBitSetUnmanaged = try .initEmpty(alloc, len);
            defer bitset.deinit(alloc);

            try testing.expectEqual(expectedEmpty, BlockQuery.bitsetIsEmpty(&bitset));
        }
    }.f;

    const alloc = testing.allocator;
    try f(alloc, 0, &.{}, true);
    try f(alloc, 1, &.{}, true);
}
// BAD
const Case = struct { key: []const u8, value: []const u8, matches: bool };
const cases = [_]Case{
    .{ .key = "a", .value = "a-value-0", .matches = true },
    .{ .key = "b", .value = "b-value-4", .matches = true },
};
for (cases) |case| {
    ...
    defer res.deinit(alloc);
    try testing.expectEqual(case.matches, res.hits.count() > 0);
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

Assertions infer types implicitly:
```zig
// GOOD
try testing.expectEqual(1, calls);
// BAD
try testing.expectEqual(@as(usize, 1), calls);
```

## Memory management

### ArrayList

Make sure first it has enough space:
```zig
// GOOD
try self.garbage.ensureUnusedCapacity(alloc, 1);
...
self.garbage.appendAssumeCapacity(buf);
// BAD
try self.garbage.append(alloc, buf);
```

### Allocator.create

If an object is destroyed in the same scope there is no benefit to create it via allocator:
```zig
// GOOD
fn init() Object {
    return .{
        ...
    };
}

const obj = Object.init();
// BAD
fn init(alloc: Allocator) !Object {
    const self = try alloc.create(SkewedMergeFixture);
    errdefer alloc.destroy(self);
    ...
    return self;
}

var obj = try Object.init(alloc);
defer obj.deinit(alloc); // this is the reason
```

### Docs

Use zigdoc to validate the API of the used Zig version, e.g.:
- zigdoc std.ArrayList
- zigdoc std.mem.Allocator
- zigdoc std.http.Server

