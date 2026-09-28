const std = @import("std");

pub const DebugIo = @import("DebugIo.zig");

test "DebugIoReportsLeakedFds" {
    const alloc = std.testing.allocator;

    var buf: [4096]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);

    var debugIo = DebugIo.init(std.testing.io, alloc, &w);
    defer debugIo.deinit();
    const testingIo = debugIo.io();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const file = try tmp.dir.createFile(testingIo, "leaked", .{});
    try std.testing.expectEqual(1, debugIo.openMap.count());
    const err = debugIo.checkNoLeaks();
    try std.testing.expectError(error.FileDescriptorLeak, err);

    file.close(testingIo);
    try debugIo.checkNoLeaks();

    try std.testing.expect(w.buffered().len > 0);
}
