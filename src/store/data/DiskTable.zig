const std = @import("std");
const Allocator = std.mem.Allocator;

const TableHeader = @import("../inmem/TableHeader.zig");

pub const DiskTable = @This();

tableHeader: TableHeader,

indexFile: std.Io.File,
columnsHeaderIndexFile: std.Io.File,
columnsHeaderFile: std.Io.File,
timestampsFile: std.Io.File,

messageBloomTokensFile: std.Io.File,
messageBloomValuesFile: std.Io.File,
bloomTokensFiles: []std.Io.File,
bloomValuesFiles: []std.Io.File,

pub fn deinit(self: *DiskTable, io: std.Io, allocator: Allocator) void {
    self.indexFile.close(io);
    self.columnsHeaderIndexFile.close(io);
    self.columnsHeaderFile.close(io);
    self.timestampsFile.close(io);

    self.messageBloomTokensFile.close(io);
    self.messageBloomValuesFile.close(io);
    for (self.bloomTokensFiles) |file| file.close(io);
    for (self.bloomValuesFiles) |file| file.close(io);

    allocator.free(self.bloomTokensFiles);
    allocator.free(self.bloomValuesFiles);
    allocator.destroy(self);
}
