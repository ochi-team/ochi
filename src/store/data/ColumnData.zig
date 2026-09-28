const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const ColumnHeader = @import("ColumnHeader.zig");
const ColumnType = ColumnHeader.ColumnType;
const ColumnDict = @import("ColumnDict.zig");
const TableReader = @import("TableReader.zig");

const maxValuesBlockSize = @import("BlockData.zig").maxValuesBlockSize;

pub const ColumnData = @This();
key: []const u8,
type: ColumnType,

min: u64,
max: u64,

// writeColumnData uses a pointer to borrow,
// therefore it may require passing  a ColumndData as a pointer,
// it takes it from ColumnHeader,
// it's lifetime coupled to ColumnHeader === BlockReader
// TODO: try making it a value, it stores a single array and used mostly as a value in the headers,
// or the other way around,
dict: *ColumnDict,
// TODO: this holds ownership of merge read, either document it's ownership
// or remove if we migrate ot file read / mmap
bloomValues: []const u8,

// TODO: try making it non optional, default as an empty string
bloomTokens: ?[]const u8,

pub fn readFrom(
    io: Io,
    alloc: Allocator,
    ch: *ColumnHeader,
    tableReader: *const TableReader,
) !ColumnData {
    const valuesSize = ch.size;
    std.debug.assert(valuesSize <= maxValuesBlockSize);

    const valuesData = try alloc.alloc(u8, valuesSize);
    errdefer alloc.free(valuesData);
    const valuesN = try tableReader.readBloomValues(io, valuesData, ch.key, ch.offset);
    std.debug.assert(valuesN == valuesData.len);

    var tokensData: ?[]const u8 = null;
    if (ch.type != .dict) {
        const tokensBuf = try alloc.alloc(u8, ch.bloomFilterSize);
        errdefer alloc.free(tokensBuf);
        const tokensN = try tableReader.readBloomTokens(
            io,
            tokensBuf,
            ch.key,
            ch.bloomFilterOffset,
        );
        std.debug.assert(tokensN == tokensBuf.len);
        tokensData = tokensBuf;
    }

    return .{
        .key = ch.key,
        .type = ch.type,

        .min = ch.min,
        .max = ch.max,

        .dict = &ch.dict,
        .bloomValues = valuesData,

        .bloomTokens = tokensData,
    };
}

pub fn deinit(self: *ColumnData, alloc: Allocator) void {
    if (self.bloomValues.len > 0) {
        alloc.free(self.bloomValues);
    }
    if (self.bloomTokens) |bloomTokens| {
        if (bloomTokens.len > 0) {
            alloc.free(bloomTokens);
        }
    }
    self.* = undefined;
}

/// copies the column.
/// ownedDictValues collects the duped dict value buffers so the caller can
/// use them after BlockReader free
pub fn copy(self: *const ColumnData, alloc: Allocator, column: *ColumnData) !void {
    const key = try alloc.dupe(u8, self.key);
    errdefer alloc.free(key);

    const bloomValues = try alloc.alloc(u8, self.bloomValues.len);
    errdefer alloc.free(bloomValues);
    @memcpy(bloomValues, self.bloomValues);

    var bloomTokens: ?[]const u8 = null;
    errdefer if (bloomTokens) |t| alloc.free(t);
    if (self.bloomTokens) |tokens| {
        const buf = try alloc.alloc(u8, tokens.len);
        @memcpy(buf, tokens);
        bloomTokens = buf;
    }

    const dict = try alloc.create(ColumnDict);
    errdefer alloc.destroy(dict);
    dict.* = try self.dict.copy(alloc);
    errdefer dict.deinit(alloc);

    column.* = .{
        .key = key,
        .type = self.type,

        .min = self.min,
        .max = self.max,

        .dict = dict,
        .bloomValues = bloomValues,

        .bloomTokens = bloomTokens,
    };
}
