const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const MemOrder = @import("../../stds/sort.zig").MemOrder;
const encoding = @import("encoding");
const Encoder = encoding.Encoder;

const StorageBlock = @import("StorageBlock.zig");
const EncodingType = @import("BlockHeader.zig").EncodingType;

const EncodedMemBlock = struct {
    firstItem: []const u8,
    prefix: []const u8,
    itemsCount: u32,
    encodingType: EncodingType,
};

fn findPrefix(first: []const u8, second: []const u8) []const u8 {
    const n = @min(first.len, second.len);
    var i: usize = 0;
    while (i < n and first[i] == second[i]) : (i += 1) {}
    return first[0..@intCast(i)];
}

const MemBlock = @This();

data: std.ArrayList([]const u8),
size: u32,
prefix: []const u8 = "",

// stateBuffer is used for ownership of new record items during the merging,
stateBuffer: ?std.ArrayList(u8) = null,

pub fn init(
    alloc: Allocator,
    maxMemBlockSize: u32,
) !*MemBlock {
    var data = try std.ArrayList([]const u8).initCapacity(alloc, maxMemBlockSize);
    errdefer data.deinit(alloc);

    const b = try alloc.create(MemBlock);
    b.* = .{
        .data = data,
        .size = 0,
    };
    return b;
}

pub fn deinit(self: *MemBlock, alloc: Allocator) void {
    self.data.deinit(alloc);
    if (self.stateBuffer) |*buf| buf.deinit(alloc);
    alloc.destroy(self);
}

pub fn reset(self: *MemBlock) void {
    self.data.clearRetainingCapacity();
    self.prefix = undefined;
}

pub fn add(self: *MemBlock, entry: []const u8) bool {
    if ((self.size + entry.len) > self.data.capacity) return false;

    self.data.appendAssumeCapacity(entry);
    self.size += @intCast(entry.len);
    return true;
}

pub fn sortData(self: *MemBlock) void {
    // TODO: evaluate the chances of the data being sorted, might improve performance a lot,
    // collect the metrics and if it's common enough optimize the algorithm

    self.setPrefix();
    self.sort();
}

pub fn setPrefix(self: *MemBlock) void {
    if (self.data.items.len == 0) return;

    if (self.data.items.len == 1) {
        self.prefix = self.data.items[0];
        return;
    }

    var prefix = self.data.items[0];
    for (self.data.items[1..]) |entry| {
        if (std.mem.startsWith(u8, entry, prefix)) {
            continue;
        }

        prefix = findPrefix(prefix, entry);
        if (prefix.len == 0) return;
    }

    self.prefix = prefix;
}
pub fn setPrefixSorted(self: *MemBlock) void {
    if (self.data.items.len <= 1) {
        self.prefix = "";
        return;
    }

    self.prefix = findPrefix(self.data.items[0], self.data.items[self.data.items.len - 1]);
}

pub fn sort(self: *MemBlock) void {
    std.mem.sortUnstable([]const u8, self.data.items, self, memBlockEntryLessThan);
}

fn memBlockEntryLessThan(self: *MemBlock, one: []const u8, another: []const u8) bool {
    const prefixLen = self.prefix.len;

    const oneSuffix = one[prefixLen..];
    const anotherSuffix = another[prefixLen..];

    return std.mem.lessThan(u8, oneSuffix, anotherSuffix);
}

pub fn encode(
    self: *MemBlock,
    alloc: Allocator,
    sb: *StorageBlock,
) !EncodedMemBlock {
    std.debug.assert(self.data.items.len != 0);
    // this API can't be called on unsorted data
    if (builtin.is_test) {
        std.debug.assert(std.sort.isSorted([]const u8, self.data.items, {}, MemOrder(u8).lessThanConst));
    }

    self.setPrefixSorted();
    const firstItem = self.data.items[0];

    // TODO: consider making len limit 128
    if (self.size - self.prefix.len * self.data.items.len < 64 or self.data.items.len < 2) {
        try self.encodePlain(alloc, sb);
        return EncodedMemBlock{
            .firstItem = firstItem,
            .prefix = self.prefix,
            .itemsCount = @intCast(self.data.items.len),
            .encodingType = .plain,
        };
    }

    var itemsBuf = std.ArrayList(u8).empty;
    defer itemsBuf.deinit(alloc);

    var lens = try std.ArrayList(u32).initCapacity(alloc, self.data.items.len - 1);
    defer lens.deinit(alloc);

    var prevItem = firstItem[self.prefix.len..];
    var prevLen: u32 = 0;

    // write prefix lens
    for (self.data.items[1..]) |item| {
        const currItem = item[self.prefix.len..];
        const prefix = findPrefix(prevItem, currItem);
        try itemsBuf.appendSlice(alloc, currItem[prefix.len..]);

        const xLen = prefix.len ^ prevLen;
        lens.appendAssumeCapacity(@intCast(xLen));

        prevItem = currItem;
        prevLen = @intCast(prefix.len);
    }

    var fallbackFba = std.heap.stackFallback(2048, alloc);
    var fba = fallbackFba.get();
    const encodedPrefixLensBufSize = Encoder.varIntsBound(u32, lens.items);
    const encodedPrefixLensBuf = try fba.alloc(u8, encodedPrefixLensBufSize);
    defer fba.free(encodedPrefixLensBuf);
    var enc = Encoder.init(encodedPrefixLensBuf);
    enc.writeVarInts(u32, lens.items);

    sb.itemsData = try std.ArrayList(u8).initCapacity(alloc, itemsBuf.items.len);
    var bound = try encoding.compressBound(itemsBuf.items.len);
    const compressedItems = try alloc.alloc(u8, bound);
    defer alloc.free(compressedItems);
    var n = try encoding.compressAuto(compressedItems, itemsBuf.items);
    try sb.itemsData.appendSlice(alloc, compressedItems[0..n]);

    // write items lens
    lens.clearRetainingCapacity();

    prevLen = @intCast(firstItem.len - self.prefix.len);
    for (self.data.items[1..]) |item| {
        const itemLen: u32 = @intCast(item.len - self.prefix.len);
        const xLen = itemLen ^ prevLen;
        prevLen = itemLen;
        lens.appendAssumeCapacity(xLen);
    }

    const encodedLensBound = Encoder.varIntsBound(u32, lens.items);
    const encodedLens = try fba.alloc(u8, encodedLensBound);
    defer fba.free(encodedLens);
    enc = Encoder.init(encodedLens);
    enc.writeVarInts(u32, lens.items);

    bound = try encoding.compressBound(encodedLens.len);
    sb.lensData = try std.ArrayList(u8).initCapacity(alloc, encodedPrefixLensBuf.len + bound);
    sb.lensData.appendSliceAssumeCapacity(encodedPrefixLensBuf);
    const compressedLens = try alloc.alloc(u8, bound);
    defer alloc.free(compressedLens);
    n = try encoding.compressAuto(compressedLens, encodedLens);
    sb.lensData.appendSliceAssumeCapacity(compressedLens[0..n]);

    // if compressed content is more than 90% of the original size - not worth it
    // TODO: consider tweaking the value up to 80-85%
    if (@as(f64, @floatFromInt(sb.itemsData.items.len)) >
        0.9 * @as(f64, @floatFromInt(self.size - self.prefix.len * self.data.items.len)))
    {
        sb.reset();
        try self.encodePlain(alloc, sb);
        return EncodedMemBlock{
            .firstItem = firstItem,
            .prefix = self.prefix,
            .itemsCount = @intCast(self.data.items.len),
            .encodingType = .plain,
        };
    }

    return EncodedMemBlock{
        .firstItem = firstItem,
        .prefix = self.prefix,
        .itemsCount = @intCast(self.data.items.len),
        .encodingType = .zstd,
    };
}

fn encodePlain(self: *MemBlock, alloc: Allocator, sb: *StorageBlock) !void {
    try sb.itemsData.ensureUnusedCapacity(
        alloc,
        self.size - self.prefix.len * self.data.items.len + self.prefix.len - self.data.items[0].len,
    );
    try sb.lensData.ensureUnusedCapacity(alloc, 2 * (self.data.items.len - 1));

    for (self.data.items[1..]) |item| {
        const suffix = item[self.prefix.len..];
        sb.itemsData.appendSliceAssumeCapacity(suffix);
    }

    // no chance any len value is larger than 16384 (0x4000)
    const slice = sb.lensData.unusedCapacitySlice();
    var enc = Encoder.init(slice);
    for (self.data.items[1..]) |item| {
        const len: u64 = @intCast(item.len - self.prefix.len);
        enc.writeVarInt(len);
    }
    sb.lensData.items.len = enc.offset;
}
