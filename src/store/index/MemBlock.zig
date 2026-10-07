const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const Conf = @import("../../Conf.zig");
const Logger = @import("logging");
const strings = @import("../../stds/strings.zig");
const encoding = @import("encoding");
const Encoder = encoding.Encoder;
const CompressionPool = @import("../compression/CompressionPool.zig");
const DecompressionPool = @import("../compression/DecompressionPool.zig");
const Io = std.Io;

const EntriesBlock = @import("EntriesBlock.zig");
const EncodingType = @import("BlockHeader.zig").EncodingType;

const tracy = @import("tracy");

// TODO: tune the value, e.g. try it 128
const maxPlainMemBlockLen = 64;

pub const EncodedMemBlock = struct {
    firstEntry: []const u8,
    prefix: []const u8,
    itemsCount: u32,
    encodingType: EncodingType,
};

const MemBlock = @This();

/// defines entry byte ranges in buf.
memEntries: std.ArrayList(MemEntry),
prefix: []const u8 = "",

// buf may hold the underlying data memory in order be the memory owner,
// it happens in reading or merging path when we decode the stored content
// TODO: it's necessary to own the data during merging and reading,
// but probably we might find tricks not to own, but borrow the memory in some cases,
// e.g. during ingestion, but:
// 1. it requires storing block size,
// 2. it requires storing max mem block size
buf: std.ArrayList(u8) = .empty,

// the smallest index entry is sid: [kind:1][tenant:8][stream:16] = 25 bytes,
// use this for initial memEntries capacity so tiny entries don't exhaust pointer slots
// TODO: tune the value on practice
const minEntrySizeHint = 64;

pub const MemBlockOpts = struct {
    maxMemBlockSize: u32 = 0,
    // blocks count only for testing purpose when the entries are 1 byte size
    blocksCountHint: usize = 0,
};

pub const MemEntry = struct {
    start: u16,
    end: u16,
};

// TODO: meter how many blocks are acquired
pub fn init(
    alloc: Allocator,
    opts: MemBlockOpts,
) !*MemBlock {
    const z = tracy.Zone.begin(.{ .src = @src(), .name = "index.MemBlock.init" });
    defer z.end();
    std.debug.assert(opts.maxMemBlockSize <= std.math.maxInt(u16));

    var maxMemBlockSize = opts.maxMemBlockSize;
    if (maxMemBlockSize == 0) maxMemBlockSize = Conf.getConf().app.maxIndexMemBlockSize;
    std.debug.assert(maxMemBlockSize <= std.math.maxInt(u16));

    var entriesSize = opts.blocksCountHint;
    if (entriesSize == 0) entriesSize = maxMemBlockSize / minEntrySizeHint;

    var data = try std.ArrayList(MemEntry).initCapacity(alloc, entriesSize);
    errdefer data.deinit(alloc);

    var buf = try std.ArrayList(u8).initCapacity(alloc, maxMemBlockSize);
    errdefer buf.deinit(alloc);

    const b = try alloc.create(MemBlock);
    b.* = .{
        .memEntries = data,
        .buf = buf,
    };
    return b;
}

pub fn deinit(self: *MemBlock, alloc: Allocator) void {
    Logger.log(.debug, "deinit index mem block", .{
        .bufcap = self.buf.capacity,
        .buflen = self.buf.items.len,
        .entriescap = self.memEntries.capacity,
        .entrieslen = self.memEntries.items.len,
    });
    self.memEntries.deinit(alloc);
    self.buf.deinit(alloc);
    alloc.destroy(self);
}

pub fn reset(self: *MemBlock) void {
    Logger.log(.debug, "reset index mem block", .{
        .bufcap = self.buf.capacity,
        .buflen = self.buf.items.len,
        .entriescap = self.memEntries.capacity,
        .entrieslen = self.memEntries.items.len,
    });
    self.memEntries.clearRetainingCapacity();
    self.buf.clearRetainingCapacity();
    self.prefix = "";
}

pub fn add(self: *MemBlock, entry: []const u8) bool {
    std.debug.assert(entry.len > 0);
    if (self.memEntries.items.len == self.memEntries.capacity) return false;
    if (self.buf.capacity - self.buf.items.len < entry.len) return false;

    const start: u16 = @intCast(self.buf.items.len);
    self.buf.appendSliceAssumeCapacity(entry);
    self.memEntries.appendAssumeCapacity(.{ .start = start, .end = @intCast(self.buf.items.len) });
    return true;
}

pub fn addOwned(self: *MemBlock, entry: MemEntry) void {
    std.debug.assert(entry.start <= entry.end);
    std.debug.assert(entry.end <= self.buf.items.len);
    self.memEntries.appendAssumeCapacity(entry);
}

fn addBuffered(self: *MemBlock, start: usize) void {
    self.memEntries.appendAssumeCapacity(.{ .start = @intCast(start), .end = @intCast(self.buf.items.len) });
}

pub fn get(self: *const MemBlock, i: usize) []const u8 {
    const e = self.memEntries.items[i];
    return self.buf.items[e.start..e.end];
}

pub fn getEntry(self: *const MemBlock, entry: MemEntry) []const u8 {
    return self.buf.items[entry.start..entry.end];
}

pub fn last(self: *const MemBlock) []const u8 {
    return self.get(self.memEntries.items.len - 1);
}

pub fn iterator(self: *const MemBlock) Iterator {
    return .{ .block = self };
}

pub const Iterator = struct {
    current: usize = 0,
    block: *const MemBlock,

    pub fn next(self: *Iterator) ?[]const u8 {
        if (self.current + 1 <= self.block.memEntries.items.len) {
            const v = self.block.get(self.current);
            self.current += 1;
            return v;
        }

        return null;
    }
};

pub fn sortData(self: *MemBlock) void {
    // TODO: evaluate the chances of the data being sorted, might improve performance a lot,
    // collect the metrics and if it's common enough optimize the algorithm

    self.setPrefix();
    self.sort();
}

pub fn setPrefix(self: *MemBlock) void {
    if (self.memEntries.items.len == 0) return;

    if (self.memEntries.items.len == 1) {
        self.prefix = self.get(0);
        return;
    }

    var iter = self.iterator();
    var prefix = iter.next().?;
    while (iter.next()) |entry| {
        if (std.mem.startsWith(u8, entry, prefix)) {
            continue;
        }

        prefix = strings.findPrefix(prefix, entry);
        if (prefix.len == 0) return;
    }

    self.prefix = prefix;
}
pub fn setPrefixSorted(self: *MemBlock) void {
    if (self.memEntries.items.len <= 1) {
        self.prefix = "";
        return;
    }

    self.prefix = strings.findPrefix(self.get(0), self.last());
}

pub fn sort(self: *MemBlock) void {
    std.sort.pdq(MemEntry, self.memEntries.items, self, memBlockEntryLessThan);
}

fn memBlockEntryLessThan(self: *const MemBlock, one: MemEntry, another: MemEntry) bool {
    const prefixLen = self.prefix.len;

    const oneSuffix = self.getEntry(one)[prefixLen..];
    const anotherSuffix = self.getEntry(another)[prefixLen..];

    return std.mem.lessThan(u8, oneSuffix, anotherSuffix);
}

pub fn isSorted(self: *const MemBlock) bool {
    return std.sort.isSorted(MemEntry, self.memEntries.items, self, memBlockEntryLessThan);
}

fn assertIsSorted(self: *const MemBlock) void {
    if (builtin.is_test) {
        std.debug.assert(self.isSorted());
    }
}

pub fn encode(
    self: *MemBlock,
    io: Io,
    alloc: Allocator,
    compressionPool: *CompressionPool,
    entriesBlock: *EntriesBlock,
) !EncodedMemBlock {
    std.debug.assert(self.memEntries.items.len != 0);
    // this API can't be called on unsorted data
    self.assertIsSorted();

    self.setPrefixSorted();
    const firstEntry = self.get(0);

    if (self.buf.items.len - self.prefix.len * self.memEntries.items.len < maxPlainMemBlockLen or self.memEntries.items.len < 2) {
        try self.encodePlain(alloc, entriesBlock);
        return EncodedMemBlock{
            .firstEntry = firstEntry,
            .prefix = self.prefix,
            .itemsCount = @intCast(self.memEntries.items.len),
            .encodingType = .plain,
        };
    }

    var entriesBuf = try std.ArrayList(u8).initCapacity(alloc, self.buf.items.len - self.prefix.len * self.memEntries.items.len);
    defer entriesBuf.deinit(alloc);
    // TODO: make it a slice if it works for us
    var lens = try std.ArrayList(u32).initCapacity(alloc, self.memEntries.items.len - 1);
    defer lens.deinit(alloc);

    // write prefix lens
    var prevEntry = firstEntry[self.prefix.len..];
    var prevLen: u32 = 0;

    for (1..self.memEntries.items.len) |i| {
        const item = self.get(i);
        const currEntry = item[self.prefix.len..];
        const prefix = strings.findPrefix(prevEntry, currEntry);
        entriesBuf.appendSliceAssumeCapacity(currEntry[prefix.len..]);

        const xLen = prefix.len ^ prevLen;
        lens.appendAssumeCapacity(@intCast(xLen));

        prevEntry = currEntry;
        prevLen = @intCast(prefix.len);
    }

    // encode lens
    var fallbackFba = std.heap.stackFallback(2048, alloc);
    var fba = fallbackFba.get();
    const encodedPrefixLensBufSize = Encoder.varIntsBound(u32, lens.items);
    const encodedPrefixLensBuf = try fba.alloc(u8, encodedPrefixLensBufSize);
    defer fba.free(encodedPrefixLensBuf);
    var enc = Encoder.init(encodedPrefixLensBuf);
    enc.writeVarInts(u32, lens.items);

    // compress items
    var bound = try encoding.compressBound(entriesBuf.items.len);
    entriesBlock.entriesBuf.clearRetainingCapacity();
    try entriesBlock.entriesBuf.ensureUnusedCapacity(alloc, bound);
    entriesBlock.entriesBuf.items.len = try compressionPool.compressAuto(
        io,
        entriesBlock.entriesBuf.unusedCapacitySlice(),
        entriesBuf.items,
    );

    // write lens
    lens.clearRetainingCapacity();
    prevLen = @intCast(firstEntry.len - self.prefix.len);
    for (1..self.memEntries.items.len) |i| {
        const item = self.get(i);
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

    // TODO: avoid concat, the following options are possible to apply:
    // - stream compression from both arrays sequentially (the best)
    // - allocate double size of lens, it must be ok
    // if most of the time it fits the stack size, so we know precisely how much memory it needs
    const lensData = try std.mem.concat(fba, u8, &[_][]const u8{ encodedPrefixLensBuf, encodedLens });
    defer fba.free(lensData);

    bound = try encoding.compressBound(lensData.len);
    entriesBlock.lensBuf.clearRetainingCapacity();
    try entriesBlock.lensBuf.ensureUnusedCapacity(alloc, bound);
    entriesBlock.lensBuf.items.len = try compressionPool.compressAuto(
        io,
        entriesBlock.lensBuf.unusedCapacitySlice(),
        lensData,
    );

    // if compressed content is more than 90% of the original size - not worth it
    // TODO: consider tweaking the value up to 80-85%, take a meter to understand why it may happen
    if (@as(f64, @floatFromInt(entriesBlock.lensBuf.items.len)) >
        0.9 * @as(f64, @floatFromInt(self.buf.items.len - self.prefix.len * self.memEntries.items.len)))
    {
        entriesBlock.reset();
        try self.encodePlain(alloc, entriesBlock);
        return EncodedMemBlock{
            .firstEntry = firstEntry,
            .prefix = self.prefix,
            .itemsCount = @intCast(self.memEntries.items.len),
            .encodingType = .plain,
        };
    }

    return EncodedMemBlock{
        .firstEntry = firstEntry,
        .prefix = self.prefix,
        .itemsCount = @intCast(self.memEntries.items.len),
        .encodingType = .zstd,
    };
}

pub fn encodePlain(self: *MemBlock, alloc: Allocator, entriesBlock: *EntriesBlock) !void {
    entriesBlock.reset();

    try entriesBlock.entriesBuf.ensureUnusedCapacity(
        alloc,
        self.buf.items.len - self.prefix.len * self.memEntries.items.len +
            self.prefix.len - self.get(0).len,
    );

    var lensSizeBound: usize = 0;
    for (1..self.memEntries.items.len) |i| {
        const item = self.get(i);
        const len: u64 = @intCast(item.len - self.prefix.len);
        lensSizeBound += Encoder.varIntBound(len);
    }
    try entriesBlock.lensBuf.ensureUnusedCapacity(alloc, lensSizeBound);

    for (1..self.memEntries.items.len) |i| {
        const item = self.get(i);
        const suffix = item[self.prefix.len..];
        entriesBlock.entriesBuf.appendSliceAssumeCapacity(suffix);
    }

    const slice = entriesBlock.lensBuf.unusedCapacitySlice();
    var enc = Encoder.init(slice);
    for (1..self.memEntries.items.len) |i| {
        const item = self.get(i);
        const len: u64 = @intCast(item.len - self.prefix.len);
        enc.writeVarInt(len);
    }
    entriesBlock.lensBuf.items.len = enc.offset;
}

pub fn decode(
    self: *MemBlock,
    io: Io,
    alloc: Allocator,
    decompressionPool: *DecompressionPool,
    entriesBlock: *EntriesBlock,
    firstItem: []const u8,
    prefix: []const u8,
    itemsCount: u32,
    encodingType: EncodingType,
) !void {
    std.debug.assert(itemsCount > 0);

    self.reset();

    // temporary borrow prefix, later in decoding we copy it from owned buffer
    self.prefix = prefix;

    switch (encodingType) {
        .plain => {
            try self.decodePlain(alloc, entriesBlock, firstItem, itemsCount);
            self.prefix = self.buf.items[0..prefix.len];
            self.assertIsSorted();
            return;
        },
        .zstd => {
            // implementation is below
        },
    }

    // decompress prefix lens
    const size = try encoding.getFrameContentSize(entriesBlock.lensBuf.items);
    const decompressedLensBuf = try alloc.alloc(u8, size);
    defer alloc.free(decompressedLensBuf);
    var n = try decompressionPool.decompress(io, decompressedLensBuf, entriesBlock.lensBuf.items);

    // decode prefix lens
    const decodedLens = try alloc.alloc(u64, itemsCount - 1);
    defer alloc.free(decodedLens);
    var dec = encoding.Decoder.init(decompressedLensBuf[0..n]);
    dec.readVarInts(decodedLens);
    std.debug.assert(dec.offset <= dec.buf.len);

    // double count, prefixes + items lens
    const lensBuf = try alloc.alloc(u64, itemsCount * 2);
    defer alloc.free(lensBuf);
    const prefixLens = lensBuf[0..itemsCount];
    const lens = lensBuf[itemsCount..];

    // read prefixes
    prefixLens[0] = 0;
    for (0..decodedLens.len) |i| {
        const xLen = decodedLens[i];
        prefixLens[i + 1] = xLen ^ prefixLens[i];
    }

    // decode items lens, same size so we reuse decodedLens
    dec.readVarInts(decodedLens);
    std.debug.assert(dec.offset == dec.buf.len);

    // read items lens
    lens[0] = @intCast(firstItem.len - prefix.len);
    var dataLen: usize = prefix.len * itemsCount + lens[0];
    for (0..decodedLens.len) |i| {
        const xLen = decodedLens[i];
        const itemLen = xLen ^ lens[i];
        lens[i + 1] = itemLen;
        dataLen += @intCast(itemLen);
    }

    // read items data
    const decompressedItemsSize = try encoding.getFrameContentSize(entriesBlock.entriesBuf.items);
    const decompressedItemsBuf = try alloc.alloc(u8, decompressedItemsSize);
    defer alloc.free(decompressedItemsBuf);
    n = try decompressionPool.decompress(io, decompressedItemsBuf, entriesBlock.entriesBuf.items);

    try self.memEntries.ensureUnusedCapacity(alloc, itemsCount);
    try self.buf.ensureUnusedCapacity(alloc, dataLen);
    self.buf.appendSliceAssumeCapacity(firstItem);
    self.addBuffered(0);

    var decompressedItemsSlice = decompressedItemsBuf[0..n];
    var prevItem = self.buf.items[prefix.len..];
    for (1..itemsCount) |i| {
        const itemLen = lens[i];
        const prefixLen = prefixLens[i];

        const suffixLen = itemLen - prefixLen;
        std.debug.assert(decompressedItemsSlice.len >= suffixLen);
        std.debug.assert(prefixLen <= prevItem.len);

        const dataStart = self.buf.items.len;
        self.buf.appendSliceAssumeCapacity(prefix);
        self.buf.appendSliceAssumeCapacity(prevItem[0..prefixLen]);
        self.buf.appendSliceAssumeCapacity(decompressedItemsSlice[0..suffixLen]);
        self.addBuffered(dataStart);

        decompressedItemsSlice = decompressedItemsSlice[suffixLen..];
        prevItem = self.buf.items[self.buf.items.len - itemLen ..];
    }

    std.debug.assert(decompressedItemsSlice.len == 0);
    std.debug.assert(self.buf.items.len == dataLen);
    self.prefix = self.buf.items[0..prefix.len];
    if (builtin.mode == .Debug) {
        self.assertIsSorted();
    }
}

pub fn decodePlain(
    self: *MemBlock,
    alloc: Allocator,
    entriesBlock: *EntriesBlock,
    firstEntry: []const u8,
    itemsCount: u32,
) !void {
    // decode lens
    const lensBuf = try alloc.alloc(u64, itemsCount);
    defer alloc.free(lensBuf);
    lensBuf[0] = firstEntry.len - self.prefix.len;

    var dec = encoding.Decoder.init(entriesBlock.lensBuf.items);
    for (1..itemsCount) |i| {
        lensBuf[i] = dec.readVarInt();
    }
    std.debug.assert(dec.offset == dec.buf.len);

    // decode items
    const dataLen: usize = self.prefix.len * (itemsCount - 1) + firstEntry.len + entriesBlock.entriesBuf.items.len;
    try self.memEntries.ensureUnusedCapacity(alloc, itemsCount);
    try self.buf.ensureUnusedCapacity(alloc, dataLen);
    self.buf.appendSliceAssumeCapacity(firstEntry);
    self.addBuffered(0);

    var itemsSlice = entriesBlock.entriesBuf.items;
    for (1..itemsCount) |i| {
        const itemLen = lensBuf[i];
        const start = self.buf.items.len;

        self.buf.appendSliceAssumeCapacity(self.prefix);
        self.buf.appendSliceAssumeCapacity(itemsSlice[0..itemLen]);
        self.addBuffered(start);
        itemsSlice = itemsSlice[itemLen..];
    }
    std.debug.assert(itemsSlice.len == 0);
    std.debug.assert(self.buf.items.len == dataLen);
}
