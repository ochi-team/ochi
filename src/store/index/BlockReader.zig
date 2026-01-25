const std = @import("std");
const Allocator = std.mem.Allocator;

const MemBlock = @import("MemBlock.zig");
const TableHeader = @import("TableHeader.zig");

const BlockReader = @This();

block: *MemBlock,
tableHeader: TableHeader,

// state

// currentI defines a current item of the block
currentI: usize,
// read defines if the block has been read
read: bool,

pub fn initFromMemBlock(alloc: Allocator, block: *MemBlock) !*BlockReader {
    block.sortData();

    const r = try alloc.create(BlockReader);
    r.* = .{
        .block = block,
        .tableHeader = .{
            .blocksCount = undefined,
            .firstItem = undefined,
            .itemsCount = undefined,
            .lastItem = undefined,
        },
        .currentI = 0,
        .read = false,
    };
    return r;
}

pub fn deinit(self: *BlockReader, alloc: Allocator) void {
    self.block.deinit(alloc);
    alloc.destroy(self);
}

pub fn blockReaderLessThan(one: *BlockReader, another: *BlockReader) bool {
    const first = one.current();
    const second = another.current();
    return std.mem.lessThan(u8, first, second);
}

pub inline fn current(self: *BlockReader) []const u8 {
    return self.block.data.items[self.currentI];
}

pub fn next(self: *BlockReader) !bool {
    // TODO: implement disk block reading

    if (self.read) return false;
    if (self.block.data.items.len == 0) return false;

    self.read = true;
    return true;
}
