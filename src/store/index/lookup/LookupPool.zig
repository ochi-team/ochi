const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const Locked = @import("../../../stds/Locked.zig").Locked;
const Ring = @import("../../../stds/Ring.zig").Ring;
const Lookup = @import("Lookup.zig");

const Self = @This();

const LockedLookup = Locked(Lookup);

lookups: []LockedLookup,
ring: Ring(LockedLookup),

pub fn init(allocator: Allocator, count: usize) !*Self {
    std.debug.assert(count > 0);

    const lookups = try allocator.alloc(LockedLookup, count);
    for (lookups) |*l| l.* = .{ .val = .empty };

    const pool = try allocator.create(Self);
    pool.* = .{
        .lookups = lookups,
        .ring = Ring(LockedLookup).init(lookups),
    };
    return pool;
}

pub fn deinit(self: *Self, io: Io, allocator: Allocator) void {
    for (self.lookups) |*l| l.val.deinit(io, allocator);
    allocator.free(self.lookups);
    allocator.destroy(self);
}

pub fn next(self: *Self) *LockedLookup {
    return self.ring.next();
}
