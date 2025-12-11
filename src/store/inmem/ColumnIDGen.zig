const std = @import("std");

const Self = @This();

keyIDs: std.StringArrayHashMap(u16),

pub fn init(allocator: std.mem.Allocator) !*Self {
    const nameIDs = std.StringArrayHashMap(u16).init(allocator);
    const s = try allocator.create(Self);
    s.* = Self{
        .keyIDs = nameIDs,
    };
    return s;
}

pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
    self.keyIDs.deinit();
    allocator.destroy(self);
}

pub fn genID(self: *Self, key: []const u8) !u16 {
    const maybeID = self.keyIDs.get(key);
    if (maybeID) |id| {
        return id;
    }

    const id: u16 = @intCast(self.keyIDs.count());
    try self.keyIDs.put(key, id);
    return id;
}

pub fn genIDAssumeCapacity(self: *Self, key: []const u8) u16 {
    const maybeID = self.keyIDs.get(key);
    if (maybeID) |id| {
        return id;
    }

    const id: u16 = @intCast(self.keyIDs.count());
    self.keyIDs.putAssumeCapacity(key, id);
    return id;
}
