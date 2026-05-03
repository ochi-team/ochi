const Field = @import("lines.zig").Field;

// TODO: support CIDR ip query

pub const Query = struct {
    start: u64,
    end: u64,
    tags: []const Field,
    fields: []const Field,
};
