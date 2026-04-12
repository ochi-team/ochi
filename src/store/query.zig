const Field = @import("lines.zig").Field;

pub const Query = struct {
    start: u64,
    end: u64,
    tags: []Field,
    fields: []Field,
};
