const std = @import("std");
const Allocator = std.mem.Allocator;

const Filenames = @import("Filenames.zig");
const DataRecorder = @import("DataRecorder.zig");
const Index = @import("store/index/Index.zig");
const IndexRecorder = @import("store/index/IndexRecorder.zig");
const Line = @import("store/lines.zig").Line;
const Field = @import("store/lines.zig").Field;
const Cache = @import("stds/Cache.zig");

const Partition = @import("Partition.zig");

const Conf = @import("Conf.zig");

pub const Store = struct {
    path: []const u8,

    partitions: std.ArrayList(*Partition),
    hot: ?*Partition,

    pub fn init(allocator: Allocator, path: []const u8) !*Store {
        const store = try allocator.create(Store);
        store.* = .{
            .path = path,
            .partitions = std.ArrayList(*Partition).empty,
            .hot = null,
        };
        // truncate separator
        if (path[store.path.len - 1] == std.fs.path.sep_str[0]) {
            store.path = path[0 .. path.len - 1];
        }
        return store;
    }

    pub fn deinit(self: *Store, allocator: Allocator) void {
        self.partitions.deinit(allocator);
        allocator.destroy(self);
    }

    pub fn addLines(
        self: *Store,
        allocator: Allocator,
        lines: std.AutoHashMap(u64, std.ArrayList(Line)),
        tags: []Field,
        encodedTags: []const u8,
    ) !void {
        var linesIterator = lines.iterator();
        while (linesIterator.next()) |it| {
            const partition = try self.getPartition(allocator, it.key_ptr.*);
            try partition.addLines(allocator, it.value_ptr.*, tags, encodedTags);
        }
    }

    fn getPartition(self: *Store, allocator: Allocator, day: u64) !*Partition {
        const n = std.sort.binarySearch(
            *Partition,
            self.partitions.items,
            day,
            orderPartitions,
        );
        if (n) |i| {
            const part = self.partitions.items[i];
            self.hot = part;
            return part;
        }

        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const partitionPath = try std.fmt.bufPrint(
            &path_buf,
            "{s}{s}{s}{s}{d}",
            .{ self.path, std.fs.path.sep_str, Filenames.partitions, std.fs.path.sep_str, day },
        );

        const res = std.fs.accessAbsolute(partitionPath, .{ .mode = .read_write });
        if (res) |_| {
            // TODO: get a partition from existing folder or create missing files
            // missing files could be due to crash
            return error.PartitionUnavailble;
        } else |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        }

        // TODO: consider using fixed buffer allocator for dataFolderPath and indexFolderPath
        const dataFolderPath = try std.mem.concat(allocator, u8, &.{ partitionPath, Filenames.tableData });
        defer allocator.free(dataFolderPath);
        const indexFolderPath = try std.mem.concat(allocator, u8, &.{ partitionPath, Filenames.tableIndex });
        defer allocator.free(indexFolderPath);

        try createParitionFiles(allocator, dataFolderPath, indexFolderPath);
        const partition = try self.openPartition(allocator, partitionPath, day);

        return partition;
    }

    fn openPartition(self: *Store, alloc: Allocator, path: []const u8, day: u64) !*Partition {
        try self.partitions.ensureUnusedCapacity(alloc, 1);

        const partition = try Partition.open(alloc, path, day);

        self.hot = partition;
        try self.partitions.append(alloc, partition);

        return partition;
    }
};

fn createParitionFiles(allocator: Allocator, indexFolderPath: []const u8, dataFolderPath: []const u8) !void {
    try std.fs.makeDirAbsolute(indexFolderPath);
    try std.fs.makeDirAbsolute(dataFolderPath);

    // TODO: consider using static buffer allocator
    const partsFilePath = try std.mem.concat(allocator, u8, &.{ dataFolderPath, Filenames.tables });
    defer allocator.free(partsFilePath);
    const file = try std.fs.createFileAbsolute(partsFilePath, .{ .exclusive = true });
    file.close();
}

fn orderPartitions(day: u64, part: *Partition) std.math.Order {
    if (day < part.day) {
        return .lt;
    }
    if (day > part.day) {
        return .gt;
    }
    return .eq;
}
