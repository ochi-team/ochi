const std = @import("std");

const Data = @import("data.zig").Data;
const Lines = @import("process.zig").Lines;
const LinesToDay = @import("process.zig").LinesToDay;
const SID = @import("process.zig").SID;

const partitionsFolderName = "partitions";
const dataFolderName = "data";
const indexFolderName = "index";

// holds parts names separated by \n
const partsFileName = "parts";

const SecNs = 1_000_000_000;

pub const Table = struct {
    flushInterval: u64,
    pub fn init(allocator: std.mem.Allocator, flushInterval: u64) !*Table {
        const t = try allocator.create(Table);
        t.* = Table{
            .flushInterval = flushInterval,
        };
        // TODO: open parts from parts
        // TODO: init shards object, ~ num cpu * 16
        // TODO: start backound workers
        return t;
    }
};

pub const Index = struct {
    table: *Table,

    pub fn init(allocator: std.mem.Allocator, table: *Table) !*Index {
        const i = try allocator.create(Index);
        i.* = Index{
            .table = table,
        };
        return i;
    }

    pub fn addLines(self: *Index, streamID: SID, encodedStream: [][]const u8) !void {
        _ = self;
        _ = streamID;
        _ = encodedStream;
    }
};

pub const Partition = struct {
    day: u64,
    path: []const u8,
    index: *Index,
    data: *Data,

    pub fn addLines(self: *Partition, allocator: std.mem.Allocator, lines: Lines) !void {
        for (lines.items) |line| {
            // TODO: calculate stream ID and validate cache, if cached then skip the step,
            // put to cache in the end of the loop

            // TODO: if eq to previous line (by stream ID) then skip, must be in the cache
            // TODO: if index has the stream ID then skip
            try self.index.addLines(line.sid, line.encodedTags);
        }

        try self.data.addLines(allocator, lines);
    }
};

pub const Store = struct {
    path: []const u8,

    partitions: std.ArrayList(*Partition),
    hot: *Partition,

    pub fn init(allocator: std.mem.Allocator, path: []const u8) !*Store {
        const store = try allocator.create(Store);
        store.path = path;
        // truncate separator
        if (path[store.path.len - 1] == std.fs.path.sep_str[0]) {
            store.path = path[0 .. path.len - 1];
        }
        return store;
    }

    pub fn deinit(self: *Store, allocator: std.mem.Allocator) void {
        // TODO: destroy all the partitions
        allocator.destroy(self);
    }

    pub fn addLines(self: *Store, allocator: std.mem.Allocator, lines: LinesToDay) !void {
        var linesIterator = lines.iterator();
        while (linesIterator.next()) |it| {
            const partition = try self.getPartition(allocator, it.key_ptr.*);
            try partition.addLines(allocator, it.value_ptr.*);
        }
    }

    fn getPartition(self: *Store, allocator: std.mem.Allocator, day: u64) !*Partition {
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
        const partitionPath = try std.fmt.bufPrint(&path_buf, "{s}{s}{s}{s}{d}", .{ self.path, std.fs.path.sep_str, partitionsFolderName, std.fs.path.sep_str, day });

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
        const dataFolderPath = try std.mem.concat(allocator, u8, &.{ partitionPath, dataFolderName });
        defer allocator.free(dataFolderPath);
        const indexFolderPath = try std.mem.concat(allocator, u8, &.{ partitionPath, indexFolderName });
        defer allocator.free(indexFolderName);

        try createParitionFiles(allocator, dataFolderPath, indexFolderPath);
        const partition = try self.openPartition(allocator, partitionPath, day);

        return partition;
    }

    fn openPartition(self: *Store, allocator: std.mem.Allocator, path: []const u8, day: u64) !*Partition {
        const indexTable = try Table.init(allocator, 5 * SecNs);
        const index = try Index.init(allocator, indexTable);

        const data = try Data.init(allocator);
        // TODO: remove unused parts directories

        const partition = try allocator.create(Partition);
        partition.* = Partition{
            .day = day,
            .path = path,
            .index = index,
            .data = data,
        };
        self.hot = partition;
        try self.partitions.append(allocator, partition);

        return partition;
    }
};

fn createParitionFiles(allocator: std.mem.Allocator, indexFolderPath: []const u8, dataFolderPath: []const u8) !void {
    try std.fs.makeDirAbsolute(indexFolderPath);
    try std.fs.makeDirAbsolute(dataFolderPath);

    // TODO: consider using static buffer allocator
    const partsFilePath = try std.mem.concat(allocator, u8, &.{ dataFolderPath, partsFileName });
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
