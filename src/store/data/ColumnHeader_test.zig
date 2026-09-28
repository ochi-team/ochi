const std = @import("std");
const Allocator = std.mem.Allocator;

const encoding = @import("encoding");
const Encoder = encoding.Encoder;
const Decoder = encoding.Decoder;

const ColumnDict = @import("ColumnDict.zig");
const ColumnHeader = @import("ColumnHeader.zig");

const testing = std.testing;

test "ColumnHeaderEncode" {
    const alloc = testing.allocator;

    const Case = struct {
        header: ColumnHeader,
        description: []const u8,

        fn makeDict(allocator: Allocator, values: []const []const u8) !ColumnDict {
            if (values.len == 0) {
                // For empty dict (non-dict column types), match what decode produces
                return ColumnDict{ .values = std.ArrayList([]const u8).empty };
            }
            var dict = try ColumnDict.init(allocator);
            for (values) |val| {
                dict.values.appendAssumeCapacity(val);
            }
            return dict;
        }
    };

    var dict1 = try Case.makeDict(alloc, &[_][]const u8{});
    defer dict1.deinit(alloc);
    var dict2 = try Case.makeDict(alloc, &[_][]const u8{ "value1", "value2", "value3" });
    defer dict2.deinit(alloc);
    var dict3 = try Case.makeDict(alloc, &[_][]const u8{});
    defer dict3.deinit(alloc);
    var dict4 = try Case.makeDict(alloc, &[_][]const u8{});
    defer dict4.deinit(alloc);
    var dict5 = try Case.makeDict(alloc, &[_][]const u8{});
    defer dict5.deinit(alloc);
    var dict6 = try Case.makeDict(alloc, &[_][]const u8{});
    defer dict6.deinit(alloc);
    var dict7 = try Case.makeDict(alloc, &[_][]const u8{});
    defer dict7.deinit(alloc);
    var dict8 = try Case.makeDict(alloc, &[_][]const u8{});
    defer dict8.deinit(alloc);
    var dict9 = try Case.makeDict(alloc, &[_][]const u8{});
    defer dict9.deinit(alloc);
    var dict10 = try Case.makeDict(alloc, &[_][]const u8{});
    defer dict10.deinit(alloc);

    const cases = [_]Case{
        .{
            .header = .{
                .key = "string_col",
                .dict = dict1,
                .type = .string,
                .min = 0,
                .max = 0,
                .size = 100,
                .offset = 1000,
                .bloomFilterSize = 50,
                .bloomFilterOffset = 2000,
            },
            .description = "string type",
        },
        .{
            .header = .{
                .key = "dict_col",
                .dict = dict2,
                .type = .dict,
                .min = 0,
                .max = 0,
                .size = 200,
                .offset = 1100,
                .bloomFilterSize = 0,
                .bloomFilterOffset = 0,
            },
            .description = "dict type with values",
        },
        .{
            .header = .{
                .key = "uint8_col",
                .dict = dict3,
                .type = .uint8,
                .min = 0,
                .max = 255,
                .size = 150,
                .offset = 1200,
                .bloomFilterSize = 60,
                .bloomFilterOffset = 2100,
            },
            .description = "uint8 type",
        },
        .{
            .header = .{
                .key = "uint16_col",
                .dict = dict4,
                .type = .uint16,
                .min = 0,
                .max = 65535,
                .size = 200,
                .offset = 1300,
                .bloomFilterSize = 70,
                .bloomFilterOffset = 2200,
            },
            .description = "uint16 type",
        },
        .{
            .header = .{
                .key = "uint32_col",
                .dict = dict5,
                .type = .uint32,
                .min = 10,
                .max = 1000,
                .size = 250,
                .offset = 1400,
                .bloomFilterSize = 80,
                .bloomFilterOffset = 2300,
            },
            .description = "uint32 type",
        },
        .{
            .header = .{
                .key = "uint64_col",
                .dict = dict6,
                .type = .uint64,
                .min = 100,
                .max = 10000,
                .size = 300,
                .offset = 1500,
                .bloomFilterSize = 90,
                .bloomFilterOffset = 2400,
            },
            .description = "uint64 type",
        },
        .{
            .header = .{
                .key = "int64_col",
                .dict = dict7,
                .type = .int64,
                .min = 0,
                .max = 5000,
                .size = 350,
                .offset = 1600,
                .bloomFilterSize = 100,
                .bloomFilterOffset = 2500,
            },
            .description = "int64 type",
        },
        .{
            .header = .{
                .key = "float64_col",
                .dict = dict8,
                .type = .float64,
                .min = 0,
                .max = 1000,
                .size = 400,
                .offset = 1700,
                .bloomFilterSize = 110,
                .bloomFilterOffset = 2600,
            },
            .description = "float64 type",
        },
        .{
            .header = .{
                .key = "ipv4_col",
                .dict = dict9,
                .type = .ipv4,
                .min = 0,
                .max = 4294967295,
                .size = 450,
                .offset = 1800,
                .bloomFilterSize = 120,
                .bloomFilterOffset = 2700,
            },
            .description = "ipv4 type",
        },
        .{
            .header = .{
                .key = "timestamp_col",
                .dict = dict10,
                .type = .timestampIso8601,
                .min = 1000000,
                .max = 2000000,
                .size = 500,
                .offset = 1900,
                .bloomFilterSize = 130,
                .bloomFilterOffset = 2800,
            },
            .description = "timestamp type",
        },
    };

    for (cases) |case| {
        // Encode
        var buf: [1024]u8 = undefined;
        var enc = Encoder.init(&buf);
        var header = case.header;
        header.encode(&enc);

        // Decode
        var dec = Decoder.init(buf[0..enc.offset]);
        var decoded = try ColumnHeader.decode(&dec, case.header.key, alloc);
        defer decoded.dict.deinit(alloc);

        // Verify using deep comparison
        try testing.expectEqualDeep(case.header, decoded);
    }
}
