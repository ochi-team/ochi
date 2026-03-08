const std = @import("std");

pub const Contract = @This();
pub const VerifyError = error{
    UnsupportedContractType,
    TypeIsNotPointer,
    MissingField,
    WrongFieldType,
    MissingFunc,
    WrongFuncType,
};

pub const FieldSpec = struct {
    name: []const u8,
    type: type,
};

pub const FuncSpec = struct {
    name: []const u8,
    type: type,
};

fields: []const FieldSpec = &.{},
funcs: []const FuncSpec = &.{},

pub fn satisfies(self: Contract, comptime T: type, comptime mutable: bool) VerifyError!void {
    return switch (@typeInfo(T)) {
        .@"struct" => blk: {
            if (mutable) return error.TypeIsNotPointer;
            break :blk self.satisfiesStruct(T);
        },
        .pointer => |ptr| blk: {
            if (ptr.size != .one) return error.UnsupportedContractType;
            break :blk self.satisfiesStruct(ptr.child);
        },
        else => error.UnsupportedContractType,
    };
}

fn satisfiesStruct(self: Contract, comptime T: type) VerifyError!void {
    inline for (self.fields) |field| {
        if (!@hasField(T, field.name)) return error.MissingField;
        if (@FieldType(T, field.name) != field.type) return error.WrongFieldType;
    }

    inline for (self.funcs) |func| {
        if (!@hasDecl(T, func.name)) return error.MissingFunc;
        if (@TypeOf(@field(T, func.name)) != func.type) return error.WrongFuncType;
    }
}

const Good = struct {
    size: u32,
    has: bool,

    pub fn lessThan(_: void, a: Good, b: Good) bool {
        return a.size < b.size;
    }
};

const WrongFieldType = struct {
    size: u64,
    has: bool,

    pub fn lessThan(_: void, a: WrongFieldType, b: WrongFieldType) bool {
        return a.size < b.size;
    }
};

const MissingField = struct {
    size: u32,

    pub fn lessThan(_: void, a: MissingField, b: MissingField) bool {
        return a.size < b.size;
    }
};

const MissingFunc = struct {
    size: u32,
    has: bool,
};

const WrongFuncType = struct {
    size: u32,
    has: bool,

    pub fn lessThan(_: void, _: WrongFuncType, _: WrongFuncType) u8 {
        return 0;
    }
};

fn contractFor(comptime T: type) Contract {
    return .{
        .fields = &.{
            .{ .name = "size", .type = u32 },
            .{ .name = "has", .type = bool },
        },
        .funcs = &.{
            .{ .name = "lessThan", .type = fn (void, T, T) bool },
        },
    };
}

test "contract.satisfies returns errors for invalid shape/signature" {
    const testing = std.testing;
    const Case = struct {
        contract: Contract,
        target: type,
        mutable: bool = false,
        expected_err: ?VerifyError = null,
    };

    const cases = [_]Case{
        .{
            .contract = contractFor(Good),
            .target = Good,
        },
        .{
            .contract = contractFor(Good),
            .target = *Good,
            .mutable = true,
        },
        .{
            .contract = contractFor(WrongFieldType),
            .target = WrongFieldType,
            .expected_err = error.WrongFieldType,
        },
        .{
            .contract = contractFor(MissingField),
            .target = MissingField,
            .expected_err = error.MissingField,
        },
        .{
            .contract = contractFor(MissingFunc),
            .target = MissingFunc,
            .expected_err = error.MissingFunc,
        },
        .{
            .contract = contractFor(WrongFuncType),
            .target = WrongFuncType,
            .expected_err = error.WrongFuncType,
        },
        .{
            .contract = contractFor(Good),
            .target = Good,
            .mutable = true,
            .expected_err = error.TypeIsNotPointer,
        },
        .{
            .contract = contractFor(Good),
            .target = u64,
            .expected_err = error.UnsupportedContractType,
        },
        .{
            .contract = contractFor(Good),
            .target = []Good,
            .expected_err = error.UnsupportedContractType,
        },
    };

    inline for (cases) |case| {
        const result = case.contract.satisfies(case.target, case.mutable);
        if (case.expected_err) |expected_err| {
            try testing.expectError(expected_err, result);
        } else {
            try result;
        }
    }
}
