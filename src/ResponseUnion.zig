const std = @import("std");
const ErrorResponse = @import("./commands/ErrorResponse.zig").ErrorResponse;
const CursorIterator = @import("./commands/CursorIterator.zig").CursorIterator;

pub fn ResponseUnion(comptime ResponseType: type, comptime ErrorResponseType: type) type {
    const fields = [_]std.builtin.Type.UnionField{
        .{ .name = "response", .type = *ResponseType, .alignment = @alignOf(*ResponseType) },
        .{ .name = "err", .type = *ErrorResponseType, .alignment = @alignOf(*ErrorResponseType) },
    };

    const enum_fields = [_]std.builtin.Type.EnumField{
        .{ .name = "response", .value = 0 },
        .{ .name = "err", .value = 2 },
    };

    const UnionEnumType = @Type(.{
        .@"enum" = .{
            .tag_type = u8,
            .is_exhaustive = true,
            .fields = &enum_fields,
            .decls = &.{},
        },
    });

    return @Type(.{
        .@"union" = .{
            .layout = .auto,
            .tag_type = UnionEnumType,
            .fields = &fields,
            .decls = &.{},
        },
    });
}

pub fn WriteResponseUnion(comptime ResponseType: type, comptime ErrorResponseType: type, comptime WriteErrorType: type) type {
    const fields = [_]std.builtin.Type.UnionField{
        .{ .name = "response", .type = *ResponseType, .alignment = @alignOf(*ResponseType) },
        .{ .name = "write_errors", .type = *WriteErrorType, .alignment = @alignOf(*WriteErrorType) },
        .{ .name = "err", .type = *ErrorResponseType, .alignment = @alignOf(*ErrorResponseType) },
    };

    const enum_fields = [_]std.builtin.Type.EnumField{
        .{ .name = "response", .value = 0 },
        .{ .name = "write_errors", .value = 1 },
        .{ .name = "err", .value = 2 },
    };

    const UnionEnumType = @Type(.{
        .@"enum" = .{
            .tag_type = u8,
            .is_exhaustive = true,
            .fields = &enum_fields,
            .decls = &.{},
        },
    });

    return @Type(.{
        .@"union" = .{
            .layout = .auto,
            .tag_type = UnionEnumType,
            .fields = &fields,
            .decls = &.{},
        },
    });
}

pub const CursorResponseUnion = union(enum) {
    cursor: CursorIterator,
    err: *ErrorResponse,
};
