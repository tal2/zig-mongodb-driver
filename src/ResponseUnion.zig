const std = @import("std");

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

pub fn WriteResponseUnion(comptime ResponseType: type, comptime ErrorResponseType: type, comptime ResponseErrorType: type) type {
    const fields = [_]std.builtin.Type.UnionField{
        .{ .name = "response", .type = *ResponseType, .alignment = @alignOf(*ResponseType) },
        .{ .name = "write_errors", .type = *ResponseErrorType, .alignment = @alignOf(*ResponseErrorType) },
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
