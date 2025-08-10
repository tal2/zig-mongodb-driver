const std = @import("std");
const utils = @import("../utils.zig");
const Allocator = std.mem.Allocator;
const BsonDocument = @import("bson").BsonDocument;
pub const JsonParseError = error{UnexpectedToken} || std.json.Scanner.NextError;

pub const WriteConcernError = struct {
    code: i32,
    errmsg: ?[]const u8 = null,
    errInfo: ?*BsonDocument = null,

    pub fn dupe(self: *const WriteConcernError, allocator: Allocator) !*WriteConcernError {
        const write_concern_error = try allocator.create(WriteConcernError);
        errdefer write_concern_error.deinit(allocator);

        write_concern_error.code = self.code;
        write_concern_error.errmsg = if (self.errmsg) |errmsg|
            try allocator.dupe(u8, errmsg)
        else
            null;
        write_concern_error.errInfo = if (self.errInfo) |err_info|
            try err_info.dupe(allocator)
        else
            null;

        return write_concern_error;
    }

    pub fn deinit(self: *const WriteConcernError, allocator: Allocator) void {
        if (self.errmsg) |errmsg| {
            allocator.free(errmsg);
        }
        if (self.errInfo) |info| {
            info.deinit(allocator);
        }
        allocator.destroy(self);
    }

    pub fn jsonParse(allocator: Allocator, source: *std.json.Scanner, options: std.json.ParseOptions) JsonParseError!WriteConcernError {
        _ = options;
        if (try source.next() != .object_begin) return error.UnexpectedToken;

        var code: ?i32 = null;
        var errmsg: ?[]const u8 = null;
        var err_info: ?*BsonDocument = null;

        blk: switch (try source.next()) {
            .string => |key| {
                if (code == null and std.mem.eql(u8, key, "code")) {
                    const code_value = try source.next();
                    code = std.fmt.parseInt(i32, code_value.number, 10) catch return error.UnexpectedToken;
                    continue :blk try source.next();
                }
                if (errmsg == null and std.mem.eql(u8, key, "errmsg")) {
                    const errmsg_value = try source.next();
                    errmsg = try allocator.dupe(u8, errmsg_value.string);
                    continue :blk try source.next();
                }
                if (err_info == null and std.mem.eql(u8, key, "errInfo")) {
                    const doc = BsonDocument.fromJsonReader(allocator, source) catch |err| {
                        switch (err) {
                            JsonParseError.OutOfMemory => return JsonParseError.OutOfMemory,
                            JsonParseError.BufferUnderrun => return JsonParseError.BufferUnderrun,
                            JsonParseError.SyntaxError => return JsonParseError.SyntaxError,
                            JsonParseError.UnexpectedToken => return JsonParseError.UnexpectedToken,
                            else => {
                                std.debug.print("UnexpectedToken: {any}\n", .{err});
                                return JsonParseError.UnexpectedToken;
                            },
                        }
                    };
                    err_info = doc;
                    continue :blk try source.next();
                }
            },
            .object_end => break :blk,
            else => |key_token| {
                std.debug.print("UnexpectedToken: {any}\n", .{key_token});
                return error.UnexpectedToken;
            },
        }

        if (code == null) return error.UnexpectedToken;

        return .{
            .code = code.?,
            .errmsg = if (errmsg) |v|
                v
            else
                null,
            .errInfo = err_info,
        };
    }

    pub fn parseBson(allocator: Allocator, document: *BsonDocument) !*WriteConcernError {
        return try document.toObject(allocator, WriteConcernError, .{ .ignore_unknown_fields = true });
    }
};
