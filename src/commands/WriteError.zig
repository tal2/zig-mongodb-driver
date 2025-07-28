const std = @import("std");
const utils = @import("../utils.zig");
const Allocator = std.mem.Allocator;
const BsonDocument = @import("bson").BsonDocument;
pub const JsonParseError = error{UnexpectedToken} || std.json.Scanner.NextError;

pub const WriteError = struct {
    index: i32,
    code: i32,
    errmsg: ?[]const u8 = null,
    err_info: ?*BsonDocument = null,

    pub fn deinit(self: *const WriteError, allocator: Allocator) void {
        if (self.errmsg != null) {
            const err_msg = self.errmsg.?;
            allocator.free(err_msg);
        }
        if (self.err_info) |err_info| {
            err_info.deinit(allocator);
        }
        allocator.destroy(self);
    }

    pub fn jsonParse(allocator: Allocator, source: *std.json.Scanner, options: std.json.ParseOptions) JsonParseError!WriteError {
        _ = options;
        if (try source.next() != .object_begin) return error.UnexpectedToken;

        var index: ?i32 = null;
        var code: ?i32 = null;
        var errmsg: ?[]const u8 = null;
        var err_info: ?*BsonDocument = null;

        blk: switch (try source.next()) {
            .string => |key| {
                if (index == null and std.mem.eql(u8, key, "index")) {
                    const index_value = try source.next();
                    index = std.fmt.parseInt(i32, index_value.number, 10) catch return error.UnexpectedToken;
                    continue :blk try source.next();
                }
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
            else => {
                return error.UnexpectedToken;
            },
        }

        if (index == null or code == null) return error.UnexpectedToken;

        return .{
            .index = index.?,
            .code = code.?,
            .errmsg = errmsg,
            .err_info = err_info,
        };
    }

    pub fn dupe(self: *const WriteError, allocator: Allocator) !*WriteError {
        const clone = try allocator.create(WriteError);
        errdefer clone.deinit(allocator);

        clone.index = self.index;
        clone.code = self.code;
        clone.errmsg = if (self.errmsg) |errmsg| try allocator.dupe(u8, errmsg) else null;
        clone.err_info = if (self.err_info) |err_info| try err_info.dupe(allocator) else null;

        return clone;
    }

    pub fn parseBson(allocator: Allocator, document: *BsonDocument) !*WriteError {
        return try utils.parseBsonToOwned(WriteError, allocator, document);
    }
};
