const std = @import("std");
const bson = @import("bson");
const utils = @import("../utils.zig");

const Allocator = std.mem.Allocator;
const BsonDocument = bson.BsonDocument;
const BsonDocumentView = bson.BsonDocumentView;

pub const JsonParseError = error{UnexpectedToken} || std.json.Scanner.NextError;

pub const ErrorResponse = struct {
    ok: f64,
    errmsg: []const u8,
    codeName: []const u8,
    code: i32,

    pub fn deinit(self: *const ErrorResponse, allocator: Allocator) void {
        allocator.free(self.errmsg);
        allocator.free(self.codeName);
        allocator.destroy(self);
    }

    pub fn jsonParse(allocator: Allocator, source: *std.json.Scanner, options: std.json.ParseOptions) JsonParseError!ErrorResponse {
        _ = options;
        if (try source.next() != .object_begin) return error.UnexpectedToken;

        var ok: ?f64 = null;
        var errmsg: ?[]const u8 = null;
        var codeName: ?[]const u8 = null;
        var code: ?i32 = null;

        blk_tkn: switch (try source.next()) {
            .string => |key| {
                if (ok == null and std.mem.eql(u8, key, "ok")) {
                    const ok_value = try source.next();
                    ok = std.fmt.parseFloat(f64, ok_value.number) catch return error.UnexpectedToken;
                    continue :blk_tkn try source.next();
                }
                if (errmsg == null and std.mem.eql(u8, key, "errmsg")) {
                    const errmsg_value = try source.next();
                    errmsg = try allocator.dupe(u8, errmsg_value.string);
                    continue :blk_tkn try source.next();
                }
                if (codeName == null and std.mem.eql(u8, key, "codeName")) {
                    const codeName_value = try source.next();
                    codeName = try allocator.dupe(u8, codeName_value.string);
                    continue :blk_tkn try source.next();
                }
                if (code == null and std.mem.eql(u8, key, "code")) {
                    const code_value = try source.next();
                    code = std.fmt.parseInt(i32, code_value.number, 10) catch return error.UnexpectedToken;
                    continue :blk_tkn try source.next();
                }
            },
            .object_end => break :blk_tkn,
            else => return error.UnexpectedToken,
        }

        if (ok == null or errmsg == null or codeName == null or code == null) return error.UnexpectedToken;

        return .{
            .ok = ok.?,
            .codeName = codeName,
            .code = code,
            .errmsg = errmsg,
        };
    }

    pub fn dupe(self: *const ErrorResponse, allocator: Allocator) !*ErrorResponse {
        const clone = try allocator.create(ErrorResponse);
        errdefer allocator.destroy(clone);

        clone.ok = self.ok;
        if (self.errmsg) |e| {
            clone.errmsg = try allocator.dupe(u8, e);
        }
        if (self.codeName) |c| {
            clone.codeName = try allocator.dupe(u8, c);
        }

        clone.code = self.code;

        return clone;
    }

    pub fn parseBson(allocator: Allocator, document: *const BsonDocument) !*ErrorResponse {
        return try document.toObject(allocator, ErrorResponse, .{ .ignore_unknown_fields = true });
    }

    pub fn isError(allocator: Allocator, document: *const BsonDocument) !bool {
        const doc_view = try BsonDocumentView.loadDocument(allocator, document);
        return try doc_view.checkElementValue("ok", @as(f64, 0.0));
    }
};
