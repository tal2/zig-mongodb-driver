const std = @import("std");
const bson = @import("bson");
const CursorInfo = @import("CursorInfo.zig").CursorInfo;
const RunCommandOptions = @import("./RunCommandOptions.zig").RunCommandOptions;
const Comment = @import("../protocol/comment.zig").Comment;

const Allocator = std.mem.Allocator;
const BsonDocument = bson.BsonDocument;

pub const GetMoreCommand = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    /// The cursor identifier.
    getMore: i64,

    collection: []const u8,

    @"$db": []const u8,

    batchSize: ?i32 = null,

    maxTimeMS: ?i64 = null,

    comment: ?Comment = null,

    apiVersion: ?[]const u8 = null,
    apiStrict: ?bool = null,
    apiDeprecationErrors: ?bool = null,

    readPreference: ?[]const u8 = null,
    timeoutMS: ?i64 = null,

    pub fn deinit(self: *GetMoreCommand, allocator: Allocator) void {
        if (self.comment) |comment| {
            if (comment == .document) {
                comment.document.deinit(allocator);
            }
        }
    }

    pub fn make(
        collection_name: []const u8,
        db_name: []const u8,
        cursor_id: i64,
        options: GetMoreCommandOptions,
    ) GetMoreCommand {
        return .{
            .getMore = cursor_id,
            .@"$db" = db_name,
            .collection = collection_name,
            .batchSize = options.batchSize,
            .maxTimeMS = options.maxTimeMS,
            .comment = options.comment,
        };
    }
};

pub const GetMoreCommandOptions = struct {
    run_command_options: ?RunCommandOptions = null,

    batchSize: ?i32 = null,

    maxTimeMS: ?i64 = null,

    comment: ?Comment = null,
};

pub const GetMoreCommandResponse = struct {
    ok: i32,
    cursor: CursorInfo,

    pub fn deinit(self: *GetMoreCommandResponse, allocator: Allocator) void {
        self.cursor.deinit(allocator);
        allocator.destroy(self);
    }

    pub fn dupe(self: *const GetMoreCommandResponse, allocator: Allocator) !*GetMoreCommandResponse {
        var clone = try allocator.create(GetMoreCommandResponse);
        errdefer allocator.destroy(clone);

        clone.ok = self.ok;
        clone.cursor = try self.cursor.dupe(allocator);

        return clone;
    }

    pub fn parseBson(allocator: std.mem.Allocator, document: *const BsonDocument) !*GetMoreCommandResponse {
        // return try utils.parseBsonToOwned(GetMoreCommandResponse, allocator, bson_document);
        return try document.toObject(allocator, GetMoreCommandResponse, .{ .ignore_unknown_fields = true });
    }
};
