const std = @import("std");
const bson = @import("bson");
const utils = @import("../utils.zig");
const opcode = @import("../protocol/opcode.zig");
const CursorInfo = @import("CursorInfo.zig").CursorInfo;
const RunCommandOptions = @import("./RunCommandOptions.zig").RunCommandOptions;
const ServerApi = @import("../server-discovery-and-monitoring/server-info.zig").ServerApi;
const Comment = @import("../protocol/comment.zig").Comment;

const Allocator = std.mem.Allocator;
const BsonDocument = bson.BsonDocument;

pub fn makeGetMoreCommand(
    allocator: std.mem.Allocator,
    collection_name: []const u8,
    cursor_id: i64,
    options: GetMoreCommandOptions,
    db_name: []const u8,
    server_api: ServerApi,
) !*opcode.OpMsg {
    var command_data: GetMoreCommand = .{
        .getMore = cursor_id,
        .@"$db" = db_name,
        .collection = collection_name,
        .batchSize = options.batchSize,
        .maxTimeMS = options.maxTimeMS,
        .comment = options.comment,
    };
    if (options.run_command_options) |run_command_options| run_command_options.addToCommand(&command_data);
    server_api.addToCommand(&command_data);

    var command = try BsonDocument.fromObject(allocator, @TypeOf(command_data), command_data);
    errdefer command.deinit(allocator);

    const result = try opcode.OpMsg.init(allocator, command, 1, 0, .{});
    return result;
}

const GetMoreCommand = struct {
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

    pub fn parseBson(allocator: std.mem.Allocator, bson_document: *const BsonDocument) !*GetMoreCommandResponse {
        return try utils.parseBsonToOwned(GetMoreCommandResponse, allocator, bson_document);
    }
};
