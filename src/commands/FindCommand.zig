const std = @import("std");
const bson = @import("bson");
const utils = @import("../utils.zig");
const opcode = @import("../protocol/opcode.zig");
const CursorInfo = @import("CursorInfo.zig").CursorInfo;
const ServerApi = @import("../server-discovery-and-monitoring/server-info.zig").ServerApi;
const RunCommandOptions = @import("./RunCommandOptions.zig").RunCommandOptions;

const Allocator = std.mem.Allocator;
const BsonDocument = bson.BsonDocument;
const parseBsonDocument = utils.parseBsonDocument;
const LimitNumbered = @import("./types.zig").LimitNumbered;

pub fn makeFindCommand(
    allocator: std.mem.Allocator,
    collection_name: []const u8,
    filter: anytype,
    limit: LimitNumbered,
    options: FindOptions,
    server_version: ?i32, // maxWireVersion from handshake,
    db_name: []const u8,
    server_api: ServerApi,
) !*opcode.OpMsg {
    const batch_size = switch (limit) {
        .all => options.batchSize orelse null,
        .one => 1,
        .n => |n| @as(i32, @intCast(@min(options.batchSize orelse std.math.maxInt(i32), n +| 1))),
    };

    const server_version_before_3_2 = server_version != null and server_version.? < 4;

    const single_batch = if (server_version_before_3_2) null else switch (limit) {
        .all => null,
        .one => true,
        .n => |num| num < 0,
    };

    const filter_parsed = try bson.BsonDocument.fromObject(allocator, @TypeOf(filter), filter);
    errdefer filter_parsed.deinit(allocator);

    var command_data: FindCommand = .{
        .find = collection_name,
        .filter = filter_parsed,
        .@"$db" = db_name,
        .limit = switch (limit) {
            .all => null,
            .one => 1,
            .n => |num| if (server_version_before_3_2) num else @as(i64, @intCast(@abs(num))),
        },
        .singleBatch = single_batch,
        .batchSize = batch_size,
        // .tailable = options.tailable,
        // .awaitData = options.await_data,
        // .allowDiskUse = options.allow_disk_use,
        // .allowPartialResults = options.allow_partial_results,
        // .comment = options.comment,
        // .cursorType = options.cursor_type,

    };
    options.addToCommand(&command_data);
    server_api.addToCommand(&command_data);

    var command = try BsonDocument.fromObject(allocator, @TypeOf(command_data), command_data);
    errdefer command.deinit(allocator);

    const result = try opcode.OpMsg.init(allocator, command, 1, 0, .{});
    return result;
}

pub const FindCommand = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    find: []const u8,

    @"$db": []const u8,

    filter: *BsonDocument,

    // limit: ?i64,

    // batchSize: ?i32,

    singleBatch: ?bool = null,

    tailable: ?bool = null,

    awaitData: ?bool = null,

    // maxTimeMS: ?u64,

    allowDiskUse: ?bool = null,

    allowPartialResults: ?bool = null,

    batchSize: ?i32 = null,

    collation: ?bson.BsonDocument = null,

    comment: ?union(enum) { // TODO:
        string: []const u8,
        document: bson.BsonDocument,
    } = null,

    // cursorType: ?types.CursorType = null,

    hint: ?union(enum) { // TODO:
        string: []const u8,
        document: bson.BsonDocument,
    } = null,

    limit: ?i64 = null,

    max: ?bson.BsonDocument = null,

    maxAwaitTimeMS: ?i64 = null,

    /// @deprecated 4.0
    maxScan: ?i64 = null,

    /// NOTE: This option is deprecated in favor of timeoutMS.
    maxTimeMS: ?i64 = null,

    min: ?bson.BsonDocument = null,

    noCursorTimeout: ?bool = null,

    /// @deprecated 4.4
    oplogReplay: ?bool = null,

    projection: ?bson.BsonDocument = null,

    returnKey: ?bool = null,

    showRecordId: ?bool = null,

    skip: ?i64 = null,

    /// @deprecated 4.0
    snapshot: ?bool = null,

    sort: ?bson.BsonDocument = null,

    let: ?bson.BsonDocument = null,

    // /// @since MongoDB 8.2
    // rawData: ?bool = null,

    // Must be value of ServerApiVersion.value()
    apiVersion: ?[]const u8 = null,
    apiStrict: ?bool = null,
    apiDeprecationErrors: ?bool = null,

    // RunCommandOptions
    readPreference: ?[]const u8 = null,
    timeoutMS: ?i64 = null,
    // session: ?ClientSession = null,
};

fn calculateFirstNumberToReturn(limit: ?i64, batch_size: ?i32) i32 {
    const limit_val = limit orelse 0;
    const batch_size_val = batch_size orelse 0;

    if (limit_val < 0) {
        return @intCast(limit_val);
    } else if (limit_val == 0) {
        return batch_size_val;
    } else if (batch_size_val == 0) {
        return @intCast(limit_val);
    } else if (limit_val < batch_size_val) {
        return @intCast(limit_val);
    } else {
        return batch_size_val;
    }
}

pub const FindCommandResponse = struct {
    ok: f64,
    cursor: *CursorInfo,
    waitedMS: ?i64 = null,

    pub fn deinit(self: *const FindCommandResponse, allocator: Allocator) void {
        self.cursor.deinit(allocator);
        allocator.destroy(self);
    }

    pub fn first(self: *const FindCommandResponse) ?*BsonDocument {
        if (self.cursor.firstBatch == null) return null;
        if (self.cursor.firstBatch.?.len == 0) return null;
        const doc = self.cursor.firstBatch.?[0];
        return doc;
    }

    pub fn firstAs(self: *const FindCommandResponse, T: type, allocator: Allocator) !?*T {
        if (self.cursor.firstBatch == null) return null;
        if (self.cursor.firstBatch.?.len == 0) return null;
        const doc = self.cursor.firstBatch.?[0];
        return try doc.toObject(allocator, T, .{ .ignore_unknown_fields = true });
    }





    pub fn dupe(self: *const FindCommandResponse, allocator: Allocator) !*FindCommandResponse {
        const response = try allocator.create(FindCommandResponse);
        errdefer response.deinit(allocator);

        response.cursor = try self.cursor.dupe(allocator);
        response.ok = self.ok;
        response.waitedMS = self.waitedMS;

        return response;
    }

    pub fn parseBson(allocator: Allocator, document: *const BsonDocument) !*FindCommandResponse {
        return try document.toObject(allocator, FindCommandResponse, .{ .ignore_unknown_fields = true });
    }
};

pub const FindOptions = struct {
    run_command_options: ?RunCommandOptions = null,

    allowDiskUse: ?bool = null,

    allowPartialResults: ?bool = null,

    batchSize: ?i32 = null,

    collation: ?bson.BsonDocument = null,

    comment: ?union(enum) { // TODO:
        string: []const u8,
        document: bson.BsonDocument,
    } = null,
    // cursorType: ?types.CursorType = null,

    hint: ?union(enum) { // TODO:
        string: []const u8,
        document: bson.BsonDocument,
    } = null,

    limit: ?i64 = null,

    max: ?bson.BsonDocument = null,

    maxAwaitTimeMS: ?i64 = null,

    /// @deprecated 4.0
    maxScan: ?i64 = null,

    /// NOTE: This option is deprecated in favor of timeoutMS.
    maxTimeMS: ?i64 = null,

    min: ?bson.BsonDocument = null,

    noCursorTimeout: ?bool = null,

    /// @deprecated 4.4
    oplogReplay: ?bool = null,

    projection: ?bson.BsonDocument = null,

    returnKey: ?bool = null,

    showRecordId: ?bool = null,

    skip: ?i64 = null,

    /// @deprecated 4.0
    snapshot: ?bool = null,

    sort: ?bson.BsonDocument = null,

    let: ?bson.BsonDocument = null,

    // /// @since MongoDB 8.2
    // rawData: ?bool = null,

    fn addToCommand(self: *const FindOptions, command_data: *FindCommand) void {
        if (self.run_command_options) |run_command_options| run_command_options.addToCommand(command_data);

        if (self.allowDiskUse) |allowDiskUse| command_data.allowDiskUse = allowDiskUse;
        if (self.allowPartialResults) |allowPartialResults| command_data.allowPartialResults = allowPartialResults;
        if (self.returnKey) |returnKey| command_data.returnKey = returnKey;
        if (self.showRecordId) |showRecordId| command_data.showRecordId = showRecordId;
        if (self.snapshot) |snapshot| command_data.snapshot = snapshot;
        if (self.maxTimeMS) |maxTimeMS| command_data.maxTimeMS = maxTimeMS;
        if (self.oplogReplay) |oplogReplay| command_data.oplogReplay = oplogReplay;
        if (self.projection) |projection| command_data.projection = projection;
        if (self.sort) |sort| command_data.sort = sort;
        if (self.let) |let| command_data.let = let;
        if (self.maxScan) |maxScan| command_data.maxScan = maxScan;
        if (self.maxAwaitTimeMS) |maxAwaitTimeMS| command_data.maxAwaitTimeMS = maxAwaitTimeMS;
        if (self.min) |min| command_data.min = min;
        if (self.collation) |collation| command_data.collation = collation;

        if (self.hint) |hint| {
            switch (hint) {
                .document => |document| {
                    command_data.hint = .{ .document = document };
                },
                .string => |string| {
                    command_data.hint = .{ .string = string };
                },
            }
        }
        if (self.comment) |comment| {
            switch (comment) {
                .document => |document| {
                    command_data.comment = .{ .document = document };
                },
                .string => |string| {
                    command_data.comment = .{ .string = string };
                },
            }
        }
    }
};
