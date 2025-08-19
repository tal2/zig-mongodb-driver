const std = @import("std");
const bson = @import("bson");
const CursorInfo = @import("CursorInfo.zig").CursorInfo;
const RunCommandOptions = @import("./RunCommandOptions.zig").RunCommandOptions;
const Hint = @import("../protocol/hint.zig").Hint;
const Comment = @import("../protocol/comment.zig").Comment;

const Allocator = std.mem.Allocator;
const BsonDocument = bson.BsonDocument;
const LimitNumbered = @import("./types.zig").LimitNumbered;

pub const FindCommand = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    find: []const u8,

    @"$db": []const u8,

    filter: *BsonDocument,

    singleBatch: ?bool = null,

    tailable: ?bool = null,

    awaitData: ?bool = null,

    // maxTimeMS: ?u64,

    allowDiskUse: ?bool = null,

    allowPartialResults: ?bool = null,

    batchSize: ?i32 = null,

    collation: ?bson.BsonDocument = null,

    comment: ?Comment = null,

    // cursorType: ?types.CursorType = null,

    hint: ?Hint = null,
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

    pub fn deinit(self: *const FindCommand, allocator: Allocator) void {
        self.filter.deinit(allocator);
        if (self.collation) |collation| {
            collation.deinit(allocator);
        }
        if (self.hint) |hint| {
            if (hint == .document) {
                hint.document.deinit(allocator);
            }
        }
        if (self.max) |max| {
            max.deinit(allocator);
        }
        if (self.min) |min| {
            min.deinit(allocator);
        }
    }

    pub fn make(
        allocator: std.mem.Allocator,
        collection_name: []const u8,
        db_name: []const u8,
        filter: anytype,
        limit: LimitNumbered,
        options: FindOptions,
    ) !FindCommand {
        const batch_size = switch (limit) {
            .all => options.batchSize orelse null,
            .one => 1,
            .n => |n| @as(i32, @intCast(@min(options.batchSize orelse std.math.maxInt(i32), n +| 1))),
        };

        const single_batch = switch (limit) {
            .all => null,
            .one => true,
            .n => |num| num < 0,
        };

        const filter_parsed = try bson.BsonDocument.fromObject(allocator, @TypeOf(filter), filter);
        errdefer filter_parsed.deinit(allocator);

        var command: FindCommand = .{
            .find = collection_name,
            .@"$db" = db_name,
            .filter = filter_parsed,
            .batchSize = batch_size,
            .singleBatch = single_batch,
            .limit = switch (limit) {
                .all => null,
                .one => 1,
                .n => |num| @as(i64, @intCast(@abs(num))),
            },
        };
        options.addToCommand(&command);

        return command;
    }

    pub fn makeFindOne(
        allocator: std.mem.Allocator,
        collection_name: []const u8,
        db_name: []const u8,
        filter: anytype,
        options: FindOptions,
    ) !FindCommand {
        const filter_parsed = try bson.BsonDocument.fromObject(allocator, @TypeOf(filter), filter);
        errdefer filter_parsed.deinit(allocator);

        var command: FindCommand = .{
            .find = collection_name,
            .@"$db" = db_name,
            .filter = filter_parsed,
            .limit = 1,
        };
        options.addToCommand(&command);

        return command;
    }
};

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

    comment: ?Comment = null,

    // cursorType: ?types.CursorType = null,

    hint: ?Hint = null,

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

    pub fn addToCommand(self: *const FindOptions, command_data: *FindCommand) void {
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

        if (self.hint) |hint| command_data.hint = hint;
        if (self.comment) |comment| command_data.comment = comment;
    }
};
