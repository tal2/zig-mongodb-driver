const std = @import("std");
const bson = @import("bson");
const utils = @import("../utils.zig");
const opcode = @import("../protocol/opcode.zig");

const CursorInfo = @import("./CursorInfo.zig").CursorInfo;
const ServerApi = @import("../server-discovery-and-monitoring/server-info.zig").ServerApi;
const commands = @import("./root.zig");
const FindOptions = commands.FindOptions;
const Hint = @import("../protocol/hint.zig").Hint;
const Comment = @import("../protocol/comment.zig").Comment;

const Allocator = std.mem.Allocator;
const BsonDocument = bson.BsonDocument;

pub fn makeAggregateCommand(
    allocator: std.mem.Allocator,
    collection_name: []const u8,
    pipeline: anytype,
    options: FindOptions,
    cursor_options: CursorOptions,
    max_wire_version: ?i32,
    db_name: []const u8,
    server_api: ServerApi,
) !*opcode.OpMsg {
    _ = max_wire_version;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    comptime {
        const pipeline_type_info = @typeInfo(@TypeOf(pipeline));
        if (pipeline_type_info != .array and (pipeline_type_info != .pointer or pipeline_type_info.pointer.size != .slice)) {
            @compileLog(pipeline_type_info);
            @compileLog(@tagName(pipeline_type_info));
            @compileError("pipeline must be an array or a pointer to an array");
        }
    }

    var pipeline_parsed = std.ArrayList(*bson.BsonDocument).init(arena_allocator);

    for (pipeline) |stage| {
        if (@TypeOf(stage) == *bson.BsonDocument) {
            try pipeline_parsed.append(stage);
        } else {
            const stage_parsed = try bson.BsonDocument.fromObject(arena_allocator, @TypeOf(stage), stage);
            try pipeline_parsed.append(stage_parsed);
        }
    }

    const cursor_parsed = try bson.BsonDocument.fromObject(arena_allocator, CursorOptions, cursor_options);

    var command = AggregateCommand{
        .aggregate = collection_name,
        .@"$db" = db_name,
        .pipeline = try pipeline_parsed.toOwnedSlice(),
        .allowDiskUse = options.allowDiskUse,
        .batchSize = options.batchSize,
        // .bypassDocumentValidation = options.bypassDocumentValidation,
        // .readConcern = options.readConcern,
        .collation = options.collation,
        // .comment = options.comment,
        // .writeConcern = options.writeConcern,
        .maxTimeMS = options.maxTimeMS,
        .let = options.let,
        .cursor = cursor_parsed,
    };
    server_api.addToCommand(&command);
    if (options.run_command_options) |run_command_options| run_command_options.addToCommand(&command);

    const command_document = try bson.BsonDocument.fromObject(allocator, @TypeOf(command), command);
    errdefer command_document.deinit(allocator);

    return try opcode.OpMsg.init(allocator, command_document, 2, 0, .{});
}

pub const AggregateCommand = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    pub fn deinit(self: *const AggregateCommand, allocator: std.mem.Allocator) void {
        for (self.pipeline) |stage| {
            stage.deinit(allocator);
        }
        allocator.free(self.pipeline);
        self.cursor.deinit(allocator);
        allocator.destroy(self);
    }

    aggregate: []const u8, // TODO: make union of string and i32

    @"$db": []const u8,

    pipeline: []const *bson.BsonDocument,

    explain: ?bool = null,

    cursor: *bson.BsonDocument,

    allowDiskUse: ?bool = null,

    batchSize: ?i32 = null,

    bypassDocumentValidation: ?bool = null,

    readConcern: ?bson.BsonDocument = null,

    collation: ?bson.BsonDocument = null,

    hint: ?Hint = null,

    comment: ?Comment = null,

    writeConcern: ?bson.BsonDocument = null,

    maxTimeMS: ?i64 = null,

    let: ?bson.BsonDocument = null,

    /// Must be value of ServerApiVersion.value()
    apiVersion: ?[]const u8 = null,
    apiStrict: ?bool = null,
    apiDeprecationErrors: ?bool = null,

    readPreference: ?[]const u8 = null,
    timeoutMS: ?i64 = null,
};

pub const CursorOptions = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    batchSize: ?i32 = null,
    // maxTimeMS: ?i64 = null,
    // maxAwaitTimeMS: ?i64 = null,
    // readPreference: ?bson.BsonDocument = null,
    // readConcern: ?bson.BsonDocument = null,
    // collation: ?bson.BsonDocument = null,
};

pub const AggregateCommandResponse = struct {
    ok: f64,
    cursor: *CursorInfo,

    pub fn deinit(self: *const AggregateCommandResponse, allocator: std.mem.Allocator) void {
        self.cursor.deinit(allocator);
        self.deinit();
    }

    pub fn dupe(self: *const AggregateCommandResponse, allocator: Allocator) !*AggregateCommandResponse {
        var clone = try allocator.create(AggregateCommandResponse);
        errdefer allocator.destroy(clone);

        clone.ok = self.ok;
        clone.cursor = try self.cursor.dupe(allocator);

        return clone;
    }

    pub fn parseBson(allocator: Allocator, document: *const BsonDocument) !*AggregateCommandResponse {
        return try utils.parseBsonToOwned(AggregateCommandResponse, allocator, document);
    }
};

pub const AggregateOptions = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    allowDiskUse: ?bool = null,

    batchSize: ?i32 = null,

    bypassDocumentValidation: ?bool = null,

    readConcern: ?bson.BsonDocument = null,

    collation: ?bson.BsonDocument = null,

    /// NOTE: This option is deprecated in favor of timeoutMS.
    maxTimeMS: ?i64 = null,

    comment: ?Comment = null,

    hint: ?Hint = null,

    writeConcern: ?bson.BsonDocument = null,

    let: ?bson.BsonDocument = null,
};
