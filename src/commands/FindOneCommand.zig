const std = @import("std");
const bson = @import("bson");
const opcode = @import("../protocol/opcode.zig");
const BsonDocument = bson.BsonDocument;
const FindCommand = @import("FindCommand.zig").FindCommand;
const ServerApi = @import("../server-discovery-and-monitoring/server-info.zig").ServerApi;
const RunCommandOptions = @import("./RunCommandOptions.zig").RunCommandOptions;

pub fn makeFindOneCommand(
    allocator: std.mem.Allocator,
    collection_name: []const u8,
    filter: anytype,
    options: FindOneOptions,
    server_version: ?i32, // maxWireVersion from handshake
    db_name: []const u8,
    server_api: ServerApi,
) !*opcode.OpMsg {
    _ = server_version;
    const filter_parsed = try bson.BsonDocument.fromObject(allocator, @TypeOf(filter), filter);
    errdefer filter_parsed.deinit(allocator);

    var command_data: FindCommand = .{
        .find = collection_name,
        .filter = filter_parsed,
        .@"$db" = db_name,
        .limit = 1,
        .singleBatch = true,
        .allowDiskUse = options.allowDiskUse,
        .allowPartialResults = options.allowPartialResults,
        .returnKey = options.returnKey,
        .showRecordId = options.showRecordId,
        .snapshot = options.snapshot,
        .maxTimeMS = options.maxTimeMS,
        .oplogReplay = options.oplogReplay,
        .projection = options.projection,
        .sort = options.sort,
        .let = options.let,
        .maxScan = options.maxScan,
        .maxAwaitTimeMS = options.maxAwaitTimeMS,
        .min = options.min,
        .collation = options.collation,
        // .comment = options.comment,
        // .hint = options.hint,

    };
    server_api.addToCommand(&command_data);
    if (options.run_command_options) |run_command_options| run_command_options.addToCommand(&command_data);

    var command = try BsonDocument.fromObject(allocator, @TypeOf(command_data), command_data);
    errdefer command.deinit(allocator);

    const result = try opcode.OpMsg.init(allocator, command, 1, 0, .{});
    return result;
}

pub const FindOneOptions = struct {
    run_command_options: ?RunCommandOptions = null,
    allowDiskUse: ?bool = null,
    allowPartialResults: ?bool = null,

    collation: ?bson.BsonDocument = null,
    // comment: ?union(enum) { // TODO:
    //     string: []const u8,
    //     document: bson.BsonDocument,
    // } = null,

    // hint: ?union(enum) { // TODO:
    //     string: []const u8,
    //     document: bson.BsonDocument,
    // } = null,

    limit: ?i64 = null,

    max: ?bson.BsonDocument = null,

    maxAwaitTimeMS: ?i64 = null,

    /// @deprecated 4.0
    maxScan: ?i64 = null,

    // /// NOTE: This option is deprecated in favor of timeoutMS.
    // maxTimeMS: ?i64 = null,

    min: ?bson.BsonDocument = null,

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
};
