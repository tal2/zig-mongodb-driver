const std = @import("std");
const bson = @import("bson");
const utils = @import("../utils.zig");
const opcode = @import("../protocol/opcode.zig");

const Allocator = std.mem.Allocator;
const BsonDocument = bson.BsonDocument;
const ServerApi = @import("../server-discovery-and-monitoring/server-info.zig").ServerApi;
const RunCommandOptions = @import("./RunCommandOptions.zig").RunCommandOptions;
const UpdateOptions = @import("./UpdateCommand.zig").UpdateOptions;
const UpdateStatement = @import("./UpdateCommand.zig").UpdateStatement;
const Collation = @import("./Collation.zig").Collation;

pub const JsonParseError = error{UnexpectedToken} || std.json.Scanner.NextError;

// TODO: add support for using different update statements
pub fn makeUpdateManyCommand(
    allocator: std.mem.Allocator,
    collection_name: []const u8,
    filter: anytype,
    update: anytype,
    options: UpdateOptions,
    max_wire_version: ?i32, // maxWireVersion from handshake
    db_name: []const u8,
    server_api: ServerApi,
) !*opcode.OpMsg {
    _ = max_wire_version;
    var filter_parsed = try BsonDocument.fromObject(allocator, @TypeOf(filter), filter);
    defer filter_parsed.deinit(allocator);

    var update_parsed = try BsonDocument.fromObject(allocator, @TypeOf(update), update);
    defer update_parsed.deinit(allocator);

    var command_data: UpdateManyCommand = .{
        .update = collection_name,
        .updates = &[_]UpdateStatement{UpdateStatement{
            .q = filter_parsed.*,
            .u = update_parsed.*,
            .multi = true,
            .upsert = options.upsert,
            .arrayFilters = options.array_filters,
        }},
        .@"$db" = db_name,
        .collation = options.collation,
        .ordered = options.ordered,
        // .writeConcern = options.write_concern,
        .let = options.let,
        // .maxTimeMS = options.max_time_ms,
    };
    server_api.addToCommand(&command_data);
    if (options.run_command_options) |run_command_options| run_command_options.addToCommand(&command_data);

    var command = try BsonDocument.fromObject(allocator, @TypeOf(command_data), command_data);
    errdefer command.deinit(allocator);

    const result = try opcode.OpMsg.init(allocator, command, 1, 0, .{});
    return result;
}

/// @see https://www.mongodb.com/docs/manual/reference/command/update/
const UpdateManyCommand = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    /// The name of the target collection.
    update: []const u8,

    updates: []const UpdateStatement,

    @"$db": []const u8,

    collation: ?Collation = null,

    // hint: ?union(enum) {
    //     string: []const u8,
    //     document: bson.BsonDocument,
    // } = null,

    let: ?bson.BsonDocument = null,

    // comment: ?union(enum) {
    //     string: []const u8,
    //     document: bson.BsonDocument,
    // } = null,

    writeConcern: ?bson.BsonDocument = null,

    maxTimeMS: ?i64 = null,

    ordered: ?bool = null,

    // Must be value of ServerApiVersion.value()
    apiVersion: ?[]const u8 = null,
    apiStrict: ?bool = null,
    apiDeprecationErrors: ?bool = null,

    // RunCommandOptions
    readPreference: ?[]const u8 = null,
    timeoutMS: ?i64 = null,
    // session: ?ClientSession = null,
};
