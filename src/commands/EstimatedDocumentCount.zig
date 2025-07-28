const std = @import("std");
const bson = @import("bson");
const opcode = @import("../protocol/opcode.zig");

const Allocator = std.mem.Allocator;
const BsonDocument = bson.BsonDocument;
const CountCommand = @import("./CountCommand.zig").CountCommand;
const ServerApi = @import("../server-discovery-and-monitoring/server-info.zig").ServerApi;
const RunCommandOptions = @import("./RunCommandOptions.zig").RunCommandOptions;

pub const JsonParseError = error{UnexpectedToken} || std.json.Scanner.NextError;

pub fn makeEstimatedDocumentCount(
    allocator: std.mem.Allocator,
    collection_name: []const u8,
    options: EstimatedDocumentCountOptions,
    max_wire_version: ?i32, // maxWireVersion from handshake
    db_name: []const u8,
    server_api: ServerApi,
) !*opcode.OpMsg {
    _ = max_wire_version;
    var command_data: CountCommand = .{
        .count = collection_name,
        .@"$db" = db_name,
        // .comment = options.comment,
    };
    server_api.addToCommand(&command_data);
    if (options.run_command_options) |run_command_options| run_command_options.addToCommand(&command_data);

    var command = try BsonDocument.fromObject(allocator, @TypeOf(command_data), command_data);
    errdefer command.deinit(allocator);

    const result = try opcode.OpMsg.init(allocator, command, 1, 0, .{});
    return result;
}

pub const EstimatedDocumentCountOptions = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    run_command_options: ?RunCommandOptions = null,

    // /// NOTE: This option is deprecated in favor of timeoutMS.
    // maxTimeMS: ?i64 = null,

    // comment: ?union(enum) {
    //     string: []const u8,
    //     document: bson.BsonDocument,
    // } = null,

    // /// @since MongoDB 8.2
    // rawData: ?bool = null,
};
