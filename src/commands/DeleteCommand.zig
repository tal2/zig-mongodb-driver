const std = @import("std");
const bson = @import("bson");
const utils = @import("../utils.zig");
const opcode = @import("../protocol/opcode.zig");

const Allocator = std.mem.Allocator;
const BsonDocument = bson.BsonDocument;
const Limit = @import("types.zig").Limit;
const ServerApi = @import("../server-discovery-and-monitoring/server-info.zig").ServerApi;
const RunCommandOptions = @import("./RunCommandOptions.zig").RunCommandOptions;
const Collation = @import("./Collation.zig").Collation;

pub const JsonParseError = error{UnexpectedToken} || std.json.Scanner.NextError;

pub fn makeDeleteCommand(
    allocator: std.mem.Allocator,
    collection_name: []const u8,
    filter: anytype,
    limit: Limit,
    options: DeleteOptions,
    max_wire_version: ?i32, // maxWireVersion from handshake
    db_name: []const u8,
    server_api: ServerApi,
) !*opcode.OpMsg {
    _ = max_wire_version;
    var filter_parsed = try BsonDocument.fromObject(allocator, @TypeOf(filter), filter);
    defer filter_parsed.deinit(allocator);

    var command_data: DeleteCommand = .{
        .delete = collection_name,
        .deletes = &[_]DeleteStatement{DeleteStatement{
            .q = filter_parsed.*,
            .limit = switch (limit) {
                .all => 0,
                .one => 1,
            },
        }},
        .@"$db" = db_name,
        .ordered = options.ordered,
        .writeConcern = options.writeConcern,
        // .comment = options.comment,
        .let = options.let,
        // .raw_data =  opts.rawData,
        .maxTimeMS = options.maxTimeMS,
    };
    server_api.addToCommand(&command_data);
    if (options.run_command_options) |run_command_options| run_command_options.addToCommand(&command_data);

    var command = try BsonDocument.fromObject(allocator, @TypeOf(command_data), command_data);
    errdefer command.deinit(allocator);

    const result = try opcode.OpMsg.init(allocator, command, 1, 0, .{});
    return result;
}

/// @see https://www.mongodb.com/docs/manual/reference/command/delete/
const DeleteCommand = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    delete: []const u8,
    deletes: []const DeleteStatement,
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

    // /// @since MongoDB 8.2
    // raw_data: ?bool = null,

    writeConcern: ?bson.BsonDocument = null,

    maxTimeMS: ?i64 = null,

    ordered: ?bool = null,

    /// Must be value of ServerApiVersion.value()
    apiVersion: ?[]const u8 = null,
    apiStrict: ?bool = null,
    apiDeprecationErrors: ?bool = null,

    // RunCommandOptions
    readPreference: ?[]const u8 = null,
    timeoutMS: ?i64 = null,
    // session: ?ClientSession = null,
};

pub const DeleteStatement = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    q: BsonDocument,

    limit: i32,

    collation: ?Collation = null,

    // hint: ?union(enum) {
    //     string: []const u8,
    //     document: bson.BsonDocument,
    // } = null,
};

pub const DeleteCommandResponse = struct {
    acknowledged: ?bool = null,

    n: i64,

    pub fn deinit(self: *const DeleteCommandResponse, allocator: Allocator) void {
        allocator.destroy(self);
    }

    pub fn jsonParse(allocator: Allocator, source: *std.json.Scanner, options: std.json.ParseOptions) JsonParseError!DeleteCommandResponse {
        _ = allocator;
        _ = options;
        if (try source.next() != .object_begin) return error.UnexpectedToken;

        var ok: ?f64 = null;
        var n: ?i64 = null; // Number of documents deleted
        blk_tkn: switch (try source.next()) {
            .string => |key| {
                if (ok == null and std.mem.eql(u8, key, "ok")) {
                    const ok_value = try source.next();
                    ok = std.fmt.parseFloat(f64, ok_value.number) catch return error.UnexpectedToken;
                    continue :blk_tkn try source.next();
                }
                if (n == null and std.mem.eql(u8, key, "n")) {
                    const n_value = try source.next();
                    n = std.fmt.parseInt(i64, n_value.number, 10) catch return error.UnexpectedToken;
                    continue :blk_tkn try source.next();
                }
            },
            .object_end => break :blk_tkn,
            else => return error.UnexpectedToken,
        }

        if (ok == null or n == null) return error.UnexpectedToken;

        return .{
            .n = n.?,
        };
    }

    pub fn dupe(self: *const DeleteCommandResponse, allocator: Allocator) !*DeleteCommandResponse {
        const clone = try allocator.create(DeleteCommandResponse);
        errdefer clone.deinit(allocator);

        clone.acknowledged = self.acknowledged;
        clone.n = self.n;

        return clone;
    }

    pub fn parseBson(allocator: Allocator, document: *const BsonDocument) !*DeleteCommandResponse {
        return try document.toObject(allocator, DeleteCommandResponse, .{ .ignore_unknown_fields = true });
    }
};

pub const DeleteOptions = struct {
    run_command_options: ?RunCommandOptions = null,

    collation: ?Collation = null,

    //  hint: ?union(enum) {
    //     string: []const u8,
    //     document: bson.BsonDocument,
    // } = null,

    let: ?bson.BsonDocument = null,
    // comment: ?union(enum) {
    //     string: []const u8,
    //     document: bson.BsonDocument,
    // } = null,
    // /// @since MongoDB 8.2
    // raw_data: ?bool = null,

    writeConcern: ?bson.BsonDocument = null,

    maxTimeMS: ?i64 = null,

    ordered: ?bool = null,
};
