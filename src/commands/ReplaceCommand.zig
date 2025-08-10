const std = @import("std");
const bson = @import("bson");
const utils = @import("../utils.zig");
const opcode = @import("../protocol/opcode.zig");

const Allocator = std.mem.Allocator;
const BsonDocument = bson.BsonDocument;
const ServerApi = @import("../server-discovery-and-monitoring/server-info.zig").ServerApi;
const RunCommandOptions = @import("./RunCommandOptions.zig").RunCommandOptions;
const Collation = @import("./Collation.zig").Collation;

pub const JsonParseError = error{UnexpectedToken} || std.json.Scanner.NextError;

pub fn makeReplaceCommand(
    allocator: std.mem.Allocator,
    collection_name: []const u8,
    filter: anytype,
    replacement: anytype,
    options: ReplaceOptions,
    max_wire_version: ?i32, // maxWireVersion from handshake
    db_name: []const u8,
    server_api: ServerApi,
) !*opcode.OpMsg {
    _ = max_wire_version;
    var filter_parsed = try BsonDocument.fromObject(allocator, @TypeOf(filter), filter);
    defer filter_parsed.deinit(allocator);

    var replacement_parsed = try BsonDocument.fromObject(allocator, @TypeOf(replacement), replacement);
    defer replacement_parsed.deinit(allocator);

    var command_data: ReplaceCommand = .{
        .update = collection_name,
        .updates = &[_]ReplaceStatement{ReplaceStatement{
            .q = filter_parsed.*,
            .u = replacement_parsed.*,
            .multi = false,
            .upsert = options.upsert,
        }},
        .@"$db" = db_name,
        // .ordered = true,
        // .writeConcern = null,
        .let = options.let,
        // .maxTimeMS = null,
    };
    server_api.addToCommand(&command_data);
    if (options.run_command_options) |run_command_options| run_command_options.addToCommand(&command_data);

    var command = try BsonDocument.fromObject(allocator, @TypeOf(command_data), command_data);
    errdefer command.deinit(allocator);

    const result = try opcode.OpMsg.init(allocator, command, 1, 0, .{});
    return result;
}

/// @see https://www.mongodb.com/docs/manual/reference/command/update/
const ReplaceCommand = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    update: []const u8,

    updates: []const ReplaceStatement,

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

pub const ReplaceStatement = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    /// The query that matches documents to update.
    q: BsonDocument,

    /// The update document or pipeline that specifies the modifications to apply.
    u: BsonDocument,

    multi: bool,

    upsert: ?bool = null,

    collation: ?Collation = null,

    // hint: ?union(enum) {
    //     string: []const u8,
    //     document: bson.BsonDocument,
    // } = null,

    sort: ?bson.BsonDocument = null,
};

pub const ReplaceCommandResponse = struct {
    // acknowledged: ?bool = null,

    n: i64,

    nModified: i64,

    nUpserted: ?i64 = null,

    upserted: ?bson.BsonDocument = null,

    pub fn deinit(self: *const ReplaceCommandResponse, allocator: Allocator) void {
        if (self.upserted) |upserted| {
            upserted.deinit(allocator);
        }
        allocator.destroy(self);
    }

    pub fn jsonParse(allocator: Allocator, source: *std.json.Scanner, options: std.json.ParseOptions) JsonParseError!ReplaceCommandResponse {
        _ = allocator;
        _ = options;
        if (try source.next() != .object_begin) return error.UnexpectedToken;

        var ok: ?f64 = null;
        var n: ?i64 = null;
        var nModified: ?i64 = null;
        var nUpserted: ?i64 = null;
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
                if (nModified == null and std.mem.eql(u8, key, "nModified")) {
                    const nModified_value = try source.next();
                    nModified = std.fmt.parseInt(i64, nModified_value.number, 10) catch return error.UnexpectedToken;
                    continue :blk_tkn try source.next();
                }
                if (nUpserted == null and std.mem.eql(u8, key, "nUpserted")) {
                    const nUpserted_value = try source.next();
                    nUpserted = std.fmt.parseInt(i64, nUpserted_value.number, 10) catch return error.UnexpectedToken;
                    continue :blk_tkn try source.next();
                }
            },
            .object_end => break :blk_tkn,
            else => return error.UnexpectedToken,
        }

        if (ok == null or n == null or nModified == null) return error.UnexpectedToken;

        return .{
            .n = n.?,
            .nModified = nModified.?,
            .nUpserted = nUpserted,
            .upserted = null,
        };
    }

    pub fn dupe(self: *const ReplaceCommandResponse, allocator: Allocator) !*ReplaceCommandResponse {
        const clone = try allocator.create(ReplaceCommandResponse);
        errdefer clone.deinit(allocator);

        // clone.acknowledged = self.acknowledged;
        clone.n = self.n;
        clone.nModified = self.nModified;
        clone.nUpserted = self.nUpserted;
        clone.upserted = self.upserted;

        return clone;
    }

    pub fn parseBson(allocator: Allocator, document: *const BsonDocument) !*ReplaceCommandResponse {
        return try document.toObject(allocator, ReplaceCommandResponse, .{ .ignore_unknown_fields = true });
    }
};

pub const ReplaceOptions = struct {
    run_command_options: ?RunCommandOptions = null,

    bypass_document_validation: ?bool = null,

    collation: ?bson.BsonDocument = null,

    // hint: ?union(enum) {
    //     string: []const u8,
    //     document: bson.BsonDocument,
    // } = null,

    upsert: ?bool = null,

    let: ?bson.BsonDocument = null,

    comment: ?bson.BsonDocument = null,

    sort: ?bson.BsonDocument = null,

    // /// @since MongoDB 8.2
    // raw_data: ?bool = null,
};
