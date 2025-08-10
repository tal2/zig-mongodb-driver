const std = @import("std");
const bson = @import("bson");
const utils = @import("../utils.zig");
const opcode = @import("../protocol/opcode.zig");

const Allocator = std.mem.Allocator;
const BsonDocument = bson.BsonDocument;
const ServerApiVersion = @import("../server-discovery-and-monitoring/server-info.zig").ServerApiVersion;
const ServerApi = @import("../server-discovery-and-monitoring/server-info.zig").ServerApi;
const RunCommandOptions = @import("./RunCommandOptions.zig").RunCommandOptions;

pub const JsonParseError = error{UnexpectedToken} || std.json.Scanner.NextError;

pub fn makeCountCommand(
    allocator: std.mem.Allocator,
    collection_name: []const u8,
    filter: anytype,
    max_wire_version: ?i32, // maxWireVersion from handshake
    db_name: []const u8,
    server_api: ServerApi,
    options: ?RunCommandOptions,
) !*opcode.OpMsg {
    _ = max_wire_version;
    const filter_parsed = try BsonDocument.fromObject(allocator, @TypeOf(filter), filter);
    errdefer filter_parsed.deinit(allocator);

    const command_data: CountCommand = .{
        .count = collection_name,
        // .query = filter_parsed.*,
        .@"$db" = db_name,
        // .ordered = options.ordered,
        // .writeConcern = options.writeConcern,
        // .comment = options.comment,
        // .let = options.let,
        // // .raw_data =  opts.rawData,
        // .maxTimeMS = options.maxTimeMS,
    };
    server_api.addToCommand(&command_data);
    if (options.run_command_options) |run_command_options| run_command_options.addToCommand(&command_data);

    var command = try BsonDocument.fromObject(allocator, @TypeOf(command_data), command_data);
    errdefer command.deinit(allocator);

    const result = try opcode.OpMsg.init(allocator, command, 1, 0, .{});
    return result;
}

pub const CountCommand = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    count: []const u8,
    query: ?BsonDocument = null,
    @"$db": []const u8,

    // Must be value of ServerApiVersion.value()
    apiVersion: ?[]const u8 = null,
    apiStrict: ?bool = null,
    apiDeprecationErrors: ?bool = null,

    readPreference: ?[]const u8 = null,
    timeoutMS: ?i64 = null,
};

pub const CountCommandResponse = struct {
    n: i64,
    ok: f64,

    pub fn jsonParse(allocator: Allocator, source: *std.json.Scanner, options: std.json.ParseOptions) JsonParseError!CountCommandResponse {
        _ = allocator;
        _ = options;
        if (try source.next() != .object_begin) return error.UnexpectedToken;

        var n: ?i64 = null;
        var ok: ?f64 = null;
        blk_tkn: switch (try source.next()) {
            .string => |key| {
                if (ok == null and std.mem.eql(u8, key, "ok")) {
                    const ok_value = try source.next();
                    ok = std.fmt.parseFloat(f64, ok_value.number) catch return error.UnexpectedToken;
                    continue :blk_tkn try source.next();
                }
                if (n == null and std.mem.eql(u8, key, "n")) {
                    const count_value = try source.next();
                    n = std.fmt.parseInt(i64, count_value.number, 10) catch return error.UnexpectedToken;
                }
                continue :blk_tkn try source.next();
            },
            .object_end => break :blk_tkn,
            else => return error.UnexpectedToken,
        }

        if (n == null or ok == null) return error.UnexpectedToken;

        return .{
            .n = n.?,
            .ok = ok.?,
        };
    }

    pub fn dupe(self: *const CountCommandResponse, allocator: Allocator) !*CountCommandResponse {
        const clone = try allocator.create(CountCommandResponse);
        errdefer clone.deinit(allocator);

        clone.n = self.n;
        clone.ok = self.ok;

        return clone;
    }

    pub fn parseBson(allocator: Allocator, document: *const BsonDocument) !*CountCommandResponse {
        return try document.toObject(allocator, CountCommandResponse, .{ .ignore_unknown_fields = true });
    }
};
