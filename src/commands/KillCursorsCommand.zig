const std = @import("std");
const bson = @import("bson");
const utils = @import("../utils.zig");
const opcode = @import("../protocol/opcode.zig");
const ServerApi = @import("../server-discovery-and-monitoring/server-info.zig").ServerApi;

const Allocator = std.mem.Allocator;
const BsonDocument = bson.BsonDocument;

pub const JsonParseError = error{UnexpectedToken} || std.json.Scanner.NextError;

pub fn makeKillCursorsCommand(
    allocator: std.mem.Allocator,
    collection_name: []const u8,
    cursors: []const i64,
    max_wire_version: ?i32, // maxWireVersion from handshake
    db_name: []const u8,
    server_api: ServerApi,
) !*opcode.OpMsg {
    _ = max_wire_version;

    var command_data: KillCursorsCommand = .{
        .killCursors = collection_name,
        .cursors = cursors,
        .@"$db" = db_name,
    };
    server_api.addToCommand(&command_data);

    var command = try BsonDocument.fromObject(allocator, @TypeOf(command_data), command_data);
    errdefer command.deinit(allocator);

    const result = try opcode.OpMsg.init(allocator, command, 1, 0, .{});
    return result;
}

pub const KillCursorsCommand = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    /// The name of the collection.
    killCursors: []const u8,
    /// The ids of the cursors to kill.
    cursors: []const i64,

    @"$db": []const u8,

    comment: ?[]const u8 = null,

    apiVersion: ?[]const u8 = null,
    apiStrict: ?bool = null,
    apiDeprecationErrors: ?bool = null,

    // readPreference: ?[]const u8 = null,
    // timeoutMS: ?i64 = null,
};

pub const KillCursorsCommandResponse = struct {
    cursors_killed: []const i64,
    cursors_not_found: []const i64,
    cursors_alive: []const i64,
    cursors_unknown: []const i64,
    ok: f64,

    pub fn deinit(self: *const KillCursorsCommandResponse, allocator: Allocator) void {
        allocator.free(self.cursors_killed);
        allocator.free(self.cursors_not_found);
        allocator.free(self.cursors_alive);
        allocator.free(self.cursors_unknown);
        allocator.destroy(self);
    }

    pub fn dupe(self: *const KillCursorsCommandResponse, allocator: Allocator) !*KillCursorsCommandResponse {
        const clone = try allocator.create(KillCursorsCommandResponse);
        clone.* = .{
            .cursors_killed = try allocator.dupe(i64, self.cursors_killed),
            .cursors_not_found = try allocator.dupe(i64, self.cursors_not_found),
            .cursors_alive = try allocator.dupe(i64, self.cursors_alive),
            .cursors_unknown = try allocator.dupe(i64, self.cursors_unknown),
            .ok = self.ok,
        };
        return clone;
    }

    pub fn jsonParse(allocator: Allocator, source: *std.json.Scanner, options: std.json.ParseOptions) JsonParseError!KillCursorsCommandResponse {
        if (try source.next() != .object_begin) return error.UnexpectedToken;

        var cursors_killed: ?[]const i64 = null;
        var cursors_not_found: ?[]const i64 = null;
        var cursors_alive: ?[]const i64 = null;
        var cursors_unknown: ?[]const i64 = null;
        var ok: ?f64 = null;

        blk_tkn: switch (try source.next()) {
            .string => |key| {
                if (cursors_killed == null and std.mem.eql(u8, key, "cursorsKilled")) {
                    cursors_killed = try jsonParseCursorsArray(allocator, source, options);
                    continue :blk_tkn try source.next();
                }
                if (cursors_not_found == null and std.mem.eql(u8, key, "cursorsNotFound")) {
                    cursors_not_found = try jsonParseCursorsArray(allocator, source, options);
                    continue :blk_tkn try source.next();
                }
                if (cursors_alive == null and std.mem.eql(u8, key, "cursorsAlive")) {
                    cursors_alive = try jsonParseCursorsArray(allocator, source, options);
                    continue :blk_tkn try source.next();
                }
                if (cursors_unknown == null and std.mem.eql(u8, key, "cursorsUnknown")) {
                    cursors_unknown = try jsonParseCursorsArray(allocator, source, options);
                    continue :blk_tkn try source.next();
                }
                if (ok == null and std.mem.eql(u8, key, "ok")) {
                    const ok_value = try source.next();
                    ok = std.fmt.parseFloat(f64, ok_value.number) catch return error.UnexpectedToken;
                    continue :blk_tkn try source.next();
                }
            },
            .object_end => break :blk_tkn,
            else => return error.UnexpectedToken,
        }

        if (cursors_killed == null or cursors_not_found == null or cursors_alive == null or cursors_unknown == null or ok == null) return error.UnexpectedToken;

        return .{
            .cursors_killed = cursors_killed.?,
            .cursors_not_found = cursors_not_found.?,
            .cursors_alive = cursors_alive.?,
            .cursors_unknown = cursors_unknown.?,
            .ok = ok.?,
        };
    }

    fn jsonParseCursorsArray(allocator: Allocator, source: *std.json.Scanner, options: std.json.ParseOptions) JsonParseError![]const i64 {
        _ = options;
        if (try source.next() != .array_begin) return error.UnexpectedToken;
        var cursors = std.ArrayList(i64).init(allocator);
        defer cursors.deinit();

        while (true) {
            switch (try source.next()) {
                .array_end => break,
                .number => |number| {
                    try cursors.append(std.fmt.parseInt(i64, number, 10) catch return error.UnexpectedToken);
                },
                else => return error.UnexpectedToken,
            }
        }

        return cursors.toOwnedSlice();
    }

    pub fn parseBson(allocator: Allocator, document: *const BsonDocument) !*KillCursorsCommandResponse {
        return try utils.parseBsonToOwned(KillCursorsCommandResponse, allocator, document);
    }
};
