const std = @import("std");
const bson = @import("bson");
const utils = @import("../utils.zig");
const opcode = @import("../protocol/opcode.zig");

const Allocator = std.mem.Allocator;
const BsonDocument = bson.BsonDocument;
const ServerApi = @import("../server-discovery-and-monitoring/server-info.zig").ServerApi;
const MongoCredential = @import("MongoCredential.zig").MongoCredential;

pub const JsonParseError = error{UnexpectedToken} || std.json.Scanner.NextError;

pub fn makeSaslStartCommand(
    allocator: std.mem.Allocator,
    mechanism: MongoCredential.AuthMechanism,
    payload: []const u8,
    db_name: []const u8,
    server_api: ServerApi,
) !*opcode.OpMsg {
    var command_data: SaslCommand = .{
        .saslStart = 1,
        .mechanism = mechanism.toString(),
        .payload = payload,

        .@"$db" = db_name,

        .options = .{
            .skipEmptyExchange = true,
        },
    };

    server_api.addToCommand(&command_data);

    var command = try BsonDocument.fromObject(allocator, @TypeOf(command_data), command_data);
    errdefer command.deinit(allocator);

    const result = try opcode.OpMsg.init(allocator, command, 1, 0, .{});
    return result;
}

pub fn makeSaslContinueCommand(
    allocator: std.mem.Allocator,
    conversation_id: i32,
    payload: []const u8,
    db_name: []const u8,
    server_api: ServerApi,
) !*opcode.OpMsg {
    _ = server_api;
    const command_data: SaslCommand = .{
        .saslContinue = 1,

        .conversationId = conversation_id,
        .payload = payload,

        .@"$db" = db_name,
    };

    var command = try BsonDocument.fromObject(allocator, @TypeOf(command_data), command_data);
    errdefer command.deinit(allocator);

    const result = try opcode.OpMsg.init(allocator, command, 1, 0, .{});
    return result;
}

const SaslCommand = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    saslStart: ?i32 = null,

    saslContinue: ?i32 = null,

    @"$db": []const u8,

    conversationId: ?i32 = null,

    mechanism: ?[]const u8 = null,

    /// a sequence of bytes or base64 encoded string
    payload: []const u8,

    options: ?SaslOptions = null,

    apiVersion: ?[]const u8 = null,
    apiStrict: ?bool = null,
    apiDeprecationErrors: ?bool = null,
};

pub const SaslOptions = struct {
    skipEmptyExchange: bool = true,
};

pub const SaslCommandResponse = struct {
    ok: f64,

    conversationId: ?i32 = null,

    /// base64 encoded
    payload: ?[]const u8 = null,

    done: bool,

    pub fn deinit(self: *SaslCommandResponse, allocator: Allocator) void {
        if (self.payload) |value| {
            allocator.free(value);
        }
    }

    pub fn jsonParse(allocator: Allocator, source: *std.json.Scanner, options: std.json.ParseOptions) JsonParseError!SaslCommandResponse {
        _ = options;
        if (try source.next() != .object_begin) return error.UnexpectedToken;

        var ok: ?f64 = null;
        var conversation_id: ?i32 = null;
        var payload: ?[]const u8 = null;
        var done: ?bool = null;

        blk_tkn: switch (try source.next()) {
            .string => |key| {
                if (ok == null and std.mem.eql(u8, key, "ok")) {
                    const ok_value = try source.next();
                    ok = std.fmt.parseFloat(f64, ok_value.number) catch return error.UnexpectedToken;
                    continue :blk_tkn try source.next();
                }
                if (conversation_id == null and std.mem.eql(u8, key, "conversationId")) {
                    const conv_value = try source.next();
                    conversation_id = std.fmt.parseInt(i32, conv_value.number, 10) catch return error.UnexpectedToken;
                    continue :blk_tkn try source.next();
                }
                if (payload == null and std.mem.eql(u8, key, "payload")) {
                    const payload_value = try source.next();
                    if (payload_value == .string) {
                        payload = try allocator.dupe(u8, payload_value.string);
                    }
                    continue :blk_tkn try source.next();
                }
                if (done == null and std.mem.eql(u8, key, "done")) {
                    const done_value = try source.next();
                    if (done_value == .true or done_value == .false) {
                        done = done_value == .true;
                    }
                    continue :blk_tkn try source.next();
                }
            },
            .object_end => break :blk_tkn,
            else => return error.UnexpectedToken,
        }

        if (ok == null or (conversation_id == null or done == null)) return error.UnexpectedToken;

        return .{
            .ok = ok.?,
            .conversationId = conversation_id,
            .payload = payload,
            .done = done orelse false,
        };
    }

    pub fn dupe(self: *const SaslCommandResponse, allocator: Allocator) !*SaslCommandResponse {
        const clone = try allocator.create(SaslCommandResponse);
        errdefer allocator.destroy(clone);

        clone.ok = self.ok;
        clone.conversationId = self.conversationId;
        clone.done = self.done;
        if (self.payload) |p| {
            clone.payload = try allocator.dupe(u8, p);
        }

        return clone;
    }

    pub fn parseBson(allocator: Allocator, document: *const BsonDocument) !*SaslCommandResponse {
        return try document.toObject(allocator, SaslCommandResponse, .{ .ignore_unknown_fields = true });
    }
};
