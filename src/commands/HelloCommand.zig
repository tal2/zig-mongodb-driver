const std = @import("std");
const builtin = @import("builtin");
const bson = @import("bson");
const utils = @import("../utils.zig");
const opcode = @import("../protocol/opcode.zig");
const topology = @import("../server-discovery-and-monitoring/topology.zig");

const Allocator = std.mem.Allocator;
const bson_types = bson.bson_types;
const BsonDocument = bson.BsonDocument;
const TopologyVersion = topology.TopologyVersion;
const parseBsonDocument = utils.parseBsonDocument;

pub fn makeHelloCommand(allocator: std.mem.Allocator, db_name: []const u8) !*opcode.OpMsg {
    const command_data = .{
        .hello = 1,
        .@"$db" = db_name,
    };
    const command = try BsonDocument.fromObject(allocator, @TypeOf(command_data), command_data);
    errdefer command.deinit(allocator);

    const result = try opcode.OpMsg.init(allocator, command, 1, 0, .{});
    return result;
}

pub fn makeHelloCommandForHandshake(allocator: std.mem.Allocator, db_name: []const u8, application_name: []const u8) !*opcode.OpMsg {
    const client_metadata_max_message_size_bytes = 512;

    std.debug.assert(application_name.len < 128);
    const driver_name = "Zig Driver"; // TODO: get from config
    const driver_version = "0.1.0"; // TODO: get from config

    const command_data = .{
        .hello = 1,
        .@"$db" = db_name,
        // .helloOk = true,
        .client = .{
            .application = .{
                .name = application_name,
            },
            .driver = .{
                .name = driver_name,
                .version = driver_version,
            },

            .os = .{
                .type = @tagName(builtin.target.os.tag),
            },
        },

        // .compression = [0][]u8{},
        .loadBalanced = false,

        // .saslSupportedMechs = [0][]u8{},
    };

    const command = try BsonDocument.fromObject(allocator, @TypeOf(command_data), command_data);
    errdefer command.deinit(allocator);
    std.debug.assert(command.len < client_metadata_max_message_size_bytes);

    const result = try opcode.OpMsg.init(allocator, command, 1, 0, .{});
    errdefer result.deinit(allocator);
    return result;
}

pub const HelloCommandResponse = struct {
    // helloOk: ?bool = null,
    isWritablePrimary: bool,
    topologyVersion: TopologyVersion,
    maxBsonObjectSize: i32,
    maxMessageSizeBytes: i32,
    maxWriteBatchSize: i32,
    localTime: bson_types.BsonUtcDatetime,
    logicalSessionTimeoutMinutes: i32,
    connectionId: i32,
    minWireVersion: i32,
    maxWireVersion: i32,
    readOnly: bool,
    ok: f64,

    pub fn parseBson(allocator: Allocator, document: *BsonDocument) !*HelloCommandResponse {
        const parsed = try parseBsonDocument(HelloCommandResponse, allocator, document, .{ .ignore_unknown_fields = false, .allocate = .alloc_if_needed });
        defer parsed.deinit();

        const response = try allocator.create(HelloCommandResponse);
        const value = parsed.value;
        // response.helloOk = value.helloOk;
        response.isWritablePrimary = value.isWritablePrimary;
        response.topologyVersion = value.topologyVersion;
        response.maxBsonObjectSize = value.maxBsonObjectSize;
        response.maxMessageSizeBytes = value.maxMessageSizeBytes;
        response.maxWriteBatchSize = value.maxWriteBatchSize;
        response.localTime = value.localTime;
        response.logicalSessionTimeoutMinutes = value.logicalSessionTimeoutMinutes;
        response.connectionId = value.connectionId;
        response.minWireVersion = value.minWireVersion;
        response.maxWireVersion = value.maxWireVersion;
        response.readOnly = value.readOnly;
        response.ok = value.ok;

        return response;
    }
};
