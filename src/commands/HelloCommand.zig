const std = @import("std");
const builtin = @import("builtin");
const bson = @import("bson");
const utils = @import("../utils.zig");
const opcode = @import("../protocol/opcode.zig");
const topology = @import("../server-discovery-and-monitoring/topology.zig");
const ServerApi = @import("../server-discovery-and-monitoring/server-info.zig").ServerApi;
const ClientMetadata = @import("../connection/ClientMetadata.zig").ClientMetadata;
const MongoCredential = @import("../auth/MongoCredential.zig").MongoCredential;

const Allocator = std.mem.Allocator;
const bson_types = bson.bson_types;
const BsonDocument = bson.BsonDocument;
const TopologyVersion = topology.TopologyVersion;

pub fn makeHelloCommand(allocator: std.mem.Allocator, db_name: []const u8, server_api: ServerApi) !*opcode.OpMsg {
    var command_data: HelloCommand = .{
        .hello = 1,
        .@"$db" = db_name,
    };
    server_api.addToCommand(&command_data);

    const command = try BsonDocument.fromObject(allocator, @TypeOf(command_data), command_data);
    errdefer command.deinit(allocator);

    const result = try opcode.OpMsg.init(allocator, command, 1, 0, .{});
    return result;
}

const HelloCommand = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;
    pub const ResponseType = HelloCommandResponse;

    hello: i32,
    @"$db": []const u8,

    helloOk: ?bool = null,

    client: ?ClientMetadata = null,

    saslSupportedMechs: ?[]const u8 = null,

    loadBalanced: ?bool = null,

    apiVersion: ?[]const u8 = null,
    apiStrict: ?bool = null,
    apiDeprecationErrors: ?bool = null,

    readPreference: ?[]const u8 = null,
    timeoutMS: ?i64 = null,

    pub fn deinit(self: *const HelloCommand, allocator: std.mem.Allocator) void {
        if (self.saslSupportedMechs) |sasl_supported_mechs| {
            allocator.free(sasl_supported_mechs);
        }
    }
};

pub fn makeHelloCommandForHandshake( //
    allocator: std.mem.Allocator,
    db_name: []const u8,
    application_name: []const u8,
    server_api: ServerApi,
    credentials: ?MongoCredential,
) !*opcode.OpMsg {
    const client_metadata_max_message_size_bytes = 512;

    std.debug.assert(application_name.len < 128);
    const driver_name = "Zig Driver"; // TODO: get from config
    const driver_version = "0.1.0"; // TODO: get from config

    var command_data: HelloCommand = .{
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
    defer command_data.deinit(allocator);

    if (credentials) |creds| {
        switch (creds.mechanism) {
            .SCRAM_SHA_256 => {
                if (creds.username == null) {
                    return error.MissingUsername;
                }
                if (creds.source == null) {
                    return error.MissingSource;
                }
                command_data.saslSupportedMechs = try std.fmt.allocPrint(allocator, "{s}.{s}", .{ creds.source.?, creds.username.? });
            },
            else => {},
        }
    }

    server_api.addToCommand(&command_data);

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

    saslSupportedMechs: ?[]const []const u8 = null,

    pub fn deinit(self: *const HelloCommandResponse, allocator: Allocator) void {
        if (self.saslSupportedMechs) |sasl_supported_mechs| {
            allocator.free(sasl_supported_mechs);
        }
        allocator.destroy(self);
    }

    pub fn dupe(self: *const HelloCommandResponse, allocator: Allocator) !*HelloCommandResponse {
        const clone = try allocator.create(HelloCommandResponse);
        errdefer clone.deinit(allocator);

        // clone.helloOk = self.helloOk;
        clone.isWritablePrimary = self.isWritablePrimary;
        clone.topologyVersion = self.topologyVersion;
        clone.maxBsonObjectSize = self.maxBsonObjectSize;
        clone.maxMessageSizeBytes = self.maxMessageSizeBytes;
        clone.maxWriteBatchSize = self.maxWriteBatchSize;
        clone.localTime = self.localTime;
        clone.logicalSessionTimeoutMinutes = self.logicalSessionTimeoutMinutes;
        clone.connectionId = self.connectionId;
        clone.minWireVersion = self.minWireVersion;
        clone.maxWireVersion = self.maxWireVersion;
        clone.readOnly = self.readOnly;
        clone.ok = self.ok;

        clone.saslSupportedMechs = self.saslSupportedMechs;

        return clone;
    }

    pub fn parseBson(allocator: Allocator, document: *const BsonDocument) !*HelloCommandResponse {
        return try document.toObject(allocator, HelloCommandResponse, .{ .ignore_unknown_fields = true });
    }
};
