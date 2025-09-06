const std = @import("std");

const Address = std.net.Address;
const topology = @import("./topology.zig");
const ClientConfig = @import("./ClientConfig.zig").ClientConfig;

const TopologyVersion = topology.TopologyVersion;
const HelloCommandResponse = @import("../commands/HelloCommand.zig").HelloCommandResponse;
const bson = @import("bson");
const bson_types = bson.bson_types;
const BsonObjectId = bson_types.BsonObjectId;
const BsonUtcDatetime = bson_types.BsonUtcDatetime;

pub const ServerType = enum {
    Standalone,
    Mongos,
    PossiblePrimary, // for single-threaded clients
    RSPrimary,
    RSSecondary,
    RSArbiter,
    RSOther,
    RSGhost,
    LoadBalancer,
    Unknown,
};

pub const ServerDescription = struct {
    address: Address, // the address of the server. (not the same as field 'me')
    @".error": ?[]const u8 = null, // information about the last error related to this server. Default null. MUST contain or be able to produce a string describing the error.
    round_trip_time: ?u64 = null, // the round trip time to the server. Default null.
    min_round_trip_time: ?u64 = null, // the minimum round trip time to the server. Default null.
    last_write_date: ?BsonUtcDatetime = null, // BSON datetime. The `lastWriteDate` from the server's most recent hello or legacy hello response.

    /// an opTime or null. An opaque value representing the position in the oplog of the most recently seen write. Default null. (Only mongos and shard servers record this field when monitoring config servers as replica sets, at least until drivers allow applications to use readConcern "afterOptime".
    opTime: ?[]const u8 = null,

    type: ServerType = .Unknown,
    min_wire_version: i32 = 0, // the wire protocol version range supported by the server.
    max_wire_version: i32 = 0, // the wire protocol version range supported by the server.
    me: ?Address = null, // The hostname or IP, and the port number, that this server was configured with in the replica set.
    hosts: std.StringHashMap(Address), // Sets of addresses. This server's opinion of the replica set's members, if any.
    passives: std.StringHashMap(Address), // Sets of addresses. This server's opinion of the replica set's members, if any.
    arbiters: std.StringHashMap(Address), // Sets of addresses. This server's opinion of the replica set's members, if any.
    tags: std.StringHashMap([]const u8), // map from string to string. Default empty.
    setName: ?[]const u8 = null,
    electionId: ?BsonObjectId = null, // an ObjectId, if this is a MongoDB 2.6+ replica set member that believes it is primary
    setVersion: ?i32 = null, // the setVersion of the replica set. Default null.
    primary: ?Address = null, // an address. This server's opinion of who the primary is. Default null.
    last_update_time: i64, // when this server was last checked. Default "infinity ago".
    logical_session_timeout_minutes: ?i32 = null,
    topology_version: ?TopologyVersion = null,
    iscryptd: ?bool = null,

    pub fn deinit(self: *ServerDescription, allocator: std.mem.Allocator) void {
        // _ = allocator;
        // self.hosts.deinit();
        // self.passives.deinit();
        // self.arbiters.deinit();
        // self.tags.deinit();
        // self.hosts.deinit();
        allocator.destroy(self);
    }

    pub fn updateWithHelloResponse(self: *ServerDescription, hello_response: *const HelloCommandResponse, lastUpdateTime: i64, roundTripTime: u64) !void {
        self.min_wire_version = hello_response.minWireVersion;
        self.max_wire_version = hello_response.maxWireVersion;
        self.last_update_time = lastUpdateTime;
        self.round_trip_time = roundTripTime;
        self.topology_version = hello_response.topologyVersion;
        self.logical_session_timeout_minutes = hello_response.logicalSessionTimeoutMinutes;
    }

    pub fn isStale(self: *ServerDescription, b: *const ServerDescription) bool {
        if (self.topology_version == null or b.topology_version == null) return true;

        return self.topology_version.?.compare(&b.topology_version.?) < 0;
    }

    pub fn isDataBearing(self: *ServerDescription) bool {
        switch (self.type) {
            .Standalone, .RSPrimary, .RSSecondary, .Mongos, .LoadBalancer => return true,
            .Unknown => return false,
        }
    }

    pub fn isEqualTo(self: *ServerDescription, b: ServerDescription) bool {
        if (self.@".error" == null and b.@".error" != null) return false;
        if (self.@".error" != null and b.@".error" == null) return false;
        if (self.@".error" != null and b.@".error" != null and !std.mem.eql(u8, self.@".error", b.@".error")) return false;

        if (self.type != b.type) return false;
        if (self.min_wire_version != b.min_wire_version) return false;
        if (self.max_wire_version != b.max_wire_version) return false;
        if (self.me == null and b.me != null) return false;
        if (self.me != null and b.me == null) return false;
        if (self.me != null and b.me != null and !self.me.?.isEqualTo(b.me)) return false;

        if (self.hosts.count() != b.hosts.count()) return false; // TODO: compare hosts
        if (self.passives.count() != b.passives.count()) return false; // TODO: compare passives
        if (self.arbiters.count() != b.arbiters.count()) return false; // TODO: compare arbiters
        if (self.tags.count() != b.tags.count()) return false; // TODO: compare tags

        if (self.setName != null and b.setName != null and !std.mem.eql(u8, self.setName, b.setName)) return false;
        if (self.electionId.isEqualTo(&b.electionId)) return false;

        if (self.primary == null and b.primary != null) return false;
        if (self.primary != null and b.primary == null) return false;
        if (self.primary != null and b.primary != null and !self.primary.?.isEqualTo(b.primary)) return false;

        // if (self.logicalSessionTimeoutMinutes == null and b.logicalSessionTimeoutMinutes != null) return false;
        // if (self.logicalSessionTimeoutMinutes != null and b.logicalSessionTimeoutMinutes == null) return false;
        // if (self.logicalSessionTimeoutMinutes != null and b.logicalSessionTimeoutMinutes != null and self.logicalSessionTimeoutMinutes.? != b.logicalSessionTimeoutMinutes.?) return false;

        if (self.setVersion == null and b.setVersion != null) return false;
        if (self.setVersion != null and b.setVersion == null) return false;
        if (self.setVersion != null and b.setVersion != null and self.setVersion.? != b.setVersion.?) return false;

        if (self.topology_version == null and b.topology_version != null) return false;
        if (self.topology_version != null and b.topology_version == null) return false;
        if (self.topology_version.?.compare(b.topology_version.?) != 0) return false;

        if (self.iscryptd == null and b.iscryptd != null) return false;
        if (self.iscryptd != null and b.iscryptd == null) return false;
        if (self.iscryptd != null and b.iscryptd != null and self.iscryptd.? != b.iscryptd.?) return false;

        return true;
    }

    pub fn checkCompatibility(self: *const ServerDescription, client_config: *const ClientConfig) !bool {
        if (self.min_wire_version > client_config.client_max_wire_version) {
            return error.IncompatibleWireVersionAboveMax;
        }
        if (self.max_wire_version < client_config.client_min_wire_version) {
            return error.IncompatibleWireVersionBelowMin;
        }

        return true;
    }
};

pub const ServerApiVersion = enum(u8) {
    /// Stable API requires MongoDB Server 5.0 or later.
    v1 = 1,

    pub fn value(self: ServerApiVersion) []const u8 {
        return switch (self) {
            .v1 => "1",
        };
    }
};

pub const ServerApi = struct {
    version: ?ServerApiVersion = null,
    strict: ?bool = null, // Default false
    deprecationErrors: ?bool = null, // Default false

    pub inline fn addToCommand(self: *const ServerApi, command: anytype) void {
        if (self.version) |version| command.apiVersion = version.value();
        command.apiStrict = self.strict;
        command.apiDeprecationErrors = self.deprecationErrors;
    }
};
