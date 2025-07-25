const std = @import("std");
const bson = @import("bson");
const bson_types = bson.bson_types;
const BsonObjectId = bson_types.BsonObjectId;
const Address = @import("./Address.zig").Address;
const server_info = @import("./server-info.zig");
const ServerDescription = server_info.ServerDescription;

pub const TopologyDescription = struct {
    type: TopologyType,
    set_name: ?[]const u8 = null,
    max_election_id: ?BsonObjectId = null,
    max_set_version: ?i32 = null,
    servers: std.AutoHashMap(Address, ServerDescription),
    stale: bool = false, // for single-threaded clients
    compatible: bool = true,
    compatibilityError: ?[]const u8 = null,
    logical_session_timeout_minutes: ?i32 = null,

    pub fn deinit(self: *TopologyDescription, allocator: std.mem.Allocator) void {
        var it = self.servers.iterator();
        while (it.next()) |entry| {
            allocator.destroy(entry.key_ptr);
            allocator.destroy(entry.value_ptr);
        }
        self.servers.deinit();
    }
};

pub const TopologyVersion = struct {
    processId: ?bson_types.BsonObjectId = null,
    counter: i64,

    pub fn compare(self: *const TopologyVersion, b: *const TopologyVersion) i32 {
        if (self.processId == null or !self.processId.?.isEqualTo(&b.processId.?)) return -1;
        if (self.counter == b.counter) return 0;
        return if (self.counter < b.counter) -1 else 1;
    }
};

pub const TopologyType = enum {
    Single,
    ReplicaSetNoPrimary,
    ReplicaSetWithPrimary,
    Sharded,
    LoadBalanced,
    Unknown,
};
