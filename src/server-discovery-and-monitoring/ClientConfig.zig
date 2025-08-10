const std = @import("std");
const Address = @import("./Address.zig").Address;

pub const ClientConfig = struct {
    client_min_wire_version: i32,
    client_max_wire_version: i32,
    heartbeat_frequency_ms: u32 = 10000, // 10s for multi-threaded, 60s for single-threaded
    min_heartbeat_frequency_ms: u32 = 500,
    direct_connection: ?bool = null,
    replica_set: ?[]const u8 = null,
    load_balanced: bool = false,
    seeds: std.ArrayList(Address),

    pub fn init(allocator: std.mem.Allocator, seeds: *const std.ArrayList(Address)) !ClientConfig {
        var seeds_copy = try std.ArrayList(Address).initCapacity(allocator, seeds.capacity);
        for (seeds.items) |seed| {
            seeds_copy.appendAssumeCapacity(try seed.clone(allocator));
        }

        return .{
            .client_min_wire_version = 0,
            .client_max_wire_version = 0,
            .seeds = seeds_copy,
        };
    }

    pub fn deinit(self: *ClientConfig, allocator: std.mem.Allocator) void {
        for (self.seeds.items) |seed| {
            seed.deinit(allocator);
        }
        self.seeds.deinit();
    }
};
