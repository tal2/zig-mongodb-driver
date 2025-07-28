const std = @import("std");
const Database = @import("../Database.zig").Database;
const Address = @import("./Address.zig").Address;

pub const MonitoringThreadContext = struct {
    allocator: std.mem.Allocator,
    database: *Database,
    server_address: Address,
    done: bool = false,
    stop_signal: bool = false,
};
