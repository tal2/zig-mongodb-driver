const std = @import("std");
const Allocator = std.mem.Allocator;

const SessionId = @import("./SessionId.zig").SessionId;

const ServerSessionPool = @import("./ServerSessionPool.zig").ServerSessionPool;

pub const ServerSession = struct {
    session_id: SessionId,
    last_used: i64,

    is_dirty: bool = false,
    server_session_pool: *ServerSessionPool,

    pub fn release(self: *ServerSession) !void {
        try self.server_session_pool.returnSession(self);
    }

    pub fn isExpiredOrAboutToExpire(self: *ServerSession) bool {
        const stale_duration = self.server_session_pool.logical_session_timeout_minutes * std.time.ms_per_min;

        return self.last_used + stale_duration < std.time.milliTimestamp() + (1 * std.time.ms_per_min);
    }
};
