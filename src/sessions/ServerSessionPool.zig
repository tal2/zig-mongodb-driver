const std = @import("std");
const Allocator = std.mem.Allocator;

const SessionId = @import("./SessionId.zig").SessionId;

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

pub const ServerSessionPool = struct {
    available_sessions: std.fifo.LinearFifo(*ServerSession, .Dynamic),
    arena: std.heap.ArenaAllocator,
    logical_session_timeout_minutes: i64 = 30,

    pub fn init(allocator: std.mem.Allocator) ServerSessionPool {
        const arena = std.heap.ArenaAllocator.init(allocator);

        return ServerSessionPool{
            .available_sessions = std.fifo.LinearFifo(*ServerSession, .Dynamic).init(allocator),
            .arena = arena,
        };
    }

    pub fn deinit(self: *ServerSessionPool) void {
        self.available_sessions.deinit();
        self.arena.deinit();
    }

    pub fn close(self: *ServerSessionPool) !void {
        self.deinit();
    }

    pub fn startSession(self: *ServerSessionPool) !*ServerSession {
        if (self.available_sessions.readItem()) |session| {
            if (session.isExpiredOrAboutToExpire()) {
                return try self.startSession();
            }
            return session;
        }

        const allocator = self.arena.allocator();
        const session_id = try SessionId.generate(allocator);
        const session = try allocator.create(ServerSession);
        session.* = ServerSession{
            .session_id = session_id,
            .last_used = 0,
            .server_session_pool = self,
        };
        return session;
    }

    pub fn returnSession(self: *ServerSessionPool, session: *ServerSession) !void {
        var to_discard: usize = 0;
        for (0..self.available_sessions.count) |i| {
            const available_session = self.available_sessions.peekItem(i);

            if (available_session.isExpiredOrAboutToExpire()) {
                to_discard += 1;
            } else {
                break;
            }
        }
        if (to_discard > 0) {
            self.available_sessions.discard(to_discard);
        }

        if (session.is_dirty or session.isExpiredOrAboutToExpire()) {
            return;
        }
        return try self.available_sessions.writeItem(session);
    }

    pub fn endSession(self: *ServerSessionPool, session: *ServerSession) !void {
        return self.endSessions(&[_]*ServerSession{session});
    }

    pub fn toOwnedSessions(self: *ServerSessionPool) Allocator.Error![]*ServerSession {
        return self.available_sessions.toOwnedSlice();
    }
};
