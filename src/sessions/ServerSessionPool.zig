const std = @import("std");
const Allocator = std.mem.Allocator;

const LinearFifo = std.fifo.LinearFifo;
const ArenaAllocator = std.heap.ArenaAllocator;
const SessionId = @import("./SessionId.zig").SessionId;

const ServerSession = @import("./ServerSession.zig").ServerSession;

pub const ServerSessionPool = struct {
    available_sessions: LinearFifo(*ServerSession, .Dynamic),
    arena: ArenaAllocator,
    logical_session_timeout_minutes: i64 = 30,


    pub fn init(allocator: Allocator) ServerSessionPool {
        const arena = ArenaAllocator.init(allocator);

        return ServerSessionPool{
            .available_sessions = LinearFifo(*ServerSession, .Dynamic).init(allocator),
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
            .last_used = std.time.milliTimestamp(),
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

    pub fn toOwnedSessions(self: *ServerSessionPool) Allocator.Error![]*ServerSession {
        return self.available_sessions.toOwnedSlice();
    }
};
