const std = @import("std");
const Allocator = std.mem.Allocator;

const bson = @import("bson");
const BsonDocument = bson.BsonDocument;
const ServerSession = @import("../sessions/ServerSessionPool.zig").ServerSession;
const OkResponse = @import("./OkResponse.zig").OkResponse;

pub const EndSessionsCommand = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;
    pub const no_lsid: void = {};

    endSessions: []const *BsonDocument,

    @"$db": []const u8 = "admin",

    @"$clusterTime": ?*BsonDocument = null,

    // Must be value of ServerApiVersion.value()
    apiVersion: ?[]const u8 = null,
    apiStrict: ?bool = null,
    apiDeprecationErrors: ?bool = null,

    // RunCommandOptions
    // readPreference: ?[]const u8 = null,
    timeoutMS: ?i64 = null,

    pub fn deinit(self: *EndSessionsCommand, allocator: Allocator) void {
        for (self.endSessions) |session| {
            session.deinit(allocator);
        }
        allocator.free(self.endSessions);

        if (self.@"$clusterTime" != null) {
            const cluster_time = self.@"$clusterTime".?;
            self.@"$clusterTime" = null;
            cluster_time.deinit(allocator);
        }
    }

    pub fn make(allocator: Allocator, sessions: []const *ServerSession) !EndSessionsCommand {
        var end_sessions = try allocator.alloc(*BsonDocument, sessions.len);
        errdefer allocator.free(end_sessions);
        for (sessions, 0..) |session, i| {
            end_sessions[i] = try session.session_id.doc.dupe(allocator);
        }

        return EndSessionsCommand{
            .endSessions = end_sessions,
        };
    }
};

pub const EndSessionsCommandResponse = OkResponse;
