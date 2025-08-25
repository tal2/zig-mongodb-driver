const std = @import("std");

const bson = @import("bson");

const BsonDocument = bson.BsonDocument;
const BaseSessionId = @import("./SessionId.zig").BaseSessionId;
const SessionId = @import("./SessionId.zig").SessionId;
const ServerSession = @import("./ServerSession.zig").ServerSession;
const OkResponse = @import("../commands/OkResponse.zig").OkResponse;
const ReadConcern = @import("../commands/ReadConcern.zig").ReadConcern;
const ReadPreference = @import("../commands/ReadPreference.zig").ReadPreference;

/// Not thread safe
pub const ClientSession = struct {
    pub const Mode = enum {
        Implicit,
        ImplicitCursor,
        Explicit,
    };

    allocator: std.mem.Allocator,
    server_session: ?*ServerSession,
    options: ?*const SessionOptions,
    mode: Mode,
    /// most recent cluster time seen by the session
    cluster_time: ?BsonDocument = null,

    pub fn deinit(self: *ClientSession) void {
        self.endSession() catch {};
        self.allocator.destroy(self);
    }

    pub fn advanceClusterTime(self: *ClientSession, cluster_time: BsonDocument) void {
        _ = self;
        _ = cluster_time;

        @panic("not implemented");
    }

    pub fn endSession(self: *ClientSession) !void {
        self.cluster_time = null;
        if (self.server_session) |server_session| {
            try server_session.release();
        }
    }


    pub fn addToCommand(self: *const ClientSession, command: anytype) !void {
        comptime {
            const command_type_info = @typeInfo(@TypeOf(command));
            if (command_type_info != .pointer) {
                @compileError("addToCommand command param must be a pointer to a struct");
            }
            if (!@hasField(@TypeOf(command.*), "lsid") and !(@hasDecl(@TypeOf(command.*), "no_lsid"))) {
                @compileError("addToCommand command \"" ++ @typeName(@TypeOf(command.*)) ++ "\" param must have a @lsid field");
            }
            if (@hasField(@TypeOf(command.*), "$query")) {
                @panic("not yet implemented");
            }
        }

        if (self.options) |options| {
            try options.addToCommand(command);
        }

        if (comptime !@hasDecl(@TypeOf(command.*), "no_lsid")) {
            if (self.server_session) |server_session| {
                command.lsid = server_session.session_id.doc;
            } else {
                return error.NoServerSession;
            }
        }
    }
};

pub const SessionOptions = struct {
    causalConsistency: ?bool = null,

    readConcern: ?ReadConcern = null,
    readPreference: ?ReadPreference = null,
    retryWrites: ?bool = null,
    writeConcern: ?bson.BsonDocument = null,

    pub fn addToCommand(self: *const SessionOptions, command: anytype) !void {
        comptime {
            const command_type_info = @typeInfo(@TypeOf(command));
            if (command_type_info != .pointer) {
                @compileError("addToCommand command param must be a pointer to a struct");
            }
        }
        if (comptime @hasDecl(@TypeOf(command.*), "readConcern")) {
            if (command.readConcern == null and self.readConcern) |read_concern| {
                command.readConcern = try read_concern.toValue();
            }
        }
        if (comptime @hasDecl(@TypeOf(command.*), "readPreference")) {
            if (command.readPreference == null and self.readPreference) |read_preference| {
                command.readPreference = read_preference.toValue();
            }
        }
        if (comptime @hasDecl(@TypeOf(command.*), "retryWrites")) {
            if (command.retryWrites == null and self.retryWrites) |retry_writes| {
                command.retryWrites = retry_writes;
            }
        }
        if (comptime @hasDecl(@TypeOf(command.*), "writeConcern")) {
            if (command.writeConcern == null and self.writeConcern) |write_concern| {
                command.writeConcern = write_concern;
            }
        }
    }
};

pub const RefreshSessionsCommand = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    refreshSessions: []const *BaseSessionId,
};

const RefreshSessionsResponse = OkResponse;
