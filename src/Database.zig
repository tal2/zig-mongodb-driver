const std = @import("std");
const bson = @import("bson");
const time = std.time;
const net = std.net;
const Thread = std.Thread;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const commands = @import("commands/root.zig");
const connection_stream = @import("connection/ConnectionStream.zig");
const ConnectionStream = connection_stream.ConnectionStream;
const ConnectionString = @import("connection/ConnectionString.zig").ConnectionString;

const Collection = @import("Collection.zig").Collection;
const RunCommandOptions = commands.RunCommandOptions.RunCommandOptions;
const HelloCommandResponse = commands.HelloCommand.HelloCommandResponse;
const ErrorResponse = commands.ErrorResponse;
const BulkWriteOpsChainable = commands.BulkWriteOpsChainable;

const server_discovery_and_monitoring = @import("server-discovery-and-monitoring/root.zig");
const MonitoringThreadContext = server_discovery_and_monitoring.MonitoringThreadContext;
const TopologyDescription = server_discovery_and_monitoring.TopologyDescription;
const ServerDescription = server_discovery_and_monitoring.ServerDescription;
const ClientConfig = server_discovery_and_monitoring.ClientConfig;
const Address = server_discovery_and_monitoring.Address;
const ServerApi = server_discovery_and_monitoring.ServerApi;

const MongoCredential = @import("./auth/MongoCredential.zig").MongoCredential;
const sasl_commands = @import("./auth/sasl.zig");
const SaslCommandResponse = sasl_commands.SaslCommandResponse;
const utils = @import("utils.zig");

const ServerSessionPool = @import("./sessions/ServerSessionPool.zig").ServerSessionPool;
const ServerSession = @import("./sessions/ServerSessionPool.zig").ServerSession;
const SessionId = @import("./sessions/SessionId.zig").SessionId;
const ClientSession = @import("./sessions/ClientSession.zig").ClientSession;
const SessionOptions = @import("./sessions/ClientSession.zig").SessionOptions;
const EndSessionsCommand = commands.EndSessionsCommand;
const EndSessionsCommandResponse = commands.EndSessionsCommandResponse;
const WriteResponseUnion = @import("./ResponseUnion.zig").WriteResponseUnion;
const BsonDocument = bson.BsonDocument;
const RequestIdGenerator = @import("./commands/RequestIdGenerator.zig");
const opcode = @import("./protocol/opcode.zig");
const ResponseUnion = @import("./ResponseUnion.zig").ResponseUnion;

pub const Database = struct {
    allocator: Allocator,
    db_name: []const u8,
    stream: ConnectionStream,
    topology_description: *TopologyDescription,
    monitoring_threads: std.ArrayList(*MonitoringThreadContext),
    pool: *std.Thread.Pool,
    server_api: ServerApi,
    client_config: ClientConfig,

    server_session_pool: ServerSessionPool,

    pub fn init(allocator: Allocator, conn_str: *ConnectionString, server_api: ServerApi) !Database {
        bson.bson_types.BsonObjectId.initializeGenerator();

        const stream = try ConnectionStream.fromConnectionString(allocator, conn_str);
        const topology_description = try allocator.create(TopologyDescription);
        topology_description.type = .Single;
        topology_description.servers = std.AutoHashMap(Address, ServerDescription).init(allocator);

        const pool_options = std.Thread.Pool.Options{ .allocator = allocator, .n_jobs = conn_str.hosts.items.len };
        var pool = try allocator.create(std.Thread.Pool);
        try pool.init(pool_options);
        errdefer pool.deinit();

        const seeds = try conn_str.hosts.clone(allocator);
        conn_str.hosts.clearAndFree(allocator);
        return .{
            .allocator = allocator,
            .db_name = if (conn_str.auth_database) |auth_database| try allocator.dupe(u8, auth_database) else "admin",
            .stream = stream,
            .topology_description = topology_description,
            .monitoring_threads = .empty,
            .pool = pool,
            .server_api = server_api,
            .client_config = ClientConfig.init(seeds),

            .server_session_pool = ServerSessionPool.init(allocator),
        };
    }

    pub fn deinit(self: *Database) void {
        self.endSessions() catch |err| {
            std.debug.print("error ending sessions: {any}\n", .{err});
        };

        self.server_session_pool.deinit();

        self.allocator.free(self.db_name);
        self.stream.deinit();
        self.topology_description.deinit(self.allocator);
        for (self.monitoring_threads.items) |thread_context| {
            if (!thread_context.done) {
                thread_context.stop_signal = true;
            }
        }
        self.pool.deinit();
        self.allocator.destroy(self.pool);
        self.monitoring_threads.deinit(self.allocator);
        self.client_config.deinit(self.allocator);
        // self.allocator.destroy(self);
    }

    pub fn connect(self: *Database, credentials: ?MongoCredential) !void {
        if (credentials) |creds| {
            try creds.validate();
        }

        try self.stream.connect();

        var current_server_description = try self.allocator.create(ServerDescription);
        defer current_server_description.deinit(self.allocator);

        current_server_description.address = self.client_config.seeds.items[0].addrs[0];

        try self.handshake(current_server_description, &self.stream, credentials);

        try self.monitoring_threads.ensureTotalCapacity(self.allocator, self.client_config.seeds.items.len);
        for (self.client_config.seeds.items) |seed| {
            const thread_context = try self.allocator.create(MonitoringThreadContext);
            errdefer self.allocator.destroy(thread_context);
            thread_context.* = try MonitoringThreadContext.init(self.allocator, self, seed, .{});

            try self.pool.spawn(MonitoringThreadContext.startServerMonitoring, .{thread_context});

            self.monitoring_threads.appendAssumeCapacity(thread_context);
        }
    }

    pub fn collection(self: *Database, collection_name: []const u8) Collection {
        return Collection.init(self, collection_name);
    }

    pub fn bulkWriteChain(self: *Database) !BulkWriteOpsChainable {
        const c = try self.allocator.create(Collection);
        c.* = self.collection("admin");
        return BulkWriteOpsChainable.init(c);
    }

    pub fn startSession(self: *Database, options: ?*const SessionOptions) !*ClientSession {
        const client_session = try self.allocator.create(ClientSession);
        client_session.* = ClientSession{
            .allocator = self.allocator,
            .server_session = null,
            .options = options,
            .mode = .Explicit,
        };
        return client_session;
    }

    fn associateServerSession(self: *Database, client_session: *ClientSession) !void {
        if (client_session.server_session != null) return;
        const server_session = try self.server_session_pool.startSession();
        client_session.server_session = server_session;
    }

    fn endSessions(self: *Database) !void {
        const sessions = try self.server_session_pool.toOwnedSessions();
        if (sessions.len == 0) {
            return;
        }

        var arena = ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        var end_sessions_command = try EndSessionsCommand.make(arena_allocator, sessions);

        // drivers must ignore any errors returned by the endSessions command
        _ = self.runCommandWithOptionalSession(arena_allocator, null, &end_sessions_command, RunCommandOptions{}, EndSessionsCommandResponse) catch {
            // ignore errors
        };
    }

    pub fn handshake(self: *Database, current_server_description: *ServerDescription, conn_stream: *ConnectionStream, credentials: ?MongoCredential) !void {
        const allocator = self.allocator;

        // const options: RunCommandOptions = .{
        //     .readPreference = .primary,
        // };
        const command = try commands.makeHelloCommandForHandshake(allocator, self.db_name, "Zig Driver", self.server_api, credentials);
        defer command.deinit(allocator);

        const start_time = time.milliTimestamp();
        var result = try conn_stream.send(allocator, command);
        defer result.deinit(allocator);

        if (try ErrorResponse.isError(allocator, result.section_document.document)) {
            return error.HandshakeFailed;
        }

        const hello_response = commands.HelloCommandResponse.parseBson(allocator, result.section_document.document) catch |err| {
            std.debug.print("error parsing hello response: {any}\n", .{err});
            //TODO: handle error
            return error.HandshakeFailed;
        };
        defer hello_response.deinit(allocator);

        const now = time.milliTimestamp();

        const end_time = now;
        const round_trip_time: u64 = @intCast(end_time - start_time);

        try self.handleHelloResponse(current_server_description, hello_response, now, round_trip_time);

        if (credentials) |creds| {
            try self.authConversation(allocator, conn_stream, creds);
        }
    }

    fn authConversation(self: *Database, allocator: Allocator, conn_stream: *ConnectionStream, creds: MongoCredential) !void {
        var arena = ArenaAllocator.init(allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        var auth_conversation = try creds.toAuthConversation(arena_allocator);
        defer auth_conversation.deinit();
        const mechanism = creds.mechanism;

        const db_name = creds.source orelse self.db_name;

        const payload_start = try auth_conversation.next();
        if (payload_start == null) {
            return error.AuthConversationPayloadIsNull;
        }

        const payload_start_base64 = try utils.base64Encode(arena_allocator, payload_start.?);
        const sasl_start_command = try sasl_commands.makeSaslStartCommand(arena_allocator, mechanism, payload_start_base64, db_name, self.server_api);
        var sasl_command = sasl_start_command;

        var is_done = false;
        while (!is_done) {
            const response = try conn_stream.send(arena_allocator, sasl_command);

            if (try ErrorResponse.isError(allocator, response.section_document.document)) {
                return error.AuthenticationFailed;
            }

            const sasl_response = try SaslCommandResponse.parseBson(arena_allocator, response.section_document.document);

            const response_payload = sasl_response.payload.?;
            const response_payload_decoded = try utils.base64Decode(arena_allocator, response_payload);
            try auth_conversation.handleResponse(response_payload_decoded);

            if (sasl_response.done) {
                break;
            }

            const payload = try auth_conversation.next();
            if (payload == null) {
                return error.AuthConversationPayloadIsNull;
            }

            const payload_base64 = try utils.base64Encode(arena_allocator, payload.?);

            sasl_command = try sasl_commands.makeSaslContinueCommand(arena_allocator, sasl_response.conversationId.?, payload_base64, db_name, self.server_api);
            is_done = sasl_response.done;
        }
    }

    pub fn handleHelloResponse(
        self: *Database,
        current_server_description: *ServerDescription,
        hello_response: *commands.HelloCommandResponse,
        last_update_time: i64,
        round_trip_time: u64,
    ) !void {
        self.topology_description.logical_session_timeout_minutes = hello_response.logicalSessionTimeoutMinutes;
        self.server_session_pool.logical_session_timeout_minutes = hello_response.logicalSessionTimeoutMinutes;

        switch (self.topology_description.type) {
            .Single => {
                try current_server_description.updateWithHelloResponse(hello_response, last_update_time, round_trip_time);

                // replace server description regardless of equality

                self.topology_description.compatible = current_server_description.checkCompatibility(&self.client_config) catch |err| blk: {
                    switch (err) {
                        error.IncompatibleWireVersionAboveMax => {
                            //TODO: add missing values
                            self.topology_description.compatibilityError = try std.fmt.allocPrint(self.allocator, "Server at host:port requires wire version {d}, but this version of {{driverName}} only supports up to {d}.", .{ hello_response.minWireVersion, self.client_config.client_max_wire_version });
                        },
                        error.IncompatibleWireVersionBelowMin => {
                            //TODO: add missing values
                            self.topology_description.compatibilityError = try std.fmt.allocPrint(self.allocator, "Server at host:port reports wire version {d}, but this version of {{driverName}} requires at least {d} (MongoDB {{mongoVersion}}).", .{ hello_response.maxWireVersion, self.client_config.client_min_wire_version });
                        },
                    }
                    break :blk false;
                };

                // self.current_server = .{
                //     .address = self.stream.address,
                //     .server_type = hello_response.server_type,
                //     .min_wire_version = hello_response.minWireVersion,
                //     .max_wire_version = hello_response.maxWireVersion,
                // };
            },

            else => {
                @panic("not implemented");
            },
        }
    }

    pub fn isServerSupportSessions(self: *const Database) bool {
        return self.topology_description.logical_session_timeout_minutes != null;
    }

    pub fn tryGetSession(self: *Database, options: RunCommandOptions) !?*ClientSession {
        if (options.session) |explicit_session| {
            if (!self.isServerSupportSessions()) {
                return error.ServerDoesNotSupportSessions;
            }
            return explicit_session;
        } else {
            if (!self.isServerSupportSessions()) {
                return null;
            }

            var implicit_session = try self.startSession(null);
            implicit_session.mode = .Implicit;
            return implicit_session;
        }
    }

    pub fn runWriteCommand(self: *Database, command: anytype, options: RunCommandOptions, comptime ResponseType: type, comptime WriteErrorType: type) !WriteResponseUnion(ResponseType, ErrorResponse, WriteErrorType) {
        const client_session = try self.tryGetSession(options);
        defer if (client_session) |session| {
            if (session.mode == .Implicit) {
                session.deinit();
            }
        };

        return self.runWriteCommandWithOptionalSession(client_session, command, options, ResponseType, WriteErrorType);
    }

    fn runWriteCommandWithOptionalSession(self: *Database, client_session: ?*ClientSession, command: anytype, options: RunCommandOptions, comptime ResponseType: type, comptime WriteErrorType: type) !WriteResponseUnion(ResponseType, ErrorResponse, WriteErrorType) {
        comptime {
            const command_type_info = @typeInfo(@TypeOf(command));
            if (command_type_info != .pointer) {
                @compileError("runCommand command param must be a pointer to a struct");
            }
            if (!@hasField(@TypeOf(command.*), "$db")) {
                @compileError("runCommand command param must have a @$db field");
            }
            if (!@hasDecl(ResponseType, "parseBson")) {
                @compileError("runCommand command param type must have a parseBson method");
            }
        }

        if (client_session) |session| {
            if (!self.isServerSupportSessions()) {
                return error.ServerDoesNotSupportSessions;
            }
            // TODO: if unacknowledged, raise error

            try self.associateServerSession(session);
            try session.addToCommand(command);
            session.server_session.?.last_used = time.milliTimestamp();
        }
        options.addToCommand(command);

        self.server_api.addToCommand(command);

        const command_serialized = try BsonDocument.fromObject(self.allocator, @TypeOf(command), command);
        errdefer command_serialized.deinit(self.allocator);

        const request_id = RequestIdGenerator.getNextRequestId();
        const command_op_msg = try opcode.OpMsg.init(self.allocator, command_serialized, request_id, 0, .{});
        defer command_op_msg.deinit(self.allocator);

        return try self.runWriteCommandOpcode(command_op_msg, ResponseType, WriteErrorType);
    }

    pub fn runWriteCommandOpcode(self: *Database, command_op_msg: *const opcode.OpMsg, comptime ResponseType: type, comptime WriteErrorType: type) !WriteResponseUnion(ResponseType, ErrorResponse, WriteErrorType) {
        const response = try self.stream.send(self.allocator, command_op_msg);
        defer response.deinit(self.allocator);

        if (try ErrorResponse.isError(self.allocator, response.section_document.document)) {
            const error_response = try ErrorResponse.parseBson(self.allocator, response.section_document.document);
            errdefer error_response.deinit(self.allocator);
            return .{ .err = error_response };
        }

        if (try WriteErrorType.isError(self.allocator, response.section_document.document)) {
            const response_with_write_errors = try WriteErrorType.parseBson(self.allocator, response.section_document.document);
            errdefer response_with_write_errors.deinit(self.allocator);
            return .{ .write_errors = response_with_write_errors };
        }

        return .{ .response = try ResponseType.parseBson(self.allocator, response.section_document.document) };
    }

    pub fn runCommand(self: *Database, allocator: Allocator, command: anytype, options: RunCommandOptions, comptime ResponseType: type) !ResponseUnion(ResponseType, ErrorResponse) {
        const client_session = try self.tryGetSession(options);
        defer if (client_session) |session| {
            if (session.mode == .Implicit) {
                session.deinit();
            }
        };

        const result = self.runCommandWithOptionalSession(allocator, client_session, command, options, ResponseType);
        return result;
    }

    fn runCommandWithOptionalSession(self: *Database, allocator: Allocator, client_session: ?*ClientSession, command: anytype, options: RunCommandOptions, comptime ResponseType: type) !ResponseUnion(ResponseType, ErrorResponse) {
        comptime {
            const command_type_info = @typeInfo(@TypeOf(command));
            if (command_type_info != .pointer) {
                @compileError("runCommand command param must be a pointer to a struct");
            }
            if (!@hasField(@TypeOf(command.*), "$db")) {
                @compileError("runCommand command param must have a @$db field");
            }
            if (!@hasDecl(ResponseType, "parseBson")) {
                @compileError("runCommand command param type must have a parseBson method");
            }
        }

        if (client_session) |session| {
            if (!self.isServerSupportSessions()) {
                return error.ServerDoesNotSupportSessions;
            }

            try self.associateServerSession(session);
            try session.addToCommand(command);

            session.server_session.?.last_used = time.milliTimestamp();
        }

        options.addToCommand(command);

        self.server_api.addToCommand(command);

        const command_serialized = try BsonDocument.fromObject(allocator, @TypeOf(command), command);
        errdefer command_serialized.deinit(allocator);

        const request_id = RequestIdGenerator.getNextRequestId();
        const command_op_msg = try opcode.OpMsg.init(allocator, command_serialized, request_id, 0, .{});
        defer command_op_msg.deinit(allocator);

        const response = try self.stream.send(allocator, command_op_msg);
        defer response.deinit(allocator);

        if (try ErrorResponse.isError(allocator, response.section_document.document)) {
            const error_response = try response.section_document.document.toObject(self.allocator, ErrorResponse, .{ .ignore_unknown_fields = true });
            errdefer error_response.deinit(self.allocator);
            return .{ .err = error_response };
        }

        const response_parsed = try response.section_document.document.toObject(allocator, ResponseType, .{ .ignore_unknown_fields = true });
        errdefer response_parsed.deinit(allocator);
        return .{ .response = response_parsed };
    }
};
