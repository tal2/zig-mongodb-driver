const std = @import("std");
const bson = @import("bson");
const time = std.time;
const net = std.net;
const Thread = std.Thread;
const Allocator = std.mem.Allocator;

const commands = @import("commands/root.zig");
const connection_stream = @import("connection/ConnectionStream.zig");
const ConnectionStream = connection_stream.ConnectionStream;
const ConnectionString = @import("connection/ConnectionString.zig").ConnectionString;

const Collection = @import("Collection.zig").Collection;
const RunCommandOptions = commands.RunCommandOptions.RunCommandOptions;
const HelloCommandResponse = commands.HelloCommand.HelloCommandResponse;

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

pub const Database = struct {
    allocator: Allocator,
    db_name: []const u8,
    stream: ConnectionStream,
    topology_description: *TopologyDescription,
    monitoring_threads: std.ArrayList(*MonitoringThreadContext),
    pool: *std.Thread.Pool,
    server_api: ServerApi,
    client_config: ClientConfig,

    pub fn init(allocator: Allocator, conn_str: *ConnectionString, server_api: ServerApi) !Database {
        bson.bson_types.BsonObjectId.initializeGenerator();

        const stream = try ConnectionStream.fromConnectionString(conn_str);
        const topology_description = try allocator.create(TopologyDescription);
        topology_description.type = .Single;
        topology_description.servers = std.AutoHashMap(Address, ServerDescription).init(allocator);

        const pool_options = std.Thread.Pool.Options{ .allocator = allocator, .n_jobs = conn_str.hosts.items.len };
        var pool = try allocator.create(std.Thread.Pool);
        try pool.init(pool_options);
        errdefer pool.deinit();

        return .{
            .allocator = allocator,
            .db_name = if (conn_str.auth_database) |auth_database| try allocator.dupe(u8, auth_database) else "admin",
            .stream = stream,
            .topology_description = topology_description,
            .monitoring_threads = std.ArrayList(*MonitoringThreadContext).init(allocator),
            .pool = pool,
            .server_api = server_api,
            .client_config = try ClientConfig.init(allocator, &conn_str.hosts),
        };
    }

    pub fn deinit(self: *Database) void {
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
        self.monitoring_threads.deinit();
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
        current_server_description.address = &self.client_config.seeds.items[0];

        try self.handshake(current_server_description, &self.stream, credentials);

        for (self.client_config.seeds.items) |seed| {
            var thread_context = try self.allocator.create(MonitoringThreadContext);
            errdefer self.allocator.destroy(thread_context);
            thread_context.allocator = self.allocator;
            thread_context.server_address = seed;
            thread_context.database = self;

            try self.pool.spawn(startServerMonitoring, .{thread_context});

            self.monitoring_threads.append(thread_context) catch @panic("failed to append thread");
        }
    }

    pub fn collection(self: *Database, collection_name: []const u8) Collection {
        return Collection.init(self, collection_name, self.server_api);
    }

    fn startServerMonitoring(context: *MonitoringThreadContext) void {
        defer context.allocator.destroy(context);

        defer if (!context.stop_signal) {
            var index: usize = 0;
            for (context.database.monitoring_threads.items) |thread_context| {
                if (context == thread_context) {
                    break;
                }
                index += 1;
            }
            _ = context.database.monitoring_threads.swapRemove(index);
        };

        std.debug.print("monitorServer started\n", .{});

        monitorServer(context) catch |err| {
            std.debug.print("error monitoring server: {any}\n", .{err});
        };
        if (context.stop_signal) {
            std.debug.print("stop signal received\n", .{});
        }

        std.debug.print("monitorServer ended\n", .{});
        context.done = true;
    }

    fn monitorServer(context: *MonitoringThreadContext) !void {
        // Authentication (including mechanism negotiation) MUST NOT happen on monitoring-only sockets.

        const database = context.database;
        const allocator = context.allocator;

        var current_server_description = try allocator.create(ServerDescription);
        defer current_server_description.deinit(allocator);
        current_server_description.address = &context.server_address;

        const stream_address = try net.Address.resolveIp(context.server_address.hostname, context.server_address.port);
        var stream = ConnectionStream.init(stream_address);
        defer stream.deinit();
        try stream.connect();

        try database.handshake(current_server_description, &stream, null);

        //TODO: replace topology description pointer instead of mutating it
        // database.topology_description.servers.put(context.server_address, current_server_description.*) catch @panic("failed to put server description");

        // const options = RunCommandOptions{
        //     .readPreference = .primary,
        // };
        const command_hello = try commands.makeHelloCommand(allocator, database.db_name, database.server_api);
        defer command_hello.deinit(allocator);

        const time_between_heartbeats = std.time.ns_per_s * 1;

        while (true) {
            if (context.stop_signal) return;
            Thread.sleep(time_between_heartbeats);
            if (context.stop_signal) return;
            // current_server_description = try database.handshake(current_server_description, &stream);

            const start_time = time.milliTimestamp();

            var response = try stream.send(allocator, command_hello);
            defer response.deinit(allocator);
            const end_time = time.milliTimestamp();
            const round_trip_time: u64 = @intCast(end_time - start_time);
            std.debug.print("handshake time: {d}ms\n", .{round_trip_time});

            const hello_response = try commands.HelloCommandResponse.parseBson(allocator, response.section_document.document);
            defer allocator.destroy(hello_response);

            const now = time.milliTimestamp();
            try database.handleHelloResponse(current_server_description, hello_response, now, round_trip_time);
        }
    }

    fn handshake(self: *Database, current_server_description: *ServerDescription, conn_stream: *ConnectionStream, credentials: ?MongoCredential) !void {
        const allocator = self.allocator;

        // const options: RunCommandOptions = .{
        //     .readPreference = .primary,
        // };
        const command = try commands.makeHelloCommandForHandshake(allocator, self.db_name, "Zig Driver", self.server_api, credentials);
        defer command.deinit(allocator);

        const start_time = time.milliTimestamp();
        var result = try conn_stream.send(allocator, command);
        const hello_response = commands.HelloCommandResponse.parseBson(allocator, result.section_document.document) catch |err| {
            std.debug.print("error parsing hello response: {any}\n", .{err});
            //TODO: handle error
            @panic("error parsing hello response");
            // return current_server_description;
        };
        defer hello_response.deinit(allocator);
        defer result.deinit(allocator);

        if (credentials) |creds| {
            try self.authConversation(allocator, conn_stream, creds);
        }

        const now = time.milliTimestamp();

        const end_time = time.milliTimestamp();
        const round_trip_time: u64 = @intCast(end_time - start_time);

        try self.handleHelloResponse(current_server_description, hello_response, now, round_trip_time);
    }

    fn authConversation(self: *Database, allocator: Allocator, conn_stream: *ConnectionStream, creds: MongoCredential) !void {
        var arena = std.heap.ArenaAllocator.init(allocator);
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

            const sasl_response = try SaslCommandResponse.parseBson(arena_allocator, response.section_document.document);

            if (sasl_response.ok != 1) {
                return error.AuthenticationFailed;
            }

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

    fn handleHelloResponse(
        self: *Database,
        current_server_description: *ServerDescription,
        hello_response: *commands.HelloCommandResponse,
        last_update_time: i64,
        round_trip_time: u64,
    ) !void {


        self.topology_description.logical_session_timeout_minutes = hello_response.logicalSessionTimeoutMinutes;


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
};
