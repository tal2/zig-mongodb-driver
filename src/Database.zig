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

const opcode = @import("protocol/opcode.zig");
const Collection = @import("Collection.zig").Collection;
const BsonDocument = bson.BsonDocument;
const RunCommandOptions = commands.RunCommandOptions.RunCommandOptions;
const HelloCommandResponse = commands.HelloCommand.HelloCommandResponse;

const server_discovery_and_monitoring = @import("server-discovery-and-monitoring/root.zig");
const MonitoringThreadContext = server_discovery_and_monitoring.MonitoringThreadContext;
const TopologyDescription = server_discovery_and_monitoring.TopologyDescription;
const ServerDescription = server_discovery_and_monitoring.ServerDescription;
const ClientConfig = server_discovery_and_monitoring.ClientConfig;
const Address = server_discovery_and_monitoring.Address;
const TopologyVersion = server_discovery_and_monitoring.TopologyVersion;
const TopologyType = server_discovery_and_monitoring.TopologyType;
const ServerType = server_discovery_and_monitoring.ServerType;
const ServerApi = server_discovery_and_monitoring.ServerApi;

pub const Database = struct {
    allocator: Allocator,
    db_name: []const u8,
    stream: ConnectionStream,
    topology_description: *TopologyDescription,
    monitoring_threads: std.ArrayList(*MonitoringThreadContext),
    pool: *std.Thread.Pool,
    server_api: ServerApi,
    client_config: ClientConfig,

    pub fn init(allocator: Allocator, conn_str: *const ConnectionString, server_api: ServerApi) !Database {
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
            .db_name = conn_str.auth_database,
            .stream = stream,
            .topology_description = topology_description,
            .monitoring_threads = std.ArrayList(*MonitoringThreadContext).init(allocator),
            .pool = pool,
            .server_api = server_api,
            .client_config = .{
                .client_min_wire_version = 0, // TODO:
                .client_max_wire_version = 0, // TODO:
                .seeds = conn_str.hosts,
            },
        };
    }

    pub fn deinit(self: *Database) void {
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
        // self.allocator.destroy(self);
    }

    pub fn connect(self: *Database) !void {
        try self.stream.connect();

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
        const database = context.database;
        const allocator = context.allocator;

        var current_server_description = try allocator.create(ServerDescription);
        defer current_server_description.deinit(allocator);
        current_server_description.address = context.server_address;

        const stream_address = try net.Address.resolveIp(context.server_address.hostname, context.server_address.port);
        var stream = ConnectionStream.init(stream_address);
        defer stream.deinit();
        try stream.connect();

        const server_description_updated = try database.handshake(current_server_description, &stream);

        var server_description_temp = current_server_description;
        current_server_description = server_description_updated;
        server_description_temp.deinit(allocator);

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
            var previous_server_description = current_server_description;
            current_server_description = try database.handleHelloResponse(previous_server_description, hello_response, now, round_trip_time);
            previous_server_description.deinit(allocator);
        }
    }

    fn handshake(self: *Database, current_server_description: *ServerDescription, conn_stream: *ConnectionStream) !*ServerDescription {
        const allocator = self.allocator;

        // const options: RunCommandOptions = .{
        //     .readPreference = .primary,
        // };
        const command = try commands.makeHelloCommand(allocator, self.db_name, self.server_api);
        defer command.deinit(allocator);

        const start_time = time.milliTimestamp();
        var result = try conn_stream.send(allocator, command);
        const hello_response = commands.HelloCommandResponse.parseBson(allocator, result.section_document.document) catch |err| {
            std.debug.print("error parsing hello response: {any}\n", .{err});
            //TODO: handle error
            @panic("error parsing hello response");
            // return current_server_description;
        };
        defer allocator.destroy(hello_response);
        defer result.deinit(allocator);

        const now = time.milliTimestamp();

        const end_time = time.milliTimestamp();
        const round_trip_time: u64 = @intCast(end_time - start_time);

        return try self.handleHelloResponse(current_server_description, hello_response, now, round_trip_time);
    }

    fn handleHelloResponse(self: *Database, current_server_description: *ServerDescription, hello_response: *commands.HelloCommandResponse, last_update_time: i64, round_trip_time: u64) !*ServerDescription {
        const allocator = self.allocator;

        var new_server_description = try current_server_description.mergeCloneWithHelloResponse(allocator, hello_response, last_update_time, round_trip_time);
        errdefer new_server_description.deinit(allocator);

        self.topology_description.logical_session_timeout_minutes = hello_response.logicalSessionTimeoutMinutes;

        var server_description_updated: *ServerDescription = undefined;

        switch (self.topology_description.type) {
            .Single => {
                // replace server description regardless of equality
                server_description_updated = new_server_description;

                self.topology_description.compatible = new_server_description.checkCompatibility(&self.client_config) catch |err| blk: {
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
                if (current_server_description.isStale(new_server_description)) {
                    server_description_updated = new_server_description;
                }

                @panic("not implemented");
            },
        }

        return server_description_updated;
    }
};
