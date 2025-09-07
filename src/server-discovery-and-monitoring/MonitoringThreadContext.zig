const std = @import("std");

const net = std.net;
const time = std.time;
const Thread = std.Thread;

const MongodbClient = @import("../MongodbClient.zig").MongodbClient;
const Address = std.net.Address;

const server_discovery_and_monitoring = @import("../server-discovery-and-monitoring/root.zig");
const ServerDescription = server_discovery_and_monitoring.ServerDescription;
const ConnectionStream = @import("../connection/ConnectionStream.zig").ConnectionStream;
const commands = @import("../commands/root.zig");

const utils = @import("../utils.zig");

pub const MonitoringThreadContext = struct {
    const min_heartbeat_frequency_ns: comptime_int = std.time.ns_per_ms * 500;
    const default_heartbeat_frequency_multithreaded_ms: comptime_int = std.time.ms_per_s * 10;
    const default_heartbeat_frequency_single_threaded_ms: comptime_int = std.time.ms_per_s * 60;

    pub const ServerMonitoringMode = enum { stream, poll, auto };

    pub const ServerMonitoringOptions = struct {
        server_monitoring_mode: ServerMonitoringMode = .auto,
        heartbeat_frequency_ms: ?u64 = default_heartbeat_frequency_multithreaded_ms,
        use_tls: bool,
    };

    allocator: std.mem.Allocator,
    client: *MongodbClient,
    server_address: Address,
    use_tls: bool,

    done: bool = false,
    stop_signal: bool = false,
    heartbeat_frequency_ns: u64 = default_heartbeat_frequency_multithreaded_ms * std.time.ns_per_ms,

    server_monitoring_mode: ServerMonitoringMode = .auto,

    pub fn init(allocator: std.mem.Allocator, client: *MongodbClient, server_address: Address, options: ServerMonitoringOptions) !MonitoringThreadContext {
        if (options.heartbeat_frequency_ms) |heartbeat_frequency_ms| {
            if ((heartbeat_frequency_ms * std.time.ns_per_ms) < min_heartbeat_frequency_ns) {
                return error.HeartbeatFrequencyTooLow;
            }
        }

        return MonitoringThreadContext{
            .allocator = allocator,
            .client = client,
            .server_address = server_address,
            .use_tls = options.use_tls,
            .server_monitoring_mode = options.server_monitoring_mode,
            .heartbeat_frequency_ns = (options.heartbeat_frequency_ms orelse default_heartbeat_frequency_multithreaded_ms) * std.time.ns_per_ms,
        };
    }

    pub fn startServerMonitoring(context: *MonitoringThreadContext) void {
        defer context.allocator.destroy(context);

        defer if (!context.stop_signal) {
            var index: usize = 0;
            for (context.client.monitoring_threads.items) |thread_context| {
                if (context == thread_context) {
                    _ = context.client.monitoring_threads.swapRemove(index);
                    break;
                }
                index += 1;
            }
        };

        context.monitorServer() catch |err| {
            std.debug.print("error monitoring server: {any}\n", .{err});
        };

        context.done = true;
    }

    fn monitorServer(context: *MonitoringThreadContext) !void {
        const client = context.client;
        const allocator = context.allocator;

        var current_server_description = try allocator.create(ServerDescription);
        defer current_server_description.deinit(allocator);
        current_server_description.* = ServerDescription{
            .address = context.server_address,
            .hosts = std.StringHashMap(Address).init(allocator),
            .passives = std.StringHashMap(Address).init(allocator),
            .arbiters = std.StringHashMap(Address).init(allocator),
            .tags = std.StringHashMap([]const u8).init(allocator),
            .last_update_time = 0,
        };

        var stream = ConnectionStream{
            .allocator = allocator,
            .address = context.server_address,
            .use_tls = context.use_tls,
        };
        defer stream.close();
        try stream.connect();

        try client.handshake(current_server_description, &stream, null);

        const command_hello = try commands.makeHelloCommand(allocator, client.db_name, client.server_api);
        defer command_hello.deinit(allocator);

        var last_heartbeat_time_ms: i64 = time.milliTimestamp();
        const time_between_heartbeats = context.heartbeat_frequency_ns;

        while (true) {
            var sleep_ns: u64 = @max(min_heartbeat_frequency_ns, time_between_heartbeats - @as(u64, @intCast((time.milliTimestamp() - last_heartbeat_time_ms) * std.time.ns_per_ms)));
            while (sleep_ns > 0) {
                if (context.stop_signal) return;
                const start_sleep_time = time.nanoTimestamp();
                Thread.sleep(@min(100 * std.time.ns_per_ms, sleep_ns));
                const end_sleep_time = time.nanoTimestamp();
                sleep_ns -|= @as(u64, @intCast(end_sleep_time - start_sleep_time));
            } else {
                if (context.stop_signal) return;
            }
            const start_time = time.milliTimestamp();

            var response = try stream.send(allocator, command_hello);
            defer response.deinit(allocator);

            const now = time.milliTimestamp();
            const end_time = now;
            last_heartbeat_time_ms = now;

            const round_trip_time: u64 = @intCast(end_time - start_time);

            const hello_response = try commands.HelloCommandResponse.parseBson(allocator, response.section_document.document);
            defer allocator.destroy(hello_response);

            try client.handleHelloResponse(current_server_description, hello_response, now, round_trip_time);
        }
    }
};
