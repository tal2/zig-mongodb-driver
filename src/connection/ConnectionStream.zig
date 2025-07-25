const std = @import("std");
const ConnectionString = @import("ConnectionString.zig").ConnectionString;
const net = std.net;
const opcode = @import("../protocol/opcode.zig");
const OpcodeMsg = opcode.OpMsg;
const RunCommandOptions = @import("../commands/RunCommandOptions.zig").RunCommandOptions;

pub const ConnectionStream = struct {
    is_connected: bool,
    address: net.Address,
    stream: ?net.Stream = null,

    pub fn init(address: net.Address) ConnectionStream {
        return .{
            .address = address,
            .stream = null,
            .is_connected = false,
        };
    }

    pub fn fromConnectionString(connection_string: *const ConnectionString) !ConnectionStream {
        const host = connection_string.hosts.items[0];
        const port = host.port;
        const ip = host.hostname; // TODO: resolve host to ip address - https://github.com/lun-4/zigdig , https://zigistry.dev/packages/github/milo-g/zigdns/

        const address = try net.Address.resolveIp(ip, port);
        return init(address);
    }

    pub fn connect(self: *ConnectionStream) !void {
        if (self.is_connected) {
            std.debug.print("[ConnectionStream] already connected\n", .{});
            return;
        }

        self.is_connected = true;
        self.stream = try net.tcpConnectToAddress(self.address);
    }

    /// caller owns the response
    pub fn send(self: *ConnectionStream, allocator: std.mem.Allocator, op: *const opcode.OpMsg) !*opcode.OpMsg {
        const stream_writer = try self.writer();
        try op.write(stream_writer);

        const response = try self.waitForResponse(allocator);
        return response;
    }

    pub fn waitForResponse(self: *ConnectionStream, allocator: std.mem.Allocator) !*OpcodeMsg {
        if (self.stream == null) {
            return error.NotConnected;
        }
        const stream = self.stream.?;
        const reader = stream.reader();
        const msg = try opcode.readMessage(allocator, reader);

        return msg;
    }

    pub fn writer(self: *ConnectionStream) !net.Stream.Writer {
        if (!self.is_connected) {
            return error.NotConnected;
        }
        return self.stream.?.writer();
    }

    pub fn close(self: *ConnectionStream) void {
        if (self.is_connected) {
            self.stream.?.close();
            self.stream = null;
            self.is_connected = false;
        }
    }

    pub fn deinit(self: *ConnectionStream) void {
        self.close();
    }
};
