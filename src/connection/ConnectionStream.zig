const std = @import("std");
const ConnectionString = @import("ConnectionString.zig").ConnectionString;
const net = std.net;
const opcode = @import("../protocol/opcode.zig");
const OpcodeMsg = opcode.OpMsg;

pub const ConnectionStream = struct {
    allocator: std.mem.Allocator,
    address: net.Address,
    stream: ?net.Stream = null,
    stream_buffer_write: [1024]u8 = undefined,
    stream_buffer_read: [1024]u8 = undefined,
    stream_writer: ?net.Stream.Writer = null,
    stream_reader: ?net.Stream.Reader = null,
    hostname: ?[]const u8 = null,

    pub fn fromConnectionString(allocator: std.mem.Allocator, connection_string: *const ConnectionString) !ConnectionStream {
        const host = connection_string.hosts.items[0];
        const address = host.addrs[0];

        return .{
            .allocator = allocator,
            .address = address,
            .stream = null,
            .stream_buffer_write = undefined,
            .stream_buffer_read = undefined,
            .stream_writer = null,
            .stream_reader = null,
            .hostname = if (host.canon_name) |canon_name| try allocator.dupe(u8, canon_name) else null,
        };
    }

    pub fn connect(self: *ConnectionStream) net.TcpConnectToAddressError!void {
        if (self.stream) |_| {
            return;
        }

        self.stream = try net.tcpConnectToAddress(self.address);
        self.stream_writer = self.stream.?.writer(&self.stream_buffer_write);
        self.stream_reader = self.stream.?.reader(&self.stream_buffer_read);
    }

    /// caller owns the response
    pub fn send(self: *ConnectionStream, allocator: std.mem.Allocator, op: *const opcode.OpMsg) !*opcode.OpMsg {
        if (opcode.OpMsg.OpMsgFlags.isFlagHasMoreToComeSet(op.flag_bits)) {
            return error.ExhaustBitIsSet;
        }
        const stream_writer = try self.writer();
        try op.write(stream_writer);
        try stream_writer.flush();

        const response = try self.waitForResponse(allocator);
        return response;
    }

    pub fn waitForResponse(self: *ConnectionStream, allocator: std.mem.Allocator) !*OpcodeMsg {
        const r = try self.reader();
        const msg = try opcode.readMessage(allocator, r);

        return msg;
    }

    pub fn writer(self: *ConnectionStream) error{NotConnected}!*std.Io.Writer {
        if (self.stream_writer == null) {
            return error.NotConnected;
        }
        return &self.stream_writer.?.interface;
    }

    fn reader(self: *ConnectionStream) error{NotConnected}!*std.Io.Reader {
        if (self.stream_reader == null) {
            return error.NotConnected;
        }
        var stream_reader = &self.stream_reader.?;
        return stream_reader.interface();
    }

    pub fn close(self: *ConnectionStream) void {
        if (self.stream) |stream| {
            stream.close();
            self.stream = null;
        }
    }

    pub fn deinit(self: *ConnectionStream) void {
        self.close();
    }
};
