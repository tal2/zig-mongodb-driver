const std = @import("std");
const tls = @import("tls");

const ConnectionString = @import("ConnectionString.zig").ConnectionString;
const net = std.net;
const opcode = @import("../protocol/opcode.zig");
const OpcodeMsg = opcode.OpMsg;

pub const ConnectionStream = struct {
    allocator: std.mem.Allocator,
    address: net.Address,
    hostname: ?[]const u8 = null,

    stream: ?net.Stream = null,
    stream_buffer_write: [1024 * 4]u8 = undefined,
    stream_buffer_read: [tls.max_ciphertext_record_len]u8 = undefined,
    stream_writer: ?net.Stream.Writer = null,
    stream_reader: ?net.Stream.Reader = null,

    use_tls: bool,
    tls_client: ?*tls.Connection = null,
    tls_buffer_write: [tls.max_ciphertext_record_len]u8 = undefined,
    tls_buffer_read: [tls.max_ciphertext_record_len]u8 = undefined,
    tls_client_writer: ?*tls.Connection.Writer = null,
    tls_client_reader: ?*tls.Connection.Reader = null,

    pub fn fromConnectionString(allocator: std.mem.Allocator, connection_string: *const ConnectionString) !ConnectionStream {
        const host = connection_string.hosts.items[0];
        const address = host.addrs[0];

        return .{
            .allocator = allocator,
            .address = address,
            .hostname = if (host.canon_name) |canon_name| try allocator.dupe(u8, canon_name) else null,
            .use_tls = connection_string.use_tls orelse false,
        };
    }

    pub fn connect(self: *ConnectionStream) !void {
        if (self.stream) |_| {
            return;
        }

        self.stream = try net.tcpConnectToAddress(self.address);
        self.stream_writer = self.stream.?.writer(&self.stream_buffer_write);
        self.stream_reader = self.stream.?.reader(&self.stream_buffer_read);

        if (self.use_tls) {
            var root_ca = try tls.config.cert.fromSystem(self.allocator);
            defer root_ca.deinit(self.allocator);

            const tls_client = try self.allocator.create(tls.Connection);
            tls_client.* = try tls.client(self.stream_reader.?.interface(), &self.stream_writer.?.interface, .{
                .host = self.hostname orelse "",
                .root_ca = root_ca,
            });
            self.tls_client = tls_client;

            const tls_client_writer = try self.allocator.create(tls.Connection.Writer);
            tls_client_writer.* = tls_client.writer(&self.tls_buffer_write);
            self.tls_client_writer = tls_client_writer;

            const tls_client_reader = try self.allocator.create(tls.Connection.Reader);
            tls_client_reader.* = tls_client.reader(&self.tls_buffer_read);
            self.tls_client_reader = tls_client_reader;
        }
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

    fn writer(self: *ConnectionStream) (error{NotConnected} || std.mem.Allocator.Error)!*std.Io.Writer {
        if (self.stream_writer == null) {
            return error.NotConnected;
        }
        if (self.use_tls) {
            if (self.tls_client_writer) |w| {
                return &w.interface;
            } else {
                return error.NotConnected;
            }
        }
        return &self.stream_writer.?.interface;
    }

    fn reader(self: *ConnectionStream) (error{NotConnected} || std.mem.Allocator.Error)!*std.Io.Reader {
        if (self.stream_reader == null) {
            return error.NotConnected;
        }
        if (self.use_tls) {
            if (self.tls_client_reader) |r| {
                return &r.interface;
            } else {
                return error.NotConnected;
            }
        }
        var stream_reader = &self.stream_reader.?;
        return stream_reader.interface();
    }

    pub fn close(self: *ConnectionStream) void {
        if (self.tls_client != null) {
            self.tls_client.?.close() catch |err| {
                std.debug.print("error closing tls client: {any}\n", .{err});
            };
            self.tls_client = null;
        }
        if (self.stream) |stream| {
            stream.close();
            self.stream = null;
        }
    }

    pub fn deinit(self: *ConnectionStream) void {
        self.close();
        if (self.tls_client != null) {
            const tls_client = self.tls_client.?;
            self.tls_client = null;
            self.allocator.destroy(tls_client);
        }
        if (self.tls_client_writer != null) {
            const tls_client_writer = self.tls_client_writer.?;
            self.tls_client_writer = null;
            self.allocator.destroy(tls_client_writer);
        }
        if (self.tls_client_reader != null) {
            const tls_client_reader = self.tls_client_reader.?;
            self.tls_client_reader = null;
            self.allocator.destroy(tls_client_reader);
        }
        if (self.hostname != null) {
            self.allocator.free(self.hostname.?);
            self.hostname = null;
        }
    }
};
