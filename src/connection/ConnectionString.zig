const std = @import("std");

const Address = @import("../server-discovery-and-monitoring/Address.zig").Address;
const AddressList = std.net.AddressList;

pub const ConnectionString = struct {
    scheme: []const u8,
    username: ?[]const u8 = null,
    password: ?[]const u8 = null,
    hosts: std.ArrayList(*AddressList),
    auth_database: ?[]const u8 = null,
    options: std.StringHashMap([]const u8),
    use_tls: ?bool = null,

    pub fn init(allocator: std.mem.Allocator) ConnectionString {
        return .{
            .scheme = "mongodb",
            .hosts = .empty,
            .options = std.StringHashMap([]const u8).init(allocator),
            .auth_database = null,
            .use_tls = null,
        };
    }

    pub fn deinit(self: *ConnectionString, allocator: std.mem.Allocator) void {
        var it_options = self.options.iterator();
        while (it_options.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            allocator.free(entry.value_ptr.*);
        }
        self.options.deinit();
        for (self.hosts.items) |host| {
            host.deinit();
        }
        self.hosts.deinit(allocator);

        if (self.auth_database) |auth_database| {
            allocator.free(auth_database);
        }
    }

    pub fn fromText(allocator: std.mem.Allocator, uri_text: []const u8) !ConnectionString {
        const uri = try std.Uri.parse(uri_text);
        return try fromUri(allocator, uri);
    }

    pub fn fromUri(allocator: std.mem.Allocator, uri: std.Uri) !ConnectionString {
        var conn = ConnectionString.init(allocator);

        if (uri.host == null) {
            return error.MissingHost;
        }

        const expected_scheme = "mongodb";
        if (!std.mem.eql(u8, uri.scheme, expected_scheme)) {
            return error.InvalidScheme;
        }

        conn.username = if (uri.user) |value| try value.toRawMaybeAlloc(allocator) else null;
        conn.password = if (uri.password) |value| try value.toRawMaybeAlloc(allocator) else null;

        const hosts_part_percent_encoded = uri.host.?.percent_encoded;
        var it = std.mem.splitScalar(u8, hosts_part_percent_encoded, ',');
        while (it.next()) |host| {
            const host_decoded_buffer = try allocator.alloc(u8, host.len);
            defer allocator.free(host_decoded_buffer);
            const host_decoded = std.Uri.percentDecodeBackwards(host_decoded_buffer, host);

            const host_obj = try Address.parse(host_decoded);
            const addresses = try std.net.getAddressList(allocator, host_obj.hostname, host_obj.port);
            addresses.canon_name = try addresses.arena.allocator().dupe(u8, host_obj.hostname);
            try conn.hosts.append(allocator, addresses);
        }

        if (uri.query) |query_string_component| {
            const options_part_percent_encoded = query_string_component.percent_encoded;
            var opts_it = std.mem.splitScalar(u8, options_part_percent_encoded, '&');
            while (opts_it.next()) |opt| {
                if (std.mem.indexOfScalar(u8, opt, '=')) |eq_pos| {
                    const key = try std.ascii.allocLowerString(allocator, opt[0..eq_pos]);
                    errdefer allocator.free(key);

                    const value = try allocator.dupe(u8, opt[eq_pos + 1 ..]);
                    errdefer allocator.free(value);
                    try conn.options.put(key, value);
                }
            }
        }

        const path = try uri.path.toRawMaybeAlloc(allocator);
        errdefer allocator.free(path);
        if (path.len > 1) {
            conn.auth_database = try allocator.dupe(u8, path[1..]);
        }

        conn.use_tls = if (conn.options.get("tls")) |tls| std.mem.eql(u8, tls, "true") else null;

        return conn;
    }
};

test "Test basic connection string" {
    const allocator = std.testing.allocator;
    var conn = try ConnectionString.fromText(allocator, "mongodb://localhost");
    defer conn.deinit(allocator);
    try std.testing.expectEqualStrings("localhost", conn.hosts.items[0].canon_name orelse "missing");
    try std.testing.expectEqual(@as(usize, 1), conn.hosts.items.len);
    try std.testing.expectEqual(@as(usize, 0), conn.options.count());
}

test "Test multiple hosts" {
    const allocator = std.testing.allocator;

    var conn = try ConnectionString.fromText(allocator, "mongodb://localhost,127.0.0.1");
    defer conn.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 2), conn.hosts.items.len);
    try std.testing.expectEqualStrings("localhost", conn.hosts.items[0].canon_name orelse "missing");
    try std.testing.expectEqualStrings("127.0.0.1", conn.hosts.items[1].canon_name orelse "missing");
}

test "Test with options" {
    const allocator = std.testing.allocator;

    var conn = try ConnectionString.fromText(allocator, "mongodb://localhost?replicaSet=test&ssl=true");
    defer conn.deinit(allocator);
    try std.testing.expectEqualStrings("true", conn.options.get("ssl") orelse "missing");
    try std.testing.expectEqualStrings("test", conn.options.get("replicaset") orelse "missing");
}

test "Test with auth database" {
    const allocator = std.testing.allocator;

    var conn = try ConnectionString.fromText(allocator, "mongodb://localhost/db1");
    defer conn.deinit(allocator);
    try std.testing.expectEqualStrings("db1", conn.auth_database orelse "missing");
}

// test "Test with escaped characters" {
//     const allocator = std.testing.allocator;

//     var conn = try ConnectionString.fromText(allocator, "mongodb://local%2Fhost");
//     defer conn.deinit(allocator);
//     try std.testing.expectEqualStrings("local/host", conn.hosts.items[0].canon_name orelse "missing");
// }

test "Test error cases" {
    const allocator = std.testing.allocator;

    try std.testing.expectError(error.InvalidScheme, ConnectionString.fromText(allocator, "invalid://localhost"));
}
