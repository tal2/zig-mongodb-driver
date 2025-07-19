const std = @import("std");
const Address = @import("../server-discovery-and-monitoring/Address.zig").Address;

pub const ConnectionString = struct {
    scheme: []const u8,
    username: ?[]const u8 = null,
    password: ?[]const u8 = null,
    hosts: std.ArrayList(Address),
    auth_database: []const u8,
    options: std.StringHashMap([]const u8),

    pub fn init(allocator: std.mem.Allocator) ConnectionString {
        return .{
            .scheme = "mongodb",
            .hosts = std.ArrayList(Address).init(allocator),
            .options = std.StringHashMap([]const u8).init(allocator),
            .auth_database = "admin",
        };
    }

    pub fn deinit(self: *ConnectionString, allocator: std.mem.Allocator) void {
        var key_it = self.options.keyIterator();
        while (key_it.next()) |key| {
            allocator.free(key.*);
        }
        self.options.deinit();
        self.hosts.deinit();
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
            errdefer allocator.free(host_decoded_buffer);
            const host_decoded = std.Uri.percentDecodeBackwards(host_decoded_buffer, host);

            const host_obj = try Address.parse(host_decoded);
            try conn.hosts.append(host_obj);
        }

        if (uri.query) |query_string_component| {
            const options_part_percent_encoded = query_string_component.percent_encoded;
            var opts_it = std.mem.splitScalar(u8, options_part_percent_encoded, '&');
            while (opts_it.next()) |opt| {
                if (std.mem.indexOfScalar(u8, opt, '=')) |eq_pos| {
                    const key = try std.ascii.allocLowerString(allocator, opt[0..eq_pos]);
                    errdefer allocator.free(key);

                    const value = opt[eq_pos + 1 ..];
                    try conn.options.put(key, value);
                }
            }
        }

        const path = try uri.path.toRawMaybeAlloc(allocator);
        errdefer allocator.free(path);
        if (path.len > 1) {
            conn.auth_database = path[1..];
        }

        return conn;
    }
};

test "Test basic connection string" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    const allocator = arena.allocator();
    defer arena.deinit();

    var conn = try ConnectionString.fromText(allocator, "mongodb://localhost");
    defer conn.deinit(allocator);
    try std.testing.expectEqualStrings("localhost", conn.hosts.items[0].hostname);
    try std.testing.expectEqual(@as(usize, 1), conn.hosts.items.len);
    try std.testing.expectEqual(@as(usize, 0), conn.options.count());
    try std.testing.expectEqualStrings("admin", conn.auth_database);
}

test "Test multiple hosts" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    const allocator = arena.allocator();
    defer arena.deinit();

    var conn = try ConnectionString.fromText(allocator, "mongodb://host1,host2,host3");
    defer conn.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 3), conn.hosts.items.len);
    try std.testing.expectEqualStrings("host1", conn.hosts.items[0].hostname);
    try std.testing.expectEqualStrings("host2", conn.hosts.items[1].hostname);
    try std.testing.expectEqualStrings("host3", conn.hosts.items[2].hostname);
}

test "Test with options" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    const allocator = arena.allocator();
    defer arena.deinit();

    var conn = try ConnectionString.fromText(allocator, "mongodb://localhost?replicaSet=test&ssl=true");
    defer conn.deinit(allocator);
    try std.testing.expectEqualStrings("true", conn.options.get("ssl") orelse "missing");
    try std.testing.expectEqualStrings("test", conn.options.get("replicaset") orelse "missing");
}

test "Test with auth database" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    const allocator = arena.allocator();
    defer arena.deinit();

    var conn = try ConnectionString.fromText(allocator, "mongodb://localhost/db1");
    defer conn.deinit(allocator);
    try std.testing.expectEqualStrings("db1", conn.auth_database);
}

test "Test with escaped characters" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    const allocator = arena.allocator();
    defer arena.deinit();

    var conn = try ConnectionString.fromText(allocator, "mongodb://local%2Fhost");
    defer conn.deinit(allocator);
    try std.testing.expectEqualStrings("local/host", conn.hosts.items[0].hostname);
}

test "Test error cases" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    const allocator = arena.allocator();
    defer arena.deinit();

    try std.testing.expectError(error.InvalidScheme, ConnectionString.fromText(allocator, "invalid://localhost"));
}
