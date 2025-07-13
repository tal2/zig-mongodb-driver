const std = @import("std");

pub const Host = @This();

const default_port = 27017;

port: u16,
domain_or_ip: []const u8,

pub fn init(domain_or_ip: []const u8, port: u16) Host {
    return .{
        .domain_or_ip = domain_or_ip,
        .port = port,
    };
}

pub fn deinit(self: *Host, allocator: std.mem.Allocator) void {
    allocator.free(self.domain_or_ip);
}

pub fn parse(host: []const u8) !Host {
    const colon_pos = std.mem.indexOfScalar(u8, host, ':');

    if (colon_pos) |pos| {
        if (pos == 0 or pos + 1 >= host.len) {
            return error.InvalidHost;
        }

        const domain_or_ip = host[0..pos];
        const port = try std.fmt.parseInt(u16, host[pos + 1 ..], 10);
        return Host.init(domain_or_ip, port);
    } else {
        return Host.init(host, default_port);
    }
}
