const std = @import("std");

const default_port = 27017;

pub const Address = struct {
    hostname: []const u8, // normalized to lowercase
    port: u16,

    pub fn init(hostname: []const u8, port: u16) Address {
        return .{
            .hostname = hostname,
            .port = port,
        };
    }

    pub fn parse(host_and_port: []const u8) !Address {
        const colon_pos = std.mem.indexOfScalar(u8, host_and_port, ':');

        if (colon_pos) |pos| {
            if (pos == 0 or pos + 1 >= host_and_port.len) {
                return error.InvalidHost;
            }

            const hostname = host_and_port[0..pos];
            const port = try std.fmt.parseInt(u16, host_and_port[pos + 1 ..], 10);
            return Address.init(hostname, port);
        } else {
            return Address.init(host_and_port, default_port);
        }
    }

    pub fn isEqualTo(self: *const Address, b: *const Address) bool {
        return self.port == b.port and std.mem.eql(u8, self.hostname, b.hostname);
    }
};
