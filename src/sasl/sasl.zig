const std = @import("std");

pub const SaslError = error{
    ProhibitedCharacter,
    InvalidUtf8,
} || std.mem.Allocator.Error;

pub fn saslPrep(allocator: std.mem.Allocator, input: []const u8) SaslError![]const u8 {
    if (input.len == 0) {
        return "";
    }

    const initial_capacity = input.len;
    var mapped = try std.ArrayList(u8).initCapacity(allocator, initial_capacity);
    errdefer mapped.deinit();

    const view = try std.unicode.Utf8View.init(input);
    var it = view.iterator();
    while (it.nextCodepointSlice()) |codepoint| {
        if (codepoint.len == 1) {
            const byte = codepoint[0];
            if (byte <= 0x7F) { // ASCII
                switch (byte) {
                    ' ' => {
                        try mapped.append(' ');
                    },
                    // control chars
                    0x00...0x1F, 0x7F, 0x80...0x9F => {
                        return error.ProhibitedCharacter;
                    },
                    else => {
                        try mapped.append(byte);
                    },
                }
                continue;
            }
        }
        // Handle UTF-8 characters
        try mapped.appendSlice(codepoint);
    }

    // TODO: Normalize - https://datatracker.ietf.org/doc/html/rfc3454#section-4

    // TODO: Check bidirectional characters - https://datatracker.ietf.org/doc/html/rfc3454#section-6

    return mapped.toOwnedSlice();
}
