const std = @import("std");
const bson = @import("bson");
const Allocator = std.mem.Allocator;

pub fn base64Encode(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    const encoded_len = std.base64.standard.Encoder.calcSize(input.len);
    const encoded = try allocator.alloc(u8, encoded_len);
    _ = std.base64.standard.Encoder.encode(encoded, input);
    return encoded;
}

pub fn base64Decode(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    const decoded_len = try std.base64.standard.Decoder.calcSizeForSlice(input);
    const decoded = try allocator.alloc(u8, decoded_len);
    _ = try std.base64.standard.Decoder.decode(decoded, input);
    return decoded;
}
