const std = @import("std");
const bson = @import("bson");
const BsonDocument = bson.BsonDocument;

const ReadConcernRaw = struct {
    level: []const u8,
};

pub const ReadConcern = struct {
    level: ReadConcernLevel,

    pub fn toValue(self: ReadConcern) ![]const u8 {
        var buf: [32]u8 = undefined;
        const allocator = std.heap.FixedBufferAllocator.init(&buf);
        const bson_doc = try BsonDocument.fromObject(allocator, ReadConcernRaw, .{ .level = self.level.toValue() });

        return bson_doc.raw_data;
    }
};

pub const ReadConcernLevel = enum {
    Local,
    Majority,
    Linearizable,
    Snapshot,
    Available,

    pub fn toValue(self: ReadConcernLevel) []const u8 {
        return switch (self) {
            .Local => "local",
            .Majority => "majority",
            .Linearizable => "linearizable",
            .Snapshot => "snapshot",
            .Available => "available",
        };
    }
};
