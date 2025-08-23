const std = @import("std");
const bson = @import("bson");

const BsonDocument = bson.BsonDocument;

pub const OkResponse = struct {
    ok: f64,

    pub fn parseBson(allocator: std.mem.Allocator, bson_doc: *BsonDocument) !OkResponse {
        return bson_doc.toObject(allocator, OkResponse, .{ .ignore_unknown_fields = true });
    }
};
