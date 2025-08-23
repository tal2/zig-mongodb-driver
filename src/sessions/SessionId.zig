const std = @import("std");
const uuid = @import("uuid");
const bson = @import("bson");

const BsonDocument = bson.BsonDocument;
const ServerSessionPool = @import("./ServerSessionPool.zig").ServerSessionPool;
const BsonBinary = bson.bson_types.BsonBinary;

pub const BaseSessionId = struct {
    /// UUID v4
    id: BsonBinary,
};

pub const SessionId = struct {
    /// UUID v4
    id: uuid.Uuid,
    doc: *BsonDocument,

    pub fn generate(allocator: std.mem.Allocator) !SessionId {
        const id = uuid.v4.new();
        const id_bytes = std.mem.toBytes(id);
        const session_id_obj = BaseSessionId{
            .id = BsonBinary.fromBytes(&id_bytes, .uuid),
        };
        const doc = try bson.BsonDocument.fromObject(allocator, BaseSessionId, session_id_obj);
        const session_id = SessionId{
            .id = id,
            .doc = doc,
        };

        return session_id;
    }

    pub fn deinit(self: *SessionId, allocator: std.mem.Allocator) void {
        self.doc.deinit(allocator);
    }
};
