const BsonDocument = @import("bson").BsonDocument;

pub const Comment = union(enum) {
    string: []const u8,
    document: *BsonDocument,
};
