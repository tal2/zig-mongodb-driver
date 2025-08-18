const BsonDocument = @import("bson").BsonDocument;

pub const Hint = union(enum) {
    string: []const u8,
    document: *BsonDocument,
};
