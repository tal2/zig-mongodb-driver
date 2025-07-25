const std = @import("std");
const bson = @import("bson");
const BsonDocument = bson.BsonDocument;

pub fn parseBsonDocument(T: type, allocator: std.mem.Allocator, document: *const BsonDocument, options: std.json.ParseOptions) !std.json.Parsed(T) {
    // TODO: parse to struct directly from bson document

    const document_json = try document.toJsonString(allocator, false);
    defer allocator.free(document_json);

    return try std.json.parseFromSlice(T, allocator, document_json, options);
}
