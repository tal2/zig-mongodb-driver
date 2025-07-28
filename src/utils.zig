const std = @import("std");
const bson = @import("bson");
const Allocator = std.mem.Allocator;
const BsonDocument = bson.BsonDocument;

pub fn parseBsonDocument(T: type, allocator: Allocator, document: *const BsonDocument, options: std.json.ParseOptions) !std.json.Parsed(T) {
    // TODO: parse to struct directly from bson document

    const document_json = try document.toJsonString(allocator, false);
    defer allocator.free(document_json);

    return try std.json.parseFromSlice(T, allocator, document_json, options);
}

pub fn parseBsonToOwned(T: type, allocator: Allocator, document: *const BsonDocument) !*T {
    comptime {
        if (!@hasDecl(T, "dupe")) {
            @compileError("T must have a dupe method");
        }
        const dupe_method = @field(T, "dupe");

        const dupe_fn_type = @typeInfo(@TypeOf(dupe_method));
        if (dupe_fn_type != .@"fn") {
            @compileError("T.dupe must be a function");
        }
        const fn_info = dupe_fn_type.@"fn";
        if (fn_info.params[0].type != *const T) {
            @compileError("T.dupe must have a T parameter");
        }
        if (fn_info.params[1].type != Allocator) {
            @compileError("T.dupe must have an Allocator parameter");
        }

        // TODO: check return type

    }
    const parsed = try parseBsonDocument(T, allocator, document, .{ .ignore_unknown_fields = false, .allocate = .alloc_always });
    defer parsed.deinit();

    const result = try parsed.value.dupe(allocator);
    errdefer result.deinit(allocator);

    return result;
}
