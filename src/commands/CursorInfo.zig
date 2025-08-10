const std = @import("std");
const bson = @import("bson");

const Allocator = std.mem.Allocator;
const BsonDocument = bson.BsonDocument;

pub const CursorInfo = struct {
    id: i64,
    ns: []const u8,
    firstBatch: ?[]*BsonDocument,
    nextBatch: ?[]*BsonDocument,

    pub fn deinit(self: *const CursorInfo, allocator: Allocator) void {
        if (self.firstBatch) |first_batch| {
            for (first_batch) |doc| {
                doc.deinit(allocator);
            }
            allocator.free(first_batch);
        }
        if (self.nextBatch) |next_batch| {
            for (next_batch) |doc| {
                doc.deinit(allocator);
            }
            allocator.free(next_batch);
        }

        allocator.free(self.ns);
        allocator.destroy(self);
    }

    pub const JsonParseError = error{UnexpectedToken} || std.json.Scanner.NextError;

    pub fn jsonParse(allocator: Allocator, source: *std.json.Scanner, options: std.json.ParseOptions) JsonParseError!CursorInfo {
        _ = options;

        if (try source.next() != .object_begin) return error.UnexpectedToken;

        var id: ?i64 = null;
        var ns: ?[]const u8 = null;
        var first_batch: ?[]*BsonDocument = null;
        var next_batch: ?[]*BsonDocument = null;

        blk_tkn: switch (try source.next()) {
            .string => |key| {
                if (first_batch == null and std.mem.eql(u8, key, "firstBatch")) {
                    first_batch = try jsonParseBatchField(allocator, source);
                    continue :blk_tkn try source.next();
                }
                if (next_batch == null and std.mem.eql(u8, key, "nextBatch")) {
                    next_batch = try jsonParseBatchField(allocator, source);
                    continue :blk_tkn try source.next();
                }
                if (id == null and std.mem.eql(u8, key, "id")) {
                    const id_token = try source.next();
                    if (id_token != .number) return JsonParseError.UnexpectedToken;

                    id = std.fmt.parseInt(i64, id_token.number, 10) catch return JsonParseError.UnexpectedToken;
                    continue :blk_tkn try source.next();
                }
                if (ns == null and std.mem.eql(u8, key, "ns")) {
                    const ns_token = try source.next();
                    if (ns_token != .string) return JsonParseError.UnexpectedToken;

                    const ns_value = try allocator.dupe(u8, ns_token.string);
                    errdefer allocator.free(ns_value);
                    ns = ns_value;

                    continue :blk_tkn try source.next();
                }
                return JsonParseError.UnexpectedToken;
            },
            .object_end => {},
            else => {
                return JsonParseError.UnexpectedToken;
            },
        }

        if (id == null or ns == null or (first_batch == null and next_batch == null)) return JsonParseError.UnexpectedToken; // TODO: use better error

        return .{
            .id = id.?,
            .ns = ns.?,
            .firstBatch = first_batch,
            .nextBatch = next_batch,
        };
    }

    fn jsonParseBatchField(allocator: Allocator, source: *std.json.Scanner) JsonParseError![]*BsonDocument {
        const batch_array_begin = try source.next();
        if (batch_array_begin != .array_begin) return error.UnexpectedToken;

        var batch_list = std.ArrayList(*BsonDocument).init(allocator);
        errdefer batch_list.deinit();

        var batch_item_token_type = try source.peekNextTokenType();
        while (batch_item_token_type != .array_end) : (batch_item_token_type = try source.peekNextTokenType()) {
            const doc = BsonDocument.fromJsonReader(allocator, source) catch |err| {
                switch (err) {
                    JsonParseError.OutOfMemory => return JsonParseError.OutOfMemory,
                    JsonParseError.BufferUnderrun => return JsonParseError.BufferUnderrun,
                    JsonParseError.SyntaxError => return JsonParseError.SyntaxError,
                    JsonParseError.UnexpectedToken => return JsonParseError.UnexpectedToken,
                    else => {
                        return JsonParseError.UnexpectedToken;
                    },
                }
            };
            errdefer doc.deinit(allocator);

            try batch_list.append(doc);
        }

        const token_array_end = try source.next();
        if (token_array_end != .array_end) return JsonParseError.UnexpectedToken;

        return try batch_list.toOwnedSlice();
    }

    pub fn dupe(self: *const CursorInfo, allocator: Allocator) !*CursorInfo {
        var cursor_info_clone = try allocator.create(CursorInfo);
        errdefer allocator.destroy(cursor_info_clone);

        cursor_info_clone.id = self.id;
        cursor_info_clone.ns = try allocator.dupe(u8, self.ns);

        if (self.firstBatch) |first_batch| {
            cursor_info_clone.firstBatch = try allocator.alloc(*BsonDocument, first_batch.len);
            errdefer allocator.free(cursor_info_clone.firstBatch.?);

            for (first_batch, 0..) |doc, i| {
                cursor_info_clone.firstBatch.?[i] = try doc.dupe(allocator);
            }
        } else {
            cursor_info_clone.firstBatch = null;
        }

        if (self.nextBatch) |next_batch| {
            cursor_info_clone.nextBatch = try allocator.alloc(*BsonDocument, next_batch.len);
            errdefer allocator.free(cursor_info_clone.nextBatch.?);

            for (next_batch, 0..) |doc, i| {
                cursor_info_clone.nextBatch.?[i] = try doc.dupe(allocator);
            }
        } else {
            cursor_info_clone.nextBatch = null;
        }

        return cursor_info_clone;
    }
};
