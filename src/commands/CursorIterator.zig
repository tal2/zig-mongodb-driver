const std = @import("std");
const bson = @import("bson");
const Collection = @import("../Collection.zig").Collection;
const FindCommandResponse = @import("./FindCommand.zig").FindCommandResponse;
const commands = @import("./root.zig");
const CursorInfo = @import("./CursorInfo.zig").CursorInfo;

const Allocator = std.mem.Allocator;
const BsonDocument = bson.BsonDocument;

pub const CursorIterator = struct {
    allocator: std.mem.Allocator,
    collection: *const Collection,
    buffer: std.ArrayList(*BsonDocument),

    cursor_id: i64,
    batch_size: ?i32,
    // limit: ?i64,

    count: usize = 0,

    pub fn init(allocator: std.mem.Allocator, collection: *const Collection, cursor: *CursorInfo, batch_size: ?i32) !CursorIterator {
        const first_batch_size = cursor.firstBatch.?.len;
        var buffer = try std.ArrayList(*BsonDocument).initCapacity(allocator, first_batch_size);
        errdefer buffer.deinit();

        if (cursor.firstBatch) |first_batch| {
            for (first_batch) |doc| {
                defer doc.deinit(allocator);
                const dupe_doc = try doc.dupe(allocator);
                try buffer.append(dupe_doc);
            }
            allocator.free(cursor.firstBatch.?);
            cursor.firstBatch = null;
        }

        return .{
            .allocator = allocator,
            .collection = collection,
            .cursor_id = cursor.id,
            .buffer = buffer,
            .batch_size = batch_size,
            // .limit = options.limit,
        };
    }

    pub fn release(self: *CursorIterator) !void {
        defer self.deinit();

        if (self.cursor_id != 0) {
            const cursors = [_]i64{self.cursor_id};
            _ = try self.collection.killCursors(&cursors);
        }
    }

    pub fn deinit(self: *CursorIterator) void {
        for (self.buffer.items) |item| {
            item.deinit(self.allocator);
        }
        self.buffer.clearAndFree();
        // self.allocator.destroy(self);
    }

    /// caller owns the returned slice
    pub fn next(self: *CursorIterator) !?[]*BsonDocument {
        if (self.cursor_id == 0) {
            return null;
        }
        if (self.buffer.items.len == 0) {
            const command = try commands.makeGetMoreCommand(self.allocator, self.collection.collection_name, self.cursor_id, .{
                .batchSize = self.batch_size,
                // .maxTimeMS = 1000,
                // .comment = "get more",
            }, self.collection.database.db_name, self.collection.database.server_api);
            defer command.deinit(self.allocator);

            const response = try self.collection.database.stream.send(self.allocator, command);
            defer response.deinit(self.allocator);
            const find_command_response = try FindCommandResponse.parseBson(self.allocator, response.section_document.document);
            defer find_command_response.deinit(self.allocator);

            self.cursor_id = find_command_response.cursor.id;

            if (find_command_response.cursor.nextBatch) |next_batch| {
                find_command_response.cursor.nextBatch = null;
                self.count += next_batch.len;
                return next_batch;
            } else {
                return null;
            }
        }
        self.count += self.buffer.items.len;
        return try self.buffer.toOwnedSlice();
    }
};
