const std = @import("std");
const bson = @import("bson");
const Collection = @import("../Collection.zig").Collection;
const FindCommandResponse = @import("./FindCommand.zig").FindCommandResponse;
const FindOptions = @import("./FindCommand.zig").FindOptions;
const commands = @import("./root.zig");

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

    pub fn init(allocator: std.mem.Allocator, collection: *const Collection, find_command_response: *const FindCommandResponse, options: FindOptions) !CursorIterator {
        const first_batch_size = find_command_response.cursor.firstBatch.?.len;
        var buffer = try std.ArrayList(*BsonDocument).initCapacity(allocator, first_batch_size);
        errdefer buffer.deinit();

        try buffer.appendSlice(find_command_response.cursor.firstBatch.?);
        find_command_response.cursor.firstBatch = null;

        return .{
            .allocator = allocator,
            .collection = collection,
            .cursor_id = find_command_response.cursor.id,
            .buffer = buffer,
            .batch_size = options.batchSize,
            // .limit = options.limit,
        };
    }

    pub fn release(self: *const CursorIterator) !void {
        defer self.deinit();

        if (self.cursor_id != 0) {
            const cursors = [_]i64{self.cursor_id};
            _ = try self.collection.killCursors(&cursors);
        }
    }

    pub fn deinit(self: *const CursorIterator) void {
        self.buffer.deinit();
        // self.allocator.destroy(self);
    }

    /// caller owns the returned slice
    pub fn next(self: *CursorIterator) !?[]*BsonDocument {
        if (self.cursor_id == 0) {
            return null;
        }
        if (self.buffer.items.len == 0) {
            //  error {"ok":0.0,"errmsg":"cannot set maxTimeMS on getMore command for a non-awaitData cursor","code":2,"codeName":"BadValue"}
            //  error {"ok":0.0,"errmsg":"cursor id 4215242176205971555 not found","code":43,"codeName":"CursorNotFound"}
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
                try self.buffer.appendSlice(next_batch);
                find_command_response.cursor.nextBatch = null;
            } else {
                return null;
            }
        }
        self.count += self.buffer.items.len;
        return try self.buffer.toOwnedSlice();
    }
};
