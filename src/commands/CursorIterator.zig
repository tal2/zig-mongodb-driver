const std = @import("std");
const bson = @import("bson");
const Collection = @import("../Collection.zig").Collection;
const FindCommandResponse = @import("./FindCommand.zig").FindCommandResponse;
const commands = @import("./root.zig");
const CursorInfo = @import("./CursorInfo.zig").CursorInfo;
const ClientSession = @import("../sessions/ClientSession.zig").ClientSession;

const Allocator = std.mem.Allocator;
const BsonDocument = bson.BsonDocument;

pub const CursorIterator = struct {
    allocator: std.mem.Allocator,
    collection: *const Collection,
    buffer: std.ArrayList(*BsonDocument),
    session: ?*ClientSession,
    cursor_id: i64,
    batch_size: ?i32,
    // limit: ?i64,

    get_more_command: ?*commands.GetMoreCommand = null,

    count: usize = 0,

    pub fn init(allocator: std.mem.Allocator, collection: *const Collection, cursor: *CursorInfo, batch_size: ?i32, session: ?*ClientSession) !CursorIterator {
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
            .session = session,
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
        if (self.session != null) {
            var session = self.session.?;
            self.session = null;
            if (session.mode == .ImplicitCursor) {
                session.deinit();
            }
        }
        if (self.get_more_command != null) {
            const command = self.get_more_command.?;
            self.allocator.destroy(command);
            self.get_more_command = null;
        }
        self.buffer.clearAndFree();
    }

    /// caller owns the returned slice
    pub fn next(self: *CursorIterator) !?[]*BsonDocument {
        if (self.cursor_id == 0) {
            return null;
        }
        if (self.buffer.items.len == 0) {
            if (self.get_more_command == null) {
                self.get_more_command = try self.allocator.create(commands.GetMoreCommand);
                self.get_more_command.?.* = commands.GetMoreCommand.make(self.collection.collection_name, self.collection.database.db_name, self.cursor_id, .{ .batchSize = self.batch_size });
            }

            const result = try self.collection.database.runCommand(self.allocator, self.get_more_command.?, .{ .session = self.session }, commands.FindCommandResponse);
            switch (result) {
                .response => |response| {
                    defer response.deinit(self.allocator);
                    var cursor = response.cursor;

                    if (cursor.id == 0 and self.session != null) { // Release session if cursor is closed before waiting for client app to read all data
                        var session = self.session.?;
                        self.session = null;
                        if (session.mode == .ImplicitCursor) {
                            session.deinit();
                        }
                    }

                    self.cursor_id = cursor.id;
                    self.get_more_command.?.getMore = self.cursor_id;
                    if (cursor.nextBatch) |next_batch| {
                        cursor.nextBatch = null;
                        self.count += next_batch.len;
                        return next_batch;
                    } else {
                        return null;
                    }
                },
                .err => {
                    return error.CursorIteratorError;
                },
            }
        }
        self.count += self.buffer.items.len;
        return try self.buffer.toOwnedSlice();
    }
};
