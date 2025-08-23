const std = @import("std");
const bson = @import("bson");
const Database = @import("Database.zig").Database;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const command_options = @import("commands/RunCommandOptions.zig");
const BsonDocument = bson.BsonDocument;
const RunCommandOptions = command_options.RunCommandOptions;

const commands = @import("commands/root.zig");
const insert_commands = @import("commands/InsertCommand.zig");
const delete_commands = @import("commands/DeleteCommand.zig");
const replace_commands = @import("commands/ReplaceCommand.zig");
const update_commands = @import("commands/UpdateCommand.zig");
const UpdateCommand = update_commands.UpdateCommand;
const UpdateCommandChainable = commands.UpdateCommandChainable;
const FindCommandResponse = commands.FindCommandResponse;
const InsertCommandResponse = insert_commands.InsertCommandResponse;
const DeleteCommandResponse = delete_commands.DeleteCommandResponse;
const ReplaceCommandResponse = replace_commands.ReplaceCommandResponse;
const UpdateCommandResponse = update_commands.UpdateCommandResponse;
const Limit = commands.command_types.Limit;
const LimitNumbered = commands.command_types.LimitNumbered;
const FindOptions = commands.FindOptions;
const InsertOneOptions = insert_commands.InsertOneOptions;
const InsertManyOptions = insert_commands.InsertManyOptions;
const DeleteOptions = delete_commands.DeleteOptions;
const ReplaceOptions = replace_commands.ReplaceOptions;
const CursorIterator = commands.CursorIterator;
const AggregateOptions = commands.AggregateOptions;
const CursorOptions = commands.CursorOptions;
const ErrorResponse = commands.ErrorResponse;
const WriteError = commands.WriteError;
const ResponseWithWriteErrors = commands.ResponseWithWriteErrors;
const WriteResponseUnion = @import("ResponseUnion.zig").WriteResponseUnion;
const ResponseUnion = @import("ResponseUnion.zig").ResponseUnion;
const CursorResponseUnion = @import("ResponseUnion.zig").CursorResponseUnion;

pub const Collection = struct {
    database: *Database,
    allocator: Allocator,
    collection_name: []const u8,

    pub fn init(database: *Database, name: []const u8) Collection {
        return .{
            .database = database,
            .allocator = database.allocator,
            .collection_name = name,
        };
    }

    // pub fn deinit(self: *Collection) void {
    //     self.allocator.free(self.collection_name);
    //     self.allocator.destroy(self);
    // }

    pub fn insertMany(self: *const Collection, docs: anytype, options: InsertManyOptions) !WriteResponseUnion(InsertCommandResponse, ErrorResponse, ResponseWithWriteErrors) {
        comptime {
            const documents_type_info = @typeInfo(@TypeOf(docs));
            if (documents_type_info != .array and (documents_type_info != .pointer or @typeInfo(documents_type_info.pointer.child) != .array)) {
                @compileError("insertMany docs param must be an array or a pointer to an array");
            }
        }

        var arena = ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        var command_insert = try insert_commands.InsertCommand.makeInsertMany(arena_allocator, self.collection_name, self.database.db_name, docs, options);

        return try self.database.runWriteCommand(&command_insert, options.run_command_options orelse RunCommandOptions{}, InsertCommandResponse, ResponseWithWriteErrors);
    }

    pub fn insertOne(self: *const Collection, doc: anytype, options: InsertOneOptions) !WriteResponseUnion(InsertCommandResponse, ErrorResponse, ResponseWithWriteErrors) {
        comptime {
            const documents_type_info = @typeInfo(@TypeOf(doc));
            if (documents_type_info == .array or (documents_type_info == .pointer and @typeInfo(documents_type_info.pointer.child) == .array)) {
                @compileError("insertOne does not support arrays");
            }
        }
        var command_insert = try insert_commands.InsertCommand.makeInsertOne(self.allocator, self.collection_name, self.database.db_name, doc, options);
        defer command_insert.deinit(self.allocator);

        return try self.database.runWriteCommand(&command_insert, options.run_command_options orelse RunCommandOptions{}, InsertCommandResponse, ResponseWithWriteErrors);
    }

    pub fn find(self: *const Collection, filter: anytype, limit: LimitNumbered, options: FindOptions) !CursorResponseUnion {
        var run_command_options = options.run_command_options orelse RunCommandOptions{};

        if (run_command_options.session == null) {
            const session = try self.database.tryGetSession(run_command_options);
            if (session != null) {
                run_command_options.session = session;
                session.?.mode = .ImplicitCursor;
            }
        }
        var command = try commands.FindCommand.make(self.allocator, self.collection_name, self.database.db_name, filter, limit, options);
        defer command.deinit(self.allocator);

        const result = try self.database.runCommand(self.allocator, &command, run_command_options, FindCommandResponse);
        return switch (result) {
            .response => |response| {
                defer response.deinit(self.allocator);
                return .{ .cursor = try CursorIterator.init(self.allocator, self, response.cursor, options.batchSize, run_command_options.session) };
            },
            .err => |err| .{ .err = err },
        };
    }

    pub fn findOne(self: *const Collection, filter: anytype, options: FindOptions) !union(enum) {
        document: *BsonDocument,
        err: *ErrorResponse,
        null,
    } {
        var command = try commands.FindCommand.makeFindOne(self.allocator, self.collection_name, self.database.db_name, filter, options);
        defer command.deinit(self.allocator);

        const result = try self.database.runCommand(self.allocator, &command, options.run_command_options orelse RunCommandOptions{}, FindCommandResponse);
        return switch (result) {
            .response => |response| {
                defer response.deinit(self.allocator);

                if (response.first()) |doc| {
                    return .{ .document = try doc.dupe(self.allocator) };
                } else {
                    return .null;
                }
            },
            .err => |err| .{ .err = err },
        };
    }

    pub fn deleteOne(self: *const Collection, filter: anytype, options: DeleteOptions) !WriteResponseUnion(DeleteCommandResponse, ErrorResponse, ResponseWithWriteErrors) {
        var command_delete = try delete_commands.DeleteCommand.makeDeleteOne(self.allocator, self.collection_name, self.database.db_name, filter, options);
        defer command_delete.deinit(self.allocator);

        return try self.database.runWriteCommand(&command_delete, options.run_command_options orelse RunCommandOptions{}, DeleteCommandResponse, ResponseWithWriteErrors);
    }

    pub fn deleteMany(self: *const Collection, filter: anytype, options: DeleteOptions) !WriteResponseUnion(DeleteCommandResponse, ErrorResponse, ResponseWithWriteErrors) {
        var command_delete = try delete_commands.DeleteCommand.makeDeleteMany(self.allocator, self.collection_name, self.database.db_name, filter, options);
        defer command_delete.deinit(self.allocator);

        return try self.database.runWriteCommand(&command_delete, options.run_command_options orelse RunCommandOptions{}, DeleteCommandResponse, ResponseWithWriteErrors);
    }

    pub fn replaceOne(self: *const Collection, filter: anytype, replacement: anytype, options: ReplaceOptions) !WriteResponseUnion(ReplaceCommandResponse, ErrorResponse, ResponseWithWriteErrors) {
        var command_replace = try replace_commands.ReplaceCommand.makeReplaceOne(self.allocator, self.collection_name, self.database.db_name, filter, replacement, options);
        defer command_replace.deinit(self.allocator);

        return try self.database.runWriteCommand(&command_replace, options.run_command_options orelse RunCommandOptions{}, ReplaceCommandResponse, ResponseWithWriteErrors);
    }

    pub fn updateOne(self: *const Collection, filter: anytype, update: anytype, options: update_commands.UpdateOneOptions) !WriteResponseUnion(UpdateCommandResponse, ErrorResponse, ResponseWithWriteErrors) {
        var command_update = try UpdateCommand.makeUpdateOne(self.allocator, self.collection_name, self.database.db_name, filter, update, options);
        defer command_update.deinit(self.allocator);

        return try self.database.runWriteCommand(&command_update, options.run_command_options orelse RunCommandOptions{}, UpdateCommandResponse, ResponseWithWriteErrors);
    }

    pub fn updateMany(self: *const Collection, filter: anytype, update: anytype, options: update_commands.UpdateManyOptions) !WriteResponseUnion(UpdateCommandResponse, ErrorResponse, ResponseWithWriteErrors) {
        var command_update = try UpdateCommand.makeUpdateMany(self.allocator, self.collection_name, self.database.db_name, filter, update, options);
        defer command_update.deinit(self.allocator);

        return try self.database.runWriteCommand(&command_update, options.run_command_options orelse RunCommandOptions{}, UpdateCommandResponse, ResponseWithWriteErrors);
    }

    pub fn updateChain(self: *const Collection) UpdateCommandChainable {
        return UpdateCommandChainable.init(self);
    }

    pub fn aggregate(self: *const Collection, pipeline: anytype, options: AggregateOptions, cursor_options: CursorOptions) !CursorResponseUnion {
        var arena = ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        var command = try commands.AggregateCommand.make(arena_allocator, self.collection_name, self.database.db_name, pipeline, options, cursor_options);

        var run_command_options = options.run_command_options orelse RunCommandOptions{};
        if (run_command_options.session == null) {
            const session = try self.database.tryGetSession(run_command_options);
            if (session != null) {
                run_command_options.session = session;
                session.?.mode = .ImplicitCursor;
            }
        }

        return switch (try self.database.runCommand(self.allocator, &command, run_command_options, FindCommandResponse)) {
            .response => |response| {
                defer response.deinit(self.allocator);
                return .{ .cursor = try CursorIterator.init(self.allocator, self, response.cursor, options.batchSize, run_command_options.session) };
            },
            .err => |err| .{ .err = err },
        };
    }

    pub fn countDocuments(self: *const Collection, filter: anytype, options: AggregateOptions) !i64 {
        var arena = ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        var pb = commands.PipelineBuilder.init(arena_allocator);
        const pipeline = try pb.match(filter).group(.{ ._id = 1, .n = .{ .@"$sum" = 1 } }).build();

        var command = try commands.AggregateCommand.make(arena_allocator, self.collection_name, self.database.db_name, pipeline, options, .{});

        const aggregate_command_response = try self.database.runCommand(arena_allocator, &command, options.run_command_options orelse RunCommandOptions{}, FindCommandResponse);
        return switch (aggregate_command_response) {
            .response => |response| {
                const SumResponse = struct {
                    _id: ?i64,
                    n: i64,
                };

                const sum_response = try response.firstAs(SumResponse, arena_allocator) orelse return 0;
                return sum_response.n;
            },
            .err => |err| {
                defer err.deinit(self.allocator);
                return error.OperationFailed;
            },
        };
    }

    pub fn estimatedDocumentCount(self: *const Collection, options: commands.EstimatedDocumentCountOptions) !union(enum) {
        n: i64,
        err: *ErrorResponse,
    } {
        var command = try commands.CountCommand.makeEstimateCount(self.collection_name, self.database.db_name, options);
        defer command.deinit(self.allocator);

        const result = try self.database.runCommand(self.allocator, &command, options.run_command_options orelse RunCommandOptions{}, commands.CountCommandResponse);
        return switch (result) {
            .response => |response| .{ .n = response.n },
            .err => |err| .{ .err = err },
        };
    }


    pub fn killCursors(self: *const Collection, cursor_ids: []const i64) !ResponseUnion(commands.KillCursorsCommandResponse, ErrorResponse) {
        var kill_cursor_command = commands.KillCursorsCommand.make(self.collection_name, self.database.db_name, cursor_ids);

        const options = RunCommandOptions{};
        return try self.database.runCommand(self.allocator, &kill_cursor_command, options, commands.KillCursorsCommandResponse);
    }
};
