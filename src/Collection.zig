const std = @import("std");
const bson = @import("bson");
const Database = @import("Database.zig").Database;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const command_options = @import("commands/RunCommandOptions.zig");
const connection_stream = @import("connection/ConnectionStream.zig");
const opcode = @import("protocol/opcode.zig");
const BsonDocument = bson.BsonDocument;
const RunCommandOptions = command_options.RunCommandOptions;
const ConnectionStream = connection_stream.ConnectionStream;
const Address = @import("server-discovery-and-monitoring/Address.zig").Address;
const ServerApi = @import("server-discovery-and-monitoring/server-info.zig").ServerApi;
const utils = @import("utils.zig");

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
const AggregateCommandResponse = commands.AggregateCommandResponse;
const ErrorResponse = commands.ErrorResponse;
const WriteError = commands.WriteError;
const ResponseWithWriteErrors = commands.ResponseWithWriteErrors;
const BulkWriteOps = @import("commands/bulk-operations.zig").BulkWriteOps;
const BulkWriteResponse = @import("commands/bulk-operations.zig").BulkWriteResponse;
const BulkWriteOpsChainable = @import("commands/bulk-operations.zig").BulkWriteOpsChainable;
const WriteResponseUnion = @import("ResponseUnion.zig").WriteResponseUnion;
const ResponseUnion = @import("ResponseUnion.zig").ResponseUnion;

pub const Collection = struct {
    database: *Database,
    allocator: std.mem.Allocator,
    collection_name: []const u8,
    server_api: ServerApi,

    pub fn init(database: *Database, name: []const u8, server_api: ServerApi) Collection {
        return .{
            .database = database,
            .allocator = database.allocator,
            .collection_name = name,
            .server_api = server_api,
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

        return try self.runWriteCommand(&command_insert, options, InsertCommandResponse, ResponseWithWriteErrors);
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

        return try self.runWriteCommand(&command_insert, options, InsertCommandResponse, ResponseWithWriteErrors);
    }

    pub fn find(self: *const Collection, filter: anytype, limit: LimitNumbered, options: FindOptions) !union(enum) {
        cursor: CursorIterator,
        err: *ErrorResponse,
    } {
        var command = try commands.FindCommand.make(self.allocator, self.collection_name, self.database.db_name, filter, limit, options);
        defer command.deinit(self.allocator);

        const result = try self.runCommand(&command, options, FindCommandResponse);
        return switch (result) {
            .response => |response| {
                defer response.deinit(self.allocator);
                return .{ .cursor = try CursorIterator.init(self.allocator, self, response.cursor, options.batchSize) };
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

        const result = try self.runCommand(&command, options, FindCommandResponse);
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
        var command_delete = try delete_commands.DeleteCommand.makeDeleteOne(self.allocator, self.database.db_name, self.collection_name, filter, options);
        defer command_delete.deinit(self.allocator);

        return try self.runWriteCommand(&command_delete, options, DeleteCommandResponse, ResponseWithWriteErrors);
    }

    pub fn deleteMany(self: *const Collection, filter: anytype, options: DeleteOptions) !WriteResponseUnion(DeleteCommandResponse, ErrorResponse, ResponseWithWriteErrors) {
        var command_delete = try delete_commands.DeleteCommand.makeDeleteMany(self.allocator, self.database.db_name, self.collection_name, filter, options);
        defer command_delete.deinit(self.allocator);

        return try self.runWriteCommand(&command_delete, options, DeleteCommandResponse, ResponseWithWriteErrors);
    }

    pub fn replaceOne(self: *const Collection, filter: anytype, replacement: anytype, options: ReplaceOptions) !WriteResponseUnion(ReplaceCommandResponse, ErrorResponse, ResponseWithWriteErrors) {
        var command_replace = try replace_commands.ReplaceCommand.makeReplaceOne(self.allocator, self.database.db_name, self.collection_name, filter, replacement, options);
        defer command_replace.deinit(self.allocator);

        return try self.runWriteCommand(&command_replace, options, ReplaceCommandResponse, ResponseWithWriteErrors);
    }

    pub fn updateOne(self: *const Collection, filter: anytype, update: anytype, options: update_commands.UpdateOneOptions) !WriteResponseUnion(UpdateCommandResponse, ErrorResponse, ResponseWithWriteErrors) {
        var command_update = try UpdateCommand.makeUpdateOne(self.allocator, self.collection_name, self.database.db_name, filter, update, options);
        defer command_update.deinit(self.allocator);

        return try self.runWriteCommand(&command_update, options, UpdateCommandResponse, ResponseWithWriteErrors);
    }

    pub fn updateMany(self: *const Collection, filter: anytype, update: anytype, options: update_commands.UpdateManyOptions) !WriteResponseUnion(UpdateCommandResponse, ErrorResponse, ResponseWithWriteErrors) {
        var command_update = try UpdateCommand.makeUpdateMany(self.allocator, self.collection_name, self.database.db_name, filter, update, options);
        defer command_update.deinit(self.allocator);

        return try self.runWriteCommand(&command_update, options, UpdateCommandResponse, ResponseWithWriteErrors);
    }

    pub fn updateChain(self: *const Collection) UpdateCommandChainable {
        return UpdateCommandChainable.init(self);
    }

    pub fn aggregate(self: *const Collection, pipeline: anytype, options: AggregateOptions, cursor_options: CursorOptions) !union(enum) {
        cursor: CursorIterator,
        err: *ErrorResponse,
    } {
        var arena = ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        var command = try commands.AggregateCommand.make(arena_allocator, self.collection_name, self.database.db_name, pipeline, options, cursor_options);




        return switch (try self.runCommand(&command, options, FindCommandResponse)) {
            .response => |response| {
                defer response.deinit(self.allocator);
                return .{ .cursor = try CursorIterator.init(self.allocator, self, response.cursor, options.batchSize) };
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

        const command = try commands.AggregateCommand.make(arena_allocator, self.collection_name, self.database.db_name, pipeline, options, .{});

        const command_serialized = try BsonDocument.fromObject(arena_allocator, @TypeOf(command), command);
        errdefer command_serialized.deinit(arena_allocator);

        const command_op_msg = try opcode.OpMsg.init(arena_allocator, command_serialized, 1, 0, .{});
        defer command_op_msg.deinit(arena_allocator);

        const response = try self.database.stream.send(arena_allocator, command_op_msg);
        const aggregate_command_response = try FindCommandResponse.parseBson(arena_allocator, response.section_document.document);

        const SumResponse = struct {
            _id: ?i64,
            n: i64,
        };

        const sum_response = try aggregate_command_response.firstAs(SumResponse, arena_allocator) orelse return 0;
        return sum_response.n;
    }

    pub fn estimatedDocumentCount(self: *const Collection, options: commands.EstimatedDocumentCountOptions) !union(enum) {
        n: i64,
        err: *ErrorResponse,
    } {
        var command = try commands.CountCommand.makeEstimateCount(self.collection_name, self.database.db_name, options);
        defer command.deinit(self.allocator);

        const result = try self.runCommand(&command, options, commands.CountCommandResponse);
        return switch (result) {
            .response => |response| .{ .n = response.n },
            .err => |err| .{ .err = err },
        };

    }


    pub fn killCursors(self: *const Collection, cursor_ids: []const i64) !ResponseUnion(commands.KillCursorsCommandResponse, ErrorResponse) {
        var kill_cursor_command = commands.KillCursorsCommand.make(self.collection_name, self.database.db_name, cursor_ids);

        return try self.runCommand(&kill_cursor_command, null, commands.KillCursorsCommandResponse);
    }



    }

    pub fn runWriteCommand(self: *const Collection, command: anytype, options: anytype, comptime ResponseType: type, comptime ResponseErrorType: type) !WriteResponseUnion(ResponseType, ErrorResponse, ResponseErrorType) {
        comptime {
            const command_type_info = @typeInfo(@TypeOf(command));
            if (command_type_info != .pointer) {
                @compileError("runCommand command param must be a pointer to a struct");
            }
            if (!@hasField(@TypeOf(command.*), "$db")) {
                @compileError("runCommand command param must have a @$db field");
            }
            if (!@hasDecl(ResponseType, "parseBson")) {
                @compileError("runCommand command param type must have a parseBson method");
            }
            if (@typeInfo(@TypeOf(options)) != .null and !@hasDecl(@TypeOf(options), "addToCommand")) {
                @compileError("runCommand options param must have an addToCommand method");
            }
        }

        command.*.@"$db" = self.database.db_name;
        if (comptime @typeInfo(@TypeOf(options)) != .null) {
            options.addToCommand(command);
        }

        self.server_api.addToCommand(command);

        const command_serialized = try BsonDocument.fromObject(self.allocator, @TypeOf(command), command);
        errdefer command_serialized.deinit(self.allocator);

        const command_op_msg = try opcode.OpMsg.init(self.allocator, command_serialized, 1, 0, .{});
        defer command_op_msg.deinit(self.allocator);

        return try self.runWriteCommandOpcode(command_op_msg, ResponseType, ResponseErrorType);
    }

    pub fn runWriteCommandOpcode(self: *const Collection, command_op_msg: *const opcode.OpMsg, comptime ResponseType: type, comptime ResponseErrorType: type) !WriteResponseUnion(ResponseType, ErrorResponse, ResponseErrorType) {
        const response = try self.database.stream.send(self.allocator, command_op_msg);
        defer response.deinit(self.allocator);

        if (try ErrorResponse.isError(self.allocator, response.section_document.document)) {
            const error_response = try ErrorResponse.parseBson(self.allocator, response.section_document.document);
            errdefer error_response.deinit(self.allocator);
            return .{ .err = error_response };
        }

        if (try ResponseErrorType.isError(self.allocator, response.section_document.document)) {
            const response_with_write_errors = try ResponseErrorType.parseBson(self.allocator, response.section_document.document);
            errdefer response_with_write_errors.deinit(self.allocator);
            return .{ .write_errors = response_with_write_errors };
        }

        return .{ .response = try ResponseType.parseBson(self.allocator, response.section_document.document) };
    }

    pub fn runCommand(self: *const Collection, command: anytype, options: anytype, comptime ResponseType: type) !ResponseUnion(ResponseType, ErrorResponse) {
        comptime {
            const command_type_info = @typeInfo(@TypeOf(command));
            if (command_type_info != .pointer) {
                @compileError("runCommand command param must be a pointer to a struct");
            }
            if (!@hasField(@TypeOf(command.*), "$db")) {
                @compileError("runCommand command param must have a @$db field");
            }
            if (!@hasDecl(ResponseType, "parseBson")) {
                @compileError("runCommand command param type must have a parseBson method");
            }
            if (@TypeOf(options) != @TypeOf(null) and !@hasDecl(@TypeOf(options), "addToCommand")) {
                @compileError("runCommand options param must have an addToCommand method");
            }
        }

        if (comptime @TypeOf(options) != @TypeOf(null)) {
            options.addToCommand(command);
        }

        self.server_api.addToCommand(command);

        var command_serialized = try BsonDocument.fromObject(self.allocator, @TypeOf(command), command);
        errdefer command_serialized.deinit(self.allocator);


        const command_op_msg = try opcode.OpMsg.init(self.allocator, command_serialized, 1, 0, .{});
        defer command_op_msg.deinit(self.allocator);

        const response = try self.database.stream.send(self.allocator, command_op_msg);
        defer response.deinit(self.allocator);

        if (try ErrorResponse.isError(self.allocator, response.section_document.document)) {
            const error_response = try response.section_document.document.toObject(self.allocator, ErrorResponse, .{ .ignore_unknown_fields = true });
            errdefer error_response.deinit(self.allocator);
            return .{ .err = error_response };
        }

        const response_parsed = try response.section_document.document.toObject(self.allocator, ResponseType, .{ .ignore_unknown_fields = true });
        errdefer response_parsed.deinit(self.allocator);
        return .{ .response = response_parsed };
    }
};
