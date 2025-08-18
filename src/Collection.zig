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
const FindOneOptions = commands.FindOneOptions;
const InsertOneOptions = insert_commands.InsertOneOptions;
const InsertManyOptions = insert_commands.InsertManyOptions;
const DeleteOptions = delete_commands.DeleteOptions;
const ReplaceOptions = replace_commands.ReplaceOptions;
const UpdateOptions = update_commands.UpdateOptions;
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

    pub fn insertMany(self: *const Collection, docs: anytype, options: InsertManyOptions) !union(enum) {
        response: *const InsertCommandResponse,
        write_errors: *ResponseWithWriteErrors,
        err: *ErrorResponse,
    } {
        comptime {
            const documents_type_info = @typeInfo(@TypeOf(docs));
            if (documents_type_info != .array and (documents_type_info != .pointer or @typeInfo(documents_type_info.pointer.child) != .array)) {
                @compileError("insertMany docs param must be an array or a pointer to an array");
            }
        }

        const command_insert = try insert_commands.makeInsertMany(self.allocator, self.collection_name, docs, options, null, self.database.db_name, self.server_api);
        defer command_insert.deinit(self.allocator);

        const result = try self.runWriteCommandOpcode(command_insert, InsertCommandResponse, ResponseWithWriteErrors);
        return switch (result) {
            .response => |response| .{ .response = response },
            .write_errors => |write_errors| .{ .write_errors = write_errors },
            .err => |err| .{ .err = err },
        };
    }

    pub fn insertOne(self: *const Collection, doc: anytype, options: InsertOneOptions) !union(enum) {
        response: *const InsertCommandResponse,
        write_errors: *ResponseWithWriteErrors,
        err: *ErrorResponse,
    } {
        comptime {
            const documents_type_info = @typeInfo(@TypeOf(doc));
            if (documents_type_info == .array or (documents_type_info == .pointer and @typeInfo(documents_type_info.pointer.child) == .array)) {
                @compileError("insertOne does not support arrays");
            }
        }
        const command_insert = try insert_commands.makeInsertOne(self.allocator, self.collection_name, doc, options, null, self.database.db_name, self.server_api);
        defer command_insert.deinit(self.allocator);

        const result = try self.runWriteCommandOpcode(command_insert, InsertCommandResponse, ResponseWithWriteErrors);
        return switch (result) {
            .response => |response| .{ .response = response },
            .write_errors => |write_errors| .{ .write_errors = write_errors },
            .err => |err| .{ .err = err },
        };
    }

    pub fn find(self: *const Collection, filter: anytype, limit: LimitNumbered, options: FindOptions) !union(enum) {
        cursor: CursorIterator,
        err: *ErrorResponse,
    } {
        const command = try commands.makeFindCommand(self.allocator, self.collection_name, // collection name
            filter, limit, options,
            // server version unknown
            null, self.database.db_name, self.server_api);
        defer command.deinit(self.allocator);

        const result = try self.runCommandOpcode(command, FindCommandResponse);
        return switch (result) {
            .response => |response| {
                defer response.deinit(self.allocator);
                return .{ .cursor = try CursorIterator.init(self.allocator, self, response.cursor, options) };
            },
            .err => |err| .{ .err = err },
        };
    }

    pub fn findOne(self: *const Collection, filter: anytype, options: FindOneOptions) !union(enum) {
        document: *BsonDocument,
        err: *ErrorResponse,
        null,
    } {
        const command = try commands.makeFindOneCommand(self.allocator, self.collection_name, filter, options,
            // server version unknown
            null, self.database.db_name, self.server_api);
        defer command.deinit(self.allocator);

        const result = try self.runCommandOpcode(command, FindCommandResponse);
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

    pub fn delete(self: *const Collection, limit: Limit, filter: anytype, options: DeleteOptions) !union(enum) {
        response: *DeleteCommandResponse,
        write_errors: *ResponseWithWriteErrors,
        err: *ErrorResponse,
    } {
        const command_delete = try delete_commands.makeDeleteCommand(self.allocator, self.collection_name, filter, limit, options, null, self.database.db_name, self.server_api);
        defer command_delete.deinit(self.allocator);

        const result = try self.runWriteCommandOpcode(command_delete, DeleteCommandResponse, ResponseWithWriteErrors);
        return switch (result) {
            .response => |response| .{ .response = response },
            .write_errors => |write_errors| .{ .write_errors = write_errors },
            .err => |err| .{ .err = err },
        };
    }

    pub fn replaceOne(self: *const Collection, filter: anytype, replacement: anytype, options: ReplaceOptions) !union(enum) {
        response: *ReplaceCommandResponse,
        write_errors: *ResponseWithWriteErrors,
        err: *ErrorResponse,
    } {
        const command_replace = try commands.makeReplaceCommand(self.allocator, self.collection_name, filter, replacement, options, null, self.database.db_name, self.server_api);
        defer command_replace.deinit(self.allocator);

        const result = try self.runWriteCommandOpcode(command_replace, ReplaceCommandResponse, ResponseWithWriteErrors);
        return switch (result) {
            .response => |response| .{ .response = response },
            .write_errors => |write_errors| .{ .write_errors = write_errors },
            .err => |err| .{ .err = err },
        };
    }

    pub fn updateOne(self: *const Collection, filter: anytype, update: anytype, options: UpdateOptions) !union(enum) {
        response: *UpdateCommandResponse,
        write_errors: *ResponseWithWriteErrors,
        err: *ErrorResponse,
    } {
        const command_update = try commands.makeUpdateOneCommand(self.allocator, self.collection_name, filter, update, options, null, self.database.db_name, self.server_api);

        defer command_update.deinit(self.allocator);

        const result = try self.runWriteCommandOpcode(command_update, UpdateCommandResponse, ResponseWithWriteErrors);
        return switch (result) {
            .response => |response| .{ .response = response },
            .write_errors => |write_errors| .{ .write_errors = write_errors },
            .err => |err| .{ .err = err },
        };
    }

    pub fn updateMany(self: *const Collection, filter: anytype, update: anytype, options: UpdateOptions) !union(enum) {
        response: *UpdateCommandResponse,
        write_errors: *ResponseWithWriteErrors,
        err: *ErrorResponse,
    } {
        const command_update = try commands.makeUpdateManyCommand(self.allocator, self.collection_name, filter, update, options, null, self.database.db_name, self.server_api);
        defer command_update.deinit(self.allocator);

        const result = try self.runWriteCommandOpcode(command_update, UpdateCommandResponse, ResponseWithWriteErrors);
        return switch (result) {
            .response => |response| .{ .response = response },
            .write_errors => |write_errors| .{ .write_errors = write_errors },
            .err => |err| .{ .err = err },
        };
    }

    pub fn updateChain(self: *const Collection) UpdateCommandChainable {
        return UpdateCommandChainable.init(self);
    }


    pub fn aggregate(self: *const Collection, pipeline: anytype, options: FindOptions, cursor_options: CursorOptions) !union(enum) {
        cursor: CursorIterator,
        err: *ErrorResponse,
    } {
        const command = try commands.makeAggregateCommand(self.allocator, self.collection_name, pipeline, options, cursor_options, null, self.database.db_name, self.server_api);
        defer command.deinit(self.allocator);




        return switch (try self.runCommandOpcode(command, FindCommandResponse)) {
            .response => |response| {
                defer response.deinit(self.allocator);
                return .{ .cursor = try CursorIterator.init(self.allocator, self, response.cursor, options) };
            },
            .err => |err| .{ .err = err },
        };
    }

    pub fn countDocuments(self: *const Collection, filter: anytype, options: FindOptions) !i64 {
        var arena = ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        var pb = commands.PipelineBuilder.init(arena_allocator);
        const pipeline = try pb.match(filter).group(.{ ._id = 1, .n = .{ .@"$sum" = 1 } }).build();

        const command = try commands.makeAggregateCommand(arena_allocator, self.collection_name, pipeline, options, .{}, null, self.database.db_name, self.server_api);
        const response = try self.database.stream.send(arena_allocator, command);
        const aggregate_command_response = try FindCommandResponse.parseBson(arena_allocator, response.section_document.document);

        const SumResponse = struct {
            _id: ?i64,
            n: i64,
        };

        const sum_response = try aggregate_command_response.firstAs(SumResponse, arena_allocator) orelse return 0;
        return sum_response.n;
    }

    pub fn estimatedDocumentCount(self: *const Collection, options: commands.estimated_document_count_commands.EstimatedDocumentCountOptions) !i64 {
        const command = try commands.makeEstimatedDocumentCount(self.allocator, self.collection_name, options, null, self.database.db_name, self.server_api);
        defer command.deinit(self.allocator);
        const response = try self.database.stream.send(self.allocator, command);
        defer response.deinit(self.allocator);

        const count_response = try commands.CountCommandResponse.parseBson(self.allocator, response.section_document.document);
        defer self.allocator.destroy(count_response);
        return count_response.n;
    }

    pub fn killCursors(self: *const Collection, cursor_ids: []const i64) !*commands.KillCursorsCommandResponse {
        const kill_cursor_command = try commands.makeKillCursorsCommand(self.allocator, self.collection_name, cursor_ids, null, self.database.db_name, self.server_api);
        defer kill_cursor_command.deinit(self.allocator);

        const response = try self.database.stream.send(self.allocator, kill_cursor_command);
        defer response.deinit(self.allocator);

        const kill_cursor_response = try commands.KillCursorsCommandResponse.parseBson(self.allocator, response.section_document.document);
        errdefer kill_cursor_response.deinit(self.allocator);

        return kill_cursor_response;
    }


    pub fn runWriteCommandOpcode(self: *const Collection, command_op_msg: *const opcode.OpMsg, comptime ResponseType: type, comptime ResponseErrorType: type) !union(enum) {
        response: *ResponseType,
        write_errors: *ResponseErrorType,
        err: *ErrorResponse,
    } {
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

    pub fn runCommandOpcode(self: *const Collection, command_op_msg: *opcode.OpMsg, comptime ResponseType: type) !union(enum) {
        response: *ResponseType,
        err: *ErrorResponse,
    } {
        const response = try self.database.stream.send(self.allocator, command_op_msg);
        defer response.deinit(self.allocator);

        if (try ErrorResponse.isError(self.allocator, response.section_document.document)) {
            const error_response = try ErrorResponse.parseBson(self.allocator, response.section_document.document);
            errdefer error_response.deinit(self.allocator);
            return .{ .err = error_response };
        }

        return .{ .response = try ResponseType.parseBson(self.allocator, response.section_document.document) };
    }

    pub fn runCommand(self: *const Collection, command: anytype, options: anytype, comptime ResponseType: type) !union(enum) {
        response: *ResponseType,
        err: *ErrorResponse,
    } {
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
