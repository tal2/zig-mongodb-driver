const std = @import("std");
const bson = @import("bson");
const Database = @import("Database.zig").Database;
const Allocator = std.mem.Allocator;

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
const FindCommandResponse = commands.FindCommandResponse;
const InsertCommandResponse = insert_commands.InsertCommandResponse;
const DeleteCommandResponse = delete_commands.DeleteCommandResponse;
const Limit = commands.command_types.Limit;
const LimitNumbered = commands.command_types.LimitNumbered;
const FindOptions = commands.FindOptions;
const FindOneOptions = commands.FindOneOptions;
const InsertOneOptions = insert_commands.InsertOneOptions;
const InsertManyOptions = insert_commands.InsertManyOptions;
const DeleteOptions = delete_commands.DeleteOptions;
const CursorIterator = commands.CursorIterator;
const AggregateOptions = commands.AggregateOptions;
const CursorOptions = commands.CursorOptions;
const AggregateCommandResponse = commands.AggregateCommandResponse;
const parseBsonDocument = utils.parseBsonDocument;

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

    pub fn insertMany(self: *const Collection, docs: anytype, options: InsertManyOptions) !*const InsertCommandResponse {
        comptime {
            const documents_type_info = @typeInfo(@TypeOf(docs));
            if (documents_type_info != .array and (documents_type_info != .pointer or @typeInfo(documents_type_info.pointer.child) != .array)) {
                @compileError("insertMany docs param must be an array or a pointer to an array");
            }
        }
        const command_insert = try insert_commands.makeInsertMany(self.allocator, self.collection_name, docs, options, null, self.database.db_name, self.server_api);

        defer command_insert.deinit(self.allocator);

        const response = try self.database.stream.send(self.allocator, command_insert);
        return try InsertCommandResponse.parseBson(self.allocator, response.section_document.document);
    }

    pub fn insertOne(self: *const Collection, doc: anytype, options: InsertOneOptions) !*const InsertCommandResponse {
        comptime {
            const documents_type_info = @typeInfo(@TypeOf(doc));
            if (documents_type_info == .array or (documents_type_info == .pointer and @typeInfo(documents_type_info.pointer.child) == .array)) {
                @compileError("insertOne does not support arrays");
            }
        }
        const command_insert = try insert_commands.makeInsertOne(self.allocator, self.collection_name, doc, options, null, self.database.db_name, self.server_api);

        defer command_insert.deinit(self.allocator);

        const response = try self.database.stream.send(self.allocator, command_insert);
        return try InsertCommandResponse.parseBson(self.allocator, response.section_document.document);
    }

    pub fn find(self: *const Collection, filter: anytype, limit: LimitNumbered, options: FindOptions) !CursorIterator {
        const command = try commands.makeFindCommand(self.allocator, self.collection_name, // collection name
            filter, limit, options,
            // server version unknown
            null, self.database.db_name, self.server_api);
        defer command.deinit(self.allocator);

        const response = try self.database.stream.send(self.database.allocator, command);
        const find_command_response = try FindCommandResponse.parseBson(self.database.allocator, response.section_document.document);
        defer find_command_response.deinit(self.allocator);

        const cursor = try CursorIterator.init(self.allocator, self, find_command_response, options);
        return cursor;
    }

    pub fn findOne(self: *const Collection, filter: anytype, options: FindOneOptions) !?*BsonDocument {
        const command = try commands.makeFindOneCommand(self.allocator, self.collection_name, filter, options,
            // server version unknown
            null, self.database.db_name, self.server_api);
        defer command.deinit(self.allocator);

        const response = try self.database.stream.send(self.database.allocator, command);
        const result = try FindCommandResponse.parseBson(self.database.allocator, response.section_document.document);

        return result.first();
    }

    pub fn delete(self: *const Collection, limit: Limit, filter: anytype, options: DeleteOptions) !*DeleteCommandResponse {
        const command_delete = try delete_commands.makeDeleteCommand(self.allocator, self.collection_name, filter, limit, options, null, self.database.db_name, self.server_api);

        defer command_delete.deinit(self.allocator);

        const response = try self.database.stream.send(self.allocator, command_delete);
        return try DeleteCommandResponse.parseBson(self.allocator, response.section_document.document);
    }

    pub fn aggregate(self: *const Collection, pipeline: anytype, options: FindOptions, cursor_options: CursorOptions) !CursorIterator {
        const command = try commands.makeAggregateCommand(self.allocator, self.collection_name, pipeline, options, cursor_options, null, self.database.db_name, self.server_api);
        defer command.deinit(self.allocator);

        const response = try self.database.stream.send(self.allocator, command);
        defer response.deinit(self.allocator);

        const aggregate_command_response = try FindCommandResponse.parseBson(self.allocator, response.section_document.document);
        defer aggregate_command_response.deinit(self.allocator);

        const cursor = try CursorIterator.init(self.allocator, self, aggregate_command_response, options);
        return cursor;
    }

    pub fn countDocuments(self: *const Collection, filter: anytype, options: FindOptions) !i64 {
        var pb = commands.PipelineBuilder.init(self.allocator);
        defer pb.deinit();
        const pipeline = try pb.match(filter).group(.{ ._id = 1, .n = .{ .@"$sum" = 1 } }).build();
        defer self.allocator.free(pipeline);

        const command = try commands.makeAggregateCommand(self.allocator, self.collection_name, pipeline, options, .{}, null, self.database.db_name, self.server_api);
        defer command.deinit(self.allocator);

        const response = try self.database.stream.send(self.allocator, command);
        defer response.deinit(self.allocator);

        const aggregate_command_response = try FindCommandResponse.parseBson(self.allocator, response.section_document.document);
        defer aggregate_command_response.deinit(self.allocator);

        const SumResponse = struct {
            _id: ?i64,
            n: i64,
        };

        const sum_response = try aggregate_command_response.firstAs(SumResponse, self.allocator) orelse return 0;
        return sum_response.n;
    }

    pub fn estimatedDocumentCount(self: *const Collection, options: commands.estimated_document_count_commands.EstimatedDocumentCountOptions) !i64 {
        const command = try commands.makeEstimatedDocumentCount(self.allocator, self.collection_name, options, null, self.database.db_name, self.server_api);
        defer command.deinit(self.allocator);
        const response = try self.database.stream.send(self.allocator, command);
        defer response.deinit(self.allocator);

        const count_response = try commands.CountCommandResponse.parseBson(self.allocator, response.section_document.document);
        defer self.allocator.destroy(count_response);
        return count_response.count;
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

    pub fn runCommand(self: *const Collection, command: anytype, options: anytype, ResponseType: type) !*ResponseType {
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
        }

        command.*.@"$db" = self.database.db_name;
        options.addToCommand(command);
        self.server_api.addToCommand(command);

        var command_serialized = try BsonDocument.fromObject(self.allocator, @TypeOf(command), command);
        errdefer command_serialized.deinit(self.allocator);

        const command_op_msg = try opcode.OpMsg.init(self.allocator, command_serialized, 1, 0, .{});
        defer command_op_msg.deinit(self.allocator);

        const response = try self.database.stream.send(self.allocator, command_op_msg);
        defer response.deinit(self.allocator);

        return try ResponseType.parseBson(self.allocator, response.section_document.document);
    }
};
