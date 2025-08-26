const std = @import("std");
const bson = @import("bson");

const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const BsonDocument = bson.BsonDocument;
const Collation = @import("./Collation.zig").Collation;
const Collection = @import("../Collection.zig").Collection;
const ErrorResponse = @import("./ErrorResponse.zig").ErrorResponse;
const Hint = @import("../protocol/hint.zig").Hint;
const Comment = @import("../protocol/comment.zig").Comment;
const ResponseWithWriteErrors = @import("./WriteError.zig").ResponseWithWriteErrors;
const WriteResponseUnion = @import("../ResponseUnion.zig").WriteResponseUnion;
const RunCommandOptions = @import("./RunCommandOptions.zig").RunCommandOptions;

pub const JsonParseError = error{UnexpectedToken} || std.json.Scanner.NextError;

/// @see https://www.mongodb.com/docs/manual/reference/command/update/
pub const UpdateCommand = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    /// The name of the target collection.
    update: []const u8,

    updates: []const UpdateStatement,

    @"$db": []const u8,

    arrayFilters: ?[]bson.BsonDocument = null,

    collation: ?Collation = null,

    hint: ?Hint = null,

    comment: ?Comment = null,

    sort: ?bson.BsonDocument = null,
    upsert: ?bool = null,

    writeConcern: ?bson.BsonDocument = null,

    maxTimeMS: ?i64 = null,

    ordered: ?bool = null,

    // Must be value of ServerApiVersion.value()
    apiVersion: ?[]const u8 = null,
    apiStrict: ?bool = null,
    apiDeprecationErrors: ?bool = null,

    // RunCommandOptions
    readPreference: ?[]const u8 = null,
    timeoutMS: ?i64 = null,
    lsid: ?*BsonDocument = null,

    pub fn deinit(self: *const UpdateCommand, allocator: Allocator) void {
        for (self.updates) |update| update.deinit(allocator);
        allocator.free(self.updates);
    }

    pub fn makeUpdateOne(
        allocator: std.mem.Allocator,
        collection_name: []const u8,
        db_name: []const u8,
        filter: anytype,
        update: anytype,
        options: UpdateOneOptions,
    ) !UpdateCommand {
        const filter_parsed = try BsonDocument.fromObject(allocator, @TypeOf(filter), filter);

        const update_parsed = try BsonDocument.fromObject(allocator, @TypeOf(update), update);

        const update_statement = UpdateStatement{
            .q = filter_parsed,
            .u = update_parsed,
            .multi = false, // updateOne only updates one document
            .upsert = options.upsert,
        };
        const update_statements = try allocator.alloc(UpdateStatement, 1);
        update_statements[0] = update_statement;

        var command: UpdateCommand = .{
            .update = collection_name,
            .@"$db" = db_name,
            .updates = update_statements,
        };
        options.addToCommand(&command);

        return command;
    }

    pub fn makeUpdateMany(
        allocator: std.mem.Allocator,
        collection_name: []const u8,
        db_name: []const u8,
        filter: anytype,
        update: anytype,
        options: UpdateManyOptions,
    ) !UpdateCommand {
        const filter_parsed = try BsonDocument.fromObject(allocator, @TypeOf(filter), filter);

        const update_parsed = try BsonDocument.fromObject(allocator, @TypeOf(update), update);

        const update_statement = UpdateStatement{
            .q = filter_parsed,
            .u = update_parsed,
            .multi = true,
            .upsert = options.upsert,
        };

        const update_statements = try allocator.alloc(UpdateStatement, 1);
        update_statements[0] = update_statement;

        var command: UpdateCommand = .{
            .update = collection_name,
            .@"$db" = db_name,
            .updates = update_statements,
        };

        options.addToCommand(&command);

        return command;
    }
};

pub const UpdateStatement = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    /// The query that matches documents to update.
    q: *BsonDocument,

    /// The update document or pipeline that specifies the modifications to apply.
    u: *BsonDocument, // TODO: add support for pipelines

    /// since mongodb v5.0
    // @see https://www.mongodb.com/docs/v7.0/reference/command/update/#std-label-update-command-c
    c: ?bson.BsonDocument = null,
    multi: bool,

    upsert: ?bool = null,

    arrayFilters: ?[]bson.BsonDocument = null,

    collation: ?Collation = null,

    hint: ?Hint = null,

    pub fn deinit(self: *const UpdateStatement, allocator: Allocator) void {
        self.q.deinit(allocator);
        self.u.deinit(allocator);
    }
};

pub const UpdateStatementOptions = struct {
    upsert: ?bool = null,
    multi: bool,
    array_filters: ?[]bson.BsonDocument = null,
    collation: ?Collation = null,

    pub fn addToCommand(self: *const UpdateStatementOptions, statement: *UpdateStatement) void {
        statement.upsert = self.upsert;
        statement.multi = self.multi;
        statement.arrayFilters = self.array_filters;
        statement.collation = self.collation;
    }
};

pub const UpdateCommandResponse = struct {
    // acknowledged: ?bool = null,

    n: i64,

    nModified: i64,

    nUpserted: ?i64 = null,

    upserted: ?bson.BsonDocument = null,

    pub fn deinit(self: *const UpdateCommandResponse, allocator: Allocator) void {
        if (self.upserted) |upserted| {
            upserted.deinit(allocator);
        }
        allocator.destroy(self);
    }

    pub fn jsonParse(allocator: Allocator, source: *std.json.Scanner, options: std.json.ParseOptions) JsonParseError!UpdateCommandResponse {
        _ = allocator;
        _ = options;
        if (try source.next() != .object_begin) return error.UnexpectedToken;

        var ok: ?f64 = null;
        var n: ?i64 = null;
        var nModified: ?i64 = null;
        var nUpserted: ?i64 = null;
        blk_tkn: switch (try source.next()) {
            .string => |key| {
                if (ok == null and std.mem.eql(u8, key, "ok")) {
                    const ok_value = try source.next();
                    ok = std.fmt.parseFloat(f64, ok_value.number) catch return error.UnexpectedToken;
                    continue :blk_tkn try source.next();
                }
                if (key[0] == 'n') {
                    if (n == null and key.len == 1) {
                        const n_value = try source.next();
                        n = std.fmt.parseInt(i64, n_value.number, 10) catch return error.UnexpectedToken;
                        continue :blk_tkn try source.next();
                    }
                    if (nModified == null and std.mem.eql(u8, key, "nModified")) {
                        const nModified_value = try source.next();
                        nModified = std.fmt.parseInt(i64, nModified_value.number, 10) catch return error.UnexpectedToken;
                        continue :blk_tkn try source.next();
                    }
                    if (nUpserted == null and std.mem.eql(u8, key, "nUpserted")) {
                        const nUpserted_value = try source.next();
                        nUpserted = std.fmt.parseInt(i64, nUpserted_value.number, 10) catch return error.UnexpectedToken;
                        continue :blk_tkn try source.next();
                    }
                }
            },
            .object_end => break :blk_tkn,
            else => return error.UnexpectedToken,
        }

        if (ok == null or n == null or nModified == null) return error.UnexpectedToken;

        return .{
            .n = n.?,
            .nModified = nModified.?,
            .nUpserted = nUpserted,
            .upserted = null,
        };
    }

    pub fn dupe(self: *const UpdateCommandResponse, allocator: Allocator) !*UpdateCommandResponse {
        const clone = try allocator.create(UpdateCommandResponse);
        errdefer clone.deinit(allocator);

        // clone.acknowledged = self.acknowledged;
        clone.n = self.n;
        clone.nModified = self.nModified;
        clone.nUpserted = self.nUpserted;
        clone.upserted = self.upserted;

        return clone;
    }

    pub fn parseBson(allocator: Allocator, document: *const BsonDocument) !*UpdateCommandResponse {
        return try document.toObject(allocator, UpdateCommandResponse, .{ .ignore_unknown_fields = true });
    }
};

pub const UpdateOneOptions = struct {
    run_command_options: ?RunCommandOptions = null,

    arrayFilters: ?[]bson.BsonDocument = null,

    collation: ?Collation = null,

    hint: ?Hint = null,

    sort: ?bson.BsonDocument = null,

    upsert: ?bool = null,

    pub fn addToCommand(self: *const UpdateOneOptions, command: *UpdateCommand) void {
        command.collation = self.collation;
        command.hint = self.hint;
        command.sort = self.sort;
        command.arrayFilters = self.arrayFilters;
    }
};

pub const UpdateManyOptions = struct {
    run_command_options: ?RunCommandOptions = null,

    arrayFilters: ?[]bson.BsonDocument = null,

    collation: ?Collation = null,

    hint: ?Hint = null,

    // /// @since MongoDB 8.2
    // raw_data: ?bool = null,
    upsert: ?bool = null,

    pub fn addToCommand(self: *const UpdateManyOptions, command: *UpdateCommand) void {
        command.collation = self.collation;
        command.hint = self.hint;
        command.arrayFilters = self.arrayFilters;
    }
};

pub const UpdateCommandChainable = struct {
    collection: *const Collection,
    builder: UpdateStatementBuilder,
    err: ?anyerror = null,

    pub fn init(collection: *const Collection) UpdateCommandChainable {
        return .{
            .collection = collection,
            .builder = UpdateStatementBuilder.init(collection.allocator),
        };
    }

    pub fn deinit(self: *UpdateCommandChainable) void {
        self.builder.deinit();
    }

    pub fn add(self: *UpdateCommandChainable, filter: anytype, update: anytype, options: UpdateStatementOptions) *UpdateCommandChainable {
        if (self.err != null) {
            return self;
        }
        self.builder.add(filter, update, options) catch |err| {
            self.err = err;
            return self;
        };
        return self;
    }

    pub fn exec(
        self: *UpdateCommandChainable,
        options: UpdateManyOptions,
    ) !WriteResponseUnion(UpdateCommandResponse, ErrorResponse, ResponseWithWriteErrors) {
        if (self.err) |err| {
            return err;
        }

        const update_statements = try self.builder.toOwnedSlice();
        defer self.builder.deinit();

        var command: UpdateCommand = .{
            .update = self.collection.collection_name,
            .updates = update_statements,
            .@"$db" = self.collection.database.db_name,
        };

        options.addToCommand(&command);

        return try self.collection.database.runWriteCommand(&command, options.run_command_options orelse RunCommandOptions{}, UpdateCommandResponse, ResponseWithWriteErrors);
    }
};

pub const UpdateStatementBuilder = struct {
    arena: std.heap.ArenaAllocator,
    update_statements: ArrayList(UpdateStatement),

    pub fn init(allocator: std.mem.Allocator) UpdateStatementBuilder {
        return .{
            .arena = std.heap.ArenaAllocator.init(allocator),
            .update_statements = .empty,
        };
    }

    pub fn deinit(self: *UpdateStatementBuilder) void {
        self.arena.deinit();
    }

    pub fn toOwnedSlice(self: *UpdateStatementBuilder) Allocator.Error![]UpdateStatement {
        const allocator = self.arena.allocator();

        return try self.update_statements.toOwnedSlice(allocator);
    }

    pub fn add(self: *UpdateStatementBuilder, filter: anytype, update: anytype, options: UpdateStatementOptions) !void {
        const allocator = self.arena.allocator();
        const filter_parsed = try BsonDocument.fromObject(allocator, @TypeOf(filter), filter);

        const update_parsed = try BsonDocument.fromObject(allocator, @TypeOf(update), update);

        const update_statement = UpdateStatement{
            .q = filter_parsed,
            .u = update_parsed, // TODO: add support for pipelines
            .multi = options.multi,
            .collation = options.collation,
            .upsert = options.upsert,
            .arrayFilters = options.array_filters,
        };

        try self.update_statements.append(allocator, update_statement);
    }

    pub fn build(self: *UpdateStatementBuilder) UpdateStatement {
        return self.update_statements;
    }
};
