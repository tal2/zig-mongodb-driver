const std = @import("std");
const bson = @import("bson");
const utils = @import("../utils.zig");
const opcode = @import("../protocol/opcode.zig");

const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const BsonDocument = bson.BsonDocument;
const ServerApi = @import("../server-discovery-and-monitoring/server-info.zig").ServerApi;
const RunCommandOptions = @import("./RunCommandOptions.zig").RunCommandOptions;
const Collation = @import("./Collation.zig").Collation;
const Collection = @import("../Collection.zig").Collection;

pub const JsonParseError = error{UnexpectedToken} || std.json.Scanner.NextError;

pub fn makeUpdateCommand(
    allocator: std.mem.Allocator,
    collection_name: []const u8,
    update_statements: []const UpdateStatement,
    options: UpdateOptions,
    max_wire_version: ?i32, // maxWireVersion from handshake
    db_name: []const u8,
    server_api: ServerApi,
) !*opcode.OpMsg {
    _ = max_wire_version;

    var command_data: UpdateCommand = .{
        .update = collection_name,
        .updates = update_statements,
        .@"$db" = db_name,
        .collation = options.collation,
        .ordered = options.ordered,
        .let = options.let,
    };
    server_api.addToCommand(&command_data);

    var command = try BsonDocument.fromObject(allocator, @TypeOf(command_data), command_data);
    errdefer command.deinit(allocator);

    const result = try opcode.OpMsg.init(allocator, command, 1, 0, .{});
    return result;
}

pub const UpdateStatementBuilder = struct {
    allocator: std.mem.Allocator,
    update_statements: ArrayList(UpdateStatement),

    pub fn init(allocator: std.mem.Allocator) UpdateStatementBuilder {
        return .{
            .allocator = allocator,
            .update_statements = ArrayList(UpdateStatement).init(allocator),
        };
    }

    pub fn deinit(self: *UpdateStatementBuilder) void {
        for (self.update_statements.items) |item| item.deinit(self.allocator);
        self.update_statements.deinit();
    }

    pub fn toOwnedSlice(self: *UpdateStatementBuilder) Allocator.Error![]UpdateStatement {
        return try self.update_statements.toOwnedSlice();
    }
    pub fn add(self: *UpdateStatementBuilder, filter: anytype, update: anytype, options: UpdateStatementOptions) !void {
        var filter_parsed = try BsonDocument.fromObject(self.allocator, @TypeOf(filter), filter);
        errdefer filter_parsed.deinit(self.allocator);

        var update_parsed = try BsonDocument.fromObject(self.allocator, @TypeOf(update), update);
        errdefer update_parsed.deinit(self.allocator);


        const update_statement = UpdateStatement{
            .q = filter_parsed,
            .u = update_parsed, // TODO: add support for pipelines
            .multi = options.multi,
            .collation = options.collation,
            .upsert = options.upsert,
            .arrayFilters = options.array_filters,
        };

        try self.update_statements.append(update_statement);
    }

    pub fn build(self: *UpdateStatementBuilder) UpdateStatement {
        return self.update_statements;
    }
};

pub fn makeUpdateOneCommand(
    allocator: std.mem.Allocator,
    collection_name: []const u8,
    filter: anytype,
    update: anytype,
    options: UpdateOptions,
    max_wire_version: ?i32, // maxWireVersion from handshake
    db_name: []const u8,
    server_api: ServerApi,
) !*opcode.OpMsg {
    var filter_parsed = try BsonDocument.fromObject(allocator, @TypeOf(filter), filter);
    defer filter_parsed.deinit(allocator);

    var update_parsed = try BsonDocument.fromObject(allocator, @TypeOf(update), update);
    defer update_parsed.deinit(allocator);

    const update_statement = UpdateStatement{
        .q = filter_parsed,
        .u = update_parsed,
        .multi = false, // updateOne only updates one document
        .upsert = options.upsert,
        .arrayFilters = options.array_filters,
    };

    const update_statements = [_]UpdateStatement{update_statement};

    return makeUpdateCommand(allocator, collection_name, &update_statements, options, max_wire_version, db_name, server_api);
}

pub fn makeUpdateManyCommand(
    allocator: std.mem.Allocator,
    collection_name: []const u8,
    filter: anytype,
    update: anytype,
    options: UpdateOptions,
    max_wire_version: ?i32, // maxWireVersion from handshake
    db_name: []const u8,
    server_api: ServerApi,
) !*opcode.OpMsg {
    var filter_parsed = try BsonDocument.fromObject(allocator, @TypeOf(filter), filter);
    defer filter_parsed.deinit(allocator);

    var update_parsed = try BsonDocument.fromObject(allocator, @TypeOf(update), update);
    defer update_parsed.deinit(allocator);

    const update_statement = UpdateStatement{
        .q = filter_parsed,
        .u = update_parsed,
        .multi = true,
        .upsert = options.upsert,
        .arrayFilters = options.array_filters,
    };

    const update_statements = [_]UpdateStatement{update_statement};

    return makeUpdateCommand(allocator, collection_name, &update_statements, options, max_wire_version, db_name, server_api);
}

/// @see https://www.mongodb.com/docs/manual/reference/command/update/
pub const UpdateCommand = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    pub fn deinit(self: *const UpdateCommand, allocator: Allocator) void {
        for (self.updates) |update| update.deinit(allocator);
    }

    /// The name of the target collection.
    update: []const u8,

    updates: []const UpdateStatement,

    @"$db": []const u8,

    collation: ?Collation = null,

    // hint: ?union(enum) {
    //     string: []const u8,
    //     document: bson.BsonDocument,
    // } = null,

    let: ?bson.BsonDocument = null,

    // comment: ?union(enum) {
    //     string: []const u8,
    //     document: bson.BsonDocument,
    // } = null,

    writeConcern: ?bson.BsonDocument = null,

    maxTimeMS: ?i64 = null,

    ordered: ?bool = null,

    // Must be value of ServerApiVersion.value()
    apiVersion: ?[]const u8 = null,
    apiStrict: ?bool = null,
    apiDeprecationErrors: ?bool = null,

    // RunCommandOptions
    timeoutMS: ?i64 = null,
    // session: ?ClientSession = null,
};

pub const UpdateStatement = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    pub fn deinit(self: *const UpdateStatement, allocator: Allocator) void {
        self.q.deinit(allocator);
        self.u.deinit(allocator);
    }

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

    // hint: ?union(enum) {
    //     string: []const u8,
    //     document: bson.BsonDocument,
    // } = null,

};

pub const UpdateStatementOptions = struct {
    upsert: ?bool = null,
    multi: bool,
    array_filters: ?[]bson.BsonDocument = null,
    collation: ?Collation = null,
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

pub const UpdateOptions = struct {
    bypass_document_validation: ?bool = null,

    collation: ?Collation = null,

    // hint: ?union(enum) {
    //     string: []const u8,
    //     document: bson.BsonDocument,
    // } = null,

    array_filters: ?[]bson.BsonDocument = null,

    upsert: ?bool = null,

    let: ?bson.BsonDocument = null,

    comment: ?bson.BsonDocument = null,

    sort: ?bson.BsonDocument = null,

    ordered: ?bool = null,

    // /// @since MongoDB 8.2
    // raw_data: ?bool = null,
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

    pub fn exec(self: *UpdateCommandChainable, options: UpdateOptions) !union(enum) {
        response: *UpdateCommandResponse,
        err: *ErrorResponse,
    } {
        if (self.err) |err| {
            return err;
        }

        const update_statements = try self.builder.toOwnedSlice();
        defer {
            for (update_statements) |update| {
                update.deinit(self.collection.allocator);
            }
            self.collection.allocator.free(update_statements);
            self.builder.deinit();
        }

        var command: UpdateCommand = .{
            .update = self.collection.collection_name,
            .updates = update_statements,
            .@"$db" = self.collection.database.db_name,
            .collation = options.collation,
            .ordered = options.ordered,
            .let = options.let,
        };
        const result = try self.collection.runCommand(&command, null, UpdateCommandResponse);
        return switch (result) {
            .err => .{ .err = result.err },
            .response => .{ .response = result.response },
        };
    }
};
