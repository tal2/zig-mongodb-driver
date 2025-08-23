const std = @import("std");
const bson = @import("bson");

const Allocator = std.mem.Allocator;
const BsonDocument = bson.BsonDocument;
const RunCommandOptions = @import("./RunCommandOptions.zig").RunCommandOptions;
const Collation = @import("./Collation.zig").Collation;
const Hint = @import("../protocol/hint.zig").Hint;
const Comment = @import("../protocol/comment.zig").Comment;

pub const JsonParseError = error{UnexpectedToken} || std.json.Scanner.NextError;

/// @see https://www.mongodb.com/docs/manual/reference/command/delete/
pub const DeleteCommand = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    delete: []const u8,
    deletes: []const DeleteStatement,
    @"$db": []const u8,
    collation: ?Collation = null,


    hint: ?Hint = null,
    let: ?bson.BsonDocument = null,

    comment: ?Comment = null,

    // /// @since MongoDB 8.2
    // raw_data: ?bool = null,

    writeConcern: ?bson.BsonDocument = null,

    maxTimeMS: ?i64 = null,

    ordered: ?bool = null,

    /// Must be value of ServerApiVersion.value()
    apiVersion: ?[]const u8 = null,
    apiStrict: ?bool = null,
    apiDeprecationErrors: ?bool = null,

    // RunCommandOptions
    readPreference: ?[]const u8 = null,
    timeoutMS: ?i64 = null,
    lsid: ?*BsonDocument = null,

    pub fn deinit(self: *const DeleteCommand, allocator: Allocator) void {
        for (self.deletes) |delete| delete.q.deinit(allocator);
        allocator.free(self.deletes);
    }

    pub fn makeDeleteOne(
        allocator: std.mem.Allocator,
        collection_name: []const u8,
        db_name: []const u8,
        filter: anytype,
        options: DeleteOptions,
    ) !DeleteCommand {
        const filter_parsed = try BsonDocument.fromObject(allocator, @TypeOf(filter), filter);

        const deletes = try allocator.alloc(DeleteStatement, 1);
        deletes[0] = DeleteStatement{
            .q = filter_parsed,
            .limit = 1,
        };

        var command: DeleteCommand = .{
            .delete = collection_name,
            .deletes = deletes,
            .@"$db" = db_name,
        };
        options.addToCommand(&command);

        return command;
    }

    pub fn makeDeleteMany(
        allocator: std.mem.Allocator,
        collection_name: []const u8,
        db_name: []const u8,
        filter: anytype,
        options: DeleteOptions,
    ) !DeleteCommand {
        const filter_parsed = try BsonDocument.fromObject(allocator, @TypeOf(filter), filter);

        const deletes = try allocator.alloc(DeleteStatement, 1);
        deletes[0] = DeleteStatement{
            .q = filter_parsed,
            .limit = 0,
        };

        var command: DeleteCommand = .{
            .delete = collection_name,
            .deletes = deletes,
            .@"$db" = db_name,
        };
        options.addToCommand(&command);

        return command;
    }
};

pub const DeleteStatement = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    q: *BsonDocument,

    limit: i32,

    collation: ?Collation = null,

    hint: ?Hint = null,
};

pub const DeleteCommandResponse = struct {
    acknowledged: ?bool = null,

    n: i64,

    pub fn deinit(self: *const DeleteCommandResponse, allocator: Allocator) void {
        allocator.destroy(self);
    }

    pub fn jsonParse(allocator: Allocator, source: *std.json.Scanner, options: std.json.ParseOptions) JsonParseError!DeleteCommandResponse {
        _ = allocator;
        _ = options;
        if (try source.next() != .object_begin) return error.UnexpectedToken;

        var ok: ?f64 = null;
        var n: ?i64 = null; // Number of documents deleted
        blk_tkn: switch (try source.next()) {
            .string => |key| {
                if (ok == null and std.mem.eql(u8, key, "ok")) {
                    const ok_value = try source.next();
                    ok = std.fmt.parseFloat(f64, ok_value.number) catch return error.UnexpectedToken;
                    continue :blk_tkn try source.next();
                }
                if (n == null and std.mem.eql(u8, key, "n")) {
                    const n_value = try source.next();
                    n = std.fmt.parseInt(i64, n_value.number, 10) catch return error.UnexpectedToken;
                    continue :blk_tkn try source.next();
                }
            },
            .object_end => break :blk_tkn,
            else => return error.UnexpectedToken,
        }

        if (ok == null or n == null) return error.UnexpectedToken;

        return .{
            .n = n.?,
        };
    }

    pub fn dupe(self: *const DeleteCommandResponse, allocator: Allocator) !*DeleteCommandResponse {
        const clone = try allocator.create(DeleteCommandResponse);
        errdefer clone.deinit(allocator);

        clone.acknowledged = self.acknowledged;
        clone.n = self.n;

        return clone;
    }

    pub fn parseBson(allocator: Allocator, document: *const BsonDocument) !*DeleteCommandResponse {
        return try document.toObject(allocator, DeleteCommandResponse, .{ .ignore_unknown_fields = true });
    }
};

pub const DeleteOptions = struct {
    run_command_options: ?RunCommandOptions = null,

    collation: ?Collation = null,

    hint: ?Hint = null,

    let: ?bson.BsonDocument = null,
    comment: ?Comment = null,
    // /// @since MongoDB 8.2
    // raw_data: ?bool = null,

    writeConcern: ?bson.BsonDocument = null,

    maxTimeMS: ?i64 = null,

    ordered: ?bool = null,

    pub fn addToCommand(self: *const DeleteOptions, command: *DeleteCommand) void {
        if (self.run_command_options) |run_command_options| run_command_options.addToCommand(command);

        command.collation = self.collation;
        command.hint = self.hint;
        command.let = self.let;
        command.comment = self.comment;
        command.writeConcern = self.writeConcern;
        command.maxTimeMS = self.maxTimeMS;
        command.ordered = self.ordered;
    }
};
