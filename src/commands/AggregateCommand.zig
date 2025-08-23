const std = @import("std");
const bson = @import("bson");

const CursorInfo = @import("./CursorInfo.zig").CursorInfo;
const commands = @import("./root.zig");
const FindOptions = commands.FindOptions;
const Hint = @import("../protocol/hint.zig").Hint;
const Comment = @import("../protocol/comment.zig").Comment;
const RunCommandOptions = @import("./RunCommandOptions.zig").RunCommandOptions;

const Allocator = std.mem.Allocator;
const BsonDocument = bson.BsonDocument;


pub const AggregateCommand = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    aggregate: []const u8, // TODO: make union of string and i32

    @"$db": []const u8,

    pipeline: []const *bson.BsonDocument,

    explain: ?bool = null,

    cursor: *bson.BsonDocument,

    allowDiskUse: ?bool = null,

    batchSize: ?i32 = null,

    bypassDocumentValidation: ?bool = null,

    readConcern: ?bson.BsonDocument = null,

    collation: ?bson.BsonDocument = null,

    hint: ?Hint = null,

    comment: ?Comment = null,

    writeConcern: ?bson.BsonDocument = null,

    maxTimeMS: ?i64 = null,

    let: ?bson.BsonDocument = null,

    /// Must be value of ServerApiVersion.value()
    apiVersion: ?[]const u8 = null,
    apiStrict: ?bool = null,
    apiDeprecationErrors: ?bool = null,

    readPreference: ?[]const u8 = null,
    timeoutMS: ?i64 = null,

    lsid: ?*BsonDocument = null,

    pub fn deinit(self: *const AggregateCommand, allocator: std.mem.Allocator) void {
        for (self.pipeline) |stage| {
            stage.deinit(allocator);
        }
        allocator.free(self.pipeline);
        self.cursor.deinit(allocator);
        allocator.destroy(self);
    }

    pub fn make(
        allocator: std.mem.Allocator,
        collection_name: []const u8,
        db_name: []const u8,
        pipeline: anytype,
        options: AggregateOptions,
        cursor_options: CursorOptions,
    ) !AggregateCommand {
        comptime {
            const pipeline_type_info = @typeInfo(@TypeOf(pipeline));
            if (pipeline_type_info != .array and (pipeline_type_info != .pointer or pipeline_type_info.pointer.size != .slice)) {
                @compileLog(pipeline_type_info);
                @compileLog(@tagName(pipeline_type_info));
                @compileError("pipeline must be an array or a pointer to an array");
            }
        }

        var pipeline_parsed = try std.ArrayList(*bson.BsonDocument).initCapacity(allocator, pipeline.len);

        for (pipeline) |stage| {
            if (@TypeOf(stage) == *bson.BsonDocument) {
                pipeline_parsed.appendAssumeCapacity(stage);
            } else {
                const stage_parsed = try bson.BsonDocument.fromObject(allocator, @TypeOf(stage), stage);
                pipeline_parsed.appendAssumeCapacity(stage_parsed);
            }
        }

        const cursor_parsed = try bson.BsonDocument.fromObject(allocator, CursorOptions, cursor_options);

        var command = AggregateCommand{
            .aggregate = collection_name,
            .@"$db" = db_name,
            .pipeline = try pipeline_parsed.toOwnedSlice(),
            .cursor = cursor_parsed,
        };

        options.addToCommand(&command);

        return command;
    }
};

pub const CursorOptions = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    batchSize: ?i32 = null,
    // maxTimeMS: ?i64 = null,
    // maxAwaitTimeMS: ?i64 = null,
    // readPreference: ?bson.BsonDocument = null,
    // readConcern: ?bson.BsonDocument = null,
    // collation: ?bson.BsonDocument = null,
};

pub const AggregateCommandResponse = struct {
    ok: f64,
    cursor: *CursorInfo,

    pub fn deinit(self: *const AggregateCommandResponse, allocator: std.mem.Allocator) void {
        self.cursor.deinit(allocator);
        self.deinit();
    }

    pub fn dupe(self: *const AggregateCommandResponse, allocator: Allocator) !*AggregateCommandResponse {
        var clone = try allocator.create(AggregateCommandResponse);
        errdefer allocator.destroy(clone);

        clone.ok = self.ok;
        clone.cursor = try self.cursor.dupe(allocator);

        return clone;
    }

    pub fn parseBson(allocator: Allocator, document: *const BsonDocument) !*AggregateCommandResponse {
        return try document.toObject(allocator, AggregateCommandResponse, .{ .ignore_unknown_fields = true });
    }
};

pub const AggregateOptions = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    run_command_options: ?RunCommandOptions = null,

    allowDiskUse: ?bool = null,

    batchSize: ?i32 = null,

    bypassDocumentValidation: ?bool = null,

    readConcern: ?bson.BsonDocument = null,

    collation: ?bson.BsonDocument = null,

    /// NOTE: This option is deprecated in favor of timeoutMS.
    maxTimeMS: ?i64 = null,

    comment: ?Comment = null,

    hint: ?Hint = null,

    writeConcern: ?bson.BsonDocument = null,

    let: ?bson.BsonDocument = null,

    pub fn addToCommand(self: *const AggregateOptions, command: *AggregateCommand) void {
        command.allowDiskUse = self.allowDiskUse;
        command.batchSize = self.batchSize;
        command.bypassDocumentValidation = self.bypassDocumentValidation;
        command.readConcern = self.readConcern;
        command.collation = self.collation;
        command.comment = self.comment;
        command.hint = self.hint;
        command.writeConcern = self.writeConcern;
        command.maxTimeMS = self.maxTimeMS;
        command.let = self.let;
    }
};
