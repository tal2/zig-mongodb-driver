const std = @import("std");
const bson = @import("bson");

const WriteError = @import("WriteError.zig").WriteError;
const WriteConcernError = @import("WriteConcernError.zig").WriteConcernError;
const RunCommandOptions = @import("./RunCommandOptions.zig").RunCommandOptions;
const Comment = @import("../protocol/comment.zig").Comment;

const Allocator = std.mem.Allocator;
const BsonDocument = bson.BsonDocument;

pub const JsonParseError = error{UnexpectedToken} || std.json.Scanner.NextError;

pub const InsertOneOptions = struct {
    run_command_options: ?RunCommandOptions = null,

    bypassDocumentValidation: ?bool = null,

    comment: ?Comment = null,

    // /// @since MongoDB 8.2
    // rawData: ?bool = null,

    pub fn addToCommand(self: *const InsertOneOptions, command: *InsertCommand) void {
        if (self.run_command_options) |run_command_options| run_command_options.addToCommand(command);

        command.bypassDocumentValidation = self.bypassDocumentValidation;
        command.comment = self.comment;
    }
};


pub const InsertManyOptions = struct {
    run_command_options: ?RunCommandOptions = null,

    bypass_document_validation: ?bool = null,
    ordered: bool = true,

    comment: ?Comment = null,

    // /// @since MongoDB 8.2
    // raw_data: ?bool = null,

    pub fn addToCommand(self: *const InsertManyOptions, command: *InsertCommand) void {
        if (self.run_command_options) |run_command_options| run_command_options.addToCommand(command);

        command.bypassDocumentValidation = self.bypass_document_validation;
        command.ordered = self.ordered;
        command.comment = self.comment;
    }
};

pub const InsertCommand = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    insert: []const u8,
    documents: []*const BsonDocument,
    @"$db": []const u8,

    bypassDocumentValidation: ?bool = null,
    ordered: ?bool = null,
    comment: ?Comment = null,
    // writeConcern: ?*WriteConcern = null,
    // writeCommandOptions: ?*WriteCommandOptions = null,

    apiVersion: ?[]const u8 = null,
    apiStrict: ?bool = null,
    apiDeprecationErrors: ?bool = null,

    readPreference: ?[]const u8 = null,
    timeoutMS: ?i64 = null,

    pub fn deinit(self: *const InsertCommand, allocator: Allocator) void {
        for (self.documents) |document| {
            document.deinit(allocator);
        }
        allocator.free(self.documents);
    }

    pub fn makeInsertOne(
        allocator: std.mem.Allocator,
        collection_name: []const u8,
        db_name: []const u8,
        document: anytype,
        options: InsertOneOptions,
    ) !InsertCommand {
        const document_parsed = try BsonDocument.fromObject(allocator, @TypeOf(document), document);
        const documents = try allocator.alloc(*const BsonDocument, 1);
        documents[0] = document_parsed;

        var command: InsertCommand = .{
            .insert = collection_name,
            .documents = documents,
            .@"$db" = db_name,
        };
        options.addToCommand(&command);

        return command;
    }

    pub fn makeInsertMany(
        allocator: std.mem.Allocator,
        collection_name: []const u8,
        db_name: []const u8,
        documents: anytype,
        options: InsertManyOptions,
    ) !InsertCommand {
        if (documents.len == 0) {
            return error.InvalidArgument;
        }

        var documents_parsed = try std.ArrayList(*const bson.BsonDocument).initCapacity(allocator, documents.len);
        errdefer documents_parsed.deinit();

        for (documents) |document| {
            if (@TypeOf(document) == *bson.BsonDocument) {
                documents_parsed.appendAssumeCapacity(document);
            } else {
                const document_parsed = try bson.BsonDocument.fromObject(allocator, @TypeOf(document), document);
                errdefer document_parsed.deinit(allocator);
                documents_parsed.appendAssumeCapacity(document_parsed);
            }
        }

        var command: InsertCommand = .{
            .insert = collection_name,
            .documents = try documents_parsed.toOwnedSlice(),
            .@"$db" = db_name,
        };
        options.addToCommand(&command);

        return command;
    }
};

pub const InsertCommandResponse = struct {
    ok: f64,
    n: i32,
    writeConcernError: ?*WriteConcernError = null,

    pub fn deinit(self: *const InsertCommandResponse, allocator: Allocator) void {
        if (self.writeConcernError) |write_concern_error| {
            write_concern_error.deinit(allocator);
        }

        allocator.destroy(self);
    }

    pub fn jsonParse(allocator: Allocator, source: *std.json.Scanner, options: std.json.ParseOptions) JsonParseError!InsertCommandResponse {
        _ = options;
        if (try source.next() != .object_begin) return error.UnexpectedToken;

        var ok: ?f64 = null;
        var n: ?i32 = null;
        var write_concern_error: ?*WriteConcernError = null;

        blk_tkn: switch (try source.next()) {
            .string => |key| {
                if (ok == null and std.mem.eql(u8, key, "ok")) {
                    const ok_value = try source.next();

                    ok = std.fmt.parseFloat(f64, ok_value.number) catch return error.UnexpectedToken;

                    continue :blk_tkn try source.next();
                }
                if (n == null and std.mem.eql(u8, key, "n")) {
                    const n_value = try source.next();

                    n = std.fmt.parseInt(i32, n_value.number, 10) catch return error.UnexpectedToken;

                    continue :blk_tkn try source.next();
                }

                if (write_concern_error == null and std.mem.eql(u8, key, "writeConcernError")) {
                    const write_concern_error_doc_parsed = try WriteConcernError.jsonParse(allocator, source, .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
                    defer write_concern_error_doc_parsed.deinit(allocator);

                    write_concern_error = try write_concern_error_doc_parsed.dupe(allocator);
                    continue :blk_tkn try source.next();
                }
            },
            .object_end => break :blk_tkn,
            else => |key_token| {
                std.debug.print("UnexpectedToken: {any}\n", .{key_token});
                return error.UnexpectedToken;
            },
        }

        if (ok == null or n == null) return error.UnexpectedToken;

        return .{
            .ok = ok.?,
            .n = n.?,
            .writeConcernError = write_concern_error,
        };
    }

    pub fn dupe(self: *const InsertCommandResponse, allocator: Allocator) !*InsertCommandResponse {
        const response = try allocator.create(InsertCommandResponse);
        errdefer response.deinit(allocator);
        response.ok = self.ok;
        response.n = self.n;

        if (self.writeConcernError) |error_doc| {
            response.writeConcernError = try error_doc.dupe(allocator);
        } else {
            response.writeConcernError = null;
        }

        return response;
    }

    pub fn parseBson(allocator: Allocator, document: *const BsonDocument) !*InsertCommandResponse {
        return try document.toObject(allocator, InsertCommandResponse, .{ .ignore_unknown_fields = true });
    }
};
