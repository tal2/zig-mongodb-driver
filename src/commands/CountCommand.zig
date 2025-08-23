const std = @import("std");
const bson = @import("bson");

const Allocator = std.mem.Allocator;
const BsonDocument = bson.BsonDocument;

const RunCommandOptions = @import("./RunCommandOptions.zig").RunCommandOptions;
const Comment = @import("../protocol/comment.zig").Comment;

pub const JsonParseError = error{UnexpectedToken} || std.json.Scanner.NextError;

pub const CountCommand = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    count: []const u8,
    query: ?*BsonDocument = null,
    @"$db": []const u8,

    comment: ?Comment = null,

    // Must be value of ServerApiVersion.value()
    apiVersion: ?[]const u8 = null,
    apiStrict: ?bool = null,
    apiDeprecationErrors: ?bool = null,

    readPreference: ?[]const u8 = null,
    timeoutMS: ?i64 = null,

    pub fn deinit(self: *const CountCommand, allocator: Allocator) void {
        if (self.query) |query| query.deinit(allocator);
    }

    pub fn makeEstimateCount(
        collection_name: []const u8,
        db_name: []const u8,
        options: EstimatedDocumentCountOptions,
    ) !CountCommand {
        var command = CountCommand{
            .count = collection_name,
            .@"$db" = db_name,
        };

        options.addToCommand(&command);

        return command;
    }
};

pub const EstimatedDocumentCountOptions = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    run_command_options: ?RunCommandOptions = null,

    comment: ?Comment = null,

    pub fn addToCommand(self: *const EstimatedDocumentCountOptions, command: *CountCommand) void {
        command.comment = self.comment;
    }
};

pub const CountCommandResponse = struct {
    n: i64,
    ok: f64,

    pub fn jsonParse(allocator: Allocator, source: *std.json.Scanner, options: std.json.ParseOptions) JsonParseError!CountCommandResponse {
        _ = allocator;
        _ = options;
        if (try source.next() != .object_begin) return error.UnexpectedToken;

        var n: ?i64 = null;
        var ok: ?f64 = null;
        blk_tkn: switch (try source.next()) {
            .string => |key| {
                if (ok == null and std.mem.eql(u8, key, "ok")) {
                    const ok_value = try source.next();
                    ok = std.fmt.parseFloat(f64, ok_value.number) catch return error.UnexpectedToken;
                    continue :blk_tkn try source.next();
                }
                if (n == null and std.mem.eql(u8, key, "n")) {
                    const count_value = try source.next();
                    n = std.fmt.parseInt(i64, count_value.number, 10) catch return error.UnexpectedToken;
                }
                continue :blk_tkn try source.next();
            },
            .object_end => break :blk_tkn,
            else => return error.UnexpectedToken,
        }

        if (n == null or ok == null) return error.UnexpectedToken;

        return .{
            .n = n.?,
            .ok = ok.?,
        };
    }

    pub fn dupe(self: *const CountCommandResponse, allocator: Allocator) !*CountCommandResponse {
        const clone = try allocator.create(CountCommandResponse);
        errdefer clone.deinit(allocator);

        clone.n = self.n;
        clone.ok = self.ok;

        return clone;
    }

    pub fn parseBson(allocator: Allocator, document: *const BsonDocument) !*CountCommandResponse {
        return try document.toObject(allocator, CountCommandResponse, .{ .ignore_unknown_fields = true });
    }
};
