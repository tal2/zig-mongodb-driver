const std = @import("std");
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const ArrayList = std.ArrayList;
const StringHashMap = std.StringHashMap;

const bson = @import("bson");
const BsonDocument = bson.BsonDocument;
const BsonDocumentView = bson.BsonDocumentView;
const Collection = @import("../Collection.zig").Collection;
const CursorInfo = @import("../commands/CursorInfo.zig").CursorInfo;
const Hint = @import("../protocol/hint.zig").Hint;
const Comment = @import("../protocol/comment.zig").Comment;

const commands = @import("../commands/root.zig");
const WriteError = commands.WriteError;
const Collation = @import("../commands/collation.zig").Collation;
const ErrorResponse = @import("./ErrorResponse.zig").ErrorResponse;
const WriteResponseUnion = @import("../ResponseUnion.zig").WriteResponseUnion;

const opcode = @import("../protocol/opcode.zig");
const SequenceSection = opcode.SequenceSection;

pub const BulkWriteOps = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;
    bulkWrite: i32 = 1,

    @"$db": []const u8 = "admin",
    ordered: ?bool = null,

    errorsOnly: ?bool = null,

    bypassDocumentValidation: ?bool = null,

    comment: ?Comment = null,
    // let: ?[]*BsonDocument = null,
    rawData: ?bool = null,

    writeConcern: ?*BsonDocument = null,

    // Must be value of ServerApiVersion.value()
    apiVersion: ?[]const u8 = null,
    apiStrict: ?bool = null,
    apiDeprecationErrors: ?bool = null,

    // RunCommandOptions
    readPreference: ?[]const u8 = null,
    timeoutMS: ?i64 = null,
    // session: ?ClientSession = null,

    pub fn init(ops: []const *BsonDocument, ordered: ?bool, writeConcern: ?*BsonDocument) BulkWriteOps {
        return .{
            .operations = ops,
            .ordered = ordered,
            .writeConcern = writeConcern,
        };
    }

    pub fn deinit(self: *const BulkWriteOps, allocator: Allocator) void {
        for (self.operations) |op| {
            op.deinit(allocator);
        }
        allocator.free(self.operations);
        if (self.writeConcern) |write_concern| {
            write_concern.deinit(allocator);
        }
        allocator.destroy(self);
    }
};

pub const NamespaceWriteModelPair = struct {
    namespace: []const u8,
    model: *BsonDocument,
};

const NamespaceDoc = struct { ns: []const u8 };

pub const BulkWriteOpsChainable = struct {
    collection: *const Collection,
    operations: ArrayList(*const BsonDocument),
    ns_info_map: StringHashMap(u32),
    arena: ArenaAllocator,

    selected_namespace_index: ?u32 = null,

    err: ?anyerror = null,

    pub fn init(collection: *const Collection) BulkWriteOpsChainable {
        return .{
            .collection = collection,
            .operations = .empty,
            .ns_info_map = StringHashMap(u32).init(collection.allocator),
            .arena = ArenaAllocator.init(collection.allocator),
        };
    }

    pub fn deinit(self: *BulkWriteOpsChainable) void {
        self.arena.deinit();
        self.operations.deinit(self.collection.allocator);
        self.ns_info_map.deinit();

        self.collection.allocator.destroy(self.collection);
    }

    pub fn selectNamespace(self: *BulkWriteOpsChainable, namespace: []const u8) *BulkWriteOpsChainable {
        self.selected_namespace_index = self.ns_info_map.get(namespace);
        if (self.selected_namespace_index == null) {
            const allocator = self.arena.allocator();
            const new_index = self.ns_info_map.count();
            self.selected_namespace_index = new_index;
            const namespace_duped = allocator.dupe(u8, namespace) catch |err| {
                self.err = err;
                return self;
            };

            self.ns_info_map.put(namespace_duped, new_index) catch |err| {
                self.err = err;
                return self;
            };
        }

        return self;
    }

    pub fn add(self: *BulkWriteOpsChainable, op: anytype) *BulkWriteOpsChainable {
        if (self.selected_namespace_index == null) {
            self.err = error.NamespaceNotSelected;
            return self;
        }

        const allocator = self.arena.allocator();
        const op_serialized = BsonDocument.fromObject(allocator, @TypeOf(op), op) catch |err| {
            self.err = err;
            return self;
        };

        self.operations.append(self.collection.allocator, op_serialized) catch |err| {
            self.err = err;
            return self;
        };
        return self;
    }

    pub fn insertOne(self: *BulkWriteOpsChainable, obj: anytype) *BulkWriteOpsChainable {
        if (self.selected_namespace_index == null) {
            self.err = error.NamespaceNotSelected;
            return self;
        }

        if (!@hasField(@TypeOf(obj), "_id")) {
            self.err = error.MissingIdField;
            return self;
        }

        const allocator = self.arena.allocator();
        const obj_doc = BsonDocument.fromObject(allocator, @TypeOf(obj), obj) catch |err| {
            self.err = err;
            return self;
        };

        const op: BulkWriteInsertModel = .{ .insert = self.selected_namespace_index.?, .document = obj_doc };
        return self.add(op);
    }

    pub fn updateOne(self: *BulkWriteOpsChainable, filter: anytype, updateMods: anytype, upsert: bool, collation: ?Collation, array_filters: ?[]const *BsonDocument, hint: ?Hint) *BulkWriteOpsChainable {
        if (!isValidUpdateDocument(updateMods)) {
            self.err = error.InvalidUpdateDocument;
            return self;
        }
        // TODO: check not array
        if (self.selected_namespace_index == null) {
            self.err = error.NamespaceNotSelected;
            return self;
        }

        const allocator = self.arena.allocator();

        const filter_doc = BsonDocument.fromObject(allocator, @TypeOf(filter), filter) catch |err| {
            self.err = err;
            return self;
        };

        const update_doc = BsonDocument.fromObject(allocator, @TypeOf(updateMods), updateMods) catch |err| {
            self.err = err;
            return self;
        };

        const op: BulkWriteUpdateModel = .{
            .update = self.selected_namespace_index.?,
            .filter = filter_doc,
            .updateMods = update_doc,
            .upsert = upsert,
            .multi = false,
            .collation = collation,
            .arrayFilters = array_filters,
            .hint = hint,
        };

        return self.add(op);
    }

    pub fn updateMany(self: *BulkWriteOpsChainable, filter: anytype, updateMods: anytype, upsert: bool, collation: ?Collation, array_filters: ?[]const *BsonDocument, hint: ?Hint) *BulkWriteOpsChainable {
        if (!isValidUpdateDocument(updateMods)) {
            self.err = error.InvalidUpdateDocument;
            return self;
        }

        if (self.selected_namespace_index == null) {
            self.err = error.NamespaceNotSelected;
            return self;
        }

        const allocator = self.arena.allocator();

        const filter_doc = BsonDocument.fromObject(allocator, @TypeOf(filter), filter) catch |err| {
            self.err = err;
            return self;
        };

        const update_doc = BsonDocument.fromObject(allocator, @TypeOf(updateMods), updateMods) catch |err| {
            self.err = err;
            return self;
        };

        const op: BulkWriteUpdateModel = .{
            .update = self.selected_namespace_index.?,
            .filter = filter_doc,
            .updateMods = update_doc,
            .upsert = upsert,
            .multi = true,
            .collation = collation,
            .arrayFilters = array_filters,
            .hint = hint,
        };

        return self.add(op);
    }

    fn isValidReplacementDocument(replacement: anytype) bool {
        comptime {
            if (containsOnlyAtomicModifiers(@TypeOf(replacement))) {
                @compileError("invalid replacement document: all fields must not be atomic modifiers - starting with $");
            }
        }

        const info = @typeInfo(@TypeOf(replacement));
        if (info != .pointer) {
            return false;
        }

        if (@typeInfo(info.pointer.child) == .array) {
            return false;
        }

        return true;
    }

    fn isValidUpdateDocument(updateMods: anytype) bool {
        comptime {
            if (!containsOnlyAtomicModifiers(@TypeOf(updateMods))) {
                @compileError("invalid update document: all fields must be atomic modifiers - starting with $");
            }
        }

        const info = @typeInfo(@TypeOf(updateMods));

        if (info == .pointer and @typeInfo(info.pointer.child) == .array and updateMods.len == 0) {
            return false;
        }

        return true;
    }

    fn containsOnlyAtomicModifiers(comptime updateModsType: type) bool {
        const info = @typeInfo(updateModsType);
        if (info == .@"struct") {
            const field_names = std.meta.fieldNames(updateModsType);
            for (field_names) |field_name| {
                if (field_name[0] != '$') {
                    return false;
                }
            }
            return true;
        }

        if (info != .pointer) {
            @compileError("invalid type");
        }

        if (@typeInfo(info.pointer.child) == .array) {
            for (updateModsType) |mod| {
                const fields = @typeInfo(@TypeOf(mod)).@"struct".fields;
                inline for (fields) |field| {
                    if (field.name[0] != '$') {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    fn containsNoAtomicModifiers(comptime replacementType: type) bool {
        const info = @typeInfo(replacementType);
        if (info != .pointer) {
            @compileError("invalid type");
        }

        if (@typeInfo(info.pointer.child) == .array) {
            @compileError("invalid type");
        }

        if (@typeInfo(info.pointer.child) == .@"struct") {
            const fields = @typeInfo(replacementType).@"struct".fields;
            inline for (fields) |field| {
                if (field.name[0] == '$')
                    return false;
            }
        }
        return true;
    }

    pub fn replaceOne(self: *BulkWriteOpsChainable, filter: anytype, replacement: anytype, upsert: bool, collation: ?Collation, hint: ?Hint, sort: anytype) *BulkWriteOpsChainable {
        if (self.selected_namespace_index == null) {
            self.err = error.NamespaceNotSelected;
            return self;
        }

        if (!isValidReplacementDocument(@TypeOf(replacement))) {
            self.err = error.InvalidReplacementDocument;
            return self;
        }

        const allocator = self.arena.allocator();
        const filter_doc = BsonDocument.fromObject(allocator, @TypeOf(filter), filter) catch |err| {
            self.err = err;
            return self;
        };

        const replacement_doc = BsonDocument.fromObject(allocator, @TypeOf(replacement), replacement) catch |err| {
            self.err = err;
            return self;
        };
        const op: BulkWriteReplaceOneModel = .{
            .update = self.selected_namespace_index.?,
            .filter = filter_doc,
            .updateMods = replacement_doc,
            .upsert = upsert,
            .collation = collation,
            .hint = hint,
            .sort = sort,
        };
        return self.add(op);
    }

    pub fn deleteOne(self: *BulkWriteOpsChainable, filter: anytype, collation: ?Collation, hint: ?Hint) *BulkWriteOpsChainable {
        if (self.selected_namespace_index == null) {
            self.err = error.NamespaceNotSelected;
            return self;
        }

        const allocator = self.arena.allocator();
        const filter_doc = BsonDocument.fromObject(allocator, @TypeOf(filter), filter) catch |err| {
            self.err = err;
            return self;
        };

        const op: BulkWriteDeleteModel = .{
            .delete = self.selected_namespace_index.?,
            .filter = filter_doc,
            .multi = false,
            .collation = collation,
            .hint = hint,
        };
        return self.add(op);
    }

    pub fn deleteMany(self: *BulkWriteOpsChainable, filter: anytype, collation: ?Collation, hint: ?Hint) *BulkWriteOpsChainable {
        if (self.selected_namespace_index == null) {
            self.err = error.NamespaceNotSelected;
            return self;
        }

        const allocator = self.arena.allocator();
        const filter_doc = BsonDocument.fromObject(allocator, @TypeOf(filter), filter) catch |err| {
            self.err = err;

            return self;
        };

        const op: BulkWriteDeleteModel = .{
            .delete = self.selected_namespace_index.?,
            .filter = filter_doc,
            .multi = true,
            .collation = collation,
            .hint = hint,
        };
        return self.add(op);
    }

    pub fn exec(self: *BulkWriteOpsChainable, options: BulkWriteOptions) !WriteResponseUnion(BulkWriteResponse, ErrorResponse, BulkWriteErrorResponse) {
        if (self.err) |err| {
            return err;
        }

        const arena_allocator = self.arena.allocator();

        const ops = try self.operations.toOwnedSlice(self.collection.allocator);
        defer self.collection.allocator.free(ops);

        var command: BulkWriteOps = .{
            .ordered = options.ordered,
            .writeConcern = options.writeConcern,
        };

        self.collection.database.server_api.addToCommand(&command);

        const command_serialized = try BsonDocument.fromObject(arena_allocator, @TypeOf(command), command);

        const ops_sequence = try SequenceSection.init(arena_allocator, "ops", ops);

        var ns_map = try ArrayList(*const BsonDocument).initCapacity(arena_allocator, self.ns_info_map.count());
        var ns_info_it = self.ns_info_map.keyIterator();
        while (ns_info_it.next()) |ns| {
            const ns_doc = try BsonDocument.fromObject(arena_allocator, NamespaceDoc, NamespaceDoc{ .ns = ns.* });
            ns_map.appendAssumeCapacity(ns_doc);
        }

        const ns_info_docs = try ns_map.toOwnedSlice(arena_allocator);
        const ns_info_sequence = try SequenceSection.init(arena_allocator, "nsInfo", ns_info_docs);

        var sequences = try ArrayList(*const SequenceSection).initCapacity(arena_allocator, 2);
        sequences.appendAssumeCapacity(ns_info_sequence);
        sequences.appendAssumeCapacity(ops_sequence);
        const sequences_slice = try sequences.toOwnedSlice(arena_allocator);

        const command_op_msg = try opcode.OpMsg.initSequence(arena_allocator, command_serialized, sequences_slice, 2, 0, .{});
        defer command_op_msg.deinit(arena_allocator);

        return try self.collection.database.runWriteCommandOpcode(command_op_msg, BulkWriteResponse, BulkWriteErrorResponse);
    }
};

pub const BulkWriteOptions = struct {
    ordered: ?bool = true,
    writeConcern: ?*BsonDocument = null,
};

pub const BulkWriteResponse = struct {
    acknowledged: ?bool = null,
    ok: f64,
    insertedIds: ?[]const *InsertedId = null,
    nInserted: i64,
    nUpserted: i64,
    nMatched: i64,
    nModified: i64,
    nDeleted: i64,
    upserted: ?[]const *InsertedId = null,
    cursor: *CursorInfo,

    pub fn parseBson(allocator: Allocator, document: *const BsonDocument) !*BulkWriteResponse {
        return try document.toObject(allocator, BulkWriteResponse, .{ .ignore_unknown_fields = true });
    }

    pub fn deinit(self: *const BulkWriteResponse, allocator: Allocator) void {
        if (self.insertedIds) |inserted_ids| {
            for (inserted_ids) |inserted_id| {
                inserted_id.deinit(allocator);
            }
        }
        if (self.upserted) |upserted| {
            for (upserted) |upserted_id| {
                upserted_id.deinit(allocator);
            }
        }
        self.cursor.deinit(allocator);
        allocator.destroy(self);
    }
};

pub const InsertedId = struct {
    id: *BsonDocument,
    index: usize,

    pub fn deinit(self: *const InsertedId, allocator: Allocator) void {
        self.id.deinit(allocator);
        allocator.destroy(self);
    }
};

pub const BulkWriteError = struct {
    err: *Error,

    pub fn deinit(self: *const BulkWriteError, allocator: Allocator) void {
        self.err.op.deinit(allocator);
        if (self.err.errmsg) |errmsg| {
            allocator.free(errmsg);
        }
        allocator.destroy(self.err);
        allocator.destroy(self);
    }

    pub const Error = struct {
        index: i32,
        errmsg: ?[]const u8 = null,
        code: ?i32 = null,
        op: *BsonDocument,
    };
};

pub const BulkWriteErrorResponse = struct {
    ok: f64,
    writeErrors: ?[]const *BulkWriteError = null,
    writeConcernErrors: ?[]const *WriteError = null,

    pub fn deinit(self: *const BulkWriteErrorResponse, allocator: Allocator) void {
        if (self.writeErrors) |write_errors| {
            for (write_errors) |write_error| {
                write_error.deinit(allocator);
            }
        }
        if (self.writeConcernErrors) |write_concern_errors| {
            for (write_concern_errors) |write_concern_error| {
                write_concern_error.deinit(allocator);
            }
        }
        allocator.destroy(self);
    }

    pub fn parseBson(allocator: Allocator, document: *const BsonDocument) !*BulkWriteErrorResponse {
        return try document.toObject(allocator, BulkWriteErrorResponse, .{ .ignore_unknown_fields = true });
    }

    pub fn isError(allocator: Allocator, document: *const BsonDocument) !bool {
        const doc_view = BsonDocumentView.loadDocument(allocator, document);
        const ok_value = try doc_view.checkElement("ok", ErrorResponse.isElementValueFalsy);
        return ok_value orelse error.UnexpectedDocumentFormat;
    }
};

const BulkWriteInsertModel = struct {
    insert: u32,
    document: *BsonDocument,
};

const BulkWriteUpdateModel = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    update: u32,
    filter: *BsonDocument,
    updateMods: *BsonDocument,
    upsert: ?bool = false,
    multi: ?bool = null,
    collation: ?Collation = null,
    arrayFilters: ?[]const *BsonDocument = null,
    hint: ?Hint = null,
};

const BulkWriteReplaceOneModel = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    update: u32, // replace operation uses "update" field
    filter: *BsonDocument,
    updateMods: *BsonDocument,
    upsert: ?bool = null,
    collation: ?Collation = null,
    sort: ?*BsonDocument = null,
    hint: ?Hint = null,
};

const BulkWriteDeleteModel = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    delete: u32,
    filter: *BsonDocument,
    multi: ?bool = null,
    collation: ?Collation = null,
    hint: ?Hint = null,
};
