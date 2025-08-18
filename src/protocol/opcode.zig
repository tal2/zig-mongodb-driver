const std = @import("std");
const bson = @import("bson");
const BsonDocument = bson.BsonDocument;
const Allocator = std.mem.Allocator;

// https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/

// https://www.mongodb.com/docs/manual/legacy-opcodes/

// pub fn writeCommand(writer: anytype, command: *BsonDocument, request_id: i32, response_to: i32, flags: OpMsg.OpMsgFlags) !void {
//     var message = OpMsg{
//         .header = MsgHeader{
//             .message_length = 0,
//             .request_id = request_id,
//             .response_to = response_to,
//             .op_code = OP_CODES.OP_MSG,
//         },
//         .flag_bits = flags.getFlagBits(),
//         .section_document = .{ .document = command },
//     };
//     message.header.message_length = calculateMessageLength(&message);

//     return writeMessage(writer, &message);
// }

pub fn readMessage(allocator: Allocator, reader: anytype) !*OpMsg {
    var message = try allocator.create(OpMsg);
    message.* = OpMsg{
        .header = MsgHeader{
            .message_length = 0,
            .request_id = 0,
            .response_to = 0,
            .op_code = OP_CODES.OP_MSG,
        },
        .flag_bits = 0,
        .section_document = .{ .document = undefined },
    };

    var reader_pos: i32 = 0;
    const expected_message_len = try reader.readInt(i32, .little); // TODO : verify message length
    message.header.message_length = expected_message_len;
    message.header.request_id = try reader.readInt(i32, .little);
    message.header.response_to = try reader.readInt(i32, .little);
    message.header.op_code = @enumFromInt(try reader.readInt(i32, .little));

    reader_pos += @sizeOf(MsgHeader);

    message.flag_bits = try reader.readInt(u32, .little);
    reader_pos += @sizeOf(u32);

    const section_payload_type = try reader.readInt(u8, .little);

    blk_section_payload_type: switch (section_payload_type) {
        DocumentSection.payload_type => {
            message.section_document.document = try BsonDocument.readDocument(allocator, reader);
            reader_pos += @sizeOf(u8) + @as(i32, @intCast(message.section_document.document.len));

            if (reader_pos == expected_message_len) {
                return message;
            }
            continue :blk_section_payload_type try reader.readInt(u8, .little);
        },
        SequenceSection.payload_type => {
            reader_pos += @sizeOf(u8);
            // message.section_sequence = try readSequence(reader);
            @panic("not implemented");
        },
        else => return error.InvalidSectionPayloadType,
    }

    unreachable;
}

fn calculateMessageLength(message: *const OpMsg) i32 {
    const msg_header_size: comptime_int = @sizeOf(MsgHeader);
    const section_payload_type_size: comptime_int = @sizeOf(u8);
    const flags_size: comptime_int = @sizeOf(u32);

    var len: i32 = msg_header_size + flags_size + section_payload_type_size;
    len += @as(i32, @intCast(message.section_document.document.len));

    if (message.section_sequences) |sequences| {
        for (sequences) |seq| {
            len += seq.size + @sizeOf(u8);
        }
    }

    return len;
}

pub const OP_CODES = enum(i32) {
    OP_MSG = 2013,
    OP_COMPRESSED = 2012,

    // Legacy opcodes for older server versions
    /// Deprecated in MongoDB 5.0. Removed in MongoDB 5.1.
    OP_REPLY = 1,
    /// Deprecated in MongoDB 5.0. Removed in MongoDB 5.1.
    OP_UPDATE = 2001,
    /// Deprecated in MongoDB 5.0. Removed in MongoDB 5.1.
    OP_INSERT = 2002,
    // RESERVED = 2003, //
    /// Deprecated in MongoDB 5.0. Removed in MongoDB 5.1.
    OP_QUERY = 2004,
    /// Deprecated in MongoDB 5.0. Removed in MongoDB 5.1.
    OP_GET_MORE = 2005,
    /// Deprecated in MongoDB 5.0. Removed in MongoDB 5.1.
    OP_DELETE = 2006,
    /// Deprecated in MongoDB 5.0. Removed in MongoDB 5.1.
    OP_KILL_CURSORS = 2007,
};

pub const OpMsg = struct {
    header: MsgHeader,
    flag_bits: u32,
    section_document: DocumentSection,
    section_sequences: ?[]*const SequenceSection = null,
    checksum: ?u32 = null,

    pub fn init(allocator: Allocator, command: *const BsonDocument, request_id: i32, response_to: i32, flags: OpMsg.OpMsgFlags) Allocator.Error!*OpMsg {
        return try initSequence(allocator, command, null, request_id, response_to, flags);
    }

    pub fn initSequence(allocator: Allocator, command: *const BsonDocument, sequences: ?[]*const SequenceSection, request_id: i32, response_to: i32, flags: OpMsg.OpMsgFlags) Allocator.Error!*OpMsg {
        var message = try allocator.create(OpMsg);
        message.* = OpMsg{
            .header = MsgHeader{
                .message_length = 0,
                .request_id = request_id,
                .response_to = response_to,
                .op_code = OP_CODES.OP_MSG,
            },
            .flag_bits = flags.getFlagBits(),
            .section_document = .{ .document = command },
            .section_sequences = sequences,
        };
        message.header.message_length = calculateMessageLength(message);
        return message;
    }

    pub fn deinit(self: *const OpMsg, allocator: Allocator) void {
        self.section_document.document.deinit(allocator);
        if (self.section_sequences) |sequences| {
            for (sequences) |seq| {
                seq.deinit(allocator);
            }
        }
        allocator.destroy(self);
    }

    pub fn write(self: *const OpMsg, writer: anytype) !void {
        try writer.writeInt(i32, self.header.message_length, .little);
        try writer.writeInt(i32, self.header.request_id, .little);
        try writer.writeInt(i32, self.header.response_to, .little);
        try writer.writeInt(i32, @intFromEnum(self.header.op_code), .little);
        try writer.writeInt(u32, self.flag_bits, .little);

        try self.section_document.write(writer);

        if (self.section_sequences) |sequences| {
            for (sequences) |seq| {
                try seq.write(writer);
            }
        }
    }

    pub const OpMsgFlags = struct {
        const checksum_present_flag = 0b1;
        const more_to_come_flag = 0b1 << 1;
        const exhaust_allowed_flag = 0b1 << 16;

        checksum_present: bool = false,
        more_to_come: bool = false,
        exhaust_allowed: bool = false,

        pub fn getFlagBits(self: *const OpMsgFlags) u32 {
            var flag_bits: u32 = 0;
            if (self.checksum_present) {
                flag_bits = flag_bits | checksum_present_flag;
            }
            if (self.more_to_come) {
                flag_bits = flag_bits | more_to_come_flag;
            }
            if (self.exhaust_allowed) {
                flag_bits = flag_bits | exhaust_allowed_flag;
            }
            return flag_bits;
        }

        pub inline fn isFlagHasMoreToComeSet(flag_bits: u32) bool {
            return flag_bits & more_to_come_flag != 0;
        }

        pub inline fn isFlagHasChecksumSet(flag_bits: u32) bool {
            return flag_bits & checksum_present_flag != 0;
        }

        pub inline fn isFlagHasExhaustAllowedSet(flag_bits: u32) bool {
            return flag_bits & exhaust_allowed_flag != 0;
        }
        // pub fn setExhaustAllowed(self: *OpMsgFlags) void {
        //     self.exhaust_allowed = true;
        // }

        // pub fn setChecksum(self: *OpMsgFlags) void {
        //     self.checksum_present = true;
        // }

        // pub fn setMoreToCome(self: *OpMsgFlags) void {
        //     self.more_to_come = true;
        // }

    };
};

pub const OpQuery = struct {
    header: MsgHeader,
    flags: u32,
    /// The full collection name, specifically its namespace. The namespace is the concatenation of the database name with the collection name, using a . for the concatenation. For example, for the database test and the collection contacts, the full collection name is test.contacts.
    full_collection_name: []const u8,
    /// number of documents to skip
    number_to_skip: i32,
    /// number of documents to return
    number_to_return: i32,
    query: *BsonDocument,
    /// Selector indicating the fields to return.
    return_fields_selector: ?*BsonDocument = null,

    pub fn init(allocator: Allocator, request_id: i32, response_to: i32, flags: OpQuery.OpQueryFlags, full_collection_name: []const u8, number_to_skip: i32, number_to_return: i32, query: *BsonDocument, return_fields_selector: ?*BsonDocument) Allocator.Error!*OpMsg {
        var message = try allocator.create(OpQuery);
        message.* = OpQuery{
            .header = MsgHeader{
                .message_length = 0,
                .request_id = request_id,
                .response_to = response_to,
                .op_code = OP_CODES.OP_QUERY,
            },
            .flags = flags.getFlagBits(),
            .full_collection_name = full_collection_name,
            .number_to_skip = number_to_skip,
            .number_to_return = number_to_return,
            .query = query,
            .return_fields_selector = return_fields_selector,
        };
        message.header.message_length = calculateMessageLength(message);
        return message;
    }

    pub fn deinit(self: *const OpQuery, allocator: Allocator) void {
        self.query.deinit(allocator);
        if (self.return_fields_selector) |selector| {
            selector.deinit(allocator);
        }
        allocator.destroy(self);
    }

    pub const OpQueryFlags = struct {

        // 0 is reserved. Must be set to 0.
        const tailable_cursor_flag: comptime_int = 0b1 << 1;
        // 1 corresponds to TailableCursor. Tailable means cursor is not closed when the last data is retrieved. Rather, the cursor marks the final object's position. You can resume using the cursor later, from where it was located, if more data were received. Like any latent cursor, the cursor may become invalid at some point (CursorNotFound) – for example if the final object it references were deleted.
        const slave_ok_flag: comptime_int = 0b1 << 2;
        // 2 corresponds to SlaveOk. Allow query of replica slave. Normally these return an error except for namespace "local".
        const oplog_replay_flag: comptime_int = 0b1 << 3;
        // 3 corresponds to OplogReplay. You need not specify this flag because the optimization automatically happens for eligible queries on the oplog. See oplogReplay for more information.
        const no_cursor_timeout_flag: comptime_int = 0b1 << 4;
        // 4 corresponds to NoCursorTimeout. The server normally times out idle cursors after an inactivity period (10 minutes) to prevent excess memory use. Set this option to prevent that.
        const await_data_flag: comptime_int = 0b1 << 5;
        // 5 corresponds to AwaitData. Use with TailableCursor. If the cursor is at the end of the data, block for a while rather than returning no data. After a timeout period, the server returns as normal.
        const exhaust_flag: comptime_int = 0b1 << 6;
        // 6 corresponds to Exhaust. Stream the data down full blast in multiple "more" packages, on the assumption that the client will fully read all data queried. Faster when you are pulling a lot of data and know you want to pull it all down. Note: the client is not allowed to not read all the data unless it closes the connection.
        const partial_flag: comptime_int = 0b1 << 7;
        // 7 corresponds to Partial. Get partial results from a mongos if some shards are down (instead of throwing an error)

        // 8-31 are reserved. Must be set to 0.

        tailable_cursor: bool = false,
        slave_ok: bool = false,
        oplog_replay: bool = false,
        no_cursor_timeout: bool = false,
        await_data: bool = false,
        exhaust: bool = false,
        partial: bool = false,

        pub fn getFlagBits(self: *const OpQueryFlags) u32 {
            var flag_bits: u32 = 0;
            if (self.tailable_cursor) {
                flag_bits = flag_bits | tailable_cursor_flag;
            }
            if (self.slave_ok) {
                flag_bits = flag_bits | slave_ok_flag;
            }
            if (self.oplog_replay) {
                flag_bits = flag_bits | oplog_replay_flag;
            }
            if (self.no_cursor_timeout) {
                flag_bits = flag_bits | no_cursor_timeout_flag;
            }
            if (self.await_data) {
                flag_bits = flag_bits | await_data_flag;
            }
            if (self.exhaust) {
                flag_bits = flag_bits | exhaust_flag;
            }
            if (self.partial) {
                flag_bits = flag_bits | partial_flag;
            }
            return flag_bits;
        }
    };
};

pub const MsgHeader = struct {
    message_length: i32,
    request_id: i32,
    response_to: i32,
    op_code: OP_CODES = OP_CODES.OP_MSG,
};

const DocumentSection = struct {
    const payload_type = 0;
    document: *const BsonDocument,

    pub fn write(self: *const DocumentSection, writer: anytype) !void {
        try writer.writeInt(u8, DocumentSection.payload_type, .little);
        try writer.writeAll(self.document.raw_data);
    }
};

pub const SequenceSection = struct {
    const payload_type = 1;

    size: i32,
    identifier: []const u8,
    documents: []*const BsonDocument,

    pub fn init(allocator: Allocator, identifier: []const u8, documents: []*const BsonDocument) Allocator.Error!*SequenceSection {
        var size: usize = @sizeOf(i32) + identifier.len + 1;
        for (documents) |doc| {
            size += doc.len;
        }

        const sequence = try allocator.create(SequenceSection);
        sequence.* = SequenceSection{
            .size = @as(i32, @intCast(size)),
            .identifier = identifier,
            .documents = documents,
        };
        return sequence;
    }

    pub fn deinit(self: *const SequenceSection, allocator: Allocator) void {
        for (self.documents) |doc| {
            doc.deinit(allocator);
        }
    }

    pub fn write(self: *const SequenceSection, writer: anytype) !void {
        try writer.writeInt(u8, SequenceSection.payload_type, .little);
        try writer.writeInt(i32, self.size, .little);
        try writer.writeAll(self.identifier[0..]);
        try writer.writeByte(0x0);
        for (self.documents) |doc| {
            try writer.writeAll(doc.raw_data);
        }
    }
};
