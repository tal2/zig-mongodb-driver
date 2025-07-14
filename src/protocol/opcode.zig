const std = @import("std");
const bson = @import("bson");
const BsonDocument = bson.BsonDocument;
const Allocator = std.mem.Allocator;

pub fn makeCommand(allocator: Allocator, command: *BsonDocument, op_code: OP_CODES, request_id: i32, response_to: i32, flags: OpMsgFlags) !*OpcodeMsg {
    var message = try allocator.create(OpcodeMsg);
    message.* = OpcodeMsg{
        .header = MsgHeader{
            .message_length = 0,
            .request_id = request_id,
            .response_to = response_to,
            .op_code = op_code,
        },
        .flag_bits = flags.getFlagBits(),
        .section_document = .{ .document = command },
    };
    message.header.message_length = calculateMessageLength(message);
    return message;
}

pub fn writeCommand(writer: anytype, command: *BsonDocument, request_id: i32, response_to: i32, flags: OpMsgFlags) !void {
    var message = OpcodeMsg{
        .header = MsgHeader{
            .message_length = 0,
            .request_id = request_id,
            .response_to = response_to,
            .op_code = OP_CODES.OP_MSG,
        },
        .flag_bits = flags.getFlagBits(),
        .section_document = .{ .document = command },
    };
    message.header.message_length = calculateMessageLength(&message);

    return writeMessage(writer, &message);
}

pub fn writeMessage(writer: anytype, message: *const OpcodeMsg) !void {
    std.debug.print("writeMessage, length: {d}\n", .{message.header.message_length});
    try writer.writeInt(i32, message.header.message_length, .little);
    try writer.writeInt(i32, message.header.request_id, .little);
    try writer.writeInt(i32, message.header.response_to, .little);
    try writer.writeInt(i32, @intFromEnum(message.header.op_code), .little);
    try writer.writeInt(u32, message.flag_bits, .little);

    try writer.writeInt(u8, DocumentSection.payload_type, .little);
    try writer.writeAll(message.section_document.document.raw_data);
    std.debug.print("writeMessage: done\n", .{});
    if (message.section_sequence) |seq| {
        _ = seq;
        @panic("not implemented");
        // try writer.writeInt(u8, SequenceSection.payload_type, .little);

        // try writer.writeInt(i32, seq.size, .little);
        // try writer.writeAll(seq.identifier[0..]);

        // for (seq.documents) |doc| {
        //     // try writer.writeInt(i32, @as(i32, @intCast(doc.len)), .little);
        //     try writer.writeAll(doc.raw_data);
        // }
    }
}

pub fn readMessage(allocator: Allocator, reader: anytype) !*OpcodeMsg {
    var message = try allocator.create(OpcodeMsg);
    message.* = OpcodeMsg{
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
    std.debug.assert(section_payload_type == DocumentSection.payload_type);
    message.section_document.document = try BsonDocument.readDocument(allocator, reader);

    reader_pos += @sizeOf(u8) + @as(i32, @intCast(message.section_document.document.len));

    if (reader_pos == expected_message_len) {
        std.debug.print("readMessage: done\n", .{});
        return message;
    }
    std.debug.print("reader pos: {d}/{d}\n", .{ reader_pos, expected_message_len });
    @panic("not implemented");
    //  if (section_payload_type == SequenceSection.payload_type) {
    //     message.section_sequence = try readSequence(reader);
    // }

    // std.debug.print("message.section_document.document.len: {}\n", .{message.section_document.document.len});
    // return message;
}

fn calculateMessageLength(message: *const OpcodeMsg) i32 {
    const msg_header_size: comptime_int = @sizeOf(MsgHeader);
    const section_payload_type_size: comptime_int = @sizeOf(u8);
    const flags_size: comptime_int = @sizeOf(u32);

    var len: i32 = msg_header_size;
    len += flags_size;
    len += section_payload_type_size;
    len += @as(i32, @intCast(message.section_document.document.len));

    if (message.section_sequence) |seq| {
        len += seq.size;
    }

    return len;
}

pub const OP_CODES = enum(i32) {
    OP_MSG = 2013,
};

pub const OpcodeMsg = struct {
    header: MsgHeader,
    flag_bits: u32,
    section_document: DocumentSection,
    section_sequence: ?SequenceSection = null,
    checksum: ?u32 = null, // Optional checksum field

    pub fn deinit(self: *OpcodeMsg, allocator: Allocator) void {
        self.section_document.document.deinit(allocator);
        if (self.section_sequence) |seq| {
            for (seq.documents) |doc| {
                doc.deinit(allocator);
            }
        }
        allocator.destroy(self);
    }
};

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
};

pub const MsgHeader = struct {
    message_length: i32,
    request_id: i32,
    response_to: i32,
    op_code: OP_CODES = OP_CODES.OP_MSG, // OP_MSG = 2013 (https://github.com/mongodb/specifications/blob/master/source/op-msg/op-msg.md)
};

const DocumentSection = struct {
    const payload_type = 0;
    document: *BsonDocument,
};

const SequenceSection = struct {
    const payload_type = 1;

    size: i32,
    identifier: []const u8,
    documents: []*BsonDocument,
};
