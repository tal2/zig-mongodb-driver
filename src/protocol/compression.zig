const std = @import("std");
const opcode = @import("opcode.zig");

const Allocator = std.mem.Allocator;
const MsgHeader = opcode.MsgHeader;
const OP_CODES = opcode.OP_CODES;

pub const OpcodeCompressed = struct {
    header: MsgHeader,
    original_opcode: OP_CODES,
    uncompressed_size: i32,

    /// The ID of the compressor that compressed the message.
    compressor_id: CompressorId,
    /// The opcode itself, excluding the MsgHeader.
    compressed_message: []const u8,

    pub fn deinit(self: *const OpcodeCompressed, allocator: Allocator) void {
        allocator.free(self.compressed_message);
        allocator.destroy(self);
    }
};

// pub fn makeCompressedOpcode(allocator: Allocator, command: *BsonDocument, op_code: OP_CODES, request_id: i32, response_to: i32, flags: OpMsgFlags) Allocator.Error!*OpMsg {

//     // struct {
//     //     MsgHeader header;            // standard message header
//     //     int32  originalOpcode;       // value of wrapped opcode
//     //     int32  uncompressedSize;     // size of deflated compressedMessage, excluding MsgHeader
//     //     uint8  compressorId;         // ID of compressor that compressed message
//     //     char    *compressedMessage;  // opcode itself, excluding MsgHeader
//     // }
//     var message = try allocator.create(OpMsg);
//     message.* = OpMsg{
//         .header = MsgHeader{
//             .message_length = 0,
//             .request_id = request_id,
//             .response_to = response_to,
//             .op_code = OP_CODES.OP_COMPRESSED,
//         },
//         .flag_bits = flags.getFlagBits(),
//         .section_document = .{ .document = command },
//     };
//     message.header.message_length = calculateMessageLength(message);
//     return message;
// }

pub const CompressorId = enum(u8) {
    noop = 0,
    snappy = 1,
    zlib = 2,
    zstd = 3,

    // _, // 4...255 reserved
};
