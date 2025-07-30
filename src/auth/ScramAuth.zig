const std = @import("std");
const crypto = std.crypto;
const Allocator = std.mem.Allocator;
const sasl = @import("../sasl/sasl.zig");
const Md5 = crypto.hash.Md5;
const saslPrep = sasl.saslPrep;
const pbkdf2 = crypto.pwhash.pbkdf2;

pub const ScramMechanism = enum {
    SCRAM_SHA_1,
    SCRAM_SHA_256,
};

const ConversationState = enum {
    Initial,
    ServerFirst,
    ClientFinal,
    ServerFinal,
    Complete,
};

pub const ScramAuthConversation = struct {
    // TODO: extract an implementation not coupled with mongodb specific requirements

    const MINIMUM_VALID_ITERATION_COUNT = 4096;

    allocator: std.mem.Allocator,
    state: ConversationState,
    conversation_id: ?i32,
    mechanism: ScramMechanism,
    username: []const u8,
    password: []const u8,
    client_nonce: []const u8,
    server_nonce: ?[]const u8,
    salt: ?[]const u8,
    iteration_count: ?u32,
    auth_message: ?[]const u8,
    server_signature: ?[]const u8,
    done: bool,
    client_first_message_bare: ?[]const u8,
    server_first_message_bytes: ?[]const u8,

    pub fn init(allocator: std.mem.Allocator, mechanism: ScramMechanism, username: []const u8, password: []const u8) !ScramAuthConversation {
        const client_nonce = try generateClientNonce(allocator);

        return ScramAuthConversation{
            .allocator = allocator,
            .state = .Initial,
            .conversation_id = null,
            .mechanism = mechanism,
            .username = try allocator.dupe(u8, username),
            .password = try allocator.dupe(u8, password),
            .client_nonce = client_nonce,
            .server_nonce = null,
            .salt = null,
            .iteration_count = null,
            .auth_message = null,
            .server_signature = null,
            .client_first_message_bare = null,
            .server_first_message_bytes = null,
            .done = false,
        };
    }

    pub fn deinit(self: *ScramAuthConversation) void {
        self.allocator.free(self.client_nonce);
        if (self.server_nonce) |server_nonce| {
            self.allocator.free(server_nonce);
        }
        if (self.salt) |salt| {
            self.allocator.free(salt);
        }
        if (self.auth_message) |auth_message| {
            self.allocator.free(auth_message);
        }
        if (self.server_signature) |server_signature| {
            self.allocator.free(server_signature);
        }
        if (self.client_first_message_bare) |client_first_message_bare| {
            self.allocator.free(client_first_message_bare);
        }
        if (self.server_first_message_bytes) |server_first_message| {
            self.allocator.free(server_first_message);
        }
        self.allocator.free(self.username);
        self.allocator.free(self.password);
    }

    pub fn next(self: *ScramAuthConversation) !?[]const u8 {
        const payload = payload: switch (self.state) {
            .Initial => {
                self.state = .ServerFirst;
                const client_first_message = try self.createClientFirstMessage();
                self.client_first_message_bare = try self.allocator.dupe(u8, client_first_message["n,,".len..]);
                break :payload client_first_message;
            },
            .ClientFinal => {
                self.state = .ServerFinal;
                const client_final_message = try self.createClientFinalMessage();
                break :payload client_final_message;
            },
            else => return error.InvalidState,
        };
        return payload;
    }

    pub fn handleResponse(self: *ScramAuthConversation, response: []const u8) !void {
        switch (self.state) {
            .ServerFirst => {
                self.server_first_message_bytes = try self.allocator.dupe(u8, response);

                const server_first_message = try self.parseServerFirstMessage(self.server_first_message_bytes.?);

                self.server_nonce = server_first_message.server_nonce;
                self.salt = server_first_message.salt;
                self.iteration_count = server_first_message.iteration_count;
                self.state = .ClientFinal;
            },
            .ServerFinal => {
                const server_final_message = try self.parseServerFinalMessage(response);
                self.server_signature = server_final_message.server_signature;
                self.state = .Complete;
            },
            else => return error.InvalidState,
        }
    }

    fn parseServerFirstMessage(self: *ScramAuthConversation, message: []const u8) ScramError!ServerFirstMessage {
        var server_nonce: ?[]const u8 = null;
        var salt: ?[]const u8 = null;
        var iteration_count: ?u32 = null;

        var iter = std.mem.splitScalar(u8, message, ',');
        while (iter.next()) |part| {
            if (part.len < 2) continue;

            const key = part[0];
            const value = part[2..];

            switch (key) {
                'r' => server_nonce = value,
                's' => salt = value,
                'i' => {
                    iteration_count = std.fmt.parseInt(u32, value, 10) catch return error.InvalidIterationCount;
                },
                'm' => return error.InvalidServerFirstMessage,
                else => {},
            }
        }

        if (server_nonce == null or salt == null or iteration_count == null) {
            return error.InvalidServerFirstMessage;
        }

        if (iteration_count.? < MINIMUM_VALID_ITERATION_COUNT) {
            return error.IterationCountTooLow;
        }

        if (!std.mem.startsWith(u8, server_nonce.?, self.client_nonce)) {
            return error.ServerNonceMismatch;
        }

        const decoded_salt = base64Decode(self.allocator, salt.?) catch return error.InvalidSalt;

        return .{
            .server_nonce = try self.allocator.dupe(u8, server_nonce.?),
            .salt = decoded_salt,
            .iteration_count = iteration_count.?,
        };
    }

    fn parseServerFinalMessage(self: *ScramAuthConversation, message: []const u8) ScramError!ServerFinalMessage {
        if (message.len < 2 or message[0] != 'v') {
            return error.InvalidServerFinalMessage;
        }

        var server_signature: ?[]const u8 = null;

        var iter = std.mem.splitScalar(u8, message, ',');
        while (iter.next()) |part| {
            if (part.len < 2) continue;

            const key = part[0];
            const value = part[2..];

            switch (key) {
                'v' => {
                    server_signature = try self.allocator.dupe(u8, value);
                },
                else => return error.InvalidServerFinalMessage,
            }
        }
        if (server_signature == null) {
            return error.InvalidServerFinalMessage;
        }
        return .{
            .server_signature = server_signature.?,
        };
    }

    fn createClientFinalMessage(self: *ScramAuthConversation) ![]u8 {
        var message = std.ArrayList(u8).init(self.allocator);
        defer message.deinit();
        const writer = message.writer();

        const channel_binding_with_cbind_input_base64 = "c=biws"; // no authzid, mongodb does not support channel binding
        try message.appendSlice(channel_binding_with_cbind_input_base64);
        try message.appendSlice(",r=");
        try message.appendSlice(self.server_nonce.?);

        const client_final_message_without_proof = message.items.ptr[0..message.items.len];
        const auth_message = try self.createAuthMessage(client_final_message_without_proof);
        defer self.allocator.free(auth_message);

        const salted_password = try self.generateSaltedPassword();
        defer self.allocator.free(salted_password);

        const client_key = try self.generateClientKey(salted_password);
        defer self.allocator.free(client_key);

        const stored_key = try self.generateStoredKey(client_key);
        defer self.allocator.free(stored_key);

        const client_signature = try self.generateClientSignature(stored_key, auth_message);
        defer self.allocator.free(client_signature);

        const client_proof = try self.generateClientProof(client_key, client_signature);
        defer self.allocator.free(client_proof);

        try message.appendSlice(",p=");
        try std.base64.standard.Encoder.encodeWriter(writer, client_proof);

        return try message.toOwnedSlice();
    }

    fn createAuthMessage(self: *ScramAuthConversation, client_final_message_without_proof: []const u8) ScramError![]u8 {
        const capacity = self.client_first_message_bare.?.len + self.server_first_message_bytes.?.len + client_final_message_without_proof.len + 2;

        const buffer = try self.allocator.alloc(u8, capacity);
        var writer = std.io.fixedBufferStream(buffer);

        _ = try writer.write(self.client_first_message_bare.?);
        _ = try writer.write(",");
        _ = try writer.write(self.server_first_message_bytes.?);
        _ = try writer.write(",");
        _ = try writer.write(client_final_message_without_proof);

        return buffer;
    }

    fn generateMongoHashedPassword(
        self: *ScramAuthConversation,
    ) ScramError![]const u8 {
        const capacity = self.username.len + ":mongo:".len + self.password.len;
        const hash_input = try self.allocator.alloc(u8, capacity);
        defer self.allocator.free(hash_input);

        var writer = std.io.fixedBufferStream(hash_input);

        _ = try writer.write(self.username);
        _ = try writer.write(":mongo:");
        _ = try writer.write(self.password);

        var digest: [16]u8 = undefined;
        Md5.hash(hash_input, &digest, .{});

        var hex_digest = std.fmt.bytesToHex(digest, .lower);
        return hex_digest[0..];
    }

    fn generateSaltedPassword(self: *ScramAuthConversation) ScramError![]const u8 {
        switch (self.mechanism) {
            .SCRAM_SHA_1 => {
                const mongo_hashed_password = try self.generateMongoHashedPassword();
                const mongo_hashed_password_normalized = try saslPrep(self.allocator, mongo_hashed_password);
                defer self.allocator.free(mongo_hashed_password_normalized);
                const sha_1_output_size = 20;
                const salted_password = try self.allocator.alloc(u8, sha_1_output_size);
                pbkdf2(salted_password, mongo_hashed_password_normalized, self.salt.?, self.iteration_count.?, crypto.auth.hmac.HmacSha1) catch {
                    return ScramError.PBKDF2Error;
                };
                return salted_password;
            },
            .SCRAM_SHA_256 => {
                const password_sasl_prepped = try saslPrep(self.allocator, self.password);
                defer self.allocator.free(password_sasl_prepped);
                const sha_256_output_size = 32;
                const salted_password = try self.allocator.alloc(u8, sha_256_output_size);
                pbkdf2(salted_password, password_sasl_prepped, self.salt.?, self.iteration_count.?, crypto.auth.hmac.sha2.HmacSha256) catch {
                    return ScramError.PBKDF2Error;
                };
                return salted_password;
            },
        }
    }

    fn generateClientNonce(allocator: std.mem.Allocator) ![]const u8 {
        const nonce = try allocator.alloc(u8, 24);
        var prng = std.Random.DefaultPrng.init(@intCast(std.time.milliTimestamp())); //TODO: use a better PRNG
        const random = prng.random();

        // Generate base64-encoded random string
        const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/";
        for (nonce) |*byte| {
            byte.* = charset[random.intRangeAtMost(u8, 0, charset.len - 1)];
        }

        return nonce;
    }

    fn createClientFirstMessage(
        self: *ScramAuthConversation,
    ) (Allocator.Error || WriteError)![]const u8 {
        const capacity = 5 + self.username.len + 3 + self.client_nonce.len;
        const buffer = try self.allocator.alloc(u8, capacity);
        var writer = std.io.fixedBufferStream(buffer);

        _ = try writer.write("n,,n=");
        _ = try writer.write(self.username);
        _ = try writer.write(",r=");
        _ = try writer.write(self.client_nonce);

        return buffer;
    }

    fn generateClientKey(
        self: *ScramAuthConversation,
        salted_password: []const u8,
    ) Allocator.Error![]const u8 {
        const key = "Client Key";

        switch (self.mechanism) {
            .SCRAM_SHA_1 => {
                const result = try self.allocator.alloc(u8, 20);
                var hmac = crypto.auth.hmac.HmacSha1.init(salted_password);
                hmac.update(key);
                hmac.final(result[0..20]);
                return result;
            },
            .SCRAM_SHA_256 => {
                const result = try self.allocator.alloc(u8, 32);
                var hmac = crypto.auth.hmac.sha2.HmacSha256.init(salted_password);
                hmac.update(key);
                hmac.final(result[0..32]);

                return result;
            },
        }
    }

    fn generateStoredKey(
        self: *ScramAuthConversation,
        client_key: []const u8,
    ) Allocator.Error![]const u8 {
        switch (self.mechanism) {
            .SCRAM_SHA_1 => {
                const result = try self.allocator.alloc(u8, 20);
                var hash = crypto.hash.Sha1.init(.{});
                hash.update(client_key);
                hash.final(result[0..20]);
                return result;
            },
            .SCRAM_SHA_256 => {
                const result = try self.allocator.alloc(u8, 32);
                var hash = crypto.hash.sha2.Sha256.init(.{});
                hash.update(client_key);
                hash.final(result[0..32]);
                return result;
            },
        }
    }

    fn generateClientSignature(
        self: *ScramAuthConversation,
        stored_key: []const u8,
        auth_message: []const u8,
    ) Allocator.Error![]const u8 {
        switch (self.mechanism) {
            .SCRAM_SHA_1 => {
                const result = try self.allocator.alloc(u8, 20);
                var hmac = crypto.auth.hmac.HmacSha1.init(stored_key);
                hmac.update(auth_message);
                hmac.final(result[0..20]);
                return result;
            },
            .SCRAM_SHA_256 => {
                const result = try self.allocator.alloc(u8, 32);
                var hmac = crypto.auth.hmac.sha2.HmacSha256.init(stored_key);
                hmac.update(auth_message);
                hmac.final(result[0..32]);
                return result;
            },
        }
    }

    fn generateClientProof(
        self: *ScramAuthConversation,
        client_key: []const u8,
        client_signature: []const u8,
    ) Allocator.Error![]const u8 {
        const result = try self.allocator.alloc(u8, client_key.len);

        for (client_key, 0..) |key_byte, i| {
            result[i] = key_byte ^ client_signature[i];
        }

        return result;
    }
};

pub const WriteError = error{NoSpaceLeft}; // FixedBufferStream.WriteError

pub const ScramError = error{
    InvalidServerFirstMessage,
    InvalidServerFinalMessage,
    IterationCountTooLow,
    InvalidClientNonce,
    InvalidServerNonce,
    InvalidSalt,
    InvalidIterationCount,
    InvalidServerSignature,
    ServerNonceMismatch,
    PBKDF2Error,
} || std.mem.Allocator.Error || sasl.SaslError || WriteError;

/// Caller owns the returned slice
fn base64Decode(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    const decoded_len = try std.base64.standard.Decoder.calcSizeForSlice(input);
    const decoded = try allocator.alloc(u8, decoded_len);
    _ = try std.base64.standard.Decoder.decode(decoded, input);
    return decoded;
}

const ServerFirstMessage = struct {
    server_nonce: []const u8,
    salt: []const u8,
    iteration_count: u32,
};

const ServerFinalMessage = struct {
    server_signature: []const u8,
};
