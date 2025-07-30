const std = @import("std");

const scram_auth = @import("ScramAuth.zig");
const ScramAuthConversation = scram_auth.ScramAuthConversation;

pub const MongoCredential = struct {
    /// Username for authentication
    /// Applies to all mechanisms
    /// Optional for MONGODB-X509, MONGODB-AWS, and MONGODB-OIDC
    username: ?[]const u8,

    /// Source database for authentication
    /// Applies to all mechanisms
    /// Always '$external' for GSSAPI and MONGODB-X509
    /// This is the database to which the authenticate command will be sent
    source: ?[]const u8,

    /// Password for authentication
    /// Does not apply to all mechanisms
    password: ?[]const u8,

    mechanism: AuthMechanism,

    mechanism_properties: ?MechanismProperties,

    pub const AuthMechanism = enum {
        MONGODB_CR,
        MONGODB_X509,
        GSSAPI,
        PLAIN,
        SCRAM_SHA_1,
        SCRAM_SHA_256,
        MONGODB_AWS,
        MONGODB_OIDC,

        pub fn fromString(str: []const u8) ?AuthMechanism {
            return std.meta.stringToEnum(AuthMechanism, str);
        }

        pub fn toString(self: AuthMechanism) []const u8 {
            return switch (self) {
                .MONGODB_CR => "MONGODB-CR",
                .MONGODB_X509 => "MONGODB-X509",
                .GSSAPI => "GSSAPI",
                .PLAIN => "PLAIN",
                .SCRAM_SHA_1 => "SCRAM-SHA-1",
                .SCRAM_SHA_256 => "SCRAM-SHA-256",
                .MONGODB_AWS => "MONGODB-AWS",
                .MONGODB_OIDC => "MONGODB-OIDC",
            };
        }
    };

    pub const MechanismProperties = union(enum) {
        gssapi: GssapiProperties,
        mongodb_aws: AwsProperties,
        mongodb_oidc: OidcProperties,

        pub const GssapiProperties = struct {
            /// Service name for GSSAPI authentication
            /// Default is "mongodb"
            service_name: ?[]const u8 = null,

            /// Hostname canonicalization mode
            /// Valid values: "none", "forward", "forwardAndReverse"
            /// Default is "none"
            canonicalize_host_name: ?[]const u8 = null,

            /// Service realm for cross-realm authentication
            service_realm: ?[]const u8 = null,

            /// Service host for the service principal name
            service_host: ?[]const u8 = null,
        };

        pub const AwsProperties = struct {
            /// AWS session token for temporary credentials
            session_token: ?[]const u8 = null,

            /// Custom AWS credential provider
            credential_provider: ?[]const u8 = null,
        };

        pub const OidcProperties = struct {
            /// Built-in OIDC environment integration
            /// Valid values: "test", "azure", "gcp", "k8s"
            environment: ?[]const u8 = null,

            /// Token resource URI for Azure/GCP
            token_resource: ?[]const u8 = null,

            /// OIDC callback for machine authentication
            oidc_callback: ?[]const u8 = null,

            /// OIDC human callback for human authentication flow
            oidc_human_callback: ?[]const u8 = null,

            /// Allowed hosts for OIDC authentication
            allowed_hosts: ?[]const []const u8 = null,

            /// AWS credential provider for OIDC
            aws_credential_provider: ?[]const u8 = null,
        };
    };

    pub fn init() MongoCredential {
        return MongoCredential{
            .username = null,
            .source = "admin",
            .password = null,
            .mechanism = .SCRAM_SHA_256,
            .mechanism_properties = null,
        };
    }

    pub fn withPassword(username: []const u8, password: []const u8, source: ?[]const u8) MongoCredential {
        return MongoCredential{
            .username = username,
            .source = source orelse "admin",
            .password = password,
            .mechanism = .SCRAM_SHA_256,
            .mechanism_properties = null,
        };
    }

    pub fn withPlain(username: []const u8, password: []const u8, source: ?[]const u8) MongoCredential {
        return MongoCredential{
            .username = username,
            .source = source orelse "admin",
            .password = password,
            .mechanism = .PLAIN,
            .mechanism_properties = null,
        };
    }

    pub fn forX509(source: ?[]const u8) MongoCredential {
        return MongoCredential{
            .username = null,
            .source = source orelse "$external",
            .password = null,
            .mechanism = .MONGODB_X509,
            .mechanism_properties = null,
        };
    }

    pub fn forGssapi(username: []const u8, password: ?[]const u8, properties: ?MechanismProperties.GssapiProperties) MongoCredential {
        return MongoCredential{
            .username = username,
            .source = "$external",
            .password = password,
            .mechanism = .GSSAPI,
            .mechanism_properties = if (properties) |props|
                MechanismProperties{ .gssapi = props }
            else
                null,
        };
    }

    pub fn forAws(username: ?[]const u8, password: ?[]const u8, properties: ?MechanismProperties.AwsProperties) MongoCredential {
        return MongoCredential{
            .username = username,
            .source = "$external",
            .password = password,
            .mechanism = .MONGODB_AWS,
            .mechanism_properties = if (properties) |props|
                MechanismProperties{ .mongodb_aws = props }
            else
                null,
        };
    }

    pub fn forOidc(username: ?[]const u8, properties: ?MechanismProperties.OidcProperties) MongoCredential {
        return MongoCredential{
            .username = username,
            .source = "$external",
            .password = null,
            .mechanism = .MONGODB_OIDC,
            .mechanism_properties = if (properties) |props|
                MechanismProperties{ .mongodb_oidc = props }
            else
                null,
        };
    }

    pub fn toAuthConversation(self: MongoCredential, allocator: std.mem.Allocator) !AuthConversation {
        return switch (self.mechanism) {
            .SCRAM_SHA_1 => {
                const mechanism = scram_auth.ScramMechanism.SCRAM_SHA_1;
                const conversation = try allocator.create(ScramAuthConversation);
                conversation.* = try ScramAuthConversation.init(allocator, mechanism, self.username.?, self.password.?);
                return AuthConversation{ .ScramAuthConversation = conversation };
            },
            .SCRAM_SHA_256 => {
                const mechanism = scram_auth.ScramMechanism.SCRAM_SHA_256;
                const conversation = try allocator.create(ScramAuthConversation);
                conversation.* = try ScramAuthConversation.init(allocator, mechanism, self.username.?, self.password.?);
                return AuthConversation{ .ScramAuthConversation = conversation };
            },
            else => {
                @panic("not implemented");
            },
        };
    }

    pub fn validate(self: MongoCredential) !void {
        if (self.source != null and self.source.?.len == 0) {
            return error.InvalidSource;
        }
        switch (self.mechanism) {
            .MONGODB_CR => {
                if (self.username == null or self.username.?.len == 0) {
                    return error.MissingUsername;
                }
                if (self.password == null) {
                    return error.MissingPassword;
                }
                if (self.mechanism_properties != null) {
                    return error.InvalidMechanismProperties;
                }
            },
            .MONGODB_X509 => {
                if (self.password != null) {
                    return error.PasswordNotAllowed;
                }
                if (self.mechanism_properties != null) {
                    return error.InvalidMechanismProperties;
                }
            },
            .GSSAPI => {
                if (self.username == null or self.username.?.len == 0) {
                    return error.MissingUsername;
                }
                if (self.source == null or !std.mem.eql(u8, self.source.?, "$external")) {
                    return error.InvalidSource;
                }
            },
            .PLAIN => {
                if (self.username == null or self.username.?.len == 0) {
                    return error.MissingUsername;
                }
                if (self.password == null) {
                    return error.MissingPassword;
                }
                if (self.mechanism_properties != null) {
                    return error.InvalidMechanismProperties;
                }
            },
            .SCRAM_SHA_1, .SCRAM_SHA_256 => {
                if (self.username == null or self.username.?.len == 0) {
                    return error.MissingUsername;
                }
                if (self.password == null or self.password.?.len == 0) {
                    return error.MissingPassword;
                }
                if (self.mechanism_properties != null) {
                    return error.InvalidMechanismProperties;
                }
            },
            .MONGODB_AWS => {
                if (self.source == null or !std.mem.eql(u8, self.source.?, "$external")) {
                    return error.InvalidSource;
                }
                // AWS allows username/password to be optional (can use environment variables)
            },
            .MONGODB_OIDC => {
                if (self.source == null or !std.mem.eql(u8, self.source.?, "$external")) {
                    return error.InvalidSource;
                }
                if (self.password != null) {
                    return error.PasswordNotAllowed;
                }
            },
        }
    }
};

pub const CredentialError = error{
    MissingUsername,
    MissingPassword,
    PasswordNotAllowed,
    InvalidSource,
    InvalidMechanismProperties,
};

pub const AuthConversation = union(enum) {
    ScramAuthConversation: *ScramAuthConversation,

    pub fn next(self: AuthConversation) anyerror!?[]const u8 {
        switch (self) {
            .ScramAuthConversation => |conversation| {
                return conversation.next();
            },
        }
    }

    pub fn handleResponse(self: AuthConversation, response: []const u8) anyerror!void {
        switch (self) {
            .ScramAuthConversation => |conversation| {
                return conversation.handleResponse(response);
            },
        }
    }

    pub fn deinit(self: AuthConversation) void {
        switch (self) {
            .ScramAuthConversation => |conversation| {
                conversation.deinit();
            },
        }
    }
};
