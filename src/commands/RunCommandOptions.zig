pub const RunCommandOptions = struct {
    readPreference: ?ReadPreference = .primary,
    timeoutMS: ?i64 = null,

    pub inline fn addToCommand(self: *const RunCommandOptions, command: anytype) void {
        if (self.readPreference) |read_preference| {
            command.readPreference = read_preference.toValue();
        }
        command.timeoutMS = self.timeoutMS;
    }
};

pub const ReadPreference = enum {
    primary,
    primary_preferred,
    secondary,
    secondary_preferred,

    pub fn toValue(self: ReadPreference) []const u8 {
        return switch (self) {
            .primary => "primary",
            .primary_preferred => "primaryPreferred",
            .secondary => "secondary",
            .secondary_preferred => "secondaryPreferred",
        };
    }
};
