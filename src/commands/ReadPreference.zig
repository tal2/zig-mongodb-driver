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
