const bson = @import("bson");

pub const Collation = struct {
    pub const null_ignored_field_names: bson.NullIgnoredFieldNames = bson.NullIgnoredFieldNames.all_optional_fields;

    locale: []const u8,

    caseLevel: ?bool = null,

    caseFirst: ?[]const u8 = null,

    strength: ?i32 = null,

    numericOrdering: ?bool = null,

    alternate: ?[]const u8 = null,

    maxVariable: ?[]const u8 = null,

    backwards: ?bool = null,
};
