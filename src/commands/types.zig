pub const Limit = union(enum) {
    all: void,
    one: void,
};

pub const LimitNumbered = union(enum) {
    all: void,
    one: void,
    n: i64,
};

pub const Offset = i64;

pub const Sort = struct {
    field: []const u8,
    direction: enum { asc, desc },
};
