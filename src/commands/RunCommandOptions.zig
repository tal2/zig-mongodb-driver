const ClientSession = @import("../../src/sessions/ClientSession.zig").ClientSession;
const ReadPreference = @import("../../src/commands/ReadPreference.zig").ReadPreference;

pub const RunCommandOptions = struct {
    readPreference: ?ReadPreference = null,
    session: ?*ClientSession = null,
    timeoutMS: ?i64 = null,

    pub fn addToCommand(self: *const RunCommandOptions, command: anytype) void {
        if (comptime @hasDecl(@TypeOf(command.*), "readPreference")) {
            if (self.readPreference) |read_preference| {
                command.readPreference = read_preference.toValue();
            }
        }
        if (comptime @hasDecl(@TypeOf(command.*), "timeoutMS")) {
            command.timeoutMS = self.timeoutMS;
        }
    }
};
