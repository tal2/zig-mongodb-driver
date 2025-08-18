const std = @import("std");

const testing = std.testing;

pub const find_commands = @import("./FindCommand.zig");
pub const makeFindCommand = find_commands.makeFindCommand;
pub const FindCommandResponse = find_commands.FindCommandResponse;
pub const FindOptions = find_commands.FindOptions;

pub const find_one_commands = @import("./FindOneCommand.zig");
pub const makeFindOneCommand = find_one_commands.makeFindOneCommand;
pub const FindOneOptions = find_one_commands.FindOneOptions;

pub const DeleteCommand = @import("./DeleteCommand.zig");
pub const DeleteCommandResponse = DeleteCommand.DeleteCommandResponse;
pub const DeleteOptions = DeleteCommand.DeleteOptions;

pub const ReplaceCommand = @import("./ReplaceCommand.zig");
pub const ReplaceCommandResponse = ReplaceCommand.ReplaceCommandResponse;
pub const makeReplaceCommand = ReplaceCommand.makeReplaceCommand;
pub const ReplaceOptions = ReplaceCommand.ReplaceOptions;

pub const command_types = @import("./types.zig");
pub const RunCommandOptions = @import("./RunCommandOptions.zig");
pub const HelloCommand = @import("./HelloCommand.zig");
pub const makeHelloCommand = HelloCommand.makeHelloCommand;
pub const HelloCommandResponse = HelloCommand.HelloCommandResponse;
pub const makeHelloCommandForHandshake = HelloCommand.makeHelloCommandForHandshake;

pub const InsertCommand = @import("./InsertCommand.zig");
pub const InsertManyOptions = InsertCommand.InsertManyOptions;
pub const InsertOneOptions = InsertCommand.InsertOneOptions;
pub const InsertCommandResponse = InsertCommand.InsertCommandResponse;

pub const UpdateCommand = @import("./UpdateCommand.zig");
pub const UpdateOptions = UpdateCommand.UpdateOptions;
pub const UpdateCommandResponse = UpdateCommand.UpdateCommandResponse;
pub const makeUpdateOneCommand = UpdateCommand.makeUpdateOneCommand;
pub const UpdateStatementBuilder = UpdateCommand.UpdateStatementBuilder;
pub const makeUpdateManyCommand = UpdateCommand.makeUpdateManyCommand;
pub const makeUpdateCommand = UpdateCommand.makeUpdateCommand;
pub const UpdateCommandChainable = UpdateCommand.UpdateCommandChainable;

pub const count_commands = @import("./CountCommand.zig");
pub const makeCountCommand = count_commands.makeCountCommand;
pub const CountCommandResponse = count_commands.CountCommandResponse;

pub const estimated_document_count_commands = @import("./EstimatedDocumentCount.zig");
pub const makeEstimatedDocumentCount = estimated_document_count_commands.makeEstimatedDocumentCount;
pub const EstimatedDocumentCountOptions = estimated_document_count_commands.EstimatedDocumentCountOptions;

pub const get_more_commands = @import("./GetMoreCommand.zig");
pub const makeGetMoreCommand = get_more_commands.makeGetMoreCommand;

pub const kill_cursors_commands = @import("./KillCursorsCommand.zig");
pub const makeKillCursorsCommand = kill_cursors_commands.makeKillCursorsCommand;
pub const KillCursorsCommandResponse = kill_cursors_commands.KillCursorsCommandResponse;

pub const cursor_iterator = @import("./CursorIterator.zig");
pub const CursorIterator = cursor_iterator.CursorIterator;

pub const aggregate_commands = @import("./AggregateCommand.zig");
pub const makeAggregateCommand = aggregate_commands.makeAggregateCommand;
pub const AggregateCommand = aggregate_commands.AggregateCommand;
pub const AggregateOptions = aggregate_commands.AggregateOptions;
pub const CursorOptions = aggregate_commands.CursorOptions;
pub const AggregateCommandResponse = aggregate_commands.AggregateCommandResponse;
pub const PipelineBuilder = @import("../aggregation/pipeline.zig").PipelineBuilder;

pub const bulk_operations = @import("./bulk-operations.zig");
pub const BulkWriteOpsChainable = bulk_operations.BulkWriteOpsChainable;

pub const ErrorResponse = @import("./ErrorResponse.zig").ErrorResponse;
pub const WriteError = @import("./WriteError.zig").WriteError;
pub const ResponseWithWriteErrors = @import("./WriteError.zig").ResponseWithWriteErrors;

test {
    _ = @import("./types.zig");
    _ = @import("./InsertCommand.zig");
    _ = @import("./FindCommand.zig");
    _ = @import("./FindOneCommand.zig");
    _ = @import("./DeleteCommand.zig");
    _ = @import("./ReplaceCommand.zig");
    _ = @import("./UpdateCommand.zig");
    _ = @import("./RunCommandOptions.zig");
    _ = @import("./HelloCommand.zig");
    _ = @import("./InsertCommand.zig");
}
