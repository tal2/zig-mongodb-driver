const std = @import("std");
const bson = @import("bson");

const Allocator = std.mem.Allocator;
const BsonDocument = bson.BsonDocument;

pub const PipelineBuilder = struct {
    pub const Pipeline = std.ArrayList(*bson.BsonDocument);

    allocator: Allocator,
    pipeline: Pipeline,
    error_at_stage_index: ?usize = null,

    pub fn init(allocator: Allocator) PipelineBuilder {
        return PipelineBuilder{
            .allocator = allocator,
            .pipeline = Pipeline.init(allocator),
            .error_at_stage_index = null,
        };
    }

    pub fn match(self: *PipelineBuilder, filter: anytype) *PipelineBuilder {
        return self.add(.{ .@"$match" = filter });
    }

    pub fn limit(self: *PipelineBuilder, num: i32) *PipelineBuilder {
        return self.add(.{ .@"$limit" = num });
    }

    pub fn skip(self: *PipelineBuilder, num: i32) *PipelineBuilder {
        return self.add(.{ .@"$skip" = num });
    }

    pub fn sort(self: *PipelineBuilder, sort_by: anytype) *PipelineBuilder {
        return self.add(.{ .@"$sort" = sort_by });
    }

    pub fn unwind(self: *PipelineBuilder, path: []const u8) *PipelineBuilder {
        return self.add(.{ .@"$unwind" = path });
    }

    pub fn group(self: *PipelineBuilder, group_by: anytype) *PipelineBuilder {
        return self.add(.{ .@"$group" = group_by });
    }

    pub fn project(self: *PipelineBuilder, project_doc: anytype) *PipelineBuilder {
        return self.add(.{ .@"$project" = project_doc });
    }

    pub fn lookupBasic(self: *PipelineBuilder, from: []const u8, local_field: []const u8, foreign_field: []const u8, as: []const u8) *PipelineBuilder {
        return self.add(.{ .@"$lookup" = .{
            .from = from,
            .localField = local_field,
            .foreignField = foreign_field,
            .as = as,
        } });
    }

    pub fn lookupWithPipeline(self: *PipelineBuilder, from: []const u8, let: anytype, pipeline: anytype, as: []const u8) *PipelineBuilder {
        return self.add(.{ .@"$lookup" = .{
            .from = from,
            .let = let,
            .pipeline = pipeline,
            .as = as,
        } });
    }

    pub fn lookup(self: *PipelineBuilder, lookup_doc: anytype) *PipelineBuilder {
        return self.add(.{ .@"$lookup" = lookup_doc });
    }

    pub fn merge(self: *PipelineBuilder, merge_doc: anytype) *PipelineBuilder {
        return self.add(.{ .@"$merge" = merge_doc });
    }

    pub fn unionWith(self: *PipelineBuilder, union_with_doc: anytype) *PipelineBuilder {
        return self.add(.{ .@"$unionWith" = union_with_doc });
    }

    pub fn out(self: *PipelineBuilder, collection_name: []const u8) *PipelineBuilder {
        return self.add(.{ .@"$out" = collection_name });
    }

    pub fn count(self: *PipelineBuilder, count_doc: anytype) *PipelineBuilder {
        return self.add(.{ .@"$count" = count_doc });
    }

    pub fn facet(self: *PipelineBuilder, facet_doc: anytype) *PipelineBuilder {
        return self.add(.{ .@"$facet" = facet_doc });
    }

    pub fn bucket(self: *PipelineBuilder, bucket_doc: anytype) *PipelineBuilder {
        return self.add(.{ .@"$bucket" = bucket_doc });
    }

    pub fn bucketAuto(self: *PipelineBuilder, bucket_auto_doc: anytype) *PipelineBuilder {
        return self.add(.{ .@"$bucketAuto" = bucket_auto_doc });
    }

    pub fn sample(self: *PipelineBuilder, sample_doc: anytype) *PipelineBuilder {
        return self.add(.{ .@"$sample" = sample_doc });
    }

    pub fn sortByCount(self: *PipelineBuilder, sort_by_count_doc: anytype) *PipelineBuilder {
        return self.add(.{ .@"$sortByCount" = sort_by_count_doc });
    }

    pub fn vectorSearch(self: *PipelineBuilder, vector_search_doc: anytype) *PipelineBuilder {
        return self.add(.{ .@"$vectorSearch" = vector_search_doc });
    }

    pub fn listSampledQueries(self: *PipelineBuilder, list_sampled_queries_doc: anytype) *PipelineBuilder {
        return self.add(.{ .@"$listSampledQueries" = list_sampled_queries_doc });
    }

    pub fn listSearchIndexes(self: *PipelineBuilder, list_search_indexes_doc: anytype) *PipelineBuilder {
        return self.add(.{ .@"$listSearchIndexes" = list_search_indexes_doc });
    }

    pub fn tumblingWindow(self: *PipelineBuilder, tumbling_window_doc: anytype) *PipelineBuilder {
        return self.add(.{ .@"$tumblingWindow" = tumbling_window_doc });
    }

    pub fn hoppingWindow(self: *PipelineBuilder, hopping_window_doc: anytype) *PipelineBuilder {
        return self.add(.{ .@"$hoppingWindow" = hopping_window_doc });
    }

    pub fn sessionWindow(self: *PipelineBuilder, session_window_doc: anytype) *PipelineBuilder {
        return self.add(.{ .@"$sessionWindow" = session_window_doc });
    }

    pub fn externalFunction(self: *PipelineBuilder, external_function_doc: anytype) *PipelineBuilder {
        return self.add(.{ .@"$externalFunction" = external_function_doc });
    }

    pub fn https(self: *PipelineBuilder, https_doc: anytype) *PipelineBuilder {
        return self.add(.{ .@"$https" = https_doc });
    }

    pub fn replaceRoot(self: *PipelineBuilder, replace_root_doc: anytype) *PipelineBuilder {
        return self.add(.{ .@"$replaceRoot" = replace_root_doc });
    }

    pub fn unset(self: *PipelineBuilder, unset_doc: anytype) *PipelineBuilder {
        return self.add(.{ .@"$unset" = unset_doc });
    }

    pub fn replaceWith(self: *PipelineBuilder, replace_with_doc: anytype) *PipelineBuilder {
        return self.add(.{ .@"$replaceWith" = replace_with_doc });
    }

    pub fn addFields(self: *PipelineBuilder, add_fields_doc: anytype) *PipelineBuilder {
        return self.add(.{ .@"$addFields" = add_fields_doc });
    }

    pub fn set(self: *PipelineBuilder, set_doc: anytype) *PipelineBuilder {
        return self.add(.{ .@"$set" = set_doc });
    }

    pub fn setOnInsert(self: *PipelineBuilder, set_on_insert_doc: anytype) *PipelineBuilder {
        return self.add(.{ .@"$setOnInsert" = set_on_insert_doc });
    }

    pub fn add(self: *PipelineBuilder, stage: anytype) *PipelineBuilder {
        if (self.error_at_stage_index != null) {
            return self;
        }

        const stage_parsed = BsonDocument.fromObject(self.allocator, @TypeOf(stage), stage) catch |err| {
            std.debug.print("Error parsing stage: {}\n", .{err});
            self.error_at_stage_index = self.pipeline.items.len;
            return self;
        };
        errdefer stage_parsed.deinit(self.allocator);

        self.pipeline.append(stage_parsed) catch |err| {
            std.debug.print("Error appending stage to pipeline: {}\n", .{err});
            self.error_at_stage_index = self.pipeline.items.len;
            return self;
        };
        return self;
    }

    pub fn build(self: *PipelineBuilder) !Pipeline.Slice {
        if (self.error_at_stage_index != null) {
            return error.InvalidPipeline;
        }

        return try self.pipeline.toOwnedSlice();
    }

    pub fn deinit(self: *const PipelineBuilder) void {
        for (self.pipeline.items) |item| {
            item.deinit(self.allocator);
        }
        self.pipeline.deinit();
        // self.allocator.destroy(self);
    }
};

pub const Stage = struct {
    pub const AddFieldsStage = struct {
        newField1: bson.BsonDocument,
        newField2: bson.BsonDocument,
    };

    pub const SetStage = struct {
        newField1: bson.BsonDocument,
        newField2: bson.BsonDocument,
    };

    pub const ProjectStage = struct {
        field1: union(enum) {
            include: bool,
            expression: bson.BsonDocument,
        },
        field2: union(enum) {
            include: bool,
            expression: bson.BsonDocument,
        },
    };

    pub const Tag = enum {
        addFields,
        set,
        project,
    };

    tag: Tag,
    data: union(Tag) {
        addFields: AddFieldsStage,
        set: SetStage,
        project: ProjectStage,
    },
};
