# Mongodb Driver - written in Zig

Examples:

- [Initialize and Connect](#initialize-and-connect)
- [Insert Command](#insert-command)
- [Replace Document Command](#replace-document-command)
- [Update One Command](#update-one-command)
- [Update Many Command - simple](#update-many-command---simple)
- [Update Many Command - chainable](#update-many-command---chainable)
- [Document Count Estimate](#document-count-estimate)
- [Document Count](#document-count)
- [Delete Command](#delete-command)
- [Find Command](#find-command)
- [Aggregate Command](#aggregate-command)

## Examples

### Initialize and Connect

```zig
  var conn_str = try ConnectionString.fromText(allocator, "mongodb://127.0.0.1/sandbox");
  defer conn_str.deinit(allocator);

  const server_api = ServerApi{
    .version = .v1,
    .strict = true,
    .deprecationErrors = true,
  };
  var db = try Database.init(allocator, &conn_str, server_api); // server api is optional, can be null
  defer db.deinit();

  try db.connect();
```

### Insert Command

```zig
  const doc1 = .{
    .name = "doc1"
  };
  const response = try db.collection(collection_name).insertOne(doc1, .{});
  defer response.deinit(allocator);
```

### Document Count Estimate

```zig
  const count_estimate = try db.collection(collection_name).estimatedDocumentCount(.{});

```

### Replace Document Command

```zig
  const filter = .{ .name = "obj1-original" };
  const replacement = .{ .name = "obj1-replaced", .value = 42, .replaced = true };
  const response = try collection.replaceOne(filter, replacement, .{});
  defer response.deinit(allocator);
```

### Update One Command

```zig
  const filter = .{ .name = "obj1" };
  const update = .{ .name = "obj1-updated", .updated = true };
  const response = try collection.updateOne(filter, update, .{});
  defer response.deinit(allocator);
```

### Update Many Command - simple

```zig
  const filter = .{ .status = "pending" };
  const update = .{ .@"$set" = .{ .status = "completed" } };
  const update_response = try collection.updateMany(filter, update, .{ .upsert = true });
  defer update_response.deinit(allocator);
```

### Update Many Command - chainable

```zig
  var update_chain = collection.updateChain();
  defer update_chain.deinit();

  const update_chain_result = try update_chain
      .add(.{ .status = .{ .@"$eq" = "pending" } }, .{ .@"$set" = .{ .status = "started" } }, .{ .multi = true })
      .add(.{ .status = .{ .@"$eq" = "in-progress" } }, .{ .@"$set" = .{ .status = "completed" } }, .{ .multi = true })
      .exec(.{ .ordered = true });
```

### Document Count

#### Document Count - count all

```zig
  const count = try db.collection(collection_name).countDocuments(.{});
```

#### Document Count - count with filter

```zig
  const filter = .{ .name = "doc1" }
  const count = try db.collection(collection_name).countDocuments(filter);
```

### Delete Command

#### delete one

```zig
  const delete_filter = .{ .name = "doc1" };
  const response = try db.collection(collection_name).delete(.one, delete_filter, .{});
  defer allocator.destroy(response);
```

#### delete all (by filter)

```zig
  const delete_filter = .{ .name = "doc1" };
  const response = try db.collection(collection_name).delete(.all, delete_filter, .{});
  defer allocator.destroy(response);
```

### Find Command

#### Find Command - find many

```zig
  const filter = .{ .name = .{ .@"$ne" = null } };
  var response = try db.collection(collection_name).find(filter, .all, .{
   .batchSize = 2,
   .limit = 6,
  });
  defer response.deinit();

  while (try response.next()) |batch| {
    for (batch) |doc| {
        defer doc.deinit(allocator);
        // do something with doc
    }
  }
  try response.release(); // optionally call to kill cursor, if not used iterator till the end
```

#### Find Command - find one

```zig
  const filter = .{ .name = .{ .@"$eq" = "doc1" } };
  const response = try db.collection(collection_name).findOne(filter, .{});
  defer if (response) |r| r.deinit(allocator);
```

### Aggregate Command

```zig
  var pipeline_builder = PipelineBuilder.init(allocator);
  defer pipeline_builder.deinit();
  const pipeline = pipeline_builder
     .match(.{ .name = .{ .@"$ne" = null } })
     .sort(.{ .name = 1 })
     .group(.{ ._id = "$name", .count = .{ .@"$sum" = 1 } })
     .build() catch |err| {
        std.debug.print("Error building pipeline: {}\n", .{err});
        return;
     };
  var response = try collection.aggregate(pipeline, .{}, .{});
  defer response.release() catch |err| {
    std.debug.print("Error releasing response: {}\n", .{err});
  };

  while (try result.next()) |batch| {
    for (batch) |doc| {
      defer doc.deinit(allocator);
      // do something with doc
    }
  }
```
