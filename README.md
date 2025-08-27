# Mongodb Driver - written in Zig

## Overview

- A driver for interacting with mongodb servers written in Zig

## Status: ⚠️ work-in-progress

zig version: 0.15.1

### compatibility

- currently aiming for mongodb v7.x and v8.x

### connectivity

- [x] single server
- [ ] replicaset
- [ ] sharded
- [x] heartbeat
- [ ] server selection

### concurrency

- [x] single-thread
- [ ] multi-thread

### authentication

- [x] SCRAM-SHA-256
- [x] SCRAM-SHA-1
- [x] SASL Mechanisms
- [ ] MONGODB-X509
- [ ] PLAIN
- [ ] GSSAPI
- [ ] MONGODB-AWS
- [ ] MONGODB-OIDC

### basic commands

- [x] generic run command
- [x] insertOne
- [x] insertMany
- [x] findOne
- [x] find
- [x] updateOne
- [x] updateMany
- [x] deleteOne
- [x] deleteMany
- [x] replaceOne
- [x] countDocuments
- [x] estimatedDocumentCount
- [ ] distinct

### additional commands

- [ ] findAndModify
- [x] kill cursors
- [x] end sessions
- [ ] mapReduce
- [ ] parallelCollectionScan
- [ ] geoNear
- [ ] geoSearch

### advanced operations

- [x] aggregation pipeline
- [x] bulk write operations
- [x] sessions (implicit & explicit)
- [ ] transactions
- [ ] timeouts
- [ ] write concern

### compression support

- [x] no compression
- [ ] snappy
- [ ] zlib
- [ ] zstd

## Commands:

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

  const credentials = MongoCredential{
    .username = "...",
    .source = "...",
    .password = "...",
    .mechanism = .SCRAM_SHA_256,
    .mechanism_properties = null,
  };

  try db.connect(credentials);
```

### Insert Command

```zig
  const doc1 = .{
    .name = "doc1"
  };
  const insert_response = try db.collection(collection_name).insertOne(doc1, .{});
  switch (insert_response) {
    .response => |response| {
        defer response.deinit(allocator);
        // ...
    },
    .write_errors => |write_errors| {
        defer write_errors.deinit(allocator);
        // ...
    },
    .err => |err| {
        defer err.deinit(allocator);
        // ...
    },
  }
```

### Document Count Estimate

```zig
  const count_estimate = try db.collection(collection_name).estimatedDocumentCount(.{});
  switch (count_estimate) {
    .n => |n| {
        std.debug.print("estimated count: {d}\n", .{n});
    },
    .err => |err| {
        defer err.deinit(gpa);
        // ...
    },
  }
```

### Replace Document Command

```zig
  const filter = .{ .name = "obj1-original" };
  const replacement = .{ .name = "obj1-replaced", .value = 42, .replaced = true };
  const replace_response = try db.collection(collection_name).replaceOne(filter, replacement, .{});
  switch (replace_response) {
    .response => |response| {
        defer response.deinit(allocator);
        // ...
    },
    .write_errors => |write_errors| {
        defer write_errors.deinit(allocator);
        // ...
    },
    .err => |err| {
        defer err.deinit(allocator);
        // ...
    },
  }
```

### Update One Command

```zig
  const filter = .{ .name = "obj1" };
  const update = .{ .name = "obj1-updated", .updated = true };
  const update_response = try db.collection(collection_name).updateOne(filter, update, .{});
  switch (update_response) {
    .response => |response| {
        defer response.deinit(allocator);
        // ...
    },
    .write_errors => |write_errors| {
        defer write_errors.deinit(allocator);
        // ...
    },
    .err => |err| {
        defer err.deinit(allocator);
        // ...
    },
  }
```

### Update Many Command - simple

```zig
  const filter = .{ .status = "pending" };
  const update = .{ .@"$set" = .{ .status = "completed" } };
  const update_response = try db.collection(collection_name).updateMany(filter, update, .{ .upsert = true });
  switch (update_response) {
    .response => |response| {
        defer response.deinit(allocator);
        // ...
    },
    .write_errors => |write_errors| {
        defer write_errors.deinit(allocator);
        // ...
    },
    .err => |err| {
        defer err.deinit(allocator);
        // ...
    },
  }
```

### Update Many Command - chainable

```zig
  var update_chain = db.collection(collection_name).updateChain();
  defer update_chain.deinit();

  const update_chain_result = try update_chain
      .add(.{ .status = .{ .@"$eq" = "pending" } }, .{ .@"$set" = .{ .status = "started" } }, .{ .multi = true })
      .add(.{ .status = .{ .@"$eq" = "in-progress" } }, .{ .@"$set" = .{ .status = "completed" } }, .{ .multi = true })
      .exec(.{ .ordered = true });

  switch (update_chain_result) {
    .response => |response| {
        defer response.deinit(allocator);
        // ...
    },
    .write_errors => |write_errors| {
        defer write_errors.deinit(allocator);
        // ...
    },
    .err => |err| {
        defer err.deinit(allocator);
        // ...
    },
  }
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
  const delete_response = try db.collection(collection_name).deleteOne(delete_filter, .{});
  switch (delete_response) {
    .response => |response| {
        defer response.deinit(allocator);
        // ...
    },
    .write_errors => |write_errors| {
        defer write_errors.deinit(allocator);
        // ...
    },
    .err => |err| {
        defer err.deinit(allocator);
        // ...
    },
  }
```

#### delete all (by filter)

```zig
  const delete_filter = .{ .name = "doc1" };
  const delete_response = try db.collection(collection_name).deleteMany(delete_filter, .{});
  switch (delete_response) {
    .response => |response| {
        defer response.deinit(allocator);
        // ...
    },
    .write_errors => |write_errors| {
        defer write_errors.deinit(allocator);
        // ...
    },
    .err => |err| {
        defer err.deinit(allocator);
        // ...
    },
  }
```

### Find Command

#### Find Command - find many

```zig
  const filter = .{ .name = .{ .@"$ne" = null } };
  const response = try db.collection(collection_name).find(filter, .all, .{
   .batchSize = 2,
   .limit = 6,
  });
  switch (response) {
    .cursor => {
        var cursor = response.cursor;
        defer cursor.deinit();
        while (try cursor.next()) |batch| {
          for (batch) |doc| {
              defer doc.deinit(allocator);
              // do something with doc
          }
        }
        try cursor.release(); // optionally call to kill cursor, if iterator not used till the end
    },
    .err => |err| {
      defer err.deinit(allocator);
      // ...
    },
  }
```

#### Find Command - find one

```zig
  const filter = .{ .name = .{ .@"$eq" = "doc1" } };
  const response = try db.collection(collection_name).findOne(filter, .{});
  switch (result) {
    .document => |doc| {
        defer doc.deinit(allocator);
        // ...
    },
    .err => |err| {
        defer err.deinit(allocator);
        // ...
    },
    .null => {
        // ...
    },
  }
```

### Aggregate Command

```zig
  var pipeline_builder = PipelineBuilder.init(allocator);
  defer pipeline_builder.deinit();
  const pipeline = pipeline_builder
     .match(.{ .name = .{ .@"$ne" = null } })
     .sort(.{ .name = 1 })
     .group(.{ ._id = "$name", .count = .{ .@"$sum" = 1 } })
     // and more ...
     .build() catch |err| {
        std.debug.print("Error building pipeline: {}\n", .{err});
        return;
     };

  const response = try collection.aggregate(pipeline, .{}, .{});
  switch (response) {
    .cursor => {
        var cursor = response.cursor;
        defer cursor.deinit();
        // same as find command
    },
    .err => |err| {
          defer err.deinit(gpa);
          std.debug.print("Error releasing result: {}\n", .{err});
      },
  }
```
