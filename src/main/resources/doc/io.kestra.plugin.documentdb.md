# How to use the DocumentDB plugin

The plugin connects to Amazon DocumentDB and uses the MongoDB Java synchronous driver as its client library. It is a DocumentDB plugin; the driver provides the MongoDB-compatible wire protocol used to communicate with the database.

## Connection

Use `connectionString` and `database` to connect to Amazon DocumentDB. `connectionString` is the recommended connection configuration and should be stored in a Kestra secret. The old `host`, `username`, and `password` properties are deprecated and retained in task configuration. The MongoDB Java Driver does not use the old HTTP Basic-auth credentials, and an HTTPS API endpoint is not a MongoDB URI. Existing workflows that use the old HTTP endpoint must migrate to `connectionString`; preserving that HTTP transport would require a separate legacy implementation. A legacy `host` is accepted by the driver path only if it is already a `mongodb://` or `mongodb+srv://` URI. Supplying both `host` and `connectionString` is rejected as ambiguous. The legacy `collection` property is also deprecated but remains required and works unchanged; no replacement property is currently available.

Set `connectionString` to an Amazon DocumentDB connection URI consumed by the MongoDB Java Driver, for example `mongodb://<username>:<password>@<cluster-endpoint>:27017/?tls=true&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false`. Replace the placeholders with your cluster details; do not put credentials directly in workflow definitions. Store the URI in a Kestra secret and reference it with `{{ secret('DOCUMENTDB_CONNECTION_STRING') }}`. If the deprecated `username` or `password` property is also set, it is ignored and a warning is logged; move the credentials into the URI.

For Amazon DocumentDB engine versions earlier than 8.0.2, set `retryWrites=false` in the URI. Enable TLS with `tls=true` for Amazon DocumentDB clusters.

`caCertificate` is optional. Provide one or more PEM-encoded X.509 certificates to enable TLS with a trust store containing every certificate in the bundle. TLS options such as `tls=true` can also be configured in the DocumentDB connection URI.

## Tasks

### Read

`Read` accepts an optional MongoDB `filter`, `limit`, and `skip`, or an `aggregationPipeline`. A non-empty aggregation pipeline takes precedence over the find filter, limit, and skip. `fetchType` defaults to `FETCH`.

- `FETCH` returns all matching rows in `rows` and sets `size` to the number fetched.
- `FETCH_ONE` returns the first result in `row` and sets `size` to `0` or `1`. For ordinary find queries, it applies a server-side limit of one. For aggregation queries, it consumes only the first result without modifying the aggregation pipeline.
- `STORE` streams rows to an Ion file in Kestra internal storage, returns its `uri`, and sets `size` to the number stored.
- `NONE` runs the find or aggregate query, returns no rows or URI, and sets `size` to the number of query results. For find queries, this reflects the documents after the filter, skip, and limit; for aggregation, it reflects the rows emitted by the pipeline.

### Insert

`Insert` accepts exactly one of:

- `document`: one document map.
- `from`: structured data provided as a map, a list of maps, a JSON string, or a Kestra-supported URI (`kestra://`, `file://`, or `nsfile://`).
- `documents`: the legacy inline list-of-maps input.

`document` inserts one document. Batch input through `from` or `documents` is limited to 10 documents and is sent in one insert request; oversized inputs are rejected before any database write. URI sources are parsed incrementally as Ion and read only far enough to detect an input over the limit. Outputs contain `insertedId` (the first returned ID, or `null` when none), `insertedIds` (all acknowledged IDs), and `insertedCount` (the acknowledged count). Insert failures propagate without partial-success task output.

### Update

`Update` accepts an optional `filter`, a required `update` document (for example, `$set` or `$inc`), and `updateMany`, which defaults to `false`. The output contains `matchedCount`, `modifiedCount`, and `upsertedId`.

### Delete

`Delete` accepts an optional `filter` and `deleteMany`, which defaults to `false`. An empty filter with `deleteMany: true` affects every document in the collection. The output contains `deletedCount`.
