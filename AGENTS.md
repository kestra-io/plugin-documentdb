# Kestra DocumentDB Plugin

## What

- Provides plugin components under `io.kestra.plugin.documentdb`.
- Includes classes such as `Delete`, `Insert`, `Update`, `Read`.

## Why

- What user problem does this solve? Teams need to manage DocumentDB collections with MongoDB-compatible insert, query, update, and delete operations from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps DocumentDB steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on DocumentDB.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `documentdb`

Infrastructure dependencies (Docker Compose services):

- `default`
- `documentdb-api`
- `mongodb`
- `mongodb-data`

### Key Plugin Classes

- `io.kestra.plugin.documentdb.Delete`
- `io.kestra.plugin.documentdb.Insert`
- `io.kestra.plugin.documentdb.Read`
- `io.kestra.plugin.documentdb.Update`

### Project Structure

```
plugin-documentdb/
├── src/main/java/io/kestra/plugin/documentdb/models/
├── src/test/java/io/kestra/plugin/documentdb/models/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
