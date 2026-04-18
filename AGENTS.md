# Kestra DocumentDB Plugin

## What

- Provides plugin components under `io.kestra.plugin.documentdb`.
- Includes classes such as `Delete`, `Insert`, `Update`, `Read`.

## Why

- This plugin integrates Kestra with DocumentDB.
- It provides tasks that manage DocumentDB collections with MongoDB-compatible insert, query, update, and delete operations.

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
