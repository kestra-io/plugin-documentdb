# Kestra DocumentDB Plugin

## What

description = 'DocumentDB plugin for Kestra Exposes 4 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with DocumentDB, allowing orchestration of DocumentDB-based operations as part of data pipelines and automation workflows.

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

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
