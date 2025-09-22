<p align="center">
  <a href="https://www.kestra.io">
    <img src="https://kestra.io/banner.png"  alt="Kestra workflow orchestrator" />
  </a>
</p>

<h1 align="center" style="border-bottom: none">
    Event-Driven Declarative Orchestrator
</h1>

<div align="center">
 <a href="https://github.com/kestra-io/kestra/releases"><img src="https://img.shields.io/github/tag-pre/kestra-io/kestra.svg?color=blueviolet" alt="Last Version" /></a>
  <a href="https://github.com/kestra-io/kestra/blob/develop/LICENSE"><img src="https://img.shields.io/github/license/kestra-io/kestra?color=blueviolet" alt="License" /></a>
  <a href="https://github.com/kestra-io/kestra/stargazers"><img src="https://img.shields.io/github/stars/kestra-io/kestra?color=blueviolet&logo=github" alt="Github star" /></a> <br>
<a href="https://kestra.io"><img src="https://img.shields.io/badge/Website-kestra.io-192A4E?color=blueviolet" alt="Kestra infinitely scalable orchestration and scheduling platform"></a>
<a href="https://kestra.io/slack"><img src="https://img.shields.io/badge/Slack-Join%20Community-blueviolet?logo=slack" alt="Slack"></a>
</div>

<br />

<p align="center">
    <a href="https://twitter.com/kestra_io"><img height="25" src="https://kestra.io/twitter.svg" alt="twitter" /></a> &nbsp;
    <a href="https://www.linkedin.com/company/kestra/"><img height="25" src="https://kestra.io/linkedin.svg" alt="linkedin" /></a> &nbsp;
<a href="https://www.youtube.com/@kestra-io"><img height="25" src="https://kestra.io/youtube.svg" alt="youtube" /></a> &nbsp;
</p>

<br />
<p align="center">
    <a href="https://go.kestra.io/video/product-overview" target="_blank">
        <img src="https://kestra.io/startvideo.png" alt="Get started in 4 minutes with Kestra" width="640px" />
    </a>
</p>
<p align="center" style="color:grey;"><i>Get started with Kestra in 4 minutes.</i></p>


# Kestra DocumentDB Plugin

> Integrate with DocumentDB - Microsoft's open-source, MongoDB-compatible document database

This plugin provides integration with [DocumentDB](https://documentdb.io), Microsoft's open-source document database built on PostgreSQL. DocumentDB is now part of the Linux Foundation and offers MongoDB compatibility with the reliability and ecosystem of PostgreSQL.

## Features

- **Document Operations**: Insert single or multiple documents with automatic ID generation
- **Advanced Querying**: Find documents with MongoDB-style filters and pagination
- **Aggregation Pipelines**: Execute complex aggregation operations for data analysis
- **MongoDB Compatibility**: Use familiar MongoDB query syntax and operators
- **Flexible Output**: Support for FETCH, FETCH_ONE, STORE, and NONE output types
- **PostgreSQL Backend**: Built on the reliability and performance of PostgreSQL
- **Open Source**: Fully MIT-licensed with no vendor lock-in

## Supported Operations

| Operation | Description | Required Parameters |
|-----------|-------------|-------------------|
| `Insert` | Insert single or multiple documents | `connectionString`, `database`, `collection`, `username`, `password`, `document` or `documents` |
| `Read` | Find documents with filtering and aggregation | `connectionString`, `database`, `collection`, `username`, `password`, optional: `filter`, `aggregationPipeline`, `limit`, `skip` |

![Kestra orchestrator](https://kestra.io/video.gif)

## Quick Start

### Basic Configuration

All tasks require these basic connection parameters:

```yaml
tasks:
  - id: documentdb_task
    type: io.kestra.plugin.documentdb.Insert
    connectionString: "https://my-documentdb-instance.com"  # DocumentDB HTTP endpoint
    database: "myapp"                                        # Database name
    collection: "users"                                      # Collection name
    username: "{{ secret('DOCUMENTDB_USERNAME') }}"         # Username
    password: "{{ secret('DOCUMENTDB_PASSWORD') }}"         # Password
```

### Example: Insert Single Document

```yaml
id: insert_user
namespace: company.documentdb

tasks:
  - id: create_user
    type: io.kestra.plugin.documentdb.Insert
    connectionString: "https://my-documentdb-instance.com"
    database: "myapp"
    collection: "users"
    username: "{{ secret('DOCUMENTDB_USERNAME') }}"
    password: "{{ secret('DOCUMENTDB_PASSWORD') }}"
    document:
      name: "John Doe"
      email: "john.doe@example.com"
      age: 30
      created_at: "{{ now() }}"
      roles: ["user", "editor"]
```

### Example: Insert Multiple Documents

```yaml
id: insert_products
namespace: company.documentdb

tasks:
  - id: create_products
    type: io.kestra.plugin.documentdb.Insert
    connectionString: "https://my-documentdb-instance.com"
    database: "inventory"
    collection: "products"
    username: "{{ secret('DOCUMENTDB_USERNAME') }}"
    password: "{{ secret('DOCUMENTDB_PASSWORD') }}"
    documents:
      - name: "Laptop"
        price: 999.99
        category: "Electronics"
        in_stock: true
      - name: "Mouse"
        price: 29.99
        category: "Electronics"
        in_stock: false
      - name: "Desk"
        price: 299.99
        category: "Furniture"
        in_stock: true
```

### Example: Find Documents with Filters

```yaml
id: find_active_users
namespace: company.documentdb

tasks:
  - id: query_users
    type: io.kestra.plugin.documentdb.Read
    connectionString: "https://my-documentdb-instance.com"
    database: "myapp"
    collection: "users"
    username: "{{ secret('DOCUMENTDB_USERNAME') }}"
    password: "{{ secret('DOCUMENTDB_PASSWORD') }}"
    filter:
      status: "active"
      age:
        $gte: 18
      roles:
        $in: ["editor", "admin"]
    limit: 100
    fetchType: FETCH
```

### Example: Aggregation Pipeline

```yaml
id: user_statistics
namespace: company.documentdb

tasks:
  - id: aggregate_users
    type: io.kestra.plugin.documentdb.Read
    connectionString: "https://my-documentdb-instance.com"
    database: "myapp"
    collection: "users"
    username: "{{ secret('DOCUMENTDB_USERNAME') }}"
    password: "{{ secret('DOCUMENTDB_PASSWORD') }}"
    aggregationPipeline:
      - $match:
          status: "active"
      - $group:
          _id: "$department"
          count: { $sum: 1 }
          avgAge: { $avg: "$age" }
      - $sort:
          count: -1
    fetchType: FETCH
```

### Example: Get Single Document

```yaml
id: get_user
namespace: company.documentdb

tasks:
  - id: find_user
    type: io.kestra.plugin.documentdb.Read
    connectionString: "https://my-documentdb-instance.com"
    database: "myapp"
    collection: "users"
    username: "{{ secret('DOCUMENTDB_USERNAME') }}"
    password: "{{ secret('DOCUMENTDB_PASSWORD') }}"
    filter:
      email: "john.doe@example.com"
    fetchType: FETCH_ONE
```

## Installation

Add this plugin to your Kestra instance:

```bash
./kestra plugins install io.kestra.plugin:plugin-documentdb:LATEST
```

## Development

### Prerequisites
- Java 21
- Docker

### Running tests
```bash
./gradlew check --parallel
```

### Local Development

**VSCode**: Follow the README.md within the `.devcontainer` folder for development setup.

**Other IDEs**:
```bash
./gradlew shadowJar && docker build -t kestra-documentdb . && docker run --rm -p 8080:8080 kestra-documentdb server local
```

Visit http://localhost:8080 to test your plugin.

## Documentation
* Full documentation can be found under: [kestra.io/docs](https://kestra.io/docs)
* Documentation for developing a plugin is included in the [Plugin Developer Guide](https://kestra.io/docs/plugin-developer-guide/)


## License
Apache 2.0 © [Kestra Technologies](https://kestra.io)


## Stay up to date

We release new versions every month. Give the [main repository](https://github.com/kestra-io/kestra) a star to stay up to date with the latest releases and get notified about future updates.

![Star the repo](https://kestra.io/star.gif)
