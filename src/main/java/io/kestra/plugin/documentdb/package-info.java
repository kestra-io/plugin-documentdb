/**
 * DocumentDB plugin for Kestra.
 *
 * This plugin provides tasks for interacting with DocumentDB, Microsoft's open-source,
 * MongoDB-compatible document database built on PostgreSQL.
 *
 * DocumentDB is now part of the Linux Foundation and provides a fully open-source
 * alternative to proprietary document databases while maintaining MongoDB compatibility.
 *
 * Available tasks:
 * - {@link io.kestra.plugin.documentdb.Insert}: Insert single or multiple documents
 * - {@link io.kestra.plugin.documentdb.Read}: Read documents with filtering and aggregation support
 *
 * @see <a href="https://documentdb.io">DocumentDB Official Website</a>
 * @see <a href="https://github.com/documentdb/documentdb">DocumentDB GitHub Repository</a>
 */
package io.kestra.plugin.documentdb;