package io.kestra.plugin.documentdb;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.slf4j.Logger;

import com.mongodb.MongoException;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.documentdb.models.DocumentDBException;
import io.kestra.plugin.documentdb.models.DocumentDBRecord;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import reactor.core.publisher.Flux;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Read documents from DocumentDB",
    description = "Query a collection with an optional filter, skip/limit, or aggregation pipeline. Results are rendered into the selected `fetchType` output; default FETCH returns all rows."
)
@Plugin(
    examples = {
        @Example(
            title = "Find all users",
            full = true,
            code = """
                id: read_all_users
                namespace: company.documentdb

                tasks:
                  - id: find_users
                    type: io.kestra.plugin.documentdb.Read
                    connectionString: "{{ secret('DOCUMENTDB_CONNECTION_STRING') }}"
                    database: "myapp"
                    collection: "users"
                    fetchType: FETCH
                """
        ),
        @Example(
            title = "Find users with filter and limit",
            full = true,
            code = """
                id: find_active_users
                namespace: company.documentdb

                tasks:
                  - id: find_filtered_users
                    type: io.kestra.plugin.documentdb.Read
                    connectionString: "{{ secret('DOCUMENTDB_CONNECTION_STRING') }}"
                    database: "myapp"
                    collection: "users"
                    filter:
                      status: "active"
                      age:
                        $gte: 18
                    limit: 100
                    fetchType: FETCH
                """
        ),
        @Example(
            title = "Get single user",
            full = true,
            code = """
                id: get_single_user
                namespace: company.documentdb

                tasks:
                  - id: find_one_user
                    type: io.kestra.plugin.documentdb.Read
                    connectionString: "{{ secret('DOCUMENTDB_CONNECTION_STRING') }}"
                    database: "myapp"
                    collection: "users"
                    filter:
                      email: "john.doe@example.com"
                    fetchType: FETCH_ONE
                """
        ),
        @Example(
            title = "Aggregation pipeline example",
            full = true,
            code = """
                id: user_statistics
                namespace: company.documentdb

                tasks:
                  - id: aggregate_users
                    type: io.kestra.plugin.documentdb.Read
                    connectionString: "{{ secret('DOCUMENTDB_CONNECTION_STRING') }}"
                    database: "myapp"
                    collection: "users"
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
                """
        )
    }
)
public class Read extends AbstractDocumentDBTask implements RunnableTask<Read.Output> {

    @Schema(
        title = "Filter",
        description = "MongoDB-style filter for the find query, rendered at runtime. Leave empty to scan the full collection."
    )
    @PluginProperty(group = "processing")
    private Property<Map<String, Object>> filter;

    @Schema(
        title = "Aggregation pipeline",
        description = "MongoDB aggregation stages to run instead of a simple find. Overrides `filter`, `skip`, and `limit`."
    )
    @PluginProperty(group = "advanced")
    private Property<List<Map<String, Object>>> aggregationPipeline;

    @Schema(
        title = "Limit",
        description = "Maximum number of documents to return; optional server-side cap."
    )
    @PluginProperty(group = "processing")
    private Property<Integer> limit;

    @Schema(
        title = "Skip",
        description = "Number of documents to skip before returning results."
    )
    @PluginProperty(group = "advanced")
    private Property<Integer> skip;

    @Schema(
        title = "Fetch type",
        description = "Controls result handling (default FETCH). STORE writes all rows to Kestra internal storage as ION, FETCH outputs all rows, FETCH_ONE outputs only the first row, NONE skips output."
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Override
    public Output run(RunContext runContext) throws Exception {
        String rConnectionString = resolveConnectionString(runContext);
        String rCaCertificate = this.caCertificate == null ? null : runContext.render(this.caCertificate).as(String.class).orElse(null);

        return withClient(rConnectionString, rCaCertificate, client -> run(runContext, client));
    }

    Output run(RunContext runContext, DocumentDBClient client) throws Exception {
        Logger logger = runContext.logger();
        String rDatabase = runContext.render(this.database).as(String.class).orElseThrow();
        String rCollection = renderCollection(runContext);
        Map<String, Object> rFilter = runContext.render(this.filter).asMap(String.class, Object.class);
        List<Map<String, Object>> rAggregationPipeline = runContext.render(this.aggregationPipeline).asList(Map.class);
        Integer rLimit = runContext.render(this.limit).as(Integer.class).orElse(null);
        Integer rSkip = runContext.render(this.skip).as(Integer.class).orElse(null);
        FetchType rFetchType = runContext.render(this.fetchType).as(FetchType.class).orElse(FetchType.FETCH);
        Output.OutputBuilder outputBuilder = Output.builder();

        MongoIterable<Document> results;
        boolean aggregation = rAggregationPipeline != null && !rAggregationPipeline.isEmpty();
        if (aggregation) {
            logger.info(
                "Executing aggregation pipeline on DocumentDB database: {} collection: {} with {} stages",
                rDatabase, rCollection, rAggregationPipeline.size()
            );
            AggregateIterable<Document> aggregate = client.aggregateIterable(
                rDatabase,
                rCollection,
                rAggregationPipeline
            );
            results = aggregate;
        } else {
            logger.info("Finding documents in DocumentDB database: {} collection: {}", rDatabase, rCollection);
            FindIterable<Document> find = client.findIterable(rDatabase, rCollection, rFilter, rLimit, rSkip);
            if (rFetchType == FetchType.FETCH_ONE) {
                find.limit(1);
            }
            results = find;
        }

        try {
            if (rFetchType == FetchType.FETCH_ONE) {
                Map<String, Object> row = withCursor(results.iterator(), cursor ->
                    cursor.hasNext()
                        ? convertRecordToMap(client.toRecord(cursor.next()))
                        : null
                );
                outputBuilder.size(row == null ? 0L : 1L).row(row);
            } else if (rFetchType == FetchType.STORE) {
                StoredResult storedResult = withCursor(
                    results.iterator(),
                    cursor -> storeRecordsAsFile(runContext, cursor, client)
                );
                outputBuilder.size(storedResult.getCount()).uri(storedResult.getUri());
            } else if (rFetchType == FetchType.NONE) {
                long count = withCursor(results.iterator(), cursor -> {
                    long resultCount = 0;
                    while (cursor.hasNext()) {
                        cursor.next();
                        resultCount++;
                    }
                    return resultCount;
                });
                outputBuilder.size(count);
            } else {
                List<Map<String, Object>> rows = withCursor(results.iterator(), cursor -> {
                    List<Map<String, Object>> collectedRows = new ArrayList<>();
                    while (cursor.hasNext()) {
                        collectedRows.add(convertRecordToMap(client.toRecord(cursor.next())));
                    }
                    return collectedRows;
                });
                outputBuilder.size((long) rows.size()).rows(rows);
            }
        } catch (MongoException e) {
            String message = aggregation
                ? "Failed to execute aggregation: "
                : "Failed to find documents: ";
            throw new DocumentDBException(message + e.getMessage(), e);
        }

        return outputBuilder.build();
    }

    private Map<String, Object> convertRecordToMap(DocumentDBRecord record) {
        Map<String, Object> map = new HashMap<>();
        if (record.getId() != null) {
            map.put("_id", record.getId());
        }
        if (record.getFields() != null) {
            map.putAll(record.getFields());
        }
        return map;
    }

    /**
     * Store records as an ION file in Kestra's internal storage.
     * Returns a StoredResult containing both the URI and record count.
     */
    private StoredResult storeRecordsAsFile(
        RunContext runContext,
        MongoCursor<Document> cursor,
        DocumentDBClient client
    ) throws IOException {
        File tempFile;
        long count;

        try {
            tempFile = runContext.workingDir().createTempFile(".ion").toFile();
            try (OutputStream outputStream = new BufferedOutputStream(
                Files.newOutputStream(tempFile.toPath()),
                FileSerde.BUFFER_SIZE
            )) {
                Flux<Map<String, Object>> recordFlux = Flux.<Map<String, Object>>generate(sink -> {
                    if (cursor.hasNext()) {
                        sink.next(convertRecordToMap(client.toRecord(cursor.next())));
                    } else {
                        sink.complete();
                    }
                });

                count = FileSerde.writeAll(outputStream, recordFlux).blockOptional().orElseThrow();
            }
        } catch (IOException e) {
            throw new IOException("Failed to store records as Ion file: " + e.getMessage(), e);
        }

        URI storedFileUri = runContext.storage().putFile(tempFile);
        return new StoredResult(storedFileUri, count);
    }

    /**
     * Helper class to return both URI and count for STORE operations.
     */
    private static class StoredResult {
        private final URI uri;
        private final long count;

        StoredResult(URI uri, long count) {
            this.uri = uri;
            this.count = count;
        }

        public URI getUri() {
            return uri;
        }

        public long getCount() {
            return count;
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "First row",
            description = "First matching document when fetchType is FETCH_ONE; null if no results."
        )
        private final Map<String, Object> row;

        @Schema(
            title = "Rows",
            description = "All returned documents when fetchType is FETCH."
        )
        private final List<Map<String, Object>> rows;

        @Schema(
            title = "Result file URI",
            description = "URI in Kestra internal storage for the ION result file; only set when fetchType is STORE."
        )
        private final URI uri;

        @Schema(
            title = "Returned document count",
            description = "Number of documents returned by the query, including when fetchType is NONE and results are not output."
        )
        private final Long size;
    }
}
