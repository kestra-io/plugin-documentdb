package io.kestra.plugin.documentdb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.documentdb.models.DocumentDBRecord;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Read documents from a DocumentDB collection",
    description = "Read documents from a DocumentDB collection with optional filtering, limiting, and aggregation support."
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
                    connectionString: "https://my-documentdb-instance.com"
                    database: "myapp"
                    collection: "users"
                    username: "{{ secret('DOCUMENTDB_USERNAME') }}"
                    password: "{{ secret('DOCUMENTDB_PASSWORD') }}"
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
                    connectionString: "https://my-documentdb-instance.com"
                    database: "myapp"
                    collection: "users"
                    username: "{{ secret('DOCUMENTDB_USERNAME') }}"
                    password: "{{ secret('DOCUMENTDB_PASSWORD') }}"
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
                    connectionString: "https://my-documentdb-instance.com"
                    database: "myapp"
                    collection: "users"
                    username: "{{ secret('DOCUMENTDB_USERNAME') }}"
                    password: "{{ secret('DOCUMENTDB_PASSWORD') }}"
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
                """
        )
    }
)
public class Read extends Task implements RunnableTask<Read.Output> {

    @Schema(
        title = "DocumentDB connection string",
        description = "The HTTP endpoint URL of your DocumentDB instance"
    )
    @NotNull
    private Property<String> connectionString;

    @Schema(
        title = "Database name",
        description = "The name of the database to read from"
    )
    @NotNull
    private Property<String> database;

    @Schema(
        title = "Collection name",
        description = "The name of the collection to read from"
    )
    @NotNull
    private Property<String> collection;

    @Schema(
        title = "Username",
        description = "DocumentDB username for authentication"
    )
    @NotNull
    private Property<String> username;

    @Schema(
        title = "Password",
        description = "DocumentDB password for authentication"
    )
    @NotNull
    private Property<String> password;

    @Schema(
        title = "Filter",
        description = "MongoDB-style filter criteria to apply to the query. Example: {\"status\": \"active\", \"age\": {\"$gte\": 18}}"
    )
    private Property<Map<String, Object>> filter;

    @Schema(
        title = "Aggregation pipeline",
        description = "MongoDB aggregation pipeline stages. If provided, this will execute an aggregation instead of a simple find."
    )
    private Property<List<Map<String, Object>>> aggregationPipeline;

    @Schema(
        title = "Limit",
        description = "Maximum number of documents to return"
    )
    private Property<Integer> limit;

    @Schema(
        title = "Skip",
        description = "Number of documents to skip"
    )
    private Property<Integer> skip;

    @Schema(
        title = "Fetch type",
        description = "How to handle query results. STORE: store all rows to a file, FETCH: output all rows as output variable, FETCH_ONE: output the first row, NONE: do nothing"
    )
    @NotNull
    @Builder.Default
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        // Render properties
        String rConnectionString = runContext.render(this.connectionString).as(String.class).orElseThrow();
        String rDatabase = runContext.render(this.database).as(String.class).orElseThrow();
        String rCollection = runContext.render(this.collection).as(String.class).orElseThrow();
        String rUsername = runContext.render(this.username).as(String.class).orElseThrow();
        String rPassword = runContext.render(this.password).as(String.class).orElseThrow();
        Map<String, Object> rFilter = runContext.render(this.filter).asMap(String.class, Object.class);
        List<Map<String, Object>> rAggregationPipeline = runContext.render(this.aggregationPipeline).asList(Map.class);
        Integer rLimit = runContext.render(this.limit).as(Integer.class).orElse(null);
        Integer rSkip = runContext.render(this.skip).as(Integer.class).orElse(null);
        FetchType rFetchType = runContext.render(this.fetchType).as(FetchType.class).orElse(FetchType.FETCH);

        DocumentDBClient client = new DocumentDBClient(rConnectionString, rUsername, rPassword, runContext);

        List<DocumentDBRecord> records;

        if (rAggregationPipeline != null && !rAggregationPipeline.isEmpty()) {
            // Execute aggregation
            logger.info("Executing aggregation pipeline on DocumentDB database: {} collection: {} with {} stages",
                rDatabase, rCollection, rAggregationPipeline.size());
            records = client.aggregate(rDatabase, rCollection, rAggregationPipeline);
        } else {
            // Execute find
            logger.info("Finding documents in DocumentDB database: {} collection: {}", rDatabase, rCollection);
            records = client.find(rDatabase, rCollection, rFilter, rLimit, rSkip);
        }

        logger.info("Found {} documents", records.size());

        // Handle fetchType logic
        Object result;
        int recordCount = records.size();

        switch (rFetchType) {
            case FETCH_ONE:
                result = records.isEmpty() ? null : convertRecordToMap(records.get(0));
                recordCount = records.isEmpty() ? 0 : 1;
                break;
            case NONE:
                result = null;
                break;
            case STORE:
                result = storeRecordsAsFile(runContext, records);
                break;
            case FETCH:
            default:
                result = records.stream()
                    .map(this::convertRecordToMap)
                    .toList();
                break;
        }

        // Build output based on fetch type
        Output.OutputBuilder outputBuilder = Output.builder().size((long) recordCount);

        switch (rFetchType) {
            case FETCH_ONE:
                @SuppressWarnings("unchecked")
                Map<String, Object> row = result instanceof Map ? (Map<String, Object>) result : null;
                outputBuilder.row(row);
                break;
            case FETCH:
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> rows = result instanceof List ? (List<Map<String, Object>>) result : null;
                outputBuilder.rows(rows);
                break;
            case STORE:
                outputBuilder.uri(result instanceof StoredResult ? ((StoredResult) result).getUri() : null);
                break;
            case NONE:
            default:
                // No output data for NONE
                break;
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
     * TODO: Implement proper file storage once storage interface is clarified
     */
    private StoredResult storeRecordsAsFile(RunContext runContext, List<DocumentDBRecord> records) {
        // Temporary implementation - return null URI but correct count
        // This needs to be implemented with proper storage interface
        return new StoredResult(null, records.size());
    }

    /**
     * Helper class to return both URI and count for STORE operations.
     */
    private static class StoredResult {
        private final URI uri;
        private final int count;

        StoredResult(URI uri, int count) {
            this.uri = uri;
            this.count = count;
        }

        public URI getUri() { return uri; }
        public int getCount() { return count; }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Map containing the first row of fetched data.",
            description = "Only populated if fetchType is FETCH_ONE."
        )
        private final Map<String, Object> row;

        @Schema(
            title = "List of map containing rows of fetched data.",
            description = "Only populated if fetchType is FETCH."
        )
        private final List<Map<String, Object>> rows;

        @Schema(
            title = "The URI of the result file on Kestra's internal storage (.ion file / Amazon Ion formatted text file).",
            description = "Only populated if fetchType is STORE."
        )
        private final URI uri;

        @Schema(
            title = "The number of documents returned by the operation."
        )
        private final Long size;
    }
}