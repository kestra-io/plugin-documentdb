package io.kestra.plugin.documentdb;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.DisplayName;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Real integration tests for DocumentDB operations using local container.
 * These tests run against a real DocumentDB container and use actual credentials.
 *
 * To run these tests:
 * 1. Start DocumentDB: ./.github/setup-unit.sh
 * 2. Run tests: ./gradlew test
 *
 * No environment variables needed - tests use real credentials directly.
 */
@KestraTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DocumentDBIntegrationTest {

    @Inject
    RunContextFactory runContextFactory;

    // Real connection details for local DocumentDB container
    private static final String CONNECTION_STRING = "http://localhost:10260";
    private static final String DATABASE = "test_db";
    private static final String COLLECTION = "integration_test";
    private static final String USERNAME = "testuser";
    private static final String PASSWORD = "testpass";

    @Test
    @Order(1)
    @DisplayName("1. Insert and Read Single Document - Real DocumentDB Test")
    void shouldInsertAndReadSingleDocument() throws Exception {
        // Insert a document using real credentials
        Insert insertTask = Insert.builder()
            .id("insert-real-test")
            .type(Insert.class.getName())
            .connectionString(Property.ofValue(CONNECTION_STRING))
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .document(Property.ofValue(Map.of(
                "_id", "real-test-doc-1-" + System.currentTimeMillis(),
                "name", "Real Integration Test Document",
                "value", 42,
                "active", true,
                "timestamp", System.currentTimeMillis()
            )))
            .build();

        RunContext insertContext = TestsUtils.mockRunContext(runContextFactory, insertTask, Map.of());
        Insert.Output insertOutput = insertTask.run(insertContext);

        assertThat("Document should be inserted", insertOutput.getInsertedCount(), equalTo(1));
        assertThat("Should return inserted ID", insertOutput.getInsertedIds(), hasSize(1));
        String insertedId = insertOutput.getInsertedIds().get(0);
        System.out.println("✅ Inserted document with ID: " + insertedId);

        // Read the document back using real credentials
        Read readTask = Read.builder()
            .id("read-real-test")
            .type(Read.class.getName())
            .connectionString(Property.ofValue(CONNECTION_STRING))
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .filter(Property.ofValue(Map.of("_id", insertedId)))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();

        RunContext readContext = TestsUtils.mockRunContext(runContextFactory, readTask, Map.of());
        Read.Output readOutput = readTask.run(readContext);

        assertThat("Should retrieve the document", readOutput.getRow(), notNullValue());
        assertThat("Document name should match", readOutput.getRow().get("name"), equalTo("Real Integration Test Document"));
        assertThat("Document value should match", readOutput.getRow().get("value"), equalTo(42));
        assertThat("Document active flag should match", readOutput.getRow().get("active"), equalTo(true));
        System.out.println("✅ Successfully read document: " + readOutput.getRow().get("name"));
    }

    @Test
    @Order(2)
    @DisplayName("2. Insert Multiple Documents - Real DocumentDB Test")
    void shouldInsertMultipleDocuments() throws Exception {
        Insert insertTask = Insert.builder()
            .id("insert-multiple-real-test")
            .type(Insert.class.getName())
            .connectionString(Property.ofValue(CONNECTION_STRING))
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .documents(Property.ofValue(List.of(
                Map.of("_id", "real-test-doc-2-" + System.currentTimeMillis(), "name", "Document 2", "category", "A", "priority", 1),
                Map.of("_id", "real-test-doc-3-" + System.currentTimeMillis(), "name", "Document 3", "category", "B", "priority", 2),
                Map.of("_id", "real-test-doc-4-" + System.currentTimeMillis(), "name", "Document 4", "category", "A", "priority", 3)
            )))
            .build();

        RunContext insertContext = TestsUtils.mockRunContext(runContextFactory, insertTask, Map.of());
        Insert.Output insertOutput = insertTask.run(insertContext);

        assertThat("Should insert all documents", insertOutput.getInsertedCount(), equalTo(3));
        assertThat("Should return all inserted IDs", insertOutput.getInsertedIds(), hasSize(3));
        System.out.println("✅ Inserted " + insertOutput.getInsertedCount() + " documents");
    }

    @Test
    @Order(3)
    @DisplayName("3. Fetch One Document with Filter - Real DocumentDB Test")
    void shouldHandleFetchOne() throws Exception {
        Read readTask = Read.builder()
            .id("fetch-one-real-test")
            .type(Read.class.getName())
            .connectionString(Property.ofValue(CONNECTION_STRING))
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .filter(Property.ofValue(Map.of("category", "A")))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();

        RunContext readContext = TestsUtils.mockRunContext(runContextFactory, readTask, Map.of());
        Read.Output readOutput = readTask.run(readContext);

        assertThat("Should retrieve one document", readOutput.getRow(), notNullValue());
        assertThat("Document should match filter", readOutput.getRow().get("category"), equalTo("A"));
        assertThat("Should not return rows array for FETCH_ONE", readOutput.getRows(), nullValue());
        System.out.println("✅ Fetched one document with category A: " + readOutput.getRow().get("name"));
    }

    @Test
    @Order(4)
    @DisplayName("4. Handle Limit and Skip - Real DocumentDB Test")
    void shouldHandleLimitAndSkip() throws Exception {
        Read readTask = Read.builder()
            .id("limit-skip-real-test")
            .type(Read.class.getName())
            .connectionString(Property.ofValue(CONNECTION_STRING))
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .filter(Property.ofValue(Map.of())) // Empty filter to get all documents
            .limit(Property.ofValue(2))
            .skip(Property.ofValue(1))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        RunContext readContext = TestsUtils.mockRunContext(runContextFactory, readTask, Map.of());
        Read.Output readOutput = readTask.run(readContext);

        assertThat("Should retrieve documents", readOutput.getRows(), notNullValue());
        assertThat("Should respect limit", readOutput.getRows().size(), lessThanOrEqualTo(2));
        assertThat("Should have at least one document", readOutput.getRows().size(), greaterThan(0));
        System.out.println("✅ Retrieved " + readOutput.getRows().size() + " documents with limit/skip");
    }

    @Test
    @Order(5)
    @DisplayName("5. Execute Aggregation Pipeline - Real DocumentDB Test")
    void shouldExecuteAggregationPipeline() throws Exception {
        Read readTask = Read.builder()
            .id("aggregation-real-test")
            .type(Read.class.getName())
            .connectionString(Property.ofValue(CONNECTION_STRING))
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .aggregationPipeline(Property.ofValue(List.of(
                Map.of("$match", Map.of("category", Map.of("$exists", true))),
                Map.of("$group", Map.of(
                    "_id", "$category",
                    "count", Map.of("$sum", 1),
                    "avgPriority", Map.of("$avg", "$priority")
                )),
                Map.of("$sort", Map.of("count", -1))
            )))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        RunContext readContext = TestsUtils.mockRunContext(runContextFactory, readTask, Map.of());
        Read.Output readOutput = readTask.run(readContext);

        assertThat("Should execute aggregation", readOutput.getRows(), notNullValue());
        assertThat("Should return aggregation results", readOutput.getRows().size(), greaterThan(0));

        // Verify aggregation structure
        Map<String, Object> firstResult = readOutput.getRows().get(0);
        assertThat("Should have _id field", firstResult.containsKey("_id"), equalTo(true));
        assertThat("Should have count field", firstResult.containsKey("count"), equalTo(true));

        System.out.println("✅ Executed aggregation pipeline, got " + readOutput.getRows().size() + " groups");
        readOutput.getRows().forEach(row ->
            System.out.println("  Category: " + row.get("_id") + ", Count: " + row.get("count"))
        );
    }
}