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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

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
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DocumentDBIntegrationTest {

    @Inject
    RunContextFactory runContextFactory;

    // Real connection details for local DocumentDB container
    private static final String HOST = "http://localhost:10260";
    private static final String DATABASE = "test_db";
    private static final String COLLECTION = "integration_test";
    private static final String USERNAME = "testuser";
    private static final String PASSWORD = "testpass";

    @BeforeAll
    void setupTestData() throws Exception {
        // Create test data for READ tests to use
        Insert insertTask = Insert.builder()
            .id("setup-test-data")
            .type(Insert.class.getName())
            .host(Property.ofValue(HOST))
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .documents(Property.ofValue(List.of(
                Map.of("_id", "setup-doc-" + System.currentTimeMillis() + "-1", "name", "Setup Document 1", "category", "A", "priority", 1),
                Map.of("_id", "setup-doc-" + System.currentTimeMillis() + "-2", "name", "Setup Document 2", "category", "B", "priority", 2),
                Map.of("_id", "setup-doc-" + System.currentTimeMillis() + "-3", "name", "Setup Document 3", "category", "A", "priority", 3)
            )))
            .build();

        RunContext setupContext = TestsUtils.mockRunContext(runContextFactory, insertTask, Map.of());
        insertTask.run(setupContext);
    }

    @Test
    @Order(1)
    @DisplayName("1. Fetch One Document with Filter - Real DocumentDB Test")
    void shouldHandleFetchOne() throws Exception {
        Read readTask = Read.builder()
            .id("fetch-one-real-test")
            .type(Read.class.getName())
.host(Property.ofValue(HOST))
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
    }

    @Test
    @Order(2)
    @DisplayName("2. Handle Limit and Skip - Real DocumentDB Test")
    void shouldHandleLimitAndSkip() throws Exception {
        Read readTask = Read.builder()
            .id("limit-skip-real-test")
            .type(Read.class.getName())
.host(Property.ofValue(HOST))
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
    }

    @Test
    @Order(3)
    @DisplayName("3. Execute Aggregation Pipeline - Real DocumentDB Test")
    void shouldExecuteAggregationPipeline() throws Exception {
        Read readTask = Read.builder()
            .id("aggregation-real-test")
            .type(Read.class.getName())
.host(Property.ofValue(HOST))
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
        Map<String, Object> firstResult = readOutput.getRows().getFirst();
        assertThat("Should have _id field", firstResult.containsKey("_id"), equalTo(true));
        assertThat("Should have count field", firstResult.containsKey("count"), equalTo(true));
    }
}