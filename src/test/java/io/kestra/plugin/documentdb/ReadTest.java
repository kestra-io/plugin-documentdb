package io.kestra.plugin.documentdb;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReadTest {

    @Inject
    private RunContextFactory runContextFactory;

    // Real connection details for local DocumentDB container
    private static final String HOST = "http://localhost:10260";
    private static final String DATABASE = "test_db";
    private static final String COLLECTION = "read_test";
    private static final String USERNAME = "testuser";
    private static final String PASSWORD = "testpass";

    @BeforeAll
    void setupTestData() throws Exception {
        // Create test data for READ tests with unique IDs
        long timestamp = System.currentTimeMillis();
        Insert insertTask = Insert.builder()
            .id("setup-read-test-data")
            .type(Insert.class.getName())
            .host(Property.ofValue(HOST))
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .documents(Property.ofValue(List.of(
                Map.of("_id", "test-" + timestamp + "-1", "name", "Test User 1", "status", "active", "age", 25, "roles", List.of("user", "editor")),
                Map.of("_id", "test-" + timestamp + "-2", "name", "Test User 2", "status", "active", "age", 30, "roles", List.of("admin")),
                Map.of("_id", "test-" + timestamp + "-3", "name", "Test User 3", "status", "inactive", "age", 17, "department", "Sales")
            )))
            .build();

        RunContext setupContext = TestsUtils.mockRunContext(runContextFactory, insertTask, Map.of());
        insertTask.run(setupContext);
    }

    @Test
    void shouldCreateTaskWithRequiredProperties() throws Exception {
        Read task = Read.builder()
            .id("test-read")
            .type(Read.class.getName())
            .host(Property.ofValue(HOST))
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        assertThat(task, is(notNullValue()));
        assertThat(task.getHost(), is(notNullValue()));
        assertThat(task.getDatabase(), is(notNullValue()));
        assertThat(task.getCollection(), is(notNullValue()));
        assertThat(task.getUsername(), is(notNullValue()));
        assertThat(task.getPassword(), is(notNullValue()));
        assertThat(task.getFetchType(), is(notNullValue()));

        // Execute the task and verify it works with real database
        Read.Output output = task.run(runContext);
        assertThat(output, is(notNullValue()));
        assertThat(output.getRows(), is(notNullValue()));
        assertThat(output.getSize(), greaterThan(0L));
    }

    @Test
    void shouldDefaultToFetchType() throws Exception {
        Read task = Read.builder()
            .id("test-default")
            .type(Read.class.getName())
            .host(Property.ofValue(HOST))
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        // Should default to FETCH
        assertThat(task.getFetchType(), is(notNullValue()));

        // Execute the task and verify default fetchType works
        Read.Output output = task.run(runContext);
        assertThat(output, is(notNullValue()));
        assertThat(output.getRows(), is(notNullValue()));
        assertThat(output.getSize(), greaterThan(0L));
    }


    @Test
    void shouldValidateTaskWithFilter() throws Exception {
        Map<String, Object> filter = Map.of(
            "status", "active",
            "age", Map.of("$gte", 18)
        );

        Read task = Read.builder()
            .id("test-filter")
            .type(Read.class.getName())
            .host(Property.ofValue(HOST))
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .filter(Property.ofValue(filter))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        // Execute the task and verify filter works
        assertThat(task.getFilter(), is(notNullValue()));
        Read.Output output = task.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getRows(), is(notNullValue()));
        // Should only return documents matching the filter (status=active and age>=18)
        assertThat(output.getSize(), greaterThan(0L));
        for (Map<String, Object> row : output.getRows()) {
            assertThat(row.get("status"), equalTo("active"));
            assertThat((Integer) row.get("age"), greaterThanOrEqualTo(18));
        }
    }

    @Test
    void shouldValidateTaskWithAggregationPipeline() throws Exception {
        List<Map<String, Object>> pipeline = List.of(
            Map.of("$match", Map.of("status", "active")),
            Map.of("$group", Map.of(
                "_id", "$status",
                "count", Map.of("$sum", 1),
                "avgAge", Map.of("$avg", "$age")
            ))
        );

        Read task = Read.builder()
            .id("test-aggregation")
            .type(Read.class.getName())
            .host(Property.ofValue(HOST))
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .aggregationPipeline(Property.ofValue(pipeline))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        // Execute the task and verify aggregation pipeline works
        assertThat(task.getAggregationPipeline(), is(notNullValue()));
        Read.Output output = task.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getRows(), is(notNullValue()));
        assertThat(output.getSize(), greaterThan(0L));

        // Verify aggregation structure
        Map<String, Object> firstResult = output.getRows().get(0);
        assertThat(firstResult.containsKey("_id"), equalTo(true));
        assertThat(firstResult.containsKey("count"), equalTo(true));
    }

    @Test
    void shouldValidateAllFetchTypes() throws Exception {
        for (FetchType fetchType : FetchType.values()) {
            Read task = Read.builder()
                .id("test-fetchtype-" + fetchType.name())
                .type(Read.class.getName())
                .host(Property.ofValue(HOST))
                .database(Property.ofValue(DATABASE))
                .collection(Property.ofValue(COLLECTION))
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .fetchType(Property.ofValue(fetchType))
                .build();

            RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

            // Execute the task and verify all fetch types work
            assertThat(task.getFetchType(), is(notNullValue()));
            Read.Output output = task.run(runContext);

            assertThat(output, is(notNullValue()));
            assertThat(output.getSize(), greaterThan(0L));

            // Verify output structure based on fetch type
            switch (fetchType) {
                case FETCH:
                    assertThat(output.getRows(), is(notNullValue()));
                    assertThat(output.getRow(), is(nullValue()));
                    assertThat(output.getUri(), is(nullValue()));
                    break;
                case FETCH_ONE:
                    assertThat(output.getRow(), is(notNullValue()));
                    assertThat(output.getRows(), is(nullValue()));
                    assertThat(output.getUri(), is(nullValue()));
                    break;
                case STORE:
                    assertThat(output.getUri(), is(notNullValue()));
                    assertThat(output.getRows(), is(nullValue()));
                    assertThat(output.getRow(), is(nullValue()));
                    break;
                case NONE:
                    assertThat(output.getRows(), is(nullValue()));
                    assertThat(output.getRow(), is(nullValue()));
                    assertThat(output.getUri(), is(nullValue()));
                    break;
            }
        }
    }
}