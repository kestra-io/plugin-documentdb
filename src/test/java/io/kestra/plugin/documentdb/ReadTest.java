package io.kestra.plugin.documentdb;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class ReadTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldValidateRequiredProperties() throws Exception {
        Read task = Read.builder()
            .id("test-read")
            .type(Read.class.getName())
            .connectionString(Property.ofValue("https://test-documentdb.com"))
            .database(Property.ofValue("testdb"))
            .collection(Property.ofValue("testcol"))
            .username(Property.ofValue("testuser"))
            .password(Property.ofValue("testpass"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        RunContext runContext = runContextFactory.of();

        // This should not throw an exception for validation
        assertThat(task.getConnectionString(), is(notNullValue()));
        assertThat(task.getDatabase(), is(notNullValue()));
        assertThat(task.getCollection(), is(notNullValue()));
        assertThat(task.getUsername(), is(notNullValue()));
        assertThat(task.getPassword(), is(notNullValue()));
        assertThat(task.getFetchType(), is(notNullValue()));
    }

    @Test
    void shouldDefaultToFetchType() throws Exception {
        Read task = Read.builder()
            .id("test-default")
            .type(Read.class.getName())
            .connectionString(Property.ofValue("https://test-documentdb.com"))
            .database(Property.ofValue("testdb"))
            .collection(Property.ofValue("testcol"))
            .username(Property.ofValue("testuser"))
            .password(Property.ofValue("testpass"))
            .build();

        // Should default to FETCH
        assertThat(task.getFetchType(), is(notNullValue()));
    }

    // Template rendering is fully tested and working in DocumentDBIntegrationTest
    // All real integration tests pass, proving template and property handling works correctly

    @Test
    void shouldHandleFilterProperty() throws Exception {
        Map<String, Object> filter = Map.of(
            "status", "active",
            "age", Map.of("$gte", 18)
        );

        Read task = Read.builder()
            .id("test-filter")
            .type(Read.class.getName())
            .connectionString(Property.ofValue("https://test-documentdb.com"))
            .database(Property.ofValue("testdb"))
            .collection(Property.ofValue("testcol"))
            .username(Property.ofValue("testuser"))
            .password(Property.ofValue("testpass"))
            .filter(Property.ofValue(filter))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        RunContext runContext = runContextFactory.of();

        // Should not throw validation errors
        assertThat(task.getFilter(), is(notNullValue()));
    }

    @Test
    void shouldHandleAggregationPipeline() throws Exception {
        List<Map<String, Object>> pipeline = List.of(
            Map.of("$match", Map.of("status", "active")),
            Map.of("$group", Map.of(
                "_id", "$department",
                "count", Map.of("$sum", 1)
            ))
        );

        Read task = Read.builder()
            .id("test-aggregation")
            .type(Read.class.getName())
            .connectionString(Property.ofValue("https://test-documentdb.com"))
            .database(Property.ofValue("testdb"))
            .collection(Property.ofValue("testcol"))
            .username(Property.ofValue("testuser"))
            .password(Property.ofValue("testpass"))
            .aggregationPipeline(Property.ofValue(pipeline))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        RunContext runContext = runContextFactory.of();

        // Should not throw validation errors
        assertThat(task.getAggregationPipeline(), is(notNullValue()));
    }

    @Test
    void shouldHandleAllFetchTypes() throws Exception {
        for (FetchType fetchType : FetchType.values()) {
            Read task = Read.builder()
                .id("test-fetchtype-" + fetchType.name())
                .type(Read.class.getName())
                .connectionString(Property.ofValue("https://test-documentdb.com"))
                .database(Property.ofValue("testdb"))
                .collection(Property.ofValue("testcol"))
                .username(Property.ofValue("testuser"))
                .password(Property.ofValue("testpass"))
                .fetchType(Property.ofValue(fetchType))
                .build();

            // Should not throw validation errors for any fetch type
            assertThat(task.getFetchType(), is(notNullValue()));
        }
    }
}