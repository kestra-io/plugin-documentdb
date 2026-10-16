package io.kestra.plugin.documentdb;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class UpdateTest {

    private static final String CONNECTION_STRING = "mongodb://testuser:testpass@localhost:27017/test_db?authSource=admin";
    private static final String DATABASE = "test_db";
    private static final String INVALID_CONNECTION_STRING = "mongodb://127.0.0.1:1/test_db?serverSelectionTimeoutMS=1000&connectTimeoutMS=1000";
    private static final String COLLECTION = "update_test";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldValidateRequiredProperties() throws Exception {
        Update task = Update.builder()
            .id("test-update")
            .type(Update.class.getName())
            .connectionString(Property.ofValue(INVALID_CONNECTION_STRING))
            .database(Property.ofValue("testdb"))
            .collection(Property.ofValue("testcol"))
            .filter(Property.ofValue(Map.of("name", "Test Document")))
            .update(Property.ofValue(Map.of("$set", Map.of("status", "updated"))))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        // This should not throw an exception for validation
        assertThat(task.getConnectionString(), is(notNullValue()));
        assertThat(task.getDatabase(), is(notNullValue()));
        assertThat(task.getCollection(), is(notNullValue()));
        assertThat(task.getFilter(), is(notNullValue()));
        assertThat(task.getUpdate(), is(notNullValue()));

        // Task should fail with connection error since this unit test uses a non-existent host
        // but this validates that the task configuration is valid
        try {
            task.run(runContext);
            throw new AssertionError("Should have thrown exception due to connection failure");
        } catch (Exception e) {
            // Expected - connection will fail to the non-existent test host
            // This validates the task can be executed with valid properties
            assertThat(
                e.getMessage(), anyOf(
                    containsString("Connection refused"),
                    containsString("Failed to update document"),
                    containsString("Name or service not known"),
                    containsString("UnknownHostException")
                )
            );
        }
    }

    @Test
    void shouldRejectMissingUpdate() throws Exception {
        Update task = Update.builder()
            .id("test-missing-update")
            .type(Update.class.getName())
            .connectionString(Property.ofValue(CONNECTION_STRING))
            .database(Property.ofValue("testdb"))
            .collection(Property.ofValue("testcol"))
            .filter(Property.ofValue(Map.of("name", "Test Document")))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        try {
            task.run(runContext);
            throw new AssertionError("Should have thrown ConstraintViolationException or IllegalArgumentException");
        } catch (Exception e) {
            // The validation can happen at framework level (ConstraintViolationException)
            // or our custom level (IllegalArgumentException)
            assertThat(
                e.getMessage(), anyOf(
                    containsString("must not be null"),
                    containsString("Update operations must be provided")
                )
            );
        }
    }

    @Test
    void shouldValidateUpdateManyFlag() throws Exception {
        Update task = Update.builder()
            .id("test-update-many")
            .type(Update.class.getName())
            .connectionString(Property.ofValue(INVALID_CONNECTION_STRING))
            .database(Property.ofValue("testdb"))
            .collection(Property.ofValue("testcol"))
            .filter(Property.ofValue(Map.of("status", "pending")))
            .update(Property.ofValue(Map.of("$set", Map.of("status", "processed"))))
            .updateMany(Property.ofValue(true))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        assertThat(task.getUpdateMany(), is(notNullValue()));

        // Task should fail with connection error since this unit test uses a non-existent host
        // but this validates that the updateMany flag is properly configured
        try {
            task.run(runContext);
            throw new AssertionError("Should have thrown exception due to connection failure");
        } catch (Exception e) {
            // Expected - connection will fail to the non-existent test host
            // This validates the task can be executed with updateMany flag
            assertThat(
                e.getMessage(), anyOf(
                    containsString("Connection refused"),
                    containsString("Failed to update document"),
                    containsString("Name or service not known"),
                    containsString("UnknownHostException")
                )
            );
        }
    }

    @Test
    @Order(10)
    @DisplayName("Integration: Update Single Document with Real DocumentDB")
    void shouldUpdateSingleDocumentWithRealServer() throws Exception {
        // First, insert a document to update
        Insert insertTask = Insert.builder()
            .id("setup-update-test")
            .type(Insert.class.getName())
            .connectionString(Property.ofValue(CONNECTION_STRING))
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .document(
                Property.ofValue(
                    Map.of(
                        "_id", "update-test-doc-" + System.currentTimeMillis(),
                        "name", "Update Test Document",
                        "status", "pending",
                        "count", 0
                    )
                )
            )
            .build();

        RunContext insertContext = TestsUtils.mockRunContext(runContextFactory, insertTask, Map.of());
        Insert.Output insertOutput = insertTask.run(insertContext);
        String docId = insertOutput.getInsertedId();

        // Now update the document
        Update updateTask = Update.builder()
            .id("update-single-integration-test")
            .type(Update.class.getName())
            .connectionString(Property.ofValue(CONNECTION_STRING))
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .filter(Property.ofValue(Map.of("_id", docId)))
            .update(
                Property.ofValue(
                    Map.of(
                        "$set", Map.of("status", "updated", "updated_at", System.currentTimeMillis()),
                        "$inc", Map.of("count", 5)
                    )
                )
            )
            .updateMany(Property.ofValue(false))
            .build();

        RunContext updateContext = TestsUtils.mockRunContext(runContextFactory, updateTask, Map.of());
        Update.Output updateOutput = updateTask.run(updateContext);

        assertThat("Should match one document", updateOutput.getMatchedCount(), equalTo(1));
        assertThat("Should modify one document", updateOutput.getModifiedCount(), equalTo(1));
        assertThat("Should not have upserted ID", updateOutput.getUpsertedId(), nullValue());
    }

    @Test
    @Order(11)
    @DisplayName("Integration: Update Multiple Documents with Real DocumentDB")
    void shouldUpdateMultipleDocumentsWithRealServer() throws Exception {
        // Insert multiple documents to update
        Insert insertTask = Insert.builder()
            .id("setup-update-many-test")
            .type(Insert.class.getName())
            .connectionString(Property.ofValue(CONNECTION_STRING))
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .documents(
                Property.ofValue(
                    List.of(
                        Map.of("_id", "update-many-1-" + System.currentTimeMillis(), "category", "bulk-test", "status", "pending", "priority", 1),
                        Map.of("_id", "update-many-2-" + System.currentTimeMillis(), "category", "bulk-test", "status", "pending", "priority", 2),
                        Map.of("_id", "update-many-3-" + System.currentTimeMillis(), "category", "bulk-test", "status", "pending", "priority", 3)
                    )
                )
            )
            .build();

        RunContext insertContext = TestsUtils.mockRunContext(runContextFactory, insertTask, Map.of());
        insertTask.run(insertContext);

        // Now update multiple documents
        Update updateTask = Update.builder()
            .id("update-many-integration-test")
            .type(Update.class.getName())
            .connectionString(Property.ofValue(CONNECTION_STRING))
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .filter(Property.ofValue(Map.of("category", "bulk-test")))
            .update(
                Property.ofValue(
                    Map.of(
                        "$set", Map.of("status", "bulk-updated", "batch_update_time", System.currentTimeMillis())
                    )
                )
            )
            .updateMany(Property.ofValue(true))
            .build();

        RunContext updateContext = TestsUtils.mockRunContext(runContextFactory, updateTask, Map.of());
        Update.Output updateOutput = updateTask.run(updateContext);

        assertThat("Should match multiple documents", updateOutput.getMatchedCount(), greaterThanOrEqualTo(3));
        assertThat("Should modify multiple documents", updateOutput.getModifiedCount(), greaterThanOrEqualTo(3));
        assertThat("Should not have upserted ID", updateOutput.getUpsertedId(), nullValue());
    }
}