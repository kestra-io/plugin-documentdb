package io.kestra.plugin.documentdb;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.DisplayName;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DeleteTest {

    // Real connection details for local DocumentDB container
    private static final String HOST = "http://localhost:10260";
    private static final String DATABASE = "test_db";
    private static final String COLLECTION = "delete_test";
    private static final String USERNAME = "testuser";
    private static final String PASSWORD = "testpass";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldValidateRequiredProperties() throws Exception {
        Delete task = Delete.builder()
            .id("test-delete")
            .type(Delete.class.getName())
            .host(Property.ofValue("https://test-documentdb.com"))
            .database(Property.ofValue("testdb"))
            .collection(Property.ofValue("testcol"))
            .username(Property.ofValue("testuser"))
            .password(Property.ofValue("testpass"))
            .filter(Property.ofValue(Map.of("name", "Test Document")))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        // This should not throw an exception for validation
        assertThat(task.getHost(), is(notNullValue()));
        assertThat(task.getDatabase(), is(notNullValue()));
        assertThat(task.getCollection(), is(notNullValue()));
        assertThat(task.getUsername(), is(notNullValue()));
        assertThat(task.getPassword(), is(notNullValue()));
        assertThat(task.getFilter(), is(notNullValue()));

        // Task should fail with connection error since this unit test uses a non-existent host
        // but this validates that the task configuration is valid
        try {
            task.run(runContext);
            throw new AssertionError("Should have thrown exception due to connection failure");
        } catch (Exception e) {
            // Expected - connection will fail to the non-existent test host
            // This validates the task can be executed with valid properties
            assertThat(e.getMessage(), anyOf(
                containsString("Connection refused"),
                containsString("Failed to delete document"),
                containsString("Name or service not known"),
                containsString("UnknownHostException")
            ));
        }
    }

    @Test
    void shouldValidateDeleteManyFlag() throws Exception {
        Delete task = Delete.builder()
            .id("test-delete-many")
            .type(Delete.class.getName())
            .host(Property.ofValue("https://test-documentdb.com"))
            .database(Property.ofValue("testdb"))
            .collection(Property.ofValue("testcol"))
            .username(Property.ofValue("testuser"))
            .password(Property.ofValue("testpass"))
            .filter(Property.ofValue(Map.of("status", "to_delete")))
            .deleteMany(Property.ofValue(true))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        assertThat(task.getDeleteMany(), is(notNullValue()));

        // Task should fail with connection error since this unit test uses a non-existent host
        // but this validates that the deleteMany flag is properly configured
        try {
            task.run(runContext);
            throw new AssertionError("Should have thrown exception due to connection failure");
        } catch (Exception e) {
            // Expected - connection will fail to the non-existent test host
            // This validates the task can be executed with deleteMany flag
            assertThat(e.getMessage(), anyOf(
                containsString("Connection refused"),
                containsString("Failed to delete document"),
                containsString("Name or service not known"),
                containsString("UnknownHostException")
            ));
        }
    }

    @Test
    @Order(10)
    @DisplayName("Integration: Delete Single Document with Real DocumentDB")
    void shouldDeleteSingleDocumentWithRealServer() throws Exception {
        // First, insert a document to delete
        Insert insertTask = Insert.builder()
            .id("setup-delete-test")
            .type(Insert.class.getName())
            .host(Property.ofValue(HOST))
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .document(Property.ofValue(Map.of(
                "_id", "delete-test-doc-" + System.currentTimeMillis(),
                "name", "Delete Test Document",
                "status", "to_delete",
                "created_at", System.currentTimeMillis()
            )))
            .build();

        RunContext insertContext = TestsUtils.mockRunContext(runContextFactory, insertTask, Map.of());
        Insert.Output insertOutput = insertTask.run(insertContext);
        String docId = insertOutput.getInsertedId();

        // Now delete the document
        Delete deleteTask = Delete.builder()
            .id("delete-single-integration-test")
            .type(Delete.class.getName())
            .host(Property.ofValue(HOST))
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .filter(Property.ofValue(Map.of("_id", docId)))
            .deleteMany(Property.ofValue(false))
            .build();

        RunContext deleteContext = TestsUtils.mockRunContext(runContextFactory, deleteTask, Map.of());
        Delete.Output deleteOutput = deleteTask.run(deleteContext);

        assertThat("Should delete one document", deleteOutput.getDeletedCount(), equalTo(1));
    }

    @Test
    @Order(11)
    @DisplayName("Integration: Delete Multiple Documents with Real DocumentDB")
    void shouldDeleteMultipleDocumentsWithRealServer() throws Exception {
        // Insert multiple documents to delete
        Insert insertTask = Insert.builder()
            .id("setup-delete-many-test")
            .type(Insert.class.getName())
            .host(Property.ofValue(HOST))
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .documents(Property.ofValue(List.of(
                Map.of("_id", "delete-many-1-" + System.currentTimeMillis(), "category", "cleanup-test", "status", "expired", "priority", 1),
                Map.of("_id", "delete-many-2-" + System.currentTimeMillis(), "category", "cleanup-test", "status", "expired", "priority", 2),
                Map.of("_id", "delete-many-3-" + System.currentTimeMillis(), "category", "cleanup-test", "status", "expired", "priority", 3)
            )))
            .build();

        RunContext insertContext = TestsUtils.mockRunContext(runContextFactory, insertTask, Map.of());
        insertTask.run(insertContext);

        // Now delete multiple documents
        Delete deleteTask = Delete.builder()
            .id("delete-many-integration-test")
            .type(Delete.class.getName())
            .host(Property.ofValue(HOST))
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .filter(Property.ofValue(Map.of("category", "cleanup-test")))
            .deleteMany(Property.ofValue(true))
            .build();

        RunContext deleteContext = TestsUtils.mockRunContext(runContextFactory, deleteTask, Map.of());
        Delete.Output deleteOutput = deleteTask.run(deleteContext);

        assertThat("Should delete multiple documents", deleteOutput.getDeletedCount(), greaterThanOrEqualTo(3));
    }

    @Test
    @Order(12)
    @DisplayName("Integration: Delete No Matching Documents")
    void shouldHandleNoMatchingDocuments() throws Exception {
        Delete deleteTask = Delete.builder()
            .id("delete-no-match-test")
            .type(Delete.class.getName())
            .host(Property.ofValue(HOST))
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .filter(Property.ofValue(Map.of("_id", "non-existent-document-" + System.currentTimeMillis())))
            .deleteMany(Property.ofValue(false))
            .build();

        RunContext deleteContext = TestsUtils.mockRunContext(runContextFactory, deleteTask, Map.of());
        Delete.Output deleteOutput = deleteTask.run(deleteContext);

        assertThat("Should delete zero documents when no match", deleteOutput.getDeletedCount(), equalTo(0));
    }
}