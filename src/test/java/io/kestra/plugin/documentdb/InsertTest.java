package io.kestra.plugin.documentdb;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
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
class InsertTest {

    // Real connection details for local DocumentDB container
    private static final String HOST = "http://localhost:10260";
    private static final String DATABASE = "test_db";
    private static final String COLLECTION = "integration_test";
    private static final String USERNAME = "testuser";
    private static final String PASSWORD = "testpass";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldValidateRequiredProperties() throws Exception {
        Insert task = Insert.builder()
            .id("test-insert")
            .type(Insert.class.getName())
            .host(Property.ofValue("https://test-documentdb.com"))
            .database(Property.ofValue("testdb"))
            .collection(Property.ofValue("testcol"))
            .username(Property.ofValue("testuser"))
            .password(Property.ofValue("testpass"))
            .document(Property.ofValue(Map.of("name", "Test Document")))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        // This should not throw an exception for validation
        assertThat(task.getHost(), is(notNullValue()));
        assertThat(task.getDatabase(), is(notNullValue()));
        assertThat(task.getCollection(), is(notNullValue()));
        assertThat(task.getUsername(), is(notNullValue()));
        assertThat(task.getPassword(), is(notNullValue()));

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
                containsString("Failed to insert document"),
                containsString("Name or service not known"),
                containsString("UnknownHostException")
            ));
        }
    }

    @Test
    void shouldRejectBothDocumentAndDocuments() throws Exception {
        Insert task = Insert.builder()
            .id("test-reject-both")
            .type(Insert.class.getName())
            .host(Property.ofValue("https://test-documentdb.com"))
            .database(Property.ofValue("testdb"))
            .collection(Property.ofValue("testcol"))
            .username(Property.ofValue("testuser"))
            .password(Property.ofValue("testpass"))
            .document(Property.ofValue(Map.of("name", "Test Document")))
            .documents(Property.ofValue(List.of(Map.of("name", "Test Document 2"))))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        try {
            task.run(runContext);
            throw new AssertionError("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Cannot specify both"));
        }
    }

    @Test
    void shouldRejectNeitherDocumentNorDocuments() throws Exception {
        Insert task = Insert.builder()
            .id("test-reject-neither")
            .type(Insert.class.getName())
            .host(Property.ofValue("https://test-documentdb.com"))
            .database(Property.ofValue("testdb"))
            .collection(Property.ofValue("testcol"))
            .username(Property.ofValue("testuser"))
            .password(Property.ofValue("testpass"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        try {
            task.run(runContext);
            throw new AssertionError("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("must be provided"));
        }
    }

    @Test
    void shouldRejectTooManyDocuments() throws Exception {
        List<Map<String, Object>> tooManyDocs = List.of(
            Map.of("name", "Doc1"), Map.of("name", "Doc2"), Map.of("name", "Doc3"),
            Map.of("name", "Doc4"), Map.of("name", "Doc5"), Map.of("name", "Doc6"),
            Map.of("name", "Doc7"), Map.of("name", "Doc8"), Map.of("name", "Doc9"),
            Map.of("name", "Doc10"), Map.of("name", "Doc11") // 11 documents - exceeds limit
        );

        Insert task = Insert.builder()
            .id("test-too-many")
            .type(Insert.class.getName())
            .host(Property.ofValue("https://test-documentdb.com"))
            .database(Property.ofValue("testdb"))
            .collection(Property.ofValue("testcol"))
            .username(Property.ofValue("testuser"))
            .password(Property.ofValue("testpass"))
            .documents(Property.ofValue(tooManyDocs))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        try {
            task.run(runContext);
            throw new AssertionError("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Cannot insert more than"));
        }
    }

    @Test
    @Order(10)
    @DisplayName("Integration: Insert Single Document with Real DocumentDB")
    void shouldInsertSingleDocumentWithRealServer() throws Exception {
        Insert insertTask = Insert.builder()
            .id("insert-single-integration-test")
            .type(Insert.class.getName())
            .host(Property.ofValue(HOST))
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .document(Property.ofValue(Map.of(
                "_id", "single-test-doc-" + System.currentTimeMillis(),
                "name", "Integration Test Document",
                "value", 42,
                "active", true,
                "timestamp", System.currentTimeMillis()
            )))
            .build();

        RunContext insertContext = TestsUtils.mockRunContext(runContextFactory, insertTask, Map.of());
        Insert.Output insertOutput = insertTask.run(insertContext);

        assertThat("Document should be inserted", insertOutput.getInsertedCount(), equalTo(1));
        assertThat("Should return inserted ID", insertOutput.getInsertedIds(), hasSize(1));
        assertThat("Inserted ID should not be null", insertOutput.getInsertedId(), notNullValue());
    }

    @Test
    @Order(11)
    @DisplayName("Integration: Insert Multiple Documents with Real DocumentDB")
    void shouldInsertMultipleDocumentsWithRealServer() throws Exception {
        Insert insertTask = Insert.builder()
            .id("insert-multiple-integration-test")
            .type(Insert.class.getName())
            .host(Property.ofValue(HOST))
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .documents(Property.ofValue(List.of(
                Map.of("_id", "multi-test-doc-1-" + System.currentTimeMillis(), "name", "Document 1", "category", "A", "priority", 1),
                Map.of("_id", "multi-test-doc-2-" + System.currentTimeMillis(), "name", "Document 2", "category", "B", "priority", 2),
                Map.of("_id", "multi-test-doc-3-" + System.currentTimeMillis(), "name", "Document 3", "category", "A", "priority", 3)
            )))
            .build();

        RunContext insertContext = TestsUtils.mockRunContext(runContextFactory, insertTask, Map.of());
        Insert.Output insertOutput = insertTask.run(insertContext);

        assertThat("Should insert all documents", insertOutput.getInsertedCount(), equalTo(3));
        assertThat("Should return all inserted IDs", insertOutput.getInsertedIds(), hasSize(3));
        assertThat("First inserted ID should not be null", insertOutput.getInsertedId(), notNullValue());
    }

    @Test
    @Order(12)
    @DisplayName("Integration: Insert and Verify Document with Real DocumentDB")
    void shouldInsertAndVerifyDocumentWithRealServer() throws Exception {
        String uniqueId = "verify-test-doc-" + System.currentTimeMillis();

        // Insert a document
        Insert insertTask = Insert.builder()
            .id("insert-verify-integration-test")
            .type(Insert.class.getName())
            .host(Property.ofValue(HOST))
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .document(Property.ofValue(Map.of(
                "_id", uniqueId,
                "name", "Verify Integration Test Document",
                "value", 99,
                "active", true,
                "timestamp", System.currentTimeMillis()
            )))
            .build();

        RunContext insertContext = TestsUtils.mockRunContext(runContextFactory, insertTask, Map.of());
        Insert.Output insertOutput = insertTask.run(insertContext);

        assertThat("Document should be inserted", insertOutput.getInsertedCount(), equalTo(1));
        assertThat("Should return inserted ID", insertOutput.getInsertedIds(), hasSize(1));
        String insertedId = insertOutput.getInsertedIds().getFirst();
        assertThat("Inserted ID should match", insertedId, equalTo(uniqueId));

        // Verify the document was inserted by reading it back
        Read readTask = Read.builder()
            .id("read-verify-integration-test")
            .type(Read.class.getName())
            .host(Property.ofValue(HOST))
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
        assertThat("Document name should match", readOutput.getRow().get("name"), equalTo("Verify Integration Test Document"));
        assertThat("Document value should match", readOutput.getRow().get("value"), equalTo(99));
        assertThat("Document active flag should match", readOutput.getRow().get("active"), equalTo(true));
    }
}