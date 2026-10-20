package io.kestra.plugin.documentdb;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.documentdb.models.DocumentDBException;
import io.kestra.plugin.documentdb.models.InsertResult;

import jakarta.inject.Inject;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@KestraTest
class InsertBatchTest {
    private static final String DATABASE = "test_db";
    private static final String COLLECTION = "insert_batch_test";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    @SuppressWarnings("unchecked")
    void shouldInsertTenInlineDocumentsInOneCallAndKeepOutputShape() throws Exception {
        List<Map<String, Object>> input = documents(10);
        DocumentDBClient client = mockClientFor();
        Insert task = task(client, input);

        Insert.Output output = task.run(context(task));

        assertEquals(10, output.getInsertedCount());
        assertEquals(10, output.getInsertedIds().size());
        assertEquals("id-0", output.getInsertedId());
        ArgumentCaptor<List<Map<String, Object>>> batches = ArgumentCaptor.forClass(List.class);
        verify(client).insertMany(anyString(), anyString(), batches.capture());
        assertEquals(List.of(10), batches.getAllValues().stream().map(List::size).toList());
        verify(client).close();
    }

    @Test
    void shouldInsertDocumentsFromJsonString() throws Exception {
        DocumentDBClient client = mockClientFor();
        Insert task = task(client, "[{\"_id\":\"json-id\",\"name\":\"JSON document\"}]");

        Insert.Output output = task.run(context(task));

        assertEquals(1, output.getInsertedCount());
        assertEquals(List.of("json-id"), output.getInsertedIds());
        verify(client).insertMany(anyString(), anyString(), anyList());
        verify(client).close();
    }

    @Test
    void shouldRenderTemplatesInInlineFromDocuments() throws Exception {
        DocumentDBClient client = mockClientFor();
        Insert task = task(
            client,
            List.of(new LinkedHashMap<>(Map.of("_id", "{{ inputs.user_id }}", "name", "Templated document")))
        );
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of("user_id", "rendered-id"));

        assertEquals("rendered-id", runContext.render(Map.of("_id", "{{ inputs.user_id }}")).get("_id"));
        Insert.Output output = task.run(runContext);

        assertEquals("rendered-id", output.getInsertedId());
        verify(client).insertMany(anyString(), anyString(), anyList());
        verify(client).close();
    }

    @Test
    void shouldRejectElevenLegacyDocumentsBeforeCreatingClientOrWriting() throws Exception {
        DocumentDBClient client = mock(DocumentDBClient.class);
        Insert task = legacyTask(client, documents(11));

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.run(context(task))
        );

        assertTrue(exception.getMessage().contains("Cannot insert more than 10"));
        verify(client, never()).insertMany(anyString(), anyString(), anyList());
        verify(client, never()).close();
    }

    @Test
    void shouldRejectElevenFromDocumentsBeforeCreatingClientOrWriting() throws Exception {
        DocumentDBClient client = mock(DocumentDBClient.class);
        Insert task = task(client, documents(11));

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.run(context(task))
        );

        assertTrue(exception.getMessage().contains("Cannot insert more than 10"));
        verify(client, never()).insertMany(anyString(), anyString(), anyList());
        verify(client, never()).close();
    }

    @Test
    void shouldReadDocumentsFromKestraStorageUri() throws Exception {
        List<Map<String, Object>> input = documents(3);
        DocumentDBClient client = mockClientFor();
        Path tempFile = Files.createTempFile("documentdb-input", ".ion");
        try {
            try (OutputStream outputStream = Files.newOutputStream(tempFile)) {
                FileSerde.writeAll(outputStream, Flux.fromIterable(input)).block();
            }
            Insert seedTask = task(client, List.of());
            String inputUri = context(seedTask).storage().putFile(tempFile.toFile()).toString();
            Insert task = task(client, inputUri);

            Insert.Output output = task.run(context(task));

            assertEquals(3, output.getInsertedCount());
            assertEquals(3, output.getInsertedIds().size());
            verify(client).insertMany(anyString(), anyString(), anyList());
            verify(client).close();
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    @Test
    void shouldRejectElevenDocumentsFromKestraStorageUriBeforeWriting() throws Exception {
        DocumentDBClient client = mock(DocumentDBClient.class);
        List<Map<String, Object>> input = documents(11);
        Insert seedTask = task(client, List.of());
        Path tempFile = Files.createTempFile("documentdb-oversized-input", ".ion");
        try {
            try (OutputStream outputStream = Files.newOutputStream(tempFile)) {
                FileSerde.writeAll(outputStream, Flux.fromIterable(input)).block();
            }
            String inputUri = context(seedTask).storage().putFile(tempFile.toFile()).toString();
            Insert task = task(client, inputUri);

            IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> task.run(context(task))
            );

            assertTrue(exception.getMessage().contains("Cannot insert more than 10"));
            verify(client, never()).insertMany(anyString(), anyString(), anyList());
            verify(client).close();
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    @Test
    void shouldRejectEmptyLegacyDocumentsInput() throws Exception {
        DocumentDBClient client = mock(DocumentDBClient.class);
        Insert task = legacyTask(client, List.of());

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.run(context(task))
        );

        assertEquals(
            "No document to insert: set one of 'document', 'from', or 'documents'",
            exception.getMessage()
        );
        verify(client, never()).insertMany(anyString(), anyString(), anyList());
        verify(client, never()).close();
    }

    @Test
    void shouldRejectMissingInputSources() throws Exception {
        Insert task = Insert.builder()
            .id("missing-insert-input")
            .type(Insert.class.getName())
            .connectionString(Property.ofValue("mongodb://localhost:27017/test_db"))
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .build();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.run(context(task))
        );

        assertEquals(
            "No document to insert: set one of 'document', 'from', or 'documents'",
            exception.getMessage()
        );
    }

    @Test
    void shouldRejectEmptyFromListInput() throws Exception {
        DocumentDBClient client = mock(DocumentDBClient.class);
        Insert task = task(client, List.of());

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.run(context(task))
        );

        assertTrue(exception.getMessage().contains("'document', 'from', or 'documents'"));
        verify(client, never()).insertMany(anyString(), anyString(), anyList());
        verify(client, never()).close();
    }

    @Test
    void shouldRejectEmptyFromPayload() throws Exception {
        DocumentDBClient client = mock(DocumentDBClient.class);
        Insert task = task(client, "[]");

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.run(context(task))
        );

        assertTrue(exception.getMessage().contains("'document', 'from', or 'documents'"));
        verify(client, never()).insertMany(anyString(), anyString(), anyList());
        verify(client).close();
    }

    @Test
    void shouldPropagateInsertFailureWithoutPartialOutputAndCloseClient() throws Exception {
        DocumentDBException failure = new DocumentDBException("insert failed");
        DocumentDBClient client = mock(DocumentDBClient.class);
        when(client.insertMany(anyString(), anyString(), anyList())).thenThrow(failure);
        Insert task = task(client, documents(10));

        DocumentDBException thrown = assertThrows(
            DocumentDBException.class,
            () -> task.run(context(task))
        );

        assertSame(failure, thrown);
        verify(client).insertMany(anyString(), anyString(), anyList());
        verify(client).close();
    }

    @Test
    void shouldPreferNonEmptyDocumentOverEmptyDocuments() throws Exception {
        DocumentDBClient client = mock(DocumentDBClient.class);
        when(client.insertOne(anyString(), anyString(), anyMap())).thenReturn(new InsertResult(List.of("single-id"), 1));
        Insert task = combinedTask(client, Map.of("_id", "single-id"), List.of());

        Insert.Output output = task.run(context(task));

        assertEquals("single-id", output.getInsertedId());
        assertEquals(List.of("single-id"), output.getInsertedIds());
        assertEquals(1, output.getInsertedCount());
        verify(client).insertOne(anyString(), anyString(), anyMap());
        verify(client, never()).insertMany(anyString(), anyString(), anyList());
    }

    @Test
    void shouldUseNonEmptyDocumentsWhenDocumentIsEmpty() throws Exception {
        DocumentDBClient client = mockClientFor();
        Insert task = combinedTask(client, Map.of(), documents(1));

        Insert.Output output = task.run(context(task));

        assertEquals(1, output.getInsertedCount());
        assertEquals("id-0", output.getInsertedId());
        verify(client).insertMany(anyString(), anyString(), anyList());
        verify(client, never()).insertOne(anyString(), anyString(), anyMap());
    }

    @Test
    void shouldRejectBothNonEmptyRenderedInputsBeforeCreatingClient() throws Exception {
        DocumentDBClient client = mock(DocumentDBClient.class);
        Insert task = combinedTask(client, Map.of("_id", "single-id"), documents(1));

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.run(context(task))
        );

        assertTrue(exception.getMessage().contains("Cannot specify both"));
        verify(client, never()).insertOne(anyString(), anyString(), anyMap());
        verify(client, never()).insertMany(anyString(), anyString(), anyList());
        verify(client, never()).close();
    }

    @Test
    void shouldRejectBothEmptyRenderedInputsBeforeCreatingClient() throws Exception {
        DocumentDBClient client = mock(DocumentDBClient.class);
        Insert task = combinedTask(client, Map.of(), List.of());

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.run(context(task))
        );

        assertTrue(exception.getMessage().contains("'document', 'from', or 'documents'"));
        verify(client, never()).insertOne(anyString(), anyString(), anyMap());
        verify(client, never()).insertMany(anyString(), anyString(), anyList());
        verify(client, never()).close();
    }

    @Test
    void shouldCancelOverLimitInputWithoutAttemptingAnInsert() throws Exception {
        AtomicReference<SignalType> cleanupSignal = new AtomicReference<>();
        Flux<Map<String, Object>> records = Flux.range(0, Integer.MAX_VALUE)
            .map(index -> Map.<String, Object> of("_id", "id-" + index))
            .doFinally(cleanupSignal::set);
        DocumentDBClient client = mock(DocumentDBClient.class);
        Insert task = task(client, List.of());

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.insertDocuments(records, client, DATABASE, COLLECTION)
        );

        assertTrue(exception.getMessage().contains("Cannot insert more than 10"));
        assertEquals(SignalType.CANCEL, cleanupSignal.get());
        verify(client, never()).insertMany(anyString(), anyString(), anyList());
    }

    @Test
    void shouldBoundUriReadAndCloseSourceWhenInputExceedsLimit() throws Exception {
        RepeatingIonInputStream input = new RepeatingIonInputStream(100_000);
        AtomicInteger parsedRecords = new AtomicInteger();
        Flux<Map<String, Object>> records = Insert.readIonRecords(() -> input)
            .doOnNext(ignored -> parsedRecords.incrementAndGet());
        DocumentDBClient client = mock(DocumentDBClient.class);
        Insert task = task(client, List.of());

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.insertDocuments(records, client, DATABASE, COLLECTION)
        );

        assertTrue(exception.getMessage().contains("Cannot insert more than 10"));
        assertTrue(parsedRecords.get() <= 11, "URI parser should stop after one record beyond the maximum");
        assertTrue(input.bytesRead() < input.totalBytes() / 2, "URI reader should stop before consuming the complete source");
        assertEquals(1, input.closeCount());
        verify(client, never()).insertMany(anyString(), anyString(), anyList());
    }

    @Test
    void shouldCloseIonSourceOnNormalCompletionAndParsingFailure() {
        RepeatingIonInputStream completeInput = new RepeatingIonInputStream(3);

        List<Map<String, Object>> output = Insert.readIonRecords(() -> completeInput).collectList().block();

        assertNotNull(output);
        assertEquals(3, output.size());
        assertEquals(1, completeInput.closeCount());

        CloseTrackingByteArrayInputStream invalidInput = new CloseTrackingByteArrayInputStream("not valid Ion".getBytes(StandardCharsets.UTF_8));
        assertThrows(RuntimeException.class, () -> Insert.readIonRecords(() -> invalidInput).collectList().block());
        assertEquals(1, invalidInput.closeCount());
    }

    @Test
    void shouldSupportTheLegacyDocumentsProperty() throws Exception {
        List<Map<String, Object>> input = documents(2);
        DocumentDBClient client = mockClientFor();
        Insert task = legacyTask(client, input);

        Insert.Output output = task.run(context(task));

        assertEquals(2, output.getInsertedCount());
        assertEquals(2, output.getInsertedIds().size());
        assertEquals("id-0", output.getInsertedId());
        verify(client).insertMany(anyString(), anyString(), anyList());
        verify(client).close();
    }

    private Insert task(DocumentDBClient client, Object from) throws Exception {
        Insert task = spy(
            Insert.builder()
                .id("batch-insert")
                .type(Insert.class.getName())
                .connectionString(Property.ofValue("mongodb://localhost/test"))
                .database(Property.ofValue(DATABASE))
                .collection(Property.ofValue(COLLECTION))
                .from(from)
                .build()
        );
        doReturn(client).when(task).createDocumentDBClient(anyString(), org.mockito.ArgumentMatchers.isNull());
        return task;
    }

    private Insert legacyTask(DocumentDBClient client, List<Map<String, Object>> input) throws Exception {
        Insert task = spy(
            Insert.builder()
                .id("legacy-documents")
                .type(Insert.class.getName())
                .connectionString(Property.ofValue("mongodb://localhost/test"))
                .database(Property.ofValue(DATABASE))
                .collection(Property.ofValue(COLLECTION))
                .documents(Property.ofValue(input))
                .build()
        );
        doReturn(client).when(task).createDocumentDBClient(anyString(), org.mockito.ArgumentMatchers.isNull());
        return task;
    }

    private Insert combinedTask(
        DocumentDBClient client,
        Map<String, Object> document,
        List<Map<String, Object>> documents) throws Exception {
        Insert task = spy(
            Insert.builder()
                .id("combined-input-insert")
                .type(Insert.class.getName())
                .connectionString(Property.ofValue("mongodb://localhost/test"))
                .database(Property.ofValue(DATABASE))
                .collection(Property.ofValue(COLLECTION))
                .document(Property.ofValue(document))
                .documents(Property.ofValue(documents))
                .build()
        );
        doReturn(client).when(task).createDocumentDBClient(anyString(), org.mockito.ArgumentMatchers.isNull());
        return task;
    }

    private RunContext context(Insert task) {
        return TestsUtils.mockRunContext(runContextFactory, task, Map.of());
    }

    private DocumentDBClient mockClientFor() throws Exception {
        DocumentDBClient client = mock(DocumentDBClient.class);
        when(client.insertMany(anyString(), anyString(), anyList()))
            .thenAnswer(invocation -> resultFor(invocation.getArgument(2)));
        return client;
    }

    private InsertResult resultFor(List<Map<String, Object>> batch) {
        List<String> ids = batch.stream().map(row -> (String) row.get("_id")).toList();
        return new InsertResult(ids, ids.size());
    }

    private List<Map<String, Object>> documents(int count) {
        List<Map<String, Object>> documents = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            documents.add(Map.of("_id", "id-" + i, "value", i));
        }
        return documents;
    }

    private static final class RepeatingIonInputStream extends InputStream {
        private static final byte[] RECORD = "{\"_id\":\"id\",\"value\":1}\n".getBytes(StandardCharsets.UTF_8);

        private final long totalRecords;
        private final AtomicLong bytesRead = new AtomicLong();
        private final AtomicInteger closeCount = new AtomicInteger();
        private long remainingRecords;
        private int recordOffset;

        private RepeatingIonInputStream(long totalRecords) {
            this.totalRecords = totalRecords;
            this.remainingRecords = totalRecords;
        }

        @Override
        public int read() {
            byte[] next = new byte[1];
            return read(next, 0, 1) == -1 ? -1 : next[0] & 0xff;
        }

        @Override
        public int read(byte[] target, int offset, int length) {
            if (length == 0) {
                return 0;
            }
            if (remainingRecords == 0) {
                return -1;
            }

            int written = 0;
            while (written < length && remainingRecords > 0) {
                int amount = Math.min(length - written, RECORD.length - recordOffset);
                System.arraycopy(RECORD, recordOffset, target, offset + written, amount);
                written += amount;
                recordOffset += amount;
                if (recordOffset == RECORD.length) {
                    recordOffset = 0;
                    remainingRecords--;
                }
            }
            bytesRead.addAndGet(written);
            return written;
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }

        private long bytesRead() {
            return bytesRead.get();
        }

        private long totalBytes() {
            return totalRecords * RECORD.length;
        }

        private int closeCount() {
            return closeCount.get();
        }
    }

    private static final class CloseTrackingByteArrayInputStream extends ByteArrayInputStream {
        private final AtomicInteger closeCount = new AtomicInteger();

        private CloseTrackingByteArrayInputStream(byte[] bytes) {
            super(bytes);
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }

        private int closeCount() {
            return closeCount.get();
        }
    }
}
