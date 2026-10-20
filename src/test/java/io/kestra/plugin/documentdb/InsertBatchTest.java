package io.kestra.plugin.documentdb;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
    @SuppressWarnings("unchecked")
    void shouldInsertMoreThanTenFromInBatchesAndAggregateResultsInOrder() throws Exception {
        List<Map<String, Object>> input = documents(205);
        DocumentDBClient client = mockClientFor();
        Insert task = task(client, input);

        Insert.Output output = task.run(context(task));

        assertEquals(205, output.getInsertedCount());
        assertEquals(205, output.getInsertedIds().size());
        assertEquals("id-0", output.getInsertedId());
        assertEquals(
            input.stream().map(row -> (String) row.get("_id")).toList(),
            output.getInsertedIds()
        );
        ArgumentCaptor<List<Map<String, Object>>> batches = ArgumentCaptor.forClass(List.class);
        verify(client, org.mockito.Mockito.times(3)).insertMany(anyString(), anyString(), batches.capture());
        assertEquals(List.of(100, 100, 5), batches.getAllValues().stream().map(List::size).toList());
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
    void shouldReadDocumentsFromKestraStorageUri() throws Exception {
        List<Map<String, Object>> input = documents(12);
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

            assertEquals(12, output.getInsertedCount());
            assertEquals(input.stream().map(row -> (String) row.get("_id")).toList(), output.getInsertedIds());
            ArgumentCaptor<List<Map<String, Object>>> batches = ArgumentCaptor.forClass(List.class);
            verify(client).insertMany(anyString(), anyString(), batches.capture());
            assertEquals(List.of(12), batches.getAllValues().stream().map(List::size).toList());
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
    void shouldPropagateLaterBatchFailureWithoutPartialOutputAndCloseClient() throws Exception {
        DocumentDBException failure = new DocumentDBException("insert failed");
        AtomicInteger calls = new AtomicInteger();
        DocumentDBClient client = mockClientFor();
        when(client.insertMany(anyString(), anyString(), anyList()))
            .thenAnswer(invocation ->
            {
                if (calls.incrementAndGet() == 2) {
                    throw failure;
                }
                return resultFor(invocation.getArgument(2));
            });
        Insert task = task(client, documents(205));

        DocumentDBException thrown = assertThrows(
            DocumentDBException.class,
            () -> task.run(context(task))
        );

        assertSame(failure, thrown);
        verify(client, org.mockito.Mockito.times(2)).insertMany(anyString(), anyString(), anyList());
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
    void shouldReleaseStreamAfterSuccessfulInsertion() throws Exception {
        AtomicInteger resourceCloseCount = new AtomicInteger();
        AtomicReference<SignalType> cleanupSignal = new AtomicReference<>();
        Flux<Map<String, Object>> records = Flux.using(
            () -> resourceCloseCount,
            ignored -> Flux.fromIterable(documents(2)).doFinally(cleanupSignal::set),
            ignored -> resourceCloseCount.incrementAndGet()
        );
        DocumentDBClient client = mockClientFor();
        Insert task = task(client, List.of());

        Insert.Output output = task.insertDocuments(records, client, DATABASE, COLLECTION);

        assertEquals(2, output.getInsertedCount());
        assertEquals(1, resourceCloseCount.get());
        assertEquals(SignalType.ON_COMPLETE, cleanupSignal.get());
    }

    @Test
    void shouldCancelAndCloseStreamWhenTaskIsKilledDuringBatchInsert() throws Exception {
        CountDownLatch insertStarted = new CountDownLatch(1);
        CountDownLatch finishInsert = new CountDownLatch(1);
        AtomicReference<SignalType> cleanupSignal = new AtomicReference<>();
        DocumentDBClient client = mock(DocumentDBClient.class);
        Insert task = task(client, List.of());
        when(client.insertMany(anyString(), anyString(), anyList())).thenAnswer(invocation ->
        {
            insertStarted.countDown();
            finishInsert.await();
            return resultFor(invocation.getArgument(2));
        });
        Flux<Map<String, Object>> records = Flux.range(0, Integer.MAX_VALUE)
            .map(index -> Map.<String, Object> of("_id", "id-" + index))
            .doFinally(cleanupSignal::set);
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            Future<Insert.Output> insertion = executor.submit(() -> task.insertDocuments(records, client, DATABASE, COLLECTION));
            assertTrue(insertStarted.await(5, TimeUnit.SECONDS));
            task.kill();
            finishInsert.countDown();

            ExecutionException exception = assertThrows(
                ExecutionException.class,
                () -> insertion.get(5, TimeUnit.SECONDS)
            );
            assertInstanceOf(CancellationException.class, exception.getCause());
            assertEquals(SignalType.CANCEL, cleanupSignal.get());
            verify(client).insertMany(anyString(), anyString(), anyList());
        } finally {
            finishInsert.countDown();
            task.kill();
            executor.shutdownNow();
        }
    }

    @Test
    void shouldNotReturnPartialOutputWhenKilledAfterFiniteInputCompletes() throws Exception {
        AtomicReference<SignalType> cleanupSignal = new AtomicReference<>();
        DocumentDBClient client = mock(DocumentDBClient.class);
        Insert task = task(client, List.of());
        when(client.insertMany(anyString(), anyString(), anyList())).thenAnswer(invocation ->
        {
            task.kill();
            return resultFor(invocation.getArgument(2));
        });
        Flux<Map<String, Object>> records = Flux.fromIterable(documents(100))
            .doFinally(cleanupSignal::set);

        assertThrows(
            CancellationException.class,
            () -> task.insertDocuments(records, client, DATABASE, COLLECTION)
        );

        assertEquals(SignalType.ON_COMPLETE, cleanupSignal.get());
        verify(client).insertMany(anyString(), anyString(), anyList());
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

}
