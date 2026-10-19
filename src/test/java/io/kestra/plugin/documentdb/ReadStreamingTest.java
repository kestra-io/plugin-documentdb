package io.kestra.plugin.documentdb;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import com.mongodb.MongoException;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.documentdb.models.DocumentDBException;

import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@KestraTest
class ReadStreamingTest {

    private static final String DATABASE = "test_db";
    private static final String COLLECTION = "streaming_test";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    @SuppressWarnings("unchecked")
    void shouldCollectFetchResultsAndCloseCursor() throws Exception {
        MongoCursor<Document> cursor = cursor(
            new Document("_id", "first").append("name", "First"),
            new Document("_id", "second").append("name", "Second")
        );
        FindIterable<Document> iterable = mock(FindIterable.class);
        when(iterable.iterator()).thenReturn(cursor);
        DocumentDBClient client = client();
        when(client.findIterable(anyString(), anyString(), any(), any(), any())).thenReturn(iterable);

        Read.Output output = runTask(FetchType.FETCH, client);

        assertEquals(2L, output.getSize());
        assertEquals(
            List.of(Map.of("_id", "first", "name", "First"), Map.of("_id", "second", "name", "Second")),
            output.getRows()
        );
        verify(cursor).close();
        verify(client, never()).find(anyString(), anyString(), any(), any(), any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldLimitFetchOneAtTheServer() throws Exception {
        MongoCursor<Document> cursor = cursor(
            new Document("_id", "first").append("name", "First"),
            new Document("_id", "second").append("name", "Second")
        );
        FindIterable<Document> iterable = mock(FindIterable.class);
        when(iterable.limit(1)).thenReturn(iterable);
        when(iterable.iterator()).thenReturn(cursor);
        DocumentDBClient client = client();
        when(client.findIterable(anyString(), anyString(), any(), any(), any())).thenReturn(iterable);

        Read.Output output = runTask(FetchType.FETCH_ONE, client);

        assertEquals(1L, output.getSize());
        assertEquals(Map.of("_id", "first", "name", "First"), output.getRow());
        verify(iterable).limit(1);
        verify(cursor, times(1)).next();
        verify(cursor).close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldStreamAggregationResultsToStorage() throws Exception {
        MongoCursor<Document> cursor = cursor(
            new Document("_id", "first").append("value", 1),
            new Document("_id", "second").append("value", 2)
        );
        AggregateIterable<Document> iterable = mock(AggregateIterable.class);
        when(iterable.iterator()).thenReturn(cursor);
        DocumentDBClient client = client();
        when(client.aggregateIterable(anyString(), anyString(), anyList())).thenReturn(iterable);

        Read task = createTask(FetchType.STORE, List.of(Map.of("$match", Map.of("active", true))));
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        Read.Output output = task.run(runContext, client);

        assertEquals(2L, output.getSize());
        assertNotNull(output.getUri());
        try (InputStream storedFile = runContext.storage().getFile(output.getUri())) {
            List<Object> storedRows = FileSerde.readAll(storedFile).collectList().block();
            assertNotNull(storedRows);
            assertEquals(2, storedRows.size());
            assertInstanceOf(Map.class, storedRows.getFirst());
            assertEquals("first", ((Map<?, ?>) storedRows.getFirst()).get("_id"));
            assertEquals("second", ((Map<?, ?>) storedRows.get(1)).get("_id"));
        }
        verify(cursor, times(3)).hasNext();
        verify(cursor).close();
        verify(client, never()).aggregate(anyString(), anyString(), anyList());
        verify(client, never()).find(anyString(), anyString(), any(), any(), any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldConsumeOnlyOneAggregationResultForFetchOne() throws Exception {
        MongoCursor<Document> cursor = cursor(
            new Document("_id", "first").append("count", 3),
            new Document("_id", "second").append("count", 4)
        );
        AggregateIterable<Document> iterable = mock(AggregateIterable.class);
        when(iterable.iterator()).thenReturn(cursor);
        DocumentDBClient client = client();
        when(client.aggregateIterable(anyString(), anyString(), anyList())).thenReturn(iterable);

        Read.Output output = runTask(FetchType.FETCH_ONE, client, List.of(Map.of("$group", Map.of("_id", "$type"))));

        assertEquals(Map.of("_id", "first", "count", 3), output.getRow());
        verify(client).aggregateIterable(anyString(), anyString(), anyList());
        verify(cursor, times(1)).next();
        verify(cursor).close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldCountFindResultsWithoutCollectingThemForNone() throws Exception {
        MongoCursor<Document> cursor = cursor(
            new Document("_id", "first"),
            new Document("_id", "second")
        );
        FindIterable<Document> iterable = mock(FindIterable.class);
        when(iterable.iterator()).thenReturn(cursor);
        DocumentDBClient client = client();
        when(client.findIterable(anyString(), anyString(), any(), any(), any())).thenReturn(iterable);

        Read.Output output = runTask(FetchType.NONE, client);

        assertEquals(2L, output.getSize());
        assertNull(output.getRows());
        assertNull(output.getRow());
        assertNull(output.getUri());
        verify(client).findIterable(anyString(), anyString(), any(), any(), any());
        verify(client, never()).aggregateIterable(anyString(), anyString(), anyList());
        verify(cursor, times(2)).next();
        verify(cursor).close();
        verify(client, never()).toRecord(any(Document.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldCountAggregationOutputRowsWithoutCollectingThemForNone() throws Exception {
        MongoCursor<Document> cursor = cursor(
            new Document("_id", "active").append("count", 3),
            new Document("_id", "inactive").append("count", 1)
        );
        AggregateIterable<Document> iterable = mock(AggregateIterable.class);
        when(iterable.iterator()).thenReturn(cursor);
        DocumentDBClient client = client();
        when(client.aggregateIterable(anyString(), anyString(), anyList())).thenReturn(iterable);

        Read.Output output = runTask(FetchType.NONE, client, List.of(Map.of("$group", Map.of("_id", "$status"))));

        assertEquals(2L, output.getSize());
        assertNull(output.getRows());
        assertNull(output.getRow());
        assertNull(output.getUri());
        verify(client).aggregateIterable(anyString(), anyString(), anyList());
        verify(client, never()).findIterable(anyString(), anyString(), any(), any(), any());
        verify(cursor, times(2)).next();
        verify(cursor).close();
        verify(client, never()).toRecord(any(Document.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldCloseCursorWhenFetchingFails() throws Exception {
        MongoCursor<Document> cursor = mock(MongoCursor.class);
        when(cursor.hasNext()).thenThrow(new MongoException("cursor failed"));
        FindIterable<Document> iterable = mock(FindIterable.class);
        when(iterable.iterator()).thenReturn(cursor);
        DocumentDBClient client = client();
        when(client.findIterable(anyString(), anyString(), any(), any(), any())).thenReturn(iterable);

        assertThrows(DocumentDBException.class, () -> runTask(FetchType.FETCH, client));

        verify(cursor).close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldCloseStoreCursorWhenSerializationFails() throws Exception {
        MongoCursor<Document> cursor = cursor(new Document("_id", "invalid"));
        AggregateIterable<Document> iterable = mock(AggregateIterable.class);
        when(iterable.iterator()).thenReturn(cursor);
        DocumentDBClient client = client();
        when(client.aggregateIterable(anyString(), anyString(), anyList())).thenReturn(iterable);
        org.mockito.Mockito.doThrow(new IllegalStateException("conversion failed"))
            .when(client).toRecord(any(Document.class));

        assertThrows(
            IllegalStateException.class,
            () -> runTask(FetchType.STORE, client, List.of(Map.of("$match", Map.of("active", true))))
        );

        verify(cursor).close();
    }

    private Read.Output runTask(FetchType fetchType, DocumentDBClient client) throws Exception {
        return runTask(fetchType, client, null);
    }

    private Read.Output runTask(
        FetchType fetchType,
        DocumentDBClient client,
        List<Map<String, Object>> pipeline
    ) throws Exception {
        Read task = createTask(fetchType, pipeline);
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        return task.run(runContext, client);
    }

    private Read createTask(FetchType fetchType, List<Map<String, Object>> pipeline) {
        return Read.builder()
            .id("streaming-read")
            .type(Read.class.getName())
            .connectionString(Property.ofValue("mongodb://localhost:27017/test_db"))
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .aggregationPipeline(pipeline == null ? null : Property.ofValue(pipeline))
            .fetchType(Property.ofValue(fetchType))
            .build();
    }

    @SuppressWarnings("unchecked")
    private MongoCursor<Document> cursor(Document... documents) {
        MongoCursor<Document> cursor = mock(MongoCursor.class);
        AtomicInteger index = new AtomicInteger();
        when(cursor.hasNext()).thenAnswer(invocation -> index.get() < documents.length);
        when(cursor.next()).thenAnswer(invocation -> documents[index.getAndIncrement()]);
        return cursor;
    }

    private DocumentDBClient client() {
        DocumentDBClient client = mock(DocumentDBClient.class);
        doCallRealMethod().when(client).toRecord(any(Document.class));
        return client;
    }
}
