package io.kestra.plugin.documentdb;

import java.io.IOException;
import java.io.InputStream;
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
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoCursor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class DocumentDBTaskCancellationTest {
    @Test
    @SuppressWarnings("unchecked")
    void shouldCloseActiveClientAndCursorWhenKilledRepeatedly() throws Exception {
        DocumentDBClient client = mock(DocumentDBClient.class);
        MongoCursor<String> cursor = mock(MongoCursor.class);
        TestDocumentDBTask task = new TestDocumentDBTask(client);
        CountDownLatch cursorInUse = new CountDownLatch(1);
        CountDownLatch finishOperation = new CountDownLatch(1);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> running = executor.submit(() -> task.runWithClient(activeClient -> task.runWithCursor(cursor, activeCursor ->
            {
                cursorInUse.countDown();
                finishOperation.await();
                return null;
            })
            ));

            assertTrue(cursorInUse.await(5, TimeUnit.SECONDS));
            task.kill();
            task.kill();

            verify(client, times(1)).close();
            verify(cursor, times(1)).close();

            finishOperation.countDown();
            running.get(5, TimeUnit.SECONDS);
        } finally {
            finishOperation.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void shouldCloseClientCreatedAfterTaskWasKilled() throws Exception {
        DocumentDBClient client = mock(DocumentDBClient.class);
        CountDownLatch creatingClient = new CountDownLatch(1);
        CountDownLatch finishCreation = new CountDownLatch(1);
        TestDocumentDBTask task = new TestDocumentDBTask(client, creatingClient, finishCreation);
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            Future<?> running = executor.submit(() -> task.runWithClient(activeClient -> null));
            assertTrue(creatingClient.await(5, TimeUnit.SECONDS));

            task.kill();
            finishCreation.countDown();

            ExecutionException exception = assertThrows(
                ExecutionException.class,
                () -> running.get(5, TimeUnit.SECONDS)
            );
            assertInstanceOf(CancellationException.class, exception.getCause());
            verify(client, times(1)).close();
        } finally {
            finishCreation.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void shouldCloseClientAfterOperationFailure() {
        DocumentDBClient client = mock(DocumentDBClient.class);
        TestDocumentDBTask task = new TestDocumentDBTask(client);

        assertThrows(IllegalStateException.class, () -> task.runWithClient(activeClient ->
        {
            throw new IllegalStateException("operation failed");
        }));

        verify(client).close();
    }

    @Test
    void shouldPreserveClientOperationFailureWhenCloseAlsoFails() {
        DocumentDBClient client = mock(DocumentDBClient.class);
        IllegalStateException operationFailure = new IllegalStateException("operation failed");
        IllegalArgumentException closeFailure = new IllegalArgumentException("close failed");
        doThrow(closeFailure).when(client).close();
        TestDocumentDBTask task = new TestDocumentDBTask(client);

        IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> task.runWithClient(activeClient ->
            {
                throw operationFailure;
            })
        );

        assertSame(operationFailure, thrown);
        assertEquals(1, thrown.getSuppressed().length);
        assertSame(closeFailure, thrown.getSuppressed()[0]);
        verify(client).close();
    }

    @Test
    void shouldCloseClientAfterOperationSucceeds() throws Exception {
        DocumentDBClient client = mock(DocumentDBClient.class);
        TestDocumentDBTask task = new TestDocumentDBTask(client);

        task.runWithClient(activeClient -> null);

        verify(client).close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldPreserveCursorOperationFailureWhenCloseAlsoFails() {
        MongoCursor<String> cursor = mock(MongoCursor.class);
        IllegalStateException operationFailure = new IllegalStateException("cursor operation failed");
        IllegalArgumentException closeFailure = new IllegalArgumentException("cursor close failed");
        doThrow(closeFailure).when(cursor).close();
        TestDocumentDBTask task = new TestDocumentDBTask(mock(DocumentDBClient.class));

        IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> task.runWithCursor(cursor, activeCursor ->
            {
                throw operationFailure;
            })
        );

        assertSame(operationFailure, thrown);
        assertEquals(1, thrown.getSuppressed().length);
        assertSame(closeFailure, thrown.getSuppressed()[0]);
        verify(cursor).close();
    }

    @Test
    void shouldCancelUriSubscriptionAndCloseReaderWhenKilledDuringRead() throws Exception {
        Insert.InputResource inputResource = new Insert.InputResource();
        TestDocumentDBTask task = new TestDocumentDBTask(mock(DocumentDBClient.class));
        task.track(inputResource);
        BlockingInputStream inputStream = new BlockingInputStream();
        AtomicReference<Boolean> cancelled = new AtomicReference<>(false);
        var records = Insert.readIonRecords(() -> inputStream, inputResource)
            .doOnCancel(() -> cancelled.set(true));
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            Future<?> consuming = executor.submit(() ->
            {
                try {
                    Stream<Map<String, Object>> stream = records.toStream(1);
                    inputResource.registerStream(stream);
                    stream.findFirst();
                } catch (CancellationException expected) {
                    // Cancellation while establishing the stream is expected.
                }
            });

            assertTrue(inputStream.readStarted.await(5, TimeUnit.SECONDS));
            task.kill();
            task.kill();

            consuming.get(5, TimeUnit.SECONDS);
            assertTrue(cancelled.get());
            assertEquals(1, inputStream.closeCount.get());
        } finally {
            task.kill();
            executor.shutdownNow();
        }
    }

    @Test
    void shouldCloseReaderCreatedAfterCancellationAndKeepRepeatedKillIdempotent() throws Exception {
        Insert.InputResource inputResource = new Insert.InputResource();
        TestDocumentDBTask task = new TestDocumentDBTask(mock(DocumentDBClient.class));
        task.track(inputResource);
        CountDownLatch openingReader = new CountDownLatch(1);
        CountDownLatch finishOpening = new CountDownLatch(1);
        BlockingInputStream inputStream = new BlockingInputStream();
        var records = Insert.readIonRecords(() ->
        {
            openingReader.countDown();
            try {
                finishOpening.await();
                return inputStream;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }, inputResource);
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            Future<?> consuming = executor.submit(() ->
            {
                try (Stream<Map<String, Object>> stream = records.toStream(1)) {
                    inputResource.registerStream(stream);
                    stream.findFirst();
                } catch (CancellationException expected) {
                    // Cancellation during resource creation is expected.
                }
            });

            assertTrue(openingReader.await(5, TimeUnit.SECONDS));
            task.kill();
            task.kill();
            finishOpening.countDown();

            consuming.get(5, TimeUnit.SECONDS);
            assertEquals(1, inputStream.closeCount.get());
        } finally {
            finishOpening.countDown();
            task.kill();
            executor.shutdownNow();
        }
    }

    private static class TestDocumentDBTask extends AbstractDocumentDBTask {
        private final DocumentDBClient client;
        private final CountDownLatch creatingClient;
        private final CountDownLatch finishCreation;

        private TestDocumentDBTask(DocumentDBClient client) {
            this(client, null, null);
        }

        private TestDocumentDBTask(
            DocumentDBClient client,
            CountDownLatch creatingClient,
            CountDownLatch finishCreation) {
            this.client = client;
            this.creatingClient = creatingClient;
            this.finishCreation = finishCreation;
        }

        @Override
        protected DocumentDBClient createDocumentDBClient(String connectionString, String caCertificate) throws Exception {
            if (creatingClient != null) {
                creatingClient.countDown();
                finishCreation.await();
            }
            return client;
        }

        private <T> T runWithClient(ClientOperation<T> operation) throws Exception {
            return withClient("mongodb://localhost/test", null, operation);
        }

        private <T, R> R runWithCursor(MongoCursor<T> cursor, CursorOperation<T, R> operation) throws Exception {
            return withCursor(cursor, operation);
        }

        private void track(AutoCloseable resource) {
            registerActiveResource(resource);
        }
    }

    private static final class BlockingInputStream extends InputStream {
        private final CountDownLatch readStarted = new CountDownLatch(1);
        private final CountDownLatch closed = new CountDownLatch(1);
        private final AtomicInteger closeCount = new AtomicInteger();

        @Override
        public int read() throws IOException {
            readStarted.countDown();
            try {
                closed.await();
                throw new IOException("Input stream closed while reading");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
            closed.countDown();
        }
    }
}
