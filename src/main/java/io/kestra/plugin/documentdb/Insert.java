package io.kestra.plugin.documentdb;

import java.io.BufferedInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.reactivestreams.Subscription;
import org.slf4j.Logger;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingIterator;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.property.URIFetcher;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.documentdb.models.InsertResult;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Insert documents into DocumentDB",
    description = "Insert one document or a stream of documents from inline data or a Kestra-supported URI."
)
@Plugin(
    examples = {
        @Example(
            title = "Insert a single user document",
            full = true,
            code = """
                id: insert_documentdb_user
                namespace: company.documentdb

                tasks:
                  - id: insert_user
                    type: io.kestra.plugin.documentdb.Insert
                    connectionString: "{{ secret('DOCUMENTDB_CONNECTION_STRING') }}"
                    database: "myapp"
                    collection: "users"
                    document:
                      name: "John Doe"
                      email: "john.doe@example.com"
                      age: 30
                      created_at: "{{ now() }}"
                """
        ),
        @Example(
            title = "Insert multiple product documents",
            full = true,
            code = """
                id: insert_products
                namespace: company.documentdb

                tasks:
                  - id: insert_product_batch
                    type: io.kestra.plugin.documentdb.Insert
                    connectionString: "{{ secret('DOCUMENTDB_CONNECTION_STRING') }}"
                    database: "inventory"
                    collection: "products"
                    from:
                      - name: "Laptop"
                        price: 999.99
                        category: "Electronics"
                        in_stock: true
                      - name: "Mouse"
                        price: 29.99
                        category: "Electronics"
                        in_stock: false
                """
        ),
        @Example(
            title = "Insert document with dynamic data",
            full = true,
            code = """
                id: insert_dynamic_order
                namespace: company.documentdb

                inputs:
                  - id: customer_id
                    type: STRING
                    required: true
                  - id: product_name
                    type: STRING
                    required: true
                  - id: quantity
                    type: INT
                    required: true

                tasks:
                  - id: insert_order
                    type: io.kestra.plugin.documentdb.Insert
                    connectionString: "{{ secret('DOCUMENTDB_CONNECTION_STRING') }}"
                    database: "sales"
                    collection: "orders"
                    document:
                      customer_id: "{{ inputs.customer_id }}"
                      product: "{{ inputs.product_name }}"
                      quantity: "{{ inputs.quantity }}"
                      order_date: "{{ now() }}"
                      status: "pending"
                """
        )
    }
)
public class Insert extends AbstractDocumentDBTask implements RunnableTask<Insert.Output>, Data.From {
    private static final int MAX_DOCUMENTS_PER_INSERT = 10;
    private static final TypeReference<Map<String, Object>> DOCUMENT_TYPE = new TypeReference<>() {
    };

    @Schema(
        title = "Single document payload",
        description = "Document to insert when sending one record. Mutually exclusive with `from` and legacy `documents`; values are rendered before execution."
    )
    @PluginProperty(group = "advanced")
    private Property<Map<String, Object>> document;

    @Schema(
        title = Data.From.TITLE,
        description = Data.From.DESCRIPTION,
        anyOf = { String.class, List.class, Map.class }
    )
    @PluginProperty(group = "main")
    private Object from;

    @Schema(
        title = "Batch documents (legacy)",
        description = "Legacy inline batch input, limited to 10 documents. Prefer `from`, which also supports Kestra storage URIs."
    )
    @PluginProperty(group = "advanced")
    private Property<List<Map<String, Object>>> documents;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        // Render properties
        String rConnectionString = resolveConnectionString(runContext);
        String rCaCertificate = this.caCertificate == null ? null : runContext.render(this.caCertificate).as(String.class).orElse(null);
        String rDatabase = runContext.render(this.database).as(String.class).orElseThrow();
        String rCollection = renderCollection(runContext);
        Map<String, Object> rDocument = runContext.render(this.document).asMap(String.class, Object.class);
        Object rFrom = this.from == null ? null : runContext.render(Property.ofValue(this.from)).as(Object.class).orElse(null);
        Object rDocuments = this.documents == null ? null : runContext.render(this.documents).asList(Map.class);

        boolean hasDocument = rDocument != null && !rDocument.isEmpty();
        boolean hasFrom = hasValue(rFrom);
        boolean hasDocuments = rDocuments instanceof List<?> documents && !documents.isEmpty();
        int inputCount = (hasDocument ? 1 : 0) + (hasFrom ? 1 : 0) + (hasDocuments ? 1 : 0);
        if (inputCount == 0) {
            throw new IllegalArgumentException("No document to insert: set one of 'document', 'from', or 'documents'");
        }
        if (inputCount > 1) {
            throw new IllegalArgumentException("Cannot specify both 'document' and batch inputs; choose only one of 'document', 'from', or 'documents'");
        }

        Object source = hasFrom ? rFrom : rDocuments;
        if (!hasDocument && source instanceof List<?> list && list.size() > MAX_DOCUMENTS_PER_INSERT) {
            throw tooManyDocuments();
        }

        return withClient(rConnectionString, rCaCertificate, client ->
        {
            if (hasDocument) {
                // Insert single document
                logger.info("Inserting single document into DocumentDB database: {} collection: {}", rDatabase, rCollection);

                InsertResult result = client.insertOne(rDatabase, rCollection, rDocument);

                logger.info("Successfully inserted document with ID: {}", result.getInsertedIds().getFirst());

                return Output.builder()
                    .insertedId(result.getInsertedIds().getFirst())
                    .insertedIds(result.getInsertedIds())
                    .insertedCount(result.getInsertedCount())
                    .build();
            }

            logger.info("Streaming document input into DocumentDB database: {} collection: {}", rDatabase, rCollection);
            if (source instanceof String uri && URIFetcher.supports(uri)) {
                InputResource inputResource = registerActiveResource(new InputResource());
                try {
                    Output output = insertDocuments(readUri(uri, runContext, inputResource), client, rDatabase, rCollection, inputResource);
                    logger.info("Successfully inserted {} documents", output.getInsertedCount());
                    return output;
                } finally {
                    releaseActiveResource(inputResource);
                }
            }

            Output output = insertDocuments(Data.from(source).read(runContext), client, rDatabase, rCollection);
            logger.info("Successfully inserted {} documents", output.getInsertedCount());
            return output;
        });
    }

    private static Flux<Map<String, Object>> readUri(String uri, RunContext runContext, InputResource inputResource) {
        return readIonRecords(() ->
        {
            try {
                return URIFetcher.of(uri).fetch(runContext);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }, inputResource);
    }

    static Flux<Map<String, Object>> readIonRecords(Supplier<InputStream> inputStreamSupplier) {
        return readIonRecords(inputStreamSupplier, null);
    }

    static Flux<Map<String, Object>> readIonRecords(Supplier<InputStream> inputStreamSupplier, InputResource inputResource) {
        return Flux.using(
            () ->
            {
                InputStream fetchedInputStream = inputStreamSupplier.get();
                InputStream registeredReader = inputResource == null ? null : inputResource.registerReader(fetchedInputStream);
                InputStream inputStream = new BufferedInputStream(
                    registeredReader == null ? fetchedInputStream : registeredReader,
                    FileSerde.BUFFER_SIZE
                );
                try {
                    MappingIterator<Map<String, Object>> iterator = JacksonMapper.ofIon()
                        .readerFor(DOCUMENT_TYPE)
                        .without(JsonParser.Feature.AUTO_CLOSE_SOURCE)
                        .readValues(inputStream);
                    return new IonInput(inputStream, registeredReader, iterator);
                } catch (IOException | RuntimeException e) {
                    try {
                        if (inputResource == null) {
                            inputStream.close();
                        } else {
                            inputResource.closeReader(registeredReader);
                        }
                    } catch (IOException closeException) {
                        e.addSuppressed(closeException);
                    }
                    if (e instanceof IOException ioException) {
                        throw new UncheckedIOException(ioException);
                    }
                    throw e;
                }
            },
            ionInput -> Flux.<Map<String, Object>> generate(sink ->
            {
                try {
                    if (ionInput.iterator.hasNextValue()) {
                        sink.next(ionInput.iterator.nextValue());
                    } else {
                        sink.complete();
                    }
                } catch (IOException e) {
                    sink.error(e);
                }
            }),
            ionInput -> ionInput.close(inputResource)
        ).doOnSubscribe(subscription ->
        {
            if (inputResource != null) {
                inputResource.registerSubscription(subscription);
            }
        });
    }

    Output insertDocuments(
        Flux<Map<String, Object>> records,
        DocumentDBClient client,
        String database,
        String collection) throws Exception {
        try (InputResource inputResource = new InputResource()) {
            return insertDocuments(records, client, database, collection, inputResource);
        }
    }

    private Output insertDocuments(
        Flux<Map<String, Object>> records,
        DocumentDBClient client,
        String database,
        String collection,
        InputResource inputResource) throws Exception {
        List<Map<String, Object>> documents = new ArrayList<>(MAX_DOCUMENTS_PER_INSERT + 1);
        try (Stream<Map<String, Object>> stream = records.take(MAX_DOCUMENTS_PER_INSERT + 1).toStream(1)) {
            inputResource.registerStream(stream);
            Iterator<Map<String, Object>> iterator = stream.iterator();
            while (iterator.hasNext()) {
                documents.add(iterator.next());
            }
        }

        if (documents.isEmpty()) {
            throw new IllegalArgumentException("No document to insert: set one of 'document', 'from', or 'documents'");
        }
        if (documents.size() > MAX_DOCUMENTS_PER_INSERT) {
            throw tooManyDocuments();
        }

        InsertResult result = client.insertMany(database, collection, documents);
        return output(result.getInsertedIds(), result.getInsertedCount());
    }

    private static IllegalArgumentException tooManyDocuments() {
        return new IllegalArgumentException("Cannot insert more than " + MAX_DOCUMENTS_PER_INSERT + " documents at once");
    }

    private static boolean hasValue(Object value) {
        if (value == null) {
            return false;
        }
        if (value instanceof Map<?, ?> map) {
            return !map.isEmpty();
        }
        if (value instanceof Iterable<?> iterable) {
            return iterable.iterator().hasNext();
        }
        return !(value instanceof CharSequence text) || !text.toString().isBlank();
    }

    private static Output output(List<String> insertedIds, Integer insertedCount) {
        return Output.builder()
            .insertedId(insertedIds.isEmpty() ? null : insertedIds.getFirst())
            .insertedIds(insertedIds)
            .insertedCount(insertedCount)
            .build();
    }

    private static final class IonInput {
        private final InputStream inputStream;
        private final InputStream registeredReader;
        private final MappingIterator<Map<String, Object>> iterator;

        private IonInput(InputStream inputStream, InputStream registeredReader, MappingIterator<Map<String, Object>> iterator) {
            this.inputStream = inputStream;
            this.registeredReader = registeredReader;
            this.iterator = iterator;
        }

        private void close(InputResource inputResource) {
            RuntimeException failure = null;
            try {
                iterator.close();
            } catch (IOException e) {
                failure = new UncheckedIOException(e);
            } catch (RuntimeException e) {
                failure = e;
            }

            try {
                if (inputResource == null) {
                    inputStream.close();
                } else {
                    inputResource.closeReader(registeredReader);
                }
            } catch (IOException e) {
                if (failure == null) {
                    failure = new UncheckedIOException(e);
                } else {
                    failure.addSuppressed(e);
                }
            }

            if (failure != null) {
                throw failure;
            }
        }
    }

    static final class InputResource implements AutoCloseable {
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicReference<Stream<?>> stream = new AtomicReference<>();
        private final AtomicReference<Subscription> subscription = new AtomicReference<>();
        private final AtomicReference<CloseOnceInputStream> reader = new AtomicReference<>();

        void registerStream(Stream<?> activeStream) {
            if (closed.get() || !stream.compareAndSet(null, activeStream)) {
                activeStream.close();
                if (closed.get()) {
                    throw new CancellationException("DocumentDB input was cancelled");
                }
                throw new IllegalStateException("A DocumentDB input stream is already active");
            }

            if (closed.get()) {
                if (stream.compareAndSet(activeStream, null)) {
                    activeStream.close();
                }
                throw new CancellationException("DocumentDB input was cancelled");
            }
        }

        void registerSubscription(Subscription activeSubscription) {
            if (closed.get() || !subscription.compareAndSet(null, activeSubscription)) {
                activeSubscription.cancel();
                if (closed.get()) {
                    return;
                }
                throw new IllegalStateException("A DocumentDB input subscription is already active");
            }

            if (closed.get() && subscription.compareAndSet(activeSubscription, null)) {
                activeSubscription.cancel();
            }
        }

        private InputStream registerReader(InputStream inputStream) throws IOException {
            CloseOnceInputStream closeOnceInputStream = new CloseOnceInputStream(inputStream);
            if (closed.get() || !reader.compareAndSet(null, closeOnceInputStream)) {
                closeOnceInputStream.close();
                if (closed.get()) {
                    throw new CancellationException("DocumentDB input was cancelled");
                }
                throw new IllegalStateException("A DocumentDB input reader is already active");
            }

            if (closed.get()) {
                if (reader.compareAndSet(closeOnceInputStream, null)) {
                    closeOnceInputStream.close();
                }
                throw new CancellationException("DocumentDB input was cancelled");
            }
            return closeOnceInputStream;
        }

        private void closeReader(InputStream inputStream) throws IOException {
            if (inputStream == null) {
                return;
            }
            if (inputStream instanceof CloseOnceInputStream closeOnceInputStream && reader.compareAndSet(closeOnceInputStream, null)) {
                closeOnceInputStream.close();
            } else if (!(inputStream instanceof CloseOnceInputStream)) {
                inputStream.close();
            }
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }

            RuntimeException failure = null;
            Stream<?> activeStream = stream.getAndSet(null);
            if (activeStream != null) {
                try {
                    activeStream.close();
                } catch (RuntimeException e) {
                    failure = e;
                }
            } else {
                Subscription activeSubscription = subscription.getAndSet(null);
                if (activeSubscription != null) {
                    try {
                        activeSubscription.cancel();
                    } catch (RuntimeException e) {
                        failure = e;
                    }
                }
            }

            CloseOnceInputStream activeReader = reader.getAndSet(null);
            if (activeReader != null) {
                try {
                    activeReader.close();
                } catch (IOException e) {
                    RuntimeException closeFailure = new UncheckedIOException(e);
                    if (failure == null) {
                        failure = closeFailure;
                    } else {
                        failure.addSuppressed(closeFailure);
                    }
                }
            }

            if (failure != null) {
                throw failure;
            }
        }
    }

    private static final class CloseOnceInputStream extends FilterInputStream {
        private final AtomicBoolean closed = new AtomicBoolean();

        private CloseOnceInputStream(InputStream inputStream) {
            super(inputStream);
        }

        @Override
        public void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                super.close();
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "First inserted ID",
            description = "ID of the single document or the first item in a batch; null if none returned."
        )
        private final String insertedId;

        @Schema(
            title = "Inserted document IDs",
            description = "List of all document IDs acknowledged by DocumentDB."
        )
        private final List<String> insertedIds;

        @Schema(
            title = "Inserted document count",
            description = "Total number of documents the API reports as inserted."
        )
        private final Integer insertedCount;
    }
}
