package io.kestra.plugin.documentdb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.slf4j.Logger;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
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
    private static final int BATCH_SIZE = 100;

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

        String rConnectionString = resolveConnectionString(runContext);
        String rCaCertificate = this.caCertificate == null ? null : runContext.render(this.caCertificate).as(String.class).orElse(null);
        String rDatabase = runContext.render(this.database).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("Unable to render the DocumentDB database name."));
        String rCollection = renderCollection(runContext);
        Map<String, Object> rDocument = runContext.render(this.document).asMap(String.class, Object.class);
        Object rFrom = this.from == null ? null : runContext.render(Property.ofValue(this.from)).as(Object.class).orElse(null);
        Object rDocuments = this.documents == null ? null : runContext.render(this.documents).asList(Map.class);

        boolean hasDocument = rDocument != null && !rDocument.isEmpty();
        boolean hasFrom = hasValue(rFrom);
        boolean hasFromConfigured = this.from != null;
        boolean hasDocuments = rDocuments instanceof List<?> documents && !documents.isEmpty();
        int inputCount = (hasDocument ? 1 : 0) + (hasFrom ? 1 : 0) + (hasDocuments ? 1 : 0);
        if (inputCount == 0 && !hasFromConfigured) {
            throw new IllegalArgumentException("No document to insert: set one of 'document', 'from', or 'documents'");
        }
        if (inputCount > 1) {
            throw new IllegalArgumentException("Cannot specify both 'document' and batch inputs; choose only one of 'document', 'from', or 'documents'");
        }

        boolean fromInput = hasFrom || (inputCount == 0 && hasFromConfigured);
        Object source = fromInput ? rFrom : rDocuments;
        if (
            hasDocuments
                && rDocuments instanceof List<?> list
                && list.size() > MAX_DOCUMENTS_PER_INSERT
        ) {
            throw new IllegalArgumentException(
                "Cannot insert more than " + MAX_DOCUMENTS_PER_INSERT + " documents through the legacy 'documents' property"
            );
        }

        return withClient(rConnectionString, rCaCertificate, client ->
        {
            if (hasDocument) {
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
            Output output = insertDocuments(Data.from(source).read(runContext), client, rDatabase, rCollection, !fromInput);
            logger.info("Successfully inserted {} documents", output.getInsertedCount());
            return output;
        });
    }

    Output insertDocuments(
        Flux<Map<String, Object>> records,
        DocumentDBClient client,
        String database,
        String collection,
        boolean collectAllIds) throws Exception {

        List<String> insertedIds = new ArrayList<>();
        int insertedCount = 0;
        int batchNumber = 0;
        String insertedId = null;
        AtomicBoolean cancelled = new AtomicBoolean();

        Stream<List<Map<String, Object>>> batches = records
            .doOnCancel(() -> cancelled.set(true))
            .buffer(BATCH_SIZE)
            .toStream(1);
        registerActiveResource(batches);

        try {
            checkCancellation(cancelled);
            Iterator<List<Map<String, Object>>> iterator = batches.iterator();

            while (iterator.hasNext()) {
                checkCancellation(cancelled);

                List<Map<String, Object>> batch = iterator.next();
                batchNumber++;

                InsertResult result;
                try {
                    result = client.insertMany(database, collection, batch);
                } catch (CancellationException e) {
                    throw e;
                } catch (Exception e) {
                    throw new IllegalStateException(
                        "Insert failed after " + insertedCount + " documents were successfully inserted; batch " + batchNumber
                            + " failed: " + e.getMessage(),
                        e
                    );
                }

                checkCancellation(cancelled);

                if (insertedId == null && !result.getInsertedIds().isEmpty()) {
                    insertedId = result.getInsertedIds().getFirst();
                }
                if (collectAllIds) {
                    insertedIds.addAll(result.getInsertedIds());
                }
                insertedCount += result.getInsertedCount();
            }

            checkCancellation(cancelled);
        } finally {
            releaseActiveResource(batches);
        }

        checkIfKilled();

        if (!collectAllIds && insertedId != null) {
            insertedIds.add(insertedId);
        }

        return output(insertedId, insertedIds, insertedCount);
    }

    private static Output output(String insertedId, List<String> insertedIds, Integer insertedCount) {
        return Output.builder()
            .insertedId(insertedId)
            .insertedIds(insertedIds)
            .insertedCount(insertedCount)
            .build();
    }

    private void checkCancellation(AtomicBoolean cancelled) {
        if (cancelled.get()) {
            throw new CancellationException("DocumentDB insertion was cancelled");
        }
        checkIfKilled();
    }

    private static boolean hasValue(Object value) {
        if (value == null) {
            return false;
        }
        if (value instanceof CharSequence string) {
            return !string.toString().isBlank();
        }
        if (value instanceof List<?> list) {
            return !list.isEmpty();
        }
        return true;
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
            description = "For bounded inputs, all IDs acknowledged by DocumentDB. For unrestricted 'from' inputs, only the first ID is returned to avoid unbounded output growth."
        )
        private final List<String> insertedIds;

        @Schema(
            title = "Inserted document count",
            description = "Total number of documents the API reports as inserted."
        )
        private final Integer insertedCount;
    }
}
