package io.kestra.plugin.documentdb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.documentdb.models.InsertResult;

import static io.kestra.plugin.documentdb.DocumentDBClient.MAX_DOCUMENTS_PER_INSERT;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Insert documents into a DocumentDB collection",
    description = "Insert one or more documents into a DocumentDB collection. Can insert a single document or multiple documents (max 10) in one operation."
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
                    host: "https://my-documentdb-instance.com"
                    database: "myapp"
                    collection: "users"
                    username: "{{ secret('DOCUMENTDB_USERNAME') }}"
                    password: "{{ secret('DOCUMENTDB_PASSWORD') }}"
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
                    host: "https://my-documentdb-instance.com"
                    database: "inventory"
                    collection: "products"
                    username: "{{ secret('DOCUMENTDB_USERNAME') }}"
                    password: "{{ secret('DOCUMENTDB_PASSWORD') }}"
                    documents:
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
                    host: "https://my-documentdb-instance.com"
                    database: "sales"
                    collection: "orders"
                    username: "{{ secret('DOCUMENTDB_USERNAME') }}"
                    password: "{{ secret('DOCUMENTDB_PASSWORD') }}"
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
public class Insert extends AbstractDocumentDBTask implements RunnableTask<Insert.Output> {

    @Schema(
        title = "Document for single insert",
        description = "Document to insert (for single document insertion). Use this OR documents, not both."
    )
    private Property<Map<String, Object>> document;

    @Schema(
        title = "Multiple documents",
        description = "List of documents to insert (max " + MAX_DOCUMENTS_PER_INSERT + "). Use this OR document, not both."
    )
    private Property<List<Map<String, Object>>> documents;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        // Render properties
        String rHost = runContext.render(this.host).as(String.class).orElseThrow();
        String rDatabase = runContext.render(this.database).as(String.class).orElseThrow();
        String rCollection = runContext.render(this.collection).as(String.class).orElseThrow();
        String rUsername = runContext.render(this.username).as(String.class).orElseThrow();
        String rPassword = runContext.render(this.password).as(String.class).orElseThrow();
        Map<String, Object> rDocument = runContext.render(this.document).asMap(String.class, Object.class);
        List<Map<String, Object>> rDocuments = runContext.render(this.documents).asList(Map.class);

        // Validate input - either document or documents should be provided, not both
        if ((rDocument == null || rDocument.isEmpty()) && (rDocuments == null || rDocuments.isEmpty())) {
            throw new IllegalArgumentException("Either 'document' for single insert or 'documents' for multiple inserts must be provided");
        }

        if (rDocument != null && !rDocument.isEmpty() && rDocuments != null && !rDocuments.isEmpty()) {
            throw new IllegalArgumentException("Cannot specify both 'document' and 'documents'. Use one or the other.");
        }

        DocumentDBClient client = new DocumentDBClient(rHost, rUsername, rPassword, runContext);

        if (rDocument != null && !rDocument.isEmpty()) {
            // Insert single document
            logger.info("Inserting single document into DocumentDB database: {} collection: {}", rDatabase, rCollection);

            InsertResult result = client.insertOne(rDatabase, rCollection, rDocument);

            logger.info("Successfully inserted document with ID: {}", result.getInsertedIds().getFirst());

            return Output.builder()
                .insertedId(result.getInsertedIds().getFirst())
                .insertedIds(result.getInsertedIds())
                .insertedCount(result.getInsertedCount())
                .build();
        } else {
            // Insert multiple documents
            if (rDocuments.size() > MAX_DOCUMENTS_PER_INSERT) {
                throw new IllegalArgumentException("Cannot insert more than " + MAX_DOCUMENTS_PER_INSERT + " documents at once");
            }

            logger.info("Inserting {} documents into DocumentDB database: {} collection: {}", rDocuments.size(), rDatabase, rCollection);

            InsertResult result = client.insertMany(rDatabase, rCollection, rDocuments);

            logger.info("Successfully inserted {} documents", result.getInsertedCount());

            return Output.builder()
                .insertedId(result.getInsertedIds().isEmpty() ? null : result.getInsertedIds().getFirst())
                .insertedIds(result.getInsertedIds())
                .insertedCount(result.getInsertedCount())
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Inserted document ID",
            description = "The ID of the first inserted document (for single insert or first of multiple)"
        )
        private final String insertedId;

        @Schema(
            title = "All inserted document IDs",
            description = "List of all inserted document IDs"
        )
        private final List<String> insertedIds;

        @Schema(
            title = "Number of documents inserted",
            description = "Total count of documents successfully inserted"
        )
        private final Integer insertedCount;
    }
}