package io.kestra.plugin.documentdb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.documentdb.models.UpdateResult;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Update documents in a DocumentDB collection",
    description = "Update one or more documents in a DocumentDB collection that match the filter criteria."
)
@Plugin(
    examples = {
        @Example(
            title = "Update a single user document",
            full = true,
            code = """
                id: update_documentdb_user
                namespace: company.documentdb

                tasks:
                  - id: update_user
                    type: io.kestra.plugin.documentdb.Update
                    host: "https://my-documentdb-instance.com"
                    database: "myapp"
                    collection: "users"
                    username: "{{ secret('DOCUMENTDB_USERNAME') }}"
                    password: "{{ secret('DOCUMENTDB_PASSWORD') }}"
                    filter:
                      email: "john.doe@example.com"
                    update:
                      $set:
                        status: "active"
                        last_login: "{{ now() }}"
                    updateMany: false
                """
        ),
        @Example(
            title = "Update multiple documents",
            full = true,
            code = """
                id: update_multiple_users
                namespace: company.documentdb

                tasks:
                  - id: update_inactive_users
                    type: io.kestra.plugin.documentdb.Update
                    host: "https://my-documentdb-instance.com"
                    database: "myapp"
                    collection: "users"
                    username: "{{ secret('DOCUMENTDB_USERNAME') }}"
                    password: "{{ secret('DOCUMENTDB_PASSWORD') }}"
                    filter:
                      last_login:
                        $lt: "2023-01-01"
                    update:
                      $set:
                        status: "inactive"
                        archived_date: "{{ now() }}"
                    updateMany: true
                """
        ),
        @Example(
            title = "Update with increment operation",
            full = true,
            code = """
                id: increment_user_views
                namespace: company.documentdb

                tasks:
                  - id: increment_views
                    type: io.kestra.plugin.documentdb.Update
                    host: "https://my-documentdb-instance.com"
                    database: "myapp"
                    collection: "profiles"
                    username: "{{ secret('DOCUMENTDB_USERNAME') }}"
                    password: "{{ secret('DOCUMENTDB_PASSWORD') }}"
                    filter:
                      user_id: "{{ inputs.user_id }}"
                    update:
                      $inc:
                        view_count: 1
                        total_interactions: 1
                      $set:
                        last_viewed: "{{ now() }}"
                    updateMany: false
                """
        )
    }
)
public class Update extends AbstractDocumentDBTask implements RunnableTask<Update.Output> {

    @Schema(
        title = "Filter criteria",
        description = "MongoDB-style filter to select which documents to update. Example: {\"status\": \"pending\", \"age\": {\"$gte\": 18}}"
    )
    private Property<Map<String, Object>> filter;

    @Schema(
        title = "Update operations",
        description = "MongoDB-style update operations to apply. Example: {\"$set\": {\"status\": \"active\"}, \"$inc\": {\"count\": 1}}"
    )
    @NotNull
    private Property<Map<String, Object>> update;

    @Schema(
        title = "Update multiple documents",
        description = "If true, updates all documents matching the filter (updateMany). If false, updates only the first match (updateOne)."
    )
    @Builder.Default
    private Property<Boolean> updateMany = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        // Render properties
        String rHost = runContext.render(this.host).as(String.class).orElseThrow();
        String rDatabase = runContext.render(this.database).as(String.class).orElseThrow();
        String rCollection = runContext.render(this.collection).as(String.class).orElseThrow();
        String rUsername = runContext.render(this.username).as(String.class).orElseThrow();
        String rPassword = runContext.render(this.password).as(String.class).orElseThrow();
        Map<String, Object> rFilter = runContext.render(this.filter).asMap(String.class, Object.class);
        Map<String, Object> rUpdate = runContext.render(this.update).asMap(String.class, Object.class);
        Boolean rUpdateMany = runContext.render(this.updateMany).as(Boolean.class).orElse(false);

        // Validate update operations
        if (rUpdate == null || rUpdate.isEmpty()) {
            throw new IllegalArgumentException("Update operations must be provided");
        }

        DocumentDBClient client = new DocumentDBClient(rHost, rUsername, rPassword, runContext);

        if (rUpdateMany) {
            // Update multiple documents
            logger.info("Updating multiple documents in DocumentDB database: {} collection: {}", rDatabase, rCollection);

            UpdateResult result = client.updateMany(rDatabase, rCollection, rFilter, rUpdate);

            logger.info("Successfully updated {} of {} matching documents", result.getModifiedCount(), result.getMatchedCount());

            return Output.builder()
                .matchedCount(result.getMatchedCount())
                .modifiedCount(result.getModifiedCount())
                .upsertedId(result.getUpsertedId())
                .build();
        } else {
            // Update single document
            logger.info("Updating single document in DocumentDB database: {} collection: {}", rDatabase, rCollection);

            UpdateResult result = client.updateOne(rDatabase, rCollection, rFilter, rUpdate);

            if (result.getModifiedCount() > 0) {
                logger.info("Successfully updated {} of {} matching documents", result.getModifiedCount(), result.getMatchedCount());
            } else {
                logger.info("No documents were modified (matched: {})", result.getMatchedCount());
            }

            return Output.builder()
                .matchedCount(result.getMatchedCount())
                .modifiedCount(result.getModifiedCount())
                .upsertedId(result.getUpsertedId())
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of documents matched",
            description = "Total count of documents that matched the filter criteria"
        )
        private final Integer matchedCount;

        @Schema(
            title = "Number of documents modified",
            description = "Total count of documents that were actually modified"
        )
        private final Integer modifiedCount;

        @Schema(
            title = "Upserted document ID",
            description = "ID of the document that was created if upsert was enabled and no match was found"
        )
        private final String upsertedId;
    }
}