package io.kestra.plugin.documentdb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.documentdb.models.DeleteResult;

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
    title = "Delete documents from DocumentDB",
    description = "Remove one document (default) or every match when `deleteMany` is true. An empty filter with `deleteMany` can wipe the collection, so set filters carefully."
)
@Plugin(
    examples = {
        @Example(
            title = "Delete a single user document",
            full = true,
            code = """
                id: delete_documentdb_user
                namespace: company.documentdb

                tasks:
                  - id: delete_user
                    type: io.kestra.plugin.documentdb.Delete
                    host: "https://my-documentdb-instance.com"
                    database: "myapp"
                    collection: "users"
                    username: "{{ secret('DOCUMENTDB_USERNAME') }}"
                    password: "{{ secret('DOCUMENTDB_PASSWORD') }}"
                    filter:
                      email: "user.to.delete@example.com"
                    deleteMany: false
                """
        ),
        @Example(
            title = "Delete multiple inactive documents",
            full = true,
            code = """
                id: cleanup_inactive_users
                namespace: company.documentdb

                tasks:
                  - id: delete_inactive_users
                    type: io.kestra.plugin.documentdb.Delete
                    host: "https://my-documentdb-instance.com"
                    database: "myapp"
                    collection: "users"
                    username: "{{ secret('DOCUMENTDB_USERNAME') }}"
                    password: "{{ secret('DOCUMENTDB_PASSWORD') }}"
                    filter:
                      status: "inactive"
                      last_login:
                        $lt: "2022-01-01"
                    deleteMany: true
                """
        ),
        @Example(
            title = "Delete documents by age criteria",
            full = true,
            code = """
                id: delete_old_logs
                namespace: company.documentdb

                tasks:
                  - id: cleanup_old_logs
                    type: io.kestra.plugin.documentdb.Delete
                    host: "https://my-documentdb-instance.com"
                    database: "logging"
                    collection: "application_logs"
                    username: "{{ secret('DOCUMENTDB_USERNAME') }}"
                    password: "{{ secret('DOCUMENTDB_PASSWORD') }}"
                    filter:
                      created_at:
                        $lt: "{{ now() | dateAdd(-30, 'DAYS') }}"
                      level:
                        $in: ["DEBUG", "INFO"]
                    deleteMany: true
                """
        )
    }
)
public class Delete extends AbstractDocumentDBTask implements RunnableTask<Delete.Output> {

    @Schema(
        title = "Filter query",
        description = "MongoDB-style filter that selects documents to delete. Rendered at runtime; an empty filter with `deleteMany` deletes every document. Example: `{\\\"status\\\": \\\"inactive\\\", \\\"age\\\": {\\\"$gte\\\": 18}}`"
    )
    private Property<Map<String, Object>> filter;

    @Schema(
        title = "Delete all matches",
        description = "Set to true to delete every document matching the filter; default false deletes only the first match."
    )
    @Builder.Default
    private Property<Boolean> deleteMany = Property.ofValue(false);

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
        Boolean rDeleteMany = runContext.render(this.deleteMany).as(Boolean.class).orElse(false);

        DocumentDBClient client = new DocumentDBClient(rHost, rUsername, rPassword, runContext);

        if (rDeleteMany) {
            // Delete multiple documents
            logger.info("Deleting multiple documents from DocumentDB database: {} collection: {}", rDatabase, rCollection);

            DeleteResult result = client.deleteMany(rDatabase, rCollection, rFilter);

            logger.info("Successfully deleted {} documents", result.getDeletedCount());

            return Output.builder()
                .deletedCount(result.getDeletedCount())
                .build();
        } else {
            // Delete single document
            logger.info("Deleting single document from DocumentDB database: {} collection: {}", rDatabase, rCollection);

            DeleteResult result = client.deleteOne(rDatabase, rCollection, rFilter);

            if (result.getDeletedCount() > 0) {
                logger.info("Successfully deleted {} document", result.getDeletedCount());
            } else {
                logger.info("No documents matched the filter criteria for deletion");
            }

            return Output.builder()
                .deletedCount(result.getDeletedCount())
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Deleted document count",
            description = "Total number of documents removed by the operation."
        )
        private final Integer deletedCount;
    }
}
