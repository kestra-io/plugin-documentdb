package io.kestra.plugin.documentdb;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * Abstract base class for DocumentDB tasks.
 * Provides common connection properties shared across all DocumentDB operations.
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractDocumentDBTask extends Task {

    @Schema(
        title = "DocumentDB host",
        description = "The HTTP endpoint URL of your DocumentDB instance"
    )
    @NotNull
    protected Property<String> host;

    @Schema(
        title = "Database name",
        description = "The name of the database"
    )
    @NotNull
    protected Property<String> database;

    @Schema(
        title = "Collection name",
        description = "The name of the collection"
    )
    @NotNull
    protected Property<String> collection;

    @Schema(
        title = "Username",
        description = "DocumentDB username for authentication"
    )
    @NotNull
    protected Property<String> username;

    @Schema(
        title = "Password",
        description = "DocumentDB password for authentication"
    )
    @NotNull
    protected Property<String> password;
}
