package io.kestra.plugin.documentdb;

import io.kestra.core.models.annotations.PluginProperty;
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
        title = "MongoDB connection string",
        description = "MongoDB connection string for the target database, for example mongodb://user:password@host:27017/database?authSource=admin."
    )
    @NotNull
    @PluginProperty(group = "main", secret = true)
    protected Property<String> connectionString;

    @Schema(
        title = "Target database",
        description = "Database name used for the operation; expressions are rendered at runtime."
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> database;

    @Schema(
        title = "Target collection",
        description = "Collection inside the database where the action is performed."
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> collection;
}
