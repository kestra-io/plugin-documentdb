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
        title = "DocumentDB endpoint",
        description = "Base HTTPS endpoint for the DocumentDB REST API; trailing slash is trimmed before calling `/data/v1/action/*`."
    )
    @NotNull
    protected Property<String> host;

    @Schema(
        title = "Target database",
        description = "Database name used for the operation; expressions are rendered at runtime."
    )
    @NotNull
    protected Property<String> database;

    @Schema(
        title = "Target collection",
        description = "Collection inside the database where the action is performed."
    )
    @NotNull
    protected Property<String> collection;

    @Schema(
        title = "HTTP username",
        description = "Basic-auth username for the DocumentDB API; prefer providing via secret."
    )
    @NotNull
    protected Property<String> username;

    @Schema(
        title = "HTTP password",
        description = "Basic-auth password for the DocumentDB API; prefer providing via secret."
    )
    @NotNull
    protected Property<String> password;
}
