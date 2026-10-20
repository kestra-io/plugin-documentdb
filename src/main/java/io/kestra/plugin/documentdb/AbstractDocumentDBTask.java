package io.kestra.plugin.documentdb;

import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoCursor;

import io.kestra.core.models.WorkerJobLifecycle;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.Builder;
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
public abstract class AbstractDocumentDBTask extends Task implements WorkerJobLifecycle {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDocumentDBTask.class);

    @Getter(AccessLevel.NONE)
    @Builder.Default
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicReference<DocumentDBClient> activeClient = new AtomicReference<>();

    @Getter(AccessLevel.NONE)
    @Builder.Default
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicReference<MongoCursor<?>> activeCursor = new AtomicReference<>();

    @Getter(AccessLevel.NONE)
    @Builder.Default
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicReference<AutoCloseable> activeResource = new AtomicReference<>();

    @Getter(AccessLevel.NONE)
    @Builder.Default
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicBoolean killed = new AtomicBoolean();

    protected final <T> T withClient(String connectionString, String caCertificate, ClientOperation<T> operation) throws Exception {
        if (killed.get()) {
            throw cancelled();
        }

        DocumentDBClient client = createDocumentDBClient(connectionString, caCertificate);
        registerClient(client);

        try {
            if (killed.get()) {
                throw cancelled();
            }
            return operation.run(client);
        } catch (Exception | Error e) {
            if (activeClient.compareAndSet(client, null)) {
                closePreservingFailure(client, e);
            }
            throw e;
        } finally {
            if (activeClient.compareAndSet(client, null)) {
                client.close();
            }
        }
    }

    protected DocumentDBClient createDocumentDBClient(String connectionString, String caCertificate) throws Exception {
        return new DocumentDBClient(connectionString, caCertificate);
    }

    protected final String resolveConnectionString(RunContext runContext) throws Exception {
        if (this.host != null) {
            runContext.logger().warn("The deprecated 'host' property is in use. Configure 'connectionString' instead.");
        }
        if (this.username != null) {
            runContext.logger().warn("The deprecated 'username' property is in use. Configure credentials in 'connectionString' instead.");
        }
        if (this.password != null) {
            runContext.logger().warn("The deprecated 'password' property is in use. Configure credentials in 'connectionString' instead.");
        }
        if (this.host != null && this.connectionString != null) {
            throw new IllegalArgumentException("Both deprecated 'host' and 'connectionString' are configured; remove 'host' and use 'connectionString'.");
        }
        if (this.connectionString != null) {
            return runContext.render(this.connectionString).as(String.class).orElseThrow();
        }
        if (this.host != null) {
            String legacyHost = runContext.render(this.host).as(String.class).orElseThrow();
            if (legacyHost.startsWith("mongodb://") || legacyHost.startsWith("mongodb+srv://")) {
                return legacyHost;
            }
            throw new IllegalArgumentException("The deprecated 'host' value is not a MongoDB connection URI. Configure 'connectionString' to connect to Amazon DocumentDB.");
        }
        throw new IllegalArgumentException("'connectionString' is required to connect to Amazon DocumentDB.");
    }

    protected final String renderCollection(RunContext runContext) throws Exception {
        return runContext.render(this.collection).as(String.class).orElseThrow();
    }

    protected final <T, R> R withCursor(MongoCursor<T> cursor, CursorOperation<T, R> operation) throws Exception {
        if (killed.get()) {
            closeQuietly(cursor, "MongoDB cursor");
            throw cancelled();
        }

        if (!activeCursor.compareAndSet(null, cursor)) {
            closeQuietly(cursor, "MongoDB cursor");
            if (killed.get()) {
                throw cancelled();
            }
            throw new IllegalStateException("A DocumentDB task can only use one active cursor at a time");
        }

        if (killed.get()) {
            if (activeCursor.compareAndSet(cursor, null)) {
                closeQuietly(cursor, "MongoDB cursor");
            }
            throw cancelled();
        }

        try {
            return operation.run(cursor);
        } catch (Exception | Error e) {
            if (activeCursor.compareAndSet(cursor, null)) {
                closePreservingFailure(cursor, e);
            }
            throw e;
        } finally {
            if (activeCursor.compareAndSet(cursor, null)) {
                cursor.close();
            }
        }
    }

    protected final <T extends AutoCloseable> T registerActiveResource(T resource) {
        if (killed.get()) {
            closeQuietly(resource, "task resource");
            throw cancelled();
        }

        if (!activeResource.compareAndSet(null, resource)) {
            closeQuietly(resource, "task resource");
            if (killed.get()) {
                throw cancelled();
            }
            throw new IllegalStateException("A DocumentDB task can only use one active resource at a time");
        }

        if (killed.get()) {
            if (activeResource.compareAndSet(resource, null)) {
                closeQuietly(resource, "task resource");
            }
            throw cancelled();
        }

        return resource;
    }

    protected final void releaseActiveResource(AutoCloseable resource) {
        if (activeResource.compareAndSet(resource, null)) {
            closeQuietly(resource, "task resource");
        }
    }

    private void registerClient(DocumentDBClient client) {
        if (killed.get()) {
            closeQuietly(client, "MongoDB client");
            throw cancelled();
        }

        if (!activeClient.compareAndSet(null, client)) {
            closeQuietly(client, "MongoDB client");
            if (killed.get()) {
                throw cancelled();
            }
            throw new IllegalStateException("A DocumentDB task can only use one active client at a time");
        }

        if (killed.get()) {
            if (activeClient.compareAndSet(client, null)) {
                closeQuietly(client, "MongoDB client");
            }
            throw cancelled();
        }
    }

    @Override
    public void kill() {
        killed.set(true);
        closeQuietly(activeClient.getAndSet(null), "MongoDB client");
        closeQuietly(activeCursor.getAndSet(null), "MongoDB cursor");
        closeQuietly(activeResource.getAndSet(null), "task resource");
    }

    private static CancellationException cancelled() {
        return new CancellationException("DocumentDB task was cancelled");
    }

    private static void closeQuietly(AutoCloseable resource, String resourceName) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (Exception e) {
            LOGGER.warn("Failed to close {} during task cancellation", resourceName, e);
        }
    }

    private static void closePreservingFailure(AutoCloseable resource, Throwable operationFailure) {
        try {
            resource.close();
        } catch (Throwable cleanupFailure) {
            if (cleanupFailure != operationFailure) {
                operationFailure.addSuppressed(cleanupFailure);
            }
        }
    }

    @FunctionalInterface
    protected interface ClientOperation<T> {
        T run(DocumentDBClient client) throws Exception;
    }

    @FunctionalInterface
    protected interface CursorOperation<T, R> {
        R run(MongoCursor<T> cursor) throws Exception;
    }

    /**
     * @deprecated Use {@link #connectionString} with an Amazon DocumentDB connection URI.
     */
    @Deprecated
    @Schema(
        title = "DocumentDB host (deprecated)",
        description = "Deprecated. Use connectionString with an Amazon DocumentDB MongoDB-compatible connection URI. A legacy host value is accepted only if it is already a mongodb:// or mongodb+srv:// URI; the former HTTPS API endpoint cannot be translated."
    )
    @ToString.Exclude
    @PluginProperty(group = "main", secret = true)
    protected Property<String> host;

    @Schema(
        title = "Amazon DocumentDB connection string",
        description = "Recommended connection URI for Amazon DocumentDB, consumed by the MongoDB Java Driver, for example: mongodb://<user>:<password>@<cluster>.docdb.amazonaws.com:27017/?tls=true&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
    )
    @ToString.Exclude
    @PluginProperty(group = "main", secret = true)
    protected Property<String> connectionString;

    /**
     * @deprecated Put authentication credentials in {@link #connectionString}.
     */
    @Deprecated
    @Schema(
        title = "HTTP username (deprecated)",
        description = "Deprecated. The MongoDB Java Driver does not use this legacy HTTP Basic-auth property; configure credentials in connectionString."
    )
    @ToString.Exclude
    @PluginProperty(group = "main", secret = true)
    protected Property<String> username;

    /**
     * @deprecated Put authentication credentials in {@link #connectionString}.
     */
    @Deprecated
    @Schema(
        title = "HTTP password (deprecated)",
        description = "Deprecated. The MongoDB Java Driver does not use this legacy HTTP Basic-auth property; configure credentials in connectionString."
    )
    @ToString.Exclude
    @PluginProperty(group = "main", secret = true)
    protected Property<String> password;

    @Schema(
        title = "CA certificate",
        description = "PEM-encoded CA certificate used to verify the TLS connection to DocumentDB."
    )
    @ToString.Exclude
    @PluginProperty(group = "connection", secret = true)
    protected Property<String> caCertificate;

    @Schema(
        title = "Target database",
        description = "Database name used for the operation; expressions are rendered at runtime."
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> database;

    @Schema(
        title = "Target collection",
        description = "Collection name used for the operation; expressions are rendered at runtime."
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> collection;
}
