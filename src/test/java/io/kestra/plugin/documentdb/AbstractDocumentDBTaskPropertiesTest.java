package io.kestra.plugin.documentdb;

import java.lang.reflect.Field;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.inject.Inject;
import jakarta.validation.constraints.NotNull;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class AbstractDocumentDBTaskPropertiesTest {
    private static final String CONNECTION_STRING = "mongodb://user:password@cluster.docdb.amazonaws.com:27017/?tls=true";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void deprecatedConnectionPropertiesRemainAvailableAndDescribed() throws Exception {
        Field host = AbstractDocumentDBTask.class.getDeclaredField("host");
        Field username = AbstractDocumentDBTask.class.getDeclaredField("username");
        Field password = AbstractDocumentDBTask.class.getDeclaredField("password");
        Field collection = AbstractDocumentDBTask.class.getDeclaredField("collection");
        Field connectionString = AbstractDocumentDBTask.class.getDeclaredField("connectionString");

        assertTrue(host.isAnnotationPresent(Deprecated.class));
        assertTrue(username.isAnnotationPresent(Deprecated.class));
        assertTrue(password.isAnnotationPresent(Deprecated.class));
        assertFalse(collection.isAnnotationPresent(Deprecated.class));
        assertTrue(host.getAnnotation(Schema.class).description().contains("connectionString"));
        assertTrue(username.getAnnotation(Schema.class).description().contains("connectionString"));
        assertTrue(password.getAnnotation(Schema.class).description().contains("connectionString"));
        assertEquals("Target collection", collection.getAnnotation(Schema.class).title());
        assertTrue(collection.isAnnotationPresent(NotNull.class));
        assertNotNull(connectionString.getAnnotation(Schema.class));
        assertFalse(connectionString.isAnnotationPresent(Deprecated.class));
        assertTrue(host.getAnnotation(PluginProperty.class).secret());
        assertTrue(connectionString.getAnnotation(PluginProperty.class).secret());
    }

    @Test
    void connectionStringAndLegacyMongoUriResolveAndWarn() throws Exception {
        Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            Insert task = Insert.builder()
                .id("property-test")
                .type(Insert.class.getName())
                .connectionString(Property.ofValue(CONNECTION_STRING))
                .username(Property.ofValue("legacy-user"))
                .password(Property.ofValue("legacy-password"))
                .database(Property.ofValue("test_db"))
                .collection(Property.ofValue("users"))
                .build();
            RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

            assertEquals(CONNECTION_STRING, task.resolveConnectionString(runContext));
            assertEquals("users", task.renderCollection(runContext));

            Insert legacyTask = Insert.builder()
                .id("legacy-property-test")
                .type(Insert.class.getName())
                .host(Property.ofValue(CONNECTION_STRING))
                .database(Property.ofValue("test_db"))
                .collection(Property.ofValue("users"))
                .build();
            RunContext legacyRunContext = TestsUtils.mockRunContext(runContextFactory, legacyTask, Map.of());
            assertEquals(CONNECTION_STRING, legacyTask.resolveConnectionString(legacyRunContext));

            assertTrue(appender.list.stream().anyMatch(event -> event.getFormattedMessage().contains("deprecated 'host'")));
            assertTrue(appender.list.stream().anyMatch(event -> event.getFormattedMessage().contains("deprecated 'username'")));
            assertTrue(appender.list.stream().anyMatch(event -> event.getFormattedMessage().contains("deprecated 'password'")));
            assertTrue(
                appender.list.stream()
                    .filter(event -> event.getFormattedMessage().contains("deprecated"))
                    .allMatch(event -> !event.getLoggerName().equals(AbstractDocumentDBTask.class.getName()))
            );
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    @Test
    void conflictingConnectionPropertiesAreRejected() throws Exception {
        Insert task = Insert.builder()
            .id("conflicting-property-test")
            .type(Insert.class.getName())
            .host(Property.ofValue("mongodb://legacy-host"))
            .connectionString(Property.ofValue(CONNECTION_STRING))
            .database(Property.ofValue("test_db"))
            .collection(Property.ofValue("users"))
            .build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> task.resolveConnectionString(runContext));
        assertTrue(exception.getMessage().contains("Both deprecated 'host' and 'connectionString' are configured"));
    }

    @Test
    void legacyHttpsApiHostRequiresMigration() throws Exception {
        Insert task = Insert.builder()
            .id("legacy-http-host-test")
            .type(Insert.class.getName())
            .host(Property.ofValue("https://documentdb.example.com"))
            .database(Property.ofValue("test_db"))
            .collection(Property.ofValue("users"))
            .build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> task.resolveConnectionString(runContext));
        assertTrue(exception.getMessage().contains("not a MongoDB connection URI"));
    }
}