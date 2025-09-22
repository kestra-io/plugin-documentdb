package io.kestra.plugin.documentdb.models;

/**
 * Exception thrown when DocumentDB operations fail.
 */
public class DocumentDBException extends Exception {
    public DocumentDBException(String message) {
        super(message);
    }

    public DocumentDBException(String message, Throwable cause) {
        super(message, cause);
    }
}