package io.kestra.plugin.documentdb.models;

import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Represents a document record from DocumentDB.
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class DocumentDBRecord {
    private String id;
    private Map<String, Object> fields;
}