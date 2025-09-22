package io.kestra.plugin.documentdb.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Map;

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