package io.kestra.plugin.documentdb.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Result of a DocumentDB update operation.
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class UpdateResult {
    private Integer matchedCount;
    private Integer modifiedCount;
    private String upsertedId;
}