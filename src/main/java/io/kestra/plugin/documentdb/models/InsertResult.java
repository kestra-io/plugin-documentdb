package io.kestra.plugin.documentdb.models;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Result of a DocumentDB insert operation.
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class InsertResult {
    private List<String> insertedIds;
    private Integer insertedCount;
}