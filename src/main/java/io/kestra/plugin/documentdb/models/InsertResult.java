package io.kestra.plugin.documentdb.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;

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