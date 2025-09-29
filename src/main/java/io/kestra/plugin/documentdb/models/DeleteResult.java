package io.kestra.plugin.documentdb.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Result of a DocumentDB delete operation.
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class DeleteResult {
    private Integer deletedCount;
}