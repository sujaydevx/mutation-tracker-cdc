package com.cdc.mutation_tracker.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.stream.Collectors;

@Data
@Builder
public class DiffResult {

    private String tableName;
    private String operation;
    private String rowId;
    private List<FieldChange> changes;
    private Long eventTimestamp;

    public boolean isEmpty() {
        return changes == null || changes.isEmpty();
    }

    public List<FieldChange> getChangesByTag(String tag) {
        if (changes == null) return List.of();
        return changes.stream()
                .filter(c -> tag.equals(c.getTag()))
                .collect(Collectors.toList());
    }

    public boolean hasPiiChanges() {
        return !getChangesByTag("pii").isEmpty();
    }

    public boolean hasFinancialChanges() {
        return !getChangesByTag("financial").isEmpty();
    }
}