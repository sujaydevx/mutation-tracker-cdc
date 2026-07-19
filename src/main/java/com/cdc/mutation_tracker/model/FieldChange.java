package com.cdc.mutation_tracker.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FieldChange {

    private String fieldName;
    private Object oldValue;
    private Object newValue;
    private String tag;

    public String toHumanReadable() {
        if (oldValue == null) {
            return String.format("'%s' was set to '%s'", fieldName, newValue);
        }
        if (newValue == null) {
            return String.format("'%s' was removed (was '%s')", fieldName, oldValue);
        }
        return String.format("'%s' changed from '%s' to '%s'",
                fieldName, oldValue, newValue);
    }
}