package com.cdc.mutation_tracker.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Covers the two behaviors that matter: known fields return their
 * configured tag, and anything unknown safely defaults to "untagged"
 * instead of throwing an error.
 */
class SchemaTagConfigTest {

    private SchemaTagConfig schemaTagConfig;

    @BeforeEach
    void setUp() {
        schemaTagConfig = new SchemaTagConfig();

        Map<String, String> userColumns = new HashMap<>();
        userColumns.put("email", "pii");
        userColumns.put("balance", "financial");

        SchemaTagConfig.TableConfig userTableConfig = new SchemaTagConfig.TableConfig();
        userTableConfig.setColumns(userColumns);

        Map<String, SchemaTagConfig.TableConfig> tables = new HashMap<>();
        tables.put("users", userTableConfig);

        schemaTagConfig.setTables(tables);
    }

    @Test
    void knownFields_shouldReturnTheirConfiguredTag() {
        assertEquals("pii", schemaTagConfig.getTag("users", "email"));
        assertEquals("financial", schemaTagConfig.getTag("users", "balance"));
    }

    @Test
    void unknownFieldOrTable_shouldDefaultToUntagged() {
        assertEquals("untagged", schemaTagConfig.getTag("users", "id"));       // unlisted field
        assertEquals("untagged", schemaTagConfig.getTag("payments", "amount")); // unconfigured table
    }
}
