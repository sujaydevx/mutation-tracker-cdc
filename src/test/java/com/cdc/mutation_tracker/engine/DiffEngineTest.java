package com.cdc.mutation_tracker.engine;

import com.cdc.mutation_tracker.config.SchemaTagConfig;
import com.cdc.mutation_tracker.exception.MalformedEventException;
import com.cdc.mutation_tracker.model.DebeziumEvent;
import com.cdc.mutation_tracker.model.DiffResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Covers the core behaviors of DiffEngine: insert, update (only changed
 * fields), a no-op update, delete, malformed events, and the password
 * redaction fix.
 */
@ExtendWith(MockitoExtension.class)
@org.mockito.junit.jupiter.MockitoSettings(strictness = org.mockito.quality.Strictness.LENIENT)
class DiffEngineTest {

    @Mock
    private SchemaTagConfig schemaTagConfig;

    @Mock
    private DebeziumTypeDecoder typeDecoder;

    @InjectMocks
    private DiffEngine diffEngine;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        when(schemaTagConfig.getTag(anyString(), anyString())).thenReturn("untagged");
    }

    @Test
    void insert_shouldCaptureAllFieldsAsNew() throws Exception {
        String raw = """
            {
              "payload": {
                "before": null,
                "after": {"id": 1, "email": "arjun@gmail.com", "balance": 5000},
                "op": "c",
                "source": {"table": "users", "db": "testdb", "schema": "public"},
                "ts_ms": 1000
              }
            }
            """;

        DebeziumEvent event = objectMapper.readValue(raw, DebeziumEvent.class);

        when(typeDecoder.decode(eq("id"), any())).thenReturn(1);
        when(typeDecoder.decode(eq("email"), any())).thenReturn("arjun@gmail.com");
        when(typeDecoder.decode(eq("balance"), any())).thenReturn(5000);

        DiffResult result = diffEngine.compute(event);

        assertEquals("INSERT", result.getOperation());
        assertEquals("users", result.getTableName());
        assertEquals("1", result.getRowId());
        assertEquals(3, result.getChanges().size());
        // for INSERT, oldValue must be null for every field
        result.getChanges().forEach(change -> assertNull(change.getOldValue()));
    }

    @Test
    void update_shouldOnlyCaptureChangedFields() throws Exception {
        // only email changed — name and balance stayed the same
        String raw = """
            {
              "payload": {
                "before": {"id": 1, "name": "Arjun", "email": "old@gmail.com", "balance": 5000},
                "after":  {"id": 1, "name": "Arjun", "email": "new@gmail.com", "balance": 5000},
                "op": "u",
                "source": {"table": "users", "db": "testdb", "schema": "public"},
                "ts_ms": 1000
              }
            }
            """;

        DebeziumEvent event = objectMapper.readValue(raw, DebeziumEvent.class);

        when(typeDecoder.decode(eq("id"), any())).thenReturn(1);
        when(typeDecoder.decode(eq("name"), any())).thenReturn("Arjun");
        when(typeDecoder.decode(eq("email"), any()))
                .thenReturn("old@gmail.com")
                .thenReturn("new@gmail.com");
        when(typeDecoder.decode(eq("balance"), any())).thenReturn(5000);

        DiffResult result = diffEngine.compute(event);

        assertEquals("UPDATE", result.getOperation());
        assertEquals(1, result.getChanges().size());
        assertEquals("email", result.getChanges().get(0).getFieldName());
        assertEquals("old@gmail.com", result.getChanges().get(0).getOldValue());
        assertEquals("new@gmail.com", result.getChanges().get(0).getNewValue());
    }

    @Test
    void update_sameValue_shouldReturnEmptyDiff() throws Exception {
        // PostgreSQL still fires a WAL event even when nothing actually changed
        String raw = """
            {
              "payload": {
                "before": {"id": 1, "email": "same@gmail.com"},
                "after":  {"id": 1, "email": "same@gmail.com"},
                "op": "u",
                "source": {"table": "users", "db": "testdb", "schema": "public"},
                "ts_ms": 1000
              }
            }
            """;

        DebeziumEvent event = objectMapper.readValue(raw, DebeziumEvent.class);
        when(typeDecoder.decode(anyString(), any())).thenReturn("same@gmail.com");

        DiffResult result = diffEngine.compute(event);

        assertTrue(result.isEmpty());
    }

    @Test
    void delete_shouldCaptureAllFieldsAsRemoved() throws Exception {
        String raw = """
            {
              "payload": {
                "before": {"id": 1, "email": "arjun@gmail.com", "balance": 5000},
                "after": null,
                "op": "d",
                "source": {"table": "users", "db": "testdb", "schema": "public"},
                "ts_ms": 1000
              }
            }
            """;

        DebeziumEvent event = objectMapper.readValue(raw, DebeziumEvent.class);

        when(typeDecoder.decode(eq("id"), any())).thenReturn(1);
        when(typeDecoder.decode(eq("email"), any())).thenReturn("arjun@gmail.com");
        when(typeDecoder.decode(eq("balance"), any())).thenReturn(5000);

        DiffResult result = diffEngine.compute(event);

        assertEquals("DELETE", result.getOperation());
        // for DELETE, newValue must be null for every field
        result.getChanges().forEach(change -> assertNull(change.getNewValue()));
    }

    @Test
    void sensitiveField_shouldBeRedacted() throws Exception {
        // password changed — the real values must never appear in the diff,
        // even though they genuinely changed and should still be recorded
        String raw = """
            {
              "payload": {
                "before": {"id": 1, "password": "oldHash123"},
                "after":  {"id": 1, "password": "newHash456"},
                "op": "u",
                "source": {"table": "users", "db": "testdb", "schema": "public"},
                "ts_ms": 1000
              }
            }
            """;

        DebeziumEvent event = objectMapper.readValue(raw, DebeziumEvent.class);

        when(typeDecoder.decode(eq("id"), any())).thenReturn(1);
        when(typeDecoder.decode(eq("password"), any()))
                .thenReturn("oldHash123")
                .thenReturn("newHash456");

        DiffResult result = diffEngine.compute(event);

        // the change must still be detected and recorded...
        assertEquals(1, result.getChanges().size());
        assertEquals("password", result.getChanges().get(0).getFieldName());
        // ...but the real values must never appear anywhere
        assertEquals("***REDACTED***", result.getChanges().get(0).getOldValue());
        assertEquals("***REDACTED***", result.getChanges().get(0).getNewValue());
    }

    @Test
    void nullPayload_shouldThrowMalformedEventException() {
        DebeziumEvent event = new DebeziumEvent();
        event.setPayload(null);

        assertThrows(MalformedEventException.class, () -> diffEngine.compute(event));
    }
}
