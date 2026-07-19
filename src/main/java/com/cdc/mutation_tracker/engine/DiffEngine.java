package com.cdc.mutation_tracker.engine;

import com.cdc.mutation_tracker.config.SchemaTagConfig;
import com.cdc.mutation_tracker.exception.MalformedEventException;
import com.cdc.mutation_tracker.model.DebeziumEvent;
import com.cdc.mutation_tracker.model.DiffResult;
import com.cdc.mutation_tracker.model.FieldChange;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Takes a raw Debezium event (a database change) and figures out
 * exactly which fields changed, from what value to what value.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class DiffEngine {

    private final SchemaTagConfig schemaTagConfig;
    private final DebeziumTypeDecoder typeDecoder;

    // Any field whose name contains one of these words is treated as
    // sensitive and its value is masked before it goes anywhere else
    // (logs, database, the AI summary call). This protects fields like
    // "password" even if someone forgets to tag them in schema-tags.yml.
    private static final List<String> SENSITIVE_FIELD_KEYWORDS =
            List.of("password", "pwd", "secret", "token", "ssn", "pin");

    private static final String REDACTED_VALUE = "***REDACTED***";

    public DiffResult compute(DebeziumEvent event) {
        if (event.getPayload() == null) {
            throw new MalformedEventException("Event has no payload");
        }

        DebeziumEvent.Payload payload = event.getPayload();
        String op = payload.getOp();
        String tableName = payload.getSource().getTable();

        if (op == null) {
            throw new MalformedEventException("op field missing");
        }

        List<FieldChange> changes;
        if (op.equals("c")) {
            changes = handleInsert(payload.getAfter(), tableName);
        } else if (op.equals("u")) {
            changes = handleUpdate(payload.getBefore(), payload.getAfter(), tableName);
        } else if (op.equals("d")) {
            changes = handleDelete(payload.getBefore(), tableName);
        } else {
            log.warn("Unknown op: {}", op);
            changes = new ArrayList<>();
        }

        String rowId = extractRowId(payload.getAfter(), payload.getBefore());

        return DiffResult.builder()
                .tableName(tableName)
                .operation(mapOperation(op))
                .rowId(rowId)
                .changes(changes)
                .eventTimestamp(payload.getTs_ms())
                .build();
    }

    // INSERT: every field is "new" — there is no old value
    private List<FieldChange> handleInsert(JsonNode after, String tableName) {
        if (after == null || after.isNull()) {
            throw new MalformedEventException("INSERT has null after");
        }

        List<FieldChange> changes = new ArrayList<>();
        Iterator<String> fieldNames = after.fieldNames();

        while (fieldNames.hasNext()) {
            String field = fieldNames.next();
            if (field.startsWith("__")) {
                continue; // Debezium's internal metadata fields, not real columns
            }

            Object newValue = typeDecoder.decode(field, after.get(field));
            if (isSensitiveField(field)) {
                newValue = REDACTED_VALUE;
            }

            String tag = schemaTagConfig.getTag(tableName, field);
            changes.add(new FieldChange(field, null, newValue, tag));
        }

        return changes;
    }

    // UPDATE: compare before vs after, only keep fields that actually changed
    private List<FieldChange> handleUpdate(JsonNode before, JsonNode after, String tableName) {
        if (before == null || before.isNull() || after == null || after.isNull()) {
            throw new MalformedEventException(
                    "UPDATE missing before/after. Run: ALTER TABLE users REPLICA IDENTITY FULL"
            );
        }

        List<FieldChange> changes = new ArrayList<>();
        Iterator<String> fieldNames = after.fieldNames();

        while (fieldNames.hasNext()) {
            String field = fieldNames.next();
            if (field.startsWith("__")) {
                continue;
            }

            Object oldValue = typeDecoder.decode(field, before.get(field));
            Object newValue = typeDecoder.decode(field, after.get(field));

            if (Objects.equals(oldValue, newValue)) {
                continue; // nothing actually changed, skip it
            }

            // Compare the real values above, but never store/log the real
            // value for a sensitive field — mask it right before saving.
            if (isSensitiveField(field)) {
                oldValue = REDACTED_VALUE;
                newValue = REDACTED_VALUE;
            }

            String tag = schemaTagConfig.getTag(tableName, field);
            changes.add(new FieldChange(field, oldValue, newValue, tag));
        }

        return changes;
    }

    // DELETE: every field is "removed" — there is no new value
    private List<FieldChange> handleDelete(JsonNode before, String tableName) {
        if (before == null || before.isNull()) {
            throw new MalformedEventException("DELETE has null before");
        }

        List<FieldChange> changes = new ArrayList<>();
        Iterator<String> fieldNames = before.fieldNames();

        while (fieldNames.hasNext()) {
            String field = fieldNames.next();
            if (field.startsWith("__")) {
                continue;
            }

            Object oldValue = typeDecoder.decode(field, before.get(field));
            if (isSensitiveField(field)) {
                oldValue = REDACTED_VALUE;
            }

            String tag = schemaTagConfig.getTag(tableName, field);
            changes.add(new FieldChange(field, oldValue, null, tag));
        }

        return changes;
    }

    private boolean isSensitiveField(String fieldName) {
        String lowerCaseField = fieldName.toLowerCase();
        for (String keyword : SENSITIVE_FIELD_KEYWORDS) {
            if (lowerCaseField.contains(keyword)) {
                return true;
            }
        }
        return false;
    }

    private String extractRowId(JsonNode after, JsonNode before) {
        if (after != null && after.has("id")) return after.get("id").asText();
        if (before != null && before.has("id")) return before.get("id").asText();
        return "unknown";
    }

    private String mapOperation(String op) {
        if (op.equals("c")) return "INSERT";
        if (op.equals("u")) return "UPDATE";
        if (op.equals("d")) return "DELETE";
        return op;
    }
}
