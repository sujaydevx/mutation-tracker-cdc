package com.cdc.mutation_tracker.engine;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Base64;

@Component
@Slf4j
public class DebeziumTypeDecoder {

    public Object decode(String fieldName, JsonNode node) {
        if (node == null || node.isNull()) return null;
        if (node.isTextual()) return node.asText();
        if (node.isNumber()) return node.numberValue();
        if (node.isBoolean()) return node.booleanValue();
        if (node.isObject() && node.has("scale") && node.has("value")) {
            return decodeNumeric(fieldName, node);
        }
        return node.toString();
    }

    private Object decodeNumeric(String fieldName, JsonNode node) {
        try {
            byte[] bytes = Base64.getDecoder()
                    .decode(node.get("value").asText());
            BigInteger unscaled = new BigInteger(bytes);
            int scale = node.get("scale").asInt();
            return new BigDecimal(unscaled, scale);
        } catch (Exception e) {
            log.warn("Could not decode NUMERIC field '{}': {}",
                    fieldName, e.getMessage());
            return node.toString();
        }
    }
}
