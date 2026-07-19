package com.cdc.mutation_tracker.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Covers the two things worth testing here: plain values decode as-is,
 * and PostgreSQL's special NUMERIC encoding decodes correctly (and never
 * crashes the pipeline even if it's malformed).
 */
class DebeziumTypeDecoderTest {

    private DebeziumTypeDecoder decoder;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        decoder = new DebeziumTypeDecoder();
        objectMapper = new ObjectMapper();
    }

    @Test
    void nullOrPlainValues_shouldDecodeAsIs() throws Exception {
        assertNull(decoder.decode("field", null));
        assertNull(decoder.decode("field", objectMapper.readTree("null")));

        JsonNode stringNode = objectMapper.readTree("\"arjun@gmail.com\"");
        assertEquals("arjun@gmail.com", decoder.decode("email", stringNode));
    }

    @Test
    void variableScaleDecimal_shouldDecodeToBigDecimal() throws Exception {
        // This is how Debezium encodes PostgreSQL NUMERIC columns:
        // scale=0, value=Base64 encoded bytes of the number 5000
        JsonNode node = objectMapper.readTree("{\"scale\": 0, \"value\": \"E4g=\"}");

        Object result = decoder.decode("balance", node);

        assertInstanceOf(BigDecimal.class, result);
    }

    @Test
    void invalidBase64_shouldReturnRawStringInsteadOfCrashing() throws Exception {
        // if decoding fails, never crash the pipeline over a type issue —
        // just fall back to the raw string
        JsonNode node = objectMapper.readTree("{\"scale\": 0, \"value\": \"!!!invalid!!!\"}");

        Object result = decoder.decode("balance", node);

        assertInstanceOf(String.class, result);
    }
}
