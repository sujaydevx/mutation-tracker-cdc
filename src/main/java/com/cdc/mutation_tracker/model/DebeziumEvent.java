package com.cdc.mutation_tracker.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class DebeziumEvent {

    private Payload payload;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Payload {
        private JsonNode before;
        private JsonNode after;
        private String op;
        private Source source;
        private Long ts_ms;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Source {
        private String table;
        private String db;
        private String schema;
        private Long lsn;
    }
}