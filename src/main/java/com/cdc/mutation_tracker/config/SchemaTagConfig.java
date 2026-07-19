package com.cdc.mutation_tracker.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
@ConfigurationProperties(prefix = "schema-tags")
@Data
public class SchemaTagConfig {

    private Map<String, TableConfig> tables;

    @Data
    public static class TableConfig {
        private Map<String, String> columns;
    }

    public String getTag(String tableName, String fieldName) {
        if (tables == null) return "untagged";
        TableConfig tableConfig = tables.get(tableName);
        if (tableConfig == null) return "untagged";
        return tableConfig.getColumns().getOrDefault(fieldName, "untagged");
    }
}