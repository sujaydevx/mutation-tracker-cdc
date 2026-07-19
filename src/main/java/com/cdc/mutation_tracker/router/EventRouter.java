package com.cdc.mutation_tracker.router;

import com.cdc.mutation_tracker.audit.AuditLogService;
import com.cdc.mutation_tracker.cache.CacheInvalidator;
import com.cdc.mutation_tracker.model.DiffResult;
import com.cdc.mutation_tracker.model.FieldChange;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@Slf4j
public class EventRouter {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final AuditLogService auditLogService;
    private final CacheInvalidator cacheInvalidator;
    private final ObjectMapper objectMapper;
    private final Counter piiAlertsCounter;
    private final Counter financialAlertsCounter;

    public EventRouter(
            KafkaTemplate<String, String> kafkaTemplate,
            AuditLogService auditLogService,
            CacheInvalidator cacheInvalidator,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.auditLogService = auditLogService;
        this.cacheInvalidator = cacheInvalidator;
        this.objectMapper = objectMapper;
        this.piiAlertsCounter = Counter.builder("router.pii.alerts")
                .description("total PII alerts fired")
                .register(meterRegistry);
        this.financialAlertsCounter = Counter.builder("router.financial.alerts")
                .description("total financial alerts fired")
                .register(meterRegistry);
    }

    public void route(DiffResult diff) {
        if (diff.hasPiiChanges()) {
            publish("privacy-alerts", diff, "pii");
            piiAlertsCounter.increment();
        }
        if (diff.hasFinancialChanges()) {
            publish("financial-audit", diff, "financial");
            financialAlertsCounter.increment();
        }
        auditLogService.createAuditLog(diff);
        cacheInvalidator.invalidate(diff.getTableName(), diff.getRowId());
    }

    private void publish(String topic, DiffResult diff, String tagFilter) {
        try {
            List<String> relevantChanges = diff.getChangesByTag(tagFilter)
                    .stream()
                    .map(FieldChange::toHumanReadable)
                    .collect(Collectors.toList());

            Map<String, Object> payload = new HashMap<>();
            payload.put("tableName", diff.getTableName());
            payload.put("rowId", diff.getRowId());
            payload.put("operation", diff.getOperation());
            payload.put("timestamp", diff.getEventTimestamp());
            payload.put("changes", relevantChanges);

            String json = objectMapper.writeValueAsString(payload);
            kafkaTemplate.send(topic, diff.getRowId(), json);

            log.info("Published to {}: table={} rowId={}",
                    topic, diff.getTableName(), diff.getRowId());
        } catch (Exception e) {
            log.error("Failed to publish to {}: {}", topic, e.getMessage());
        }
    }
}