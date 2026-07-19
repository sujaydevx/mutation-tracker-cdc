package com.cdc.mutation_tracker.audit;

import com.cdc.mutation_tracker.model.AuditLog;
import com.cdc.mutation_tracker.model.DiffResult;
import com.cdc.mutation_tracker.model.FieldChange;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class AuditLogService {

    private final AuditLogRepository auditLogRepository;
    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private final Counter auditLogCounter;

    @Value("${groq.api.key}")
    private String groqApiKey;

    @Value("${groq.api.url}")
    private String groqApiUrl;

    @Value("${groq.api.model}")
    private String groqModel;

    public AuditLogService(
            AuditLogRepository auditLogRepository,
            WebClient.Builder webClientBuilder,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry) {
        this.auditLogRepository = auditLogRepository;
        this.webClient = webClientBuilder.build();
        this.objectMapper = objectMapper;
        this.auditLogCounter = Counter.builder("audit.logs.saved")
                .description("total audit logs saved")
                .register(meterRegistry);
    }

    // REQUIRES_NEW = brand new transaction, commits immediately
    // independent from Kafka listener transaction
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void createAuditLog(DiffResult diff) {
        String humanReadableLog = generateHumanReadableLog(diff);

        String tags = diff.getChanges().stream()
                .map(FieldChange::getTag)
                .distinct()
                .collect(Collectors.joining(","));

        String changedFieldsJson = serializeChanges(diff.getChanges());

        AuditLog auditLog = AuditLog.builder()
                .tableName(diff.getTableName())
                .rowId(diff.getRowId())
                .operation(diff.getOperation())
                .changedFields(changedFieldsJson)
                .humanReadableLog(humanReadableLog)
                .tags(tags)
                .build();

        auditLogRepository.save(auditLog);
        auditLogRepository.flush();
        auditLogCounter.increment();

        log.info("Audit log saved for {}.{} — {}",
                diff.getTableName(), diff.getRowId(), diff.getOperation());
    }

    private String generateHumanReadableLog(DiffResult diff) {
        // Only pay for an AI call when the change is actually sensitive
        // (pii or financial). Routine/operational changes just use the
        // plain fallback sentence directly — it's already readable, and
        // it means we're not hitting Groq for every single row change.
        boolean isSensitiveChange = diff.hasPiiChanges() || diff.hasFinancialChanges();

        if (!isSensitiveChange) {
            return generateFallbackLog(diff);
        }

        try {
            return callGroqApi(diff);
        } catch (Exception e) {
            log.warn("Groq API failed, using fallback: {}", e.getMessage());
            return generateFallbackLog(diff);
        }
    }

    private String callGroqApi(DiffResult diff) throws Exception {
        String changesDescription = diff.getChanges().stream()
                .map(FieldChange::toHumanReadable)
                .collect(Collectors.joining(", "));

        String prompt = String.format(
                "Generate one clear audit log sentence. " +
                        "Table: %s, Operation: %s, Row ID: %s, Changes: %s.",
                diff.getTableName(),
                diff.getOperation(),
                diff.getRowId(),
                changesDescription
        );

        Map<String, Object> message = new HashMap<>();
        message.put("role", "user");
        message.put("content", prompt);

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("model", groqModel);
        requestBody.put("max_tokens", 150);
        requestBody.put("messages", List.of(message));

        String requestJson = objectMapper.writeValueAsString(requestBody);

        String response = webClient.post()
                .uri(groqApiUrl)
                .header("Authorization", "Bearer " + groqApiKey)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .bodyValue(requestJson)
                .retrieve()
                .bodyToMono(String.class)
                .block();

        var responseNode = objectMapper.readTree(response);
        return responseNode
                .path("choices")
                .get(0)
                .path("message")
                .path("content")
                .asText();
    }

    private String generateFallbackLog(DiffResult diff) {
        String changes = diff.getChanges().stream()
                .map(FieldChange::toHumanReadable)
                .collect(Collectors.joining(", "));
        return String.format("%s on %s (ID: %s): %s",
                diff.getOperation(),
                diff.getTableName(),
                diff.getRowId(),
                changes);
    }

    private String serializeChanges(List<FieldChange> changes) {
        try {
            return objectMapper.writeValueAsString(changes);
        } catch (Exception e) {
            return "[]";
        }
    }
}