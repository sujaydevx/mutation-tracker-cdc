package com.cdc.mutation_tracker.router;

import com.cdc.mutation_tracker.audit.AuditLogService;
import com.cdc.mutation_tracker.cache.CacheInvalidator;
import com.cdc.mutation_tracker.model.DiffResult;
import com.cdc.mutation_tracker.model.FieldChange;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;

import static org.mockito.Mockito.*;

/**
 * Covers the routing rules: pii/financial changes trigger alerts,
 * everything always gets an audit log + cache invalidation, and a
 * Kafka failure never blocks the audit log or cache steps.
 */
@ExtendWith(MockitoExtension.class)
class EventRouterTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private AuditLogService auditLogService;

    @Mock
    private CacheInvalidator cacheInvalidator;

    private EventRouter eventRouter;

    @BeforeEach
    void setUp() {
        eventRouter = new EventRouter(
                kafkaTemplate,
                auditLogService,
                cacheInvalidator,
                new ObjectMapper(),
                new SimpleMeterRegistry()
        );
    }

    @Test
    void piiAndFinancialChanges_shouldPublishToTheRightTopicsOnly() {
        DiffResult diff = DiffResult.builder()
                .tableName("users")
                .rowId("1")
                .operation("UPDATE")
                .eventTimestamp(1000L)
                .changes(List.of(
                        new FieldChange("email", "old@gmail.com", "new@gmail.com", "pii"),
                        new FieldChange("balance", 1000, 9999, "financial")
                ))
                .build();

        eventRouter.route(diff);

        verify(kafkaTemplate).send(eq("privacy-alerts"), eq("1"), anyString());
        verify(kafkaTemplate).send(eq("financial-audit"), eq("1"), anyString());
    }

    @Test
    void operationalChange_shouldNotPublishAnyAlert() {
        DiffResult diff = DiffResult.builder()
                .tableName("orders")
                .rowId("2")
                .operation("UPDATE")
                .eventTimestamp(1000L)
                .changes(List.of(
                        new FieldChange("status", "pending", "shipped", "operational")
                ))
                .build();

        eventRouter.route(diff);

        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    void anyChange_shouldAlwaysCreateAuditLogAndInvalidateCache() {
        DiffResult diff = DiffResult.builder()
                .tableName("users")
                .rowId("3")
                .operation("INSERT")
                .eventTimestamp(1000L)
                .changes(List.of(
                        new FieldChange("email", null, "new@gmail.com", "pii")
                ))
                .build();

        eventRouter.route(diff);

        // these must happen regardless of whether an alert fired
        verify(auditLogService).createAuditLog(diff);
        verify(cacheInvalidator).invalidate("users", "3");
    }

    @Test
    void kafkaPublishFails_shouldNotStopAuditLogOrCacheInvalidation() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenThrow(new RuntimeException("Kafka down"));

        DiffResult diff = DiffResult.builder()
                .tableName("users")
                .rowId("4")
                .operation("UPDATE")
                .eventTimestamp(1000L)
                .changes(List.of(
                        new FieldChange("email", "old@gmail.com", "new@gmail.com", "pii")
                ))
                .build();

        eventRouter.route(diff); // should not throw

        verify(auditLogService).createAuditLog(diff);
        verify(cacheInvalidator).invalidate("users", "4");
    }
}
