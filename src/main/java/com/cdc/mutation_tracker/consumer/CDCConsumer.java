package com.cdc.mutation_tracker.consumer;

import com.cdc.mutation_tracker.engine.DiffEngine;
import com.cdc.mutation_tracker.exception.MalformedEventException;
import com.cdc.mutation_tracker.model.DebeziumEvent;
import com.cdc.mutation_tracker.model.DiffResult;
import com.cdc.mutation_tracker.router.EventRouter;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class CDCConsumer {

    private final ObjectMapper objectMapper;
    private final DiffEngine diffEngine;
    private final EventRouter eventRouter;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @KafkaListener(
            topics = {
                    "cdc.public.users",
                    "cdc.public.orders",
                    "cdc.public.payments"
            },
            groupId = "cdc-consumer-group"
    )
    public void consume(
            @org.springframework.messaging.handler.annotation.Payload(required = false) String rawMessage,
            Acknowledgment acknowledgment) {
        try {

            // handle Debezium tombstone message after DELETE
            if (rawMessage == null) {
                acknowledgment.acknowledge();
                return;
            }

            DebeziumEvent event = objectMapper
                    .readValue(rawMessage, DebeziumEvent.class);

            if (event.getPayload() == null) {
                throw new MalformedEventException("Payload is null");
            }

            if ("r".equals(event.getPayload().getOp())) {
                acknowledgment.acknowledge();
                return;
            }

            DiffResult diff = diffEngine.compute(event);

            if (diff.isEmpty()) {
                acknowledgment.acknowledge();
                return;
            }

            eventRouter.route(diff);
            acknowledgment.acknowledge();

            log.info("Successfully processed: op={} table={} rowId={}",
                    diff.getOperation(),
                    diff.getTableName(),
                    diff.getRowId());

        } catch (MalformedEventException e) {
            log.error("Malformed event, routing to DLQ: {}", e.getMessage());
            kafkaTemplate.send("audit.dead-letter", rawMessage);
            acknowledgment.acknowledge();

        } catch (Exception e) {
            // Unknown failure (Redis/DB down, bug, etc.). We don't retry forever here —
            // send to DLQ so one bad message can't block the whole consumer group.
            log.error("Processing failed, routing to DLQ: {}", e.getMessage());
            kafkaTemplate.send("audit.dead-letter", rawMessage);
            acknowledgment.acknowledge();
        }
    }
}