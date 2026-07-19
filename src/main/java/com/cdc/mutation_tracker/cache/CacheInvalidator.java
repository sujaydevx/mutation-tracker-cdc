package com.cdc.mutation_tracker.cache;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component
@Slf4j
public class CacheInvalidator {

    private final RedisTemplate<String, String> redisTemplate;
    private final Counter successCounter;
    private final Counter failureCounter;

    public CacheInvalidator(
            RedisTemplate<String, String> redisTemplate,
            MeterRegistry meterRegistry) {
        this.redisTemplate = redisTemplate;
        this.successCounter = Counter.builder("cache.invalidation.success")
                .description("successful cache invalidations")
                .register(meterRegistry);
        this.failureCounter = Counter.builder("cache.invalidation.failure")
                .description("failed cache invalidations")
                .register(meterRegistry);
    }

    public void invalidate(String tableName, String rowId) {
        try {
            String exactKey = tableName + ":" + rowId;
            redisTemplate.delete(exactKey);

            Set<String> keys = redisTemplate.keys(tableName + ":*");
            if (keys != null && !keys.isEmpty()) {
                redisTemplate.delete(keys);
            }

            successCounter.increment();
            log.debug("Cache invalidated: {}", exactKey);

        } catch (Exception e) {
            failureCounter.increment();
            log.error("Cache invalidation failed for {}:{} — {}",
                    tableName, rowId, e.getMessage());
        }
    }
}