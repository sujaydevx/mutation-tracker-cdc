package com.cdc.mutation_tracker.cache;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Set;

import static org.mockito.Mockito.*;

/**
 * Covers the two main jobs of CacheInvalidator: deleting the exact key
 * plus any related list-caches, and never letting a Redis failure crash
 * the pipeline.
 */
@ExtendWith(MockitoExtension.class)
class CacheInvalidatorTest {

    @Mock
    private RedisTemplate<String, String> redisTemplate;

    private CacheInvalidator cacheInvalidator;

    @BeforeEach
    void setUp() {
        cacheInvalidator = new CacheInvalidator(redisTemplate, new SimpleMeterRegistry());
    }

    @Test
    void invalidate_shouldDeleteExactKeyAndPatternKeys() {
        // when users:5 changes, delete that exact key AND any list caches
        // like users:all, users:page:1
        Set<String> listKeys = Set.of("users:all", "users:page:1");
        when(redisTemplate.keys("users:*")).thenReturn(listKeys);

        cacheInvalidator.invalidate("users", "5");

        verify(redisTemplate).delete("users:5");
        verify(redisTemplate).delete(listKeys);
    }

    @Test
    void invalidate_redisThrowsException_shouldNotPropagate() {
        // Redis being down must never stop the CDC pipeline
        when(redisTemplate.keys(anyString())).thenThrow(new RuntimeException("Redis down"));

        cacheInvalidator.invalidate("users", "5"); // should not throw
    }
}
