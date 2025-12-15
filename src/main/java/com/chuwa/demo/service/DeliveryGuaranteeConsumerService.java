package com.chuwa.demo.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Service demonstrating consumer behavior for different delivery guarantees
 */
@Service
public class DeliveryGuaranteeConsumerService {

    private final AtomicInteger atMostOnceCounter = new AtomicInteger(0);
    private final AtomicInteger atLeastOnceCounter = new AtomicInteger(0);
    private final AtomicInteger exactlyOnceCounter = new AtomicInteger(0);

    /**
     * AT-MOST-ONCE Consumer
     * Auto-commit enabled - offset committed before processing
     * Risk: If consumer crashes during processing, message is lost
     */
    @KafkaListener(
        topics = "${kafka.delivery.topic.name}",
        groupId = "at-most-once-group",
        containerFactory = "atMostOnceListenerFactory",
        autoStartup = "${kafka.delivery.at-most-once.enabled:true}"
    )
    public void consumeAtMostOnce(
        @Payload String message,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.OFFSET) long offset,
        @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key
    ) {
        int count = atMostOnceCounter.incrementAndGet();

        System.out.println(String.format(
            "\n[AT-MOST-ONCE CONSUMER #%d] Partition: %d | Offset: %d | Key: %s | Message: %s",
            count, partition, offset, key, message
        ));

        // Simulate processing
        try {
            Thread.sleep(100); // Simulate work

            // If crash happens here, message is LOST (offset already committed)
            if (message.contains("crash")) {
                System.err.println("[AT-MOST-ONCE CONSUMER] Simulating crash - message lost!");
                throw new RuntimeException("Simulated crash");
            }

            System.out.println(String.format("[AT-MOST-ONCE CONSUMER] Processing completed: %s",message));
        } catch (Exception e) {
            System.err.println("[AT-MOST-ONCE CONSUMER] Error: " + e.getMessage());
            // Message is lost - offset was already committed by auto-commit
        }
    }

    /**
     * AT-LEAST-ONCE Consumer
     * Manual commit after processing
     * Risk: If consumer crashes after processing but before commit, message is reprocessed
     */
    @KafkaListener(
        topics = "${kafka.delivery.topic.name}",
        groupId = "at-least-once-group",
        containerFactory = "atLeastOnceListenerFactory",
        autoStartup = "${kafka.delivery.at-least-once.enabled:true}"
    )
    public void consumeAtLeastOnce(
        @Payload String message,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.OFFSET) long offset,
        @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key,
        Acknowledgment acknowledgment
    ) {
        int count = atLeastOnceCounter.incrementAndGet();

        System.out.println(String.format(
            "\n[AT-LEAST-ONCE CONSUMER #%d] Partition: %d | Offset: %d | Key: %s | Message: %s",
            count, partition, offset, key, message
        ));

        try {
            // Process the message
            Thread.sleep(100); // Simulate work


            // If crash happens here, message is REPROCESSED (not yet committed)
            if (message.contains("crash")) {
                System.err.println("[AT-LEAST-ONCE CONSUMER] Simulating crash before commit - message will be redelivered!");
                throw new RuntimeException("Simulated crash before commit");
            }
            System.out.println(String.format("[AT-LEAST-ONCE CONSUMER] Processing completed: %s",message));
            // Manual commit - only after successful processing
            acknowledgment.acknowledge();
            System.out.println("[AT-LEAST-ONCE CONSUMER] Offset committed");

        } catch (Exception e) {
            System.err.println("[AT-LEAST-ONCE CONSUMER] Error: " + e.getMessage());
            // Don't commit offset - message will be redelivered
            // In production: Consider implementing retry logic and dead letter queue
        }
    }

    /**
     * EXACTLY-ONCE Consumer
     * Reads only committed messages (transactional), manual commit
     * Guarantee: Each message processed exactly once
     * Note: Application must be idempotent to handle potential reprocessing
     */
    @KafkaListener(
        topics = "${kafka.delivery.topic.name}",
        groupId = "exactly-once-group",
        containerFactory = "exactlyOnceListenerFactory",
        autoStartup = "${kafka.delivery.exactly-once.enabled:true}"
    )
    public void consumeExactlyOnce(
        @Payload String message,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.OFFSET) long offset,
        @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key,
        Acknowledgment acknowledgment
    ) {
        int count = exactlyOnceCounter.incrementAndGet();

        System.out.println(String.format(
            "\n[EXACTLY-ONCE CONSUMER #%d] Partition: %d | Offset: %d | Key: %s | Message: %s",
            count, partition, offset, key, message
        ));

        try {
            // Check for duplicate processing using idempotency key (message key)
            if (key != null && isDuplicate(key)) {
                System.out.println("[EXACTLY-ONCE CONSUMER] Duplicate detected, skipping processing");
                acknowledgment.acknowledge();
                return;
            }

            // Process the message
            Thread.sleep(100); // Simulate work

            // Simulate idempotent operation (e.g., database upsert with unique key)
            processIdempotently(key, message);

            System.out.println("[EXACTLY-ONCE CONSUMER] Processing completed");

            // Commit offset
            acknowledgment.acknowledge();
            System.out.println("[EXACTLY-ONCE CONSUMER] Offset committed");

        } catch (Exception e) {
            System.err.println("[EXACTLY-ONCE CONSUMER] Error: " + e.getMessage());
            // Don't commit - message will be redelivered
            // But since we use idempotency key, reprocessing is safe
        }
    }

    /**
     * Check if message was already processed (in production: check database/cache)
     */
    private boolean isDuplicate(String idempotencyKey) {
        // In production: Check Redis/Database for this key
        // For demo: always return false
        return false;
    }

    /**
     * Process message idempotently (in production: use unique constraints, upserts)
     */
    private void processIdempotently(String key, String message) {
        // In production:
        // 1. Use database unique constraints on idempotency key
        // 2. Use UPSERT operations instead of INSERT
        // 3. Store processing results with the idempotency key

        System.out.println("[EXACTLY-ONCE CONSUMER] Storing with idempotency key: " + key);
    }

    /**
     * Reset counters for testing
     */
    public void resetCounters() {
        atMostOnceCounter.set(0);
        atLeastOnceCounter.set(0);
        exactlyOnceCounter.set(0);
        System.out.println("\n[CONSUMERS] All counters reset");
    }

    /**
     * Get current counter values
     */
    public String getStats() {
        return String.format(
            "At-Most-Once: %d, At-Least-Once: %d, Exactly-Once: %d",
            atMostOnceCounter.get(),
            atLeastOnceCounter.get(),
            exactlyOnceCounter.get()
        );
    }
}
