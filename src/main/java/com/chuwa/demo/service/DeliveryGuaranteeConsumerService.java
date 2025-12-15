package com.chuwa.demo.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Service demonstrating consumer behavior for different delivery guarantees
 */
@Service
public class DeliveryGuaranteeConsumerService {

    private final AtomicInteger atMostOnceCounter = new AtomicInteger(0);
    private final AtomicInteger atLeastOnceCounter = new AtomicInteger(0);
    private final AtomicInteger exactlyOnceCounter = new AtomicInteger(0);

    // Track retry attempts per offset (partition-offset -> attempt count)
    private final Map<String, Integer> retryAttempts = new ConcurrentHashMap<>();
    private static final int MAX_RETRY_ATTEMPTS = 3;

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
            if (message.contains("crash-at-most-once")) {
                System.err.println("[AT-MOST-ONCE CONSUMER] Simulating crash - message lost!");
                throw new RuntimeException("Simulated crash");
            }

            System.out.println("[AT-MOST-ONCE CONSUMER] Processing completed");
        } catch (Exception e) {
            System.err.println("[AT-MOST-ONCE CONSUMER] Error: " + e.getMessage());
            // Message is lost - offset was already committed by auto-commit
        }
    }

    /**
     * AT-LEAST-ONCE Consumer
     * Manual commit after processing
     * Risk: If consumer crashes after processing but before commit, message is reprocessed
     * Now includes: Retry limits and Dead Letter Queue handling
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
        String offsetKey = partition + "-" + offset;

        // Track retry attempts for this specific message
        int attemptNumber = retryAttempts.getOrDefault(offsetKey, 0) + 1;
        retryAttempts.put(offsetKey, attemptNumber);

        System.out.println(String.format(
            "\n[AT-LEAST-ONCE CONSUMER #%d] Partition: %d | Offset: %d | Key: %s | Message: %s | Attempt: %d/%d",
            count, partition, offset, key, message, attemptNumber, MAX_RETRY_ATTEMPTS
        ));

        try {
            // Process the message
            Thread.sleep(100); // Simulate work
            System.out.println("[AT-LEAST-ONCE CONSUMER] Processing completed");

            // If crash happens here, message is REPROCESSED (not yet committed)
            if (message.contains("crash-at-least-once")) {
                System.err.println("[AT-LEAST-ONCE CONSUMER] Simulating crash before commit - message will be redelivered!");
                throw new RuntimeException("Simulated crash before commit");
            }

            // Success! Manual commit and clear retry tracking
            acknowledgment.acknowledge();
            retryAttempts.remove(offsetKey);
            System.out.println("[AT-LEAST-ONCE CONSUMER] Offset committed successfully");

        } catch (Exception e) {
            System.err.println("[AT-LEAST-ONCE CONSUMER] Error: " + e.getMessage());

            // Check if we've exceeded max retry attempts
            if (attemptNumber >= MAX_RETRY_ATTEMPTS) {
                System.err.println(String.format(
                    "[AT-LEAST-ONCE CONSUMER] Max retries (%d) reached for offset %d. Sending to Dead Letter Queue.",
                    MAX_RETRY_ATTEMPTS, offset
                ));

                // Send to Dead Letter Queue (DLQ)
                sendToDeadLetterQueue(partition, offset, key, message, e);

                // Commit the offset to move past this poison message
                acknowledgment.acknowledge();
                retryAttempts.remove(offsetKey);
                System.err.println("[AT-LEAST-ONCE CONSUMER] Poison message handled. Offset committed to move forward.");
            } else {
                // Don't commit - RE-THROW the exception so Spring Kafka will retry
                System.err.println(String.format(
                    "[AT-LEAST-ONCE CONSUMER] Will retry (%d/%d). Offset NOT committed. Re-throwing exception for retry.",
                    attemptNumber, MAX_RETRY_ATTEMPTS
                ));
                // RE-THROW the exception - this is CRITICAL for Spring Kafka to retry!
                throw new RuntimeException("Retry attempt " + attemptNumber + "/" + MAX_RETRY_ATTEMPTS, e);
            }
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
     * Send failed message to Dead Letter Queue
     * In production: Send to actual DLQ topic, log to monitoring system, alert team
     */
    private void sendToDeadLetterQueue(int partition, long offset, String key, String message, Exception error) {
        System.err.println("\n========== DEAD LETTER QUEUE ==========");
        System.err.println(String.format("Partition: %d, Offset: %d", partition, offset));
        System.err.println(String.format("Key: %s", key));
        System.err.println(String.format("Message: %s", message));
        System.err.println(String.format("Error: %s", error.getMessage()));
        System.err.println("========================================\n");

        // In production, you would:
        // 1. Send to a dedicated DLQ Kafka topic (e.g., "delivery-guarantee-demo-dlq")
        // 2. Store in database with full context for later replay
        // 3. Send alert to monitoring system (PagerDuty, Slack, etc.)
        // 4. Log to centralized logging system
        //
        // Example:
        // kafkaTemplate.send("delivery-guarantee-demo-dlq", key, message);
        // alertService.sendAlert("Poison message detected", message);
    }

    /**
     * Reset counters for testing
     */
    public void resetCounters() {
        atMostOnceCounter.set(0);
        atLeastOnceCounter.set(0);
        exactlyOnceCounter.set(0);
        retryAttempts.clear();
        System.out.println("\n[CONSUMERS] All counters and retry attempts reset");
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
