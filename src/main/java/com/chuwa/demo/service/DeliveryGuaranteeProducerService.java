package com.chuwa.demo.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Service demonstrating different Kafka delivery guarantees
 */
@Service
public class DeliveryGuaranteeProducerService {

    @Value("${kafka.delivery.topic.name}")
    private String topicName;

    @Autowired
    @Qualifier("atMostOnceKafkaTemplate")
    private KafkaTemplate<String, String> atMostOnceTemplate;

    @Autowired
    @Qualifier("atLeastOnceKafkaTemplate")
    private KafkaTemplate<String, String> atLeastOnceTemplate;

    @Autowired
    @Qualifier("exactlyOnceKafkaTemplate")
    private KafkaTemplate<String, String> exactlyOnceTemplate;

    /**
     * AT-MOST-ONCE: Fire and forget, no retries
     * Message may be lost but never duplicated
     */
    public void sendAtMostOnce(String key, String message) {
        System.out.println("\n[AT-MOST-ONCE PRODUCER] Sending message: " + message);

        // Fire and forget - don't wait for acknowledgment
        atMostOnceTemplate.send(topicName, key, message);

        System.out.println("[AT-MOST-ONCE PRODUCER] Message sent (fire and forget)");
    }

    /**
     * AT-LEAST-ONCE: Wait for acknowledgment, with retries
     * Message will never be lost but may be duplicated
     */
    public void sendAtLeastOnce(String key, String message) {
        System.out.println("\n[AT-LEAST-ONCE PRODUCER] Sending message: " + message);

        try {
            // Wait for acknowledgment from all replicas
            CompletableFuture<SendResult<String, String>> future =
                atLeastOnceTemplate.send(topicName, key, message);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    System.out.println(String.format(
                        "[AT-LEAST-ONCE PRODUCER] Message sent successfully to partition %d, offset %d",
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset()
                    ));
                } else {
                    System.err.println("[AT-LEAST-ONCE PRODUCER] Failed to send message: " + ex.getMessage());
                    // In production: implement retry logic or dead letter queue
                }
            });
        } catch (Exception e) {
            System.err.println("[AT-LEAST-ONCE PRODUCER] Error: " + e.getMessage());
        }
    }

    /**
     * EXACTLY-ONCE: Transactional send with idempotence
     * Message delivered exactly once, no loss, no duplication
     */
    public void sendExactlyOnce(String key, String message) {
        System.out.println("\n[EXACTLY-ONCE PRODUCER] Sending message: " + message);

        try {
            // Execute within a transaction
            exactlyOnceTemplate.executeInTransaction(operations -> {
                CompletableFuture<SendResult<String, String>> future =
                    operations.send(topicName, key, message);

                future.whenComplete((result, ex) -> {
                    if (ex == null) {
                        System.out.println(String.format(
                            "[EXACTLY-ONCE PRODUCER] Message sent transactionally to partition %d, offset %d",
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset()
                        ));
                    } else {
                        System.err.println("[EXACTLY-ONCE PRODUCER] Transaction failed: " + ex.getMessage());
                    }
                });

                return true;
            });

            System.out.println("[EXACTLY-ONCE PRODUCER] Transaction committed");
        } catch (Exception e) {
            System.err.println("[EXACTLY-ONCE PRODUCER] Transaction aborted: " + e.getMessage());
        }
    }

    /**
     * Send batch of messages with exactly-once semantics
     */
    public void sendBatchExactlyOnce(String keyPrefix, int count) {
        System.out.println("\n[EXACTLY-ONCE PRODUCER] Sending batch of " + count + " messages");

        try {
            exactlyOnceTemplate.executeInTransaction(operations -> {
                for (int i = 1; i <= count; i++) {
                    String key = keyPrefix + "-" + i;
                    String message = "Batch message " + i;
                    operations.send(topicName, key, message);
                }
                System.out.println("[EXACTLY-ONCE PRODUCER] Batch of " + count + " messages sent in transaction");
                return true;
            });

            System.out.println("[EXACTLY-ONCE PRODUCER] Batch transaction committed");
        } catch (Exception e) {
            System.err.println("[EXACTLY-ONCE PRODUCER] Batch transaction failed: " + e.getMessage());
        }
    }
}
