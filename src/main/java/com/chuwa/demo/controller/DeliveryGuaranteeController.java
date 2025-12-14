package com.chuwa.demo.controller;

import com.chuwa.demo.service.DeliveryGuaranteeConsumerService;
import com.chuwa.demo.service.DeliveryGuaranteeProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * Controller for demonstrating different Kafka delivery guarantees
 */
@RestController
@RequestMapping("/delivery-guarantee")
public class DeliveryGuaranteeController {

    @Autowired
    private DeliveryGuaranteeProducerService producerService;

    @Autowired
    private DeliveryGuaranteeConsumerService consumerService;

    /**
     * AT-MOST-ONCE: Send message with fire-and-forget
     * Use case: Metrics, logs where occasional loss is acceptable
     */
    @PostMapping("/at-most-once")
    public String sendAtMostOnce(
            @RequestParam(value = "key", required = false) String key,
            @RequestParam("message") String message) {

        producerService.sendAtMostOnce(key, message);
        return "Message sent with AT-MOST-ONCE guarantee (fire and forget)";
    }

    /**
     * AT-LEAST-ONCE: Send message with acknowledgment and retries
     * Use case: Most common - acceptable if consumer is idempotent
     */
    @PostMapping("/at-least-once")
    public String sendAtLeastOnce(
            @RequestParam(value = "key", required = false) String key,
            @RequestParam("message") String message) {

        producerService.sendAtLeastOnce(key, message);
        return "Message sent with AT-LEAST-ONCE guarantee (with retries)";
    }

    /**
     * EXACTLY-ONCE: Send message transactionally
     * Use case: Financial transactions, critical data where no loss/duplication allowed
     */
    @PostMapping("/exactly-once")
    public String sendExactlyOnce(
            @RequestParam(value = "key", required = false) String key,
            @RequestParam("message") String message) {

        producerService.sendExactlyOnce(key, message);
        return "Message sent with EXACTLY-ONCE guarantee (transactional)";
    }

    /**
     * Send batch of messages with exactly-once semantics
     */
    @PostMapping("/exactly-once/batch")
    public String sendBatchExactlyOnce(
            @RequestParam("keyPrefix") String keyPrefix,
            @RequestParam(value = "count", defaultValue = "5") int count) {

        producerService.sendBatchExactlyOnce(keyPrefix, count);
        return String.format("Batch of %d messages sent with EXACTLY-ONCE guarantee", count);
    }

    /**
     * Test at-most-once with simulated consumer crash
     * Message will be LOST
     */
    @PostMapping("/test/at-most-once/crash")
    public String testAtMostOnceCrash() {
        producerService.sendAtMostOnce("test", "crash-at-most-once");
        return "Sent message that will cause consumer crash. Message will be LOST due to auto-commit.";
    }

    /**
     * Test at-least-once with simulated consumer crash
     * Message will be REDELIVERED
     */
    @PostMapping("/test/at-least-once/crash")
    public String testAtLeastOnceCrash() {
        producerService.sendAtLeastOnce("test", "crash-at-least-once");
        return "Sent message that will cause consumer crash. Message will be REDELIVERED due to no commit.";
    }

    /**
     * Get consumer statistics
     */
    @GetMapping("/stats")
    public String getStats() {
        return consumerService.getStats();
    }

    /**
     * Reset consumer counters
     */
    @PostMapping("/reset")
    public String reset() {
        consumerService.resetCounters();
        return "Consumer counters reset";
    }

    /**
     * Get explanation of delivery guarantees
     */
    @GetMapping("/info")
    public String getInfo() {
        return """
                KAFKA DELIVERY GUARANTEES DEMO
                ===============================

                1. AT-MOST-ONCE (acks=1, no retries, auto-commit)
                   - Fastest, lowest latency
                   - Messages may be LOST
                   - Messages NEVER duplicated
                   - Use case: Metrics, logs, non-critical data
                   - Endpoint: POST /delivery-guarantee/at-most-once?message=test

                2. AT-LEAST-ONCE (acks=all, retries, manual commit after processing)
                   - Most common
                   - Messages NEVER lost
                   - Messages may be DUPLICATED
                   - Consumer should be idempotent
                   - Use case: Most applications (with idempotent processing)
                   - Endpoint: POST /delivery-guarantee/at-least-once?message=test

                3. EXACTLY-ONCE (acks=all, idempotence, transactions, read_committed)
                   - Strongest guarantee
                   - Messages delivered EXACTLY ONCE
                   - Higher latency, more overhead
                   - Use case: Financial transactions, critical data
                   - Endpoint: POST /delivery-guarantee/exactly-once?message=test

                TEST ENDPOINTS:
                - POST /delivery-guarantee/test/at-most-once/crash - Demonstrates message loss
                - POST /delivery-guarantee/test/at-least-once/crash - Demonstrates redelivery
                - GET /delivery-guarantee/stats - View consumer counters
                - POST /delivery-guarantee/reset - Reset counters

                CONFIGURATION COMPARISON:
                | Setting                    | At-Most-Once | At-Least-Once    | Exactly-Once     |
                |---------------------------|--------------|------------------|------------------|
                | acks                       | 1            | all              | all              |
                | retries                    | 0            | MAX              | MAX              |
                | enable.idempotence         | false        | false            | true             |
                | transactional.id           | -            | -                | set              |
                | enable.auto.commit         | true         | false            | false            |
                | isolation.level            | read_uncommitted | read_uncommitted | read_committed |
                """;
    }
}
