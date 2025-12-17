package com.chuwa.demo.service;

import com.chuwa.demo.entity.KafkaMessage;
import com.chuwa.demo.repository.KafkaMessageRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * At-Most-Once Consumer Service
 *
 * Guarantees that every message will be processed at most once (no duplicates).
 *
 * How it works:
 * 1. Offset is committed IMMEDIATELY when message is received (via auto-commit)
 * 2. Then the message is processed and saved to database
 *
 * Trade-off:
 * - If the consumer crashes AFTER committing the offset but BEFORE saving to DB,
 *   the message is lost forever (no redelivery)
 * - This ensures no duplicates but may cause message loss
 *
 * Use case: When duplicates are worse than loss (e.g., metrics, logs, non-critical data)
 */
@Service
public class AtMostOnceConsumerService {

    @Autowired
    private KafkaMessageRepository kafkaMessageRepository;

    @KafkaListener(
            topics = "${kafka.topic.name}",
            groupId = "${spring.kafka.consumer.group-id}_at_most_once",
            containerFactory = "atMostOnceKafkaListenerContainerFactory"
    )
    public void consumeAtMostOnce(ConsumerRecord<String, String> record) {
        System.out.println("=== At-Most-Once Consumer ===");
        System.out.println("Received message - Key: " + record.key() + ", Value: " + record.value());
        System.out.println("Topic: " + record.topic() + ", Partition: " + record.partition() + ", Offset: " + record.offset());

        // Note: Offset is already committed via auto-commit BEFORE we reach here
        // If we crash after this point, the message is lost (at-most-once guarantee)
        System.out.println("Offset already committed (auto-commit). Processing message...");

        try {
            // Step 2: Process and save to database (offset already committed)
            KafkaMessage kafkaMessage = new KafkaMessage(
                    record.key(),
                    record.value(),
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    "consumer_group_1_at_most_once",
                    "AT_MOST_ONCE"
            );

            kafkaMessageRepository.save(kafkaMessage);
            System.out.println("Message saved to database with ID: " + kafkaMessage.getId());

        } catch (Exception e) {
            // Even if saving fails, offset is already committed - message won't be redelivered
            System.err.println("Error processing message: " + e.getMessage());
            System.err.println("WARNING: Offset already committed - message is LOST");
            // You could log this for monitoring/alerting purposes
        }
    }
}
