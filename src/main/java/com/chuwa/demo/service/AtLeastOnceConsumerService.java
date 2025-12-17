package com.chuwa.demo.service;

import com.chuwa.demo.entity.KafkaMessage;
import com.chuwa.demo.repository.KafkaMessageRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

/**
 * At-Least-Once Consumer Service
 *
 * Guarantees that every message will be processed at least once.
 *
 * How it works:
 * 1. Message is received from Kafka
 * 2. Message is processed and saved to database
 * 3. ONLY AFTER successful DB save, the offset is committed manually
 *
 * Trade-off:
 * - If the consumer crashes AFTER saving to DB but BEFORE committing the offset,
 *   the message will be redelivered and processed again (duplicate processing)
 * - This ensures no message loss but may cause duplicates
 *
 * Use case: When message loss is unacceptable (e.g., financial transactions, orders)
 */
@Service
public class AtLeastOnceConsumerService {

    @Autowired
    private KafkaMessageRepository kafkaMessageRepository;

    @KafkaListener(
            topics = "${kafka.topic.name}",
            groupId = "${spring.kafka.consumer.group-id}_at_least_once",
            containerFactory = "atLeastOnceKafkaListenerContainerFactory"
    )
    public void consumeAtLeastOnce(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        System.out.println("=== At-Least-Once Consumer ===");
        System.out.println("Received message - Key: " + record.key() + ", Value: " + record.value());
        System.out.println("Topic: " + record.topic() + ", Partition: " + record.partition() + ", Offset: " + record.offset());

        try {
            // Step 1: Process and save message to database FIRST
            KafkaMessage kafkaMessage = new KafkaMessage(
                    record.key(),
                    record.value(),
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    "consumer_group_1_at_least_once",
                    "AT_LEAST_ONCE"
            );

            kafkaMessageRepository.save(kafkaMessage);
            System.out.println("Message saved to database with ID: " + kafkaMessage.getId());

            // Step 2: Commit offset ONLY AFTER successful save
            // If we crash here, the message will be redelivered (at-least-once guarantee)
            acknowledgment.acknowledge();
            System.out.println("Offset committed successfully for offset: " + record.offset());

        } catch (Exception e) {
            // If saving fails, don't commit offset - message will be redelivered
            System.err.println("Error processing message: " + e.getMessage());
            System.err.println("Offset NOT committed - message will be redelivered");
            // You could also send to dead letter queue here
            throw new RuntimeException("Failed to process message", e);
        }
    }
}
