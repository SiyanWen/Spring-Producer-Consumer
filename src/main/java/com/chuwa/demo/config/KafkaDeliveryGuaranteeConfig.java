package com.chuwa.demo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for demonstrating different Kafka delivery guarantees:
 * 1. At-Most-Once: Messages may be lost but never redelivered
 * 2. At-Least-Once: Messages never lost but may be redelivered
 * 3. Exactly-Once: Messages delivered exactly once
 */
@Configuration
public class KafkaDeliveryGuaranteeConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.delivery.topic.name}")
    private String deliveryTopic;

    // ==================== AT-MOST-ONCE DELIVERY ====================
    // Producer: No retries, acks=1 (leader only)
    // Consumer: Auto-commit enabled, commit before processing
    // Risk: Messages can be lost if broker fails

    @Bean
    public ProducerFactory<String, String> atMostOnceProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // At-Most-Once Configuration
        props.put(ProducerConfig.ACKS_CONFIG, "1"); // Leader acknowledgment only
        props.put(ProducerConfig.RETRIES_CONFIG, 0); // No retries
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomRoundRobinPartitioner.class);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> atMostOnceKafkaTemplate() {
        return new KafkaTemplate<>(atMostOnceProducerFactory());
    }

    @Bean
    public ConsumerFactory<String, String> atMostOnceConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "at-most-once-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // At-Most-Once Configuration
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true); // Auto-commit enabled
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000); // Commit every 1 second
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> atMostOnceListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(atMostOnceConsumerFactory());
        factory.setConcurrency(1);
        return factory;
    }

    // ==================== AT-LEAST-ONCE DELIVERY ====================
    // Producer: Retries enabled, acks=all (all replicas)
    // Consumer: Manual commit after processing
    // Risk: Messages can be duplicated if consumer crashes after processing but before commit

    @Bean
    public ProducerFactory<String, String> atLeastOnceProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // At-Least-Once Configuration
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // All replicas must acknowledge
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE); // Unlimited retries
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomRoundRobinPartitioner.class);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> atLeastOnceKafkaTemplate() {
        return new KafkaTemplate<>(atLeastOnceProducerFactory());
    }

    @Bean
    public ConsumerFactory<String, String> atLeastOnceConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "at-least-once-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // At-Least-Once Configuration
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Manual commit
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> atLeastOnceListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(atLeastOnceConsumerFactory());
        factory.setConcurrency(1);
        factory.getContainerProperties().setAckMode(
            org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL_IMMEDIATE
        );

        // Configure error handler to seek back to current offset on exception
        // This enables retry behavior when exceptions are thrown
        factory.setCommonErrorHandler(
            new org.springframework.kafka.listener.DefaultErrorHandler(
                // No recovery callback - we handle it in the listener
                (record, exception) -> {
                    System.err.println("[ERROR HANDLER] Exception occurred, seeking to current offset for retry");
                },
                // Backoff: wait 2 seconds between retries
                new org.springframework.util.backoff.FixedBackOff(2000L, Long.MAX_VALUE)
            )
        );

        return factory;
    }

    // ==================== EXACTLY-ONCE DELIVERY ====================
    // Producer: Idempotence enabled, acks=all, transactional
    // Consumer: Read committed messages only, manual commit
    // Guarantee: Messages delivered exactly once

    @Bean
    public ProducerFactory<String, String> exactlyOnceProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Exactly-Once Configuration
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // All replicas must acknowledge
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // Idempotent producer
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomRoundRobinPartitioner.class);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "exactly-once-tx-"); // Enable transactions

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> exactlyOnceKafkaTemplate() {
        return new KafkaTemplate<>(exactlyOnceProducerFactory());
    }

    @Bean
    public ConsumerFactory<String, String> exactlyOnceConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "exactly-once-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Exactly-Once Configuration
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Manual commit
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"); // Only read committed messages
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> exactlyOnceListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(exactlyOnceConsumerFactory());
        factory.setConcurrency(1);
        factory.getContainerProperties().setAckMode(
            org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL_IMMEDIATE
        );
        return factory;
    }

    @Bean
    public NewTopic createDeliveryGuaranteeTopic() {
        return new NewTopic(
                "delivery-guarantee-demo",
                4,
                (short) 2);
    }
}
