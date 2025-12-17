package com.chuwa.demo.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    public String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    public String consumerGroupId;

    @Value("${kafka.topic.name}")
    public String topic;

    /**
     * Consumer Factory for At-Least-Once delivery semantic
     * - Auto commit is disabled
     * - Offset is committed manually after successful processing
     */
    @Bean
    public ConsumerFactory<String, String> atLeastOnceConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId + "_at_least_once");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Disable auto commit
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Consumer Factory for At-Most-Once delivery semantic
     * - Auto commit is enabled with short interval
     * - Offset is committed before processing (via auto-commit)
     */
    @Bean
    public ConsumerFactory<String, String> atMostOnceConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId + "_at_most_once");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true); // Enable auto commit
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100); // Commit quickly
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Kafka Listener Container Factory for At-Least-Once delivery
     * Uses MANUAL ack mode - offset committed only after successful processing
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> atLeastOnceKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(atLeastOnceConsumerFactory());
        factory.setConcurrency(4);
        // MANUAL ack mode: offset is committed only when acknowledge() is called
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }

    /**
     * Kafka Listener Container Factory for At-Most-Once delivery
     * Uses RECORD ack mode with auto-commit - offset committed before processing
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> atMostOnceKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(atMostOnceConsumerFactory());
        factory.setConcurrency(4);
        // RECORD ack mode with auto-commit enabled
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }

    // Default factory for backward compatibility
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(4);
        return factory;
    }
}
