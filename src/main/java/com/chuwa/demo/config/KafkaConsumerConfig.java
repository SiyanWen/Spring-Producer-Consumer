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

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    public String bootstrapServers;

    @Value("${kafka.consumer.group1.id}")
    private String consumerGroup1Id;

    @Value("${kafka.consumer.group2.id}")
    private String consumerGroup2Id;

    @Value("${kafka.consumer.group3.id}")
    private String consumerGroup3Id;

    @Value("${kafka.topic.name}")
    public String topic;

    // Consumer Factory for Group 1
    @Bean
    public ConsumerFactory<String, String> consumerFactoryGroup1() {
        return createConsumerFactory(consumerGroup1Id);
    }

    // Consumer Factory for Group 2
    @Bean
    public ConsumerFactory<String, String> consumerFactoryGroup2() {
        return createConsumerFactory(consumerGroup2Id);
    }

    // Consumer Factory for Group 3
    @Bean
    public ConsumerFactory<String, String> consumerFactoryGroup3() {
        return createConsumerFactory(consumerGroup3Id);
    }

    // Helper method to create consumer factory
    private ConsumerFactory<String, String> createConsumerFactory(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    // Container Factory for Group 1 - 3 concurrent consumers
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String>
    kafkaListenerContainerFactoryGroup1() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactoryGroup1());
        factory.setConcurrency(3); // 3 consumers in group 1
        return factory;
    }

    // Container Factory for Group 2 - 2 concurrent consumers
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String>
    kafkaListenerContainerFactoryGroup2() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactoryGroup2());
        factory.setConcurrency(2); // 2 consumers in group 2
        return factory;
    }

    // Container Factory for Group 3 - 1 consumer
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String>
    kafkaListenerContainerFactoryGroup3() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactoryGroup3());
        factory.setConcurrency(1); // 1 consumer in group 3
        return factory;
    }
}
