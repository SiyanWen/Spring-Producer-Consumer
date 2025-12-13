package com.chuwa.demo.service;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;


@Service
public class KafkaConsumerService {
    @Value("${kafka.topic.name}")
    private String topic;

    //How do kafka consumers "consume" messages from broker
    //Kafka consumer: poll
    //Read the topic-partion on assigned broker, by offset

    //What will Kafka consumer do when above operation failed
    //If above operation fails: indicate assigned broker is down
    //The consumer will read from the new leader

    // Consumer Group 1: 3 concurrent consumers
    @KafkaListener(
            topics = "${kafka.topic.name}",
            groupId = "${kafka.consumer.group1.id}",
            containerFactory = "kafkaListenerContainerFactoryGroup1",
            autoStartup = "${kafka.consumer.group1.enabled}"
    )
    public void listenGroup1(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.GROUP_ID) String groupId,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key) {

        String consumerThread = Thread.currentThread().getName();
        logMessage("GROUP1", groupId, consumerThread, partition, offset, key, message);
    }

    // Consumer Group 2: 2 concurrent consumers
    @KafkaListener(
            topics = "${kafka.topic.name}",
            groupId = "${kafka.consumer.group2.id}",
            containerFactory = "kafkaListenerContainerFactoryGroup2",
            autoStartup = "${kafka.consumer.group2.enabled}"
    )
    public void listenGroup2(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.GROUP_ID) String groupId,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key) {

        String consumerThread = Thread.currentThread().getName();
        logMessage("GROUP2", groupId, consumerThread, partition, offset, key, message);
    }

    // Consumer Group 3: 1 consumer
    @KafkaListener(
            topics = "${kafka.topic.name}",
            groupId = "${kafka.consumer.group3.id}",
            containerFactory = "kafkaListenerContainerFactoryGroup3",
            autoStartup = "${kafka.consumer.group3.enabled}"
    )
    public void listenGroup3(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.GROUP_ID) String groupId,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key) {

        String consumerThread = Thread.currentThread().getName();
        logMessage("GROUP3", groupId, consumerThread, partition, offset, key, message);
    }

    private void logMessage(String groupLabel, String groupId, String consumerThread,
                            int partition, long offset, String key, String message) {
        System.out.println(String.format(
                "[%s | %s | Thread: %s] Partition: %d | Offset: %d | Key: %s | Message: %s",
                groupLabel, groupId, consumerThread, partition, offset, key, message
        ));

        // {"messageKey":1234 //idempotency key
        // }
//        try {
//            //process and persist message
//        } catch (Exception e) {
//            //send to dead letter queue
//        }
        //commit to offset here
        //return
    }

}
