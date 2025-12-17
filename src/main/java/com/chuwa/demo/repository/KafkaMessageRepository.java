package com.chuwa.demo.repository;

import com.chuwa.demo.entity.KafkaMessage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface KafkaMessageRepository extends JpaRepository<KafkaMessage, Long> {

    List<KafkaMessage> findByTopic(String topic);

    List<KafkaMessage> findByConsumerGroup(String consumerGroup);

    List<KafkaMessage> findByDeliverySemantic(String deliverySemantic);

    Optional<KafkaMessage> findByMessageKeyAndTopicAndPartitionIdAndOffsetValue(
            String messageKey, String topic, Integer partitionId, Long offsetValue);

    boolean existsByMessageKeyAndTopicAndPartitionIdAndOffsetValue(
            String messageKey, String topic, Integer partitionId, Long offsetValue);
}
