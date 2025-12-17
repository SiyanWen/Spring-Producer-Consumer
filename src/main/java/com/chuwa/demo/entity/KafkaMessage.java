package com.chuwa.demo.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "kafka_messages")
public class KafkaMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "message_key")
    private String messageKey;

    @Column(name = "message_value", columnDefinition = "TEXT")
    private String messageValue;

    @Column(name = "topic")
    private String topic;

    @Column(name = "partition_id")
    private Integer partitionId;

    @Column(name = "offset_value")
    private Long offsetValue;

    @Column(name = "consumer_group")
    private String consumerGroup;

    @Column(name = "delivery_semantic")
    private String deliverySemantic;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    public KafkaMessage() {
        this.createdAt = LocalDateTime.now();
    }

    public KafkaMessage(String messageKey, String messageValue, String topic,
                        Integer partitionId, Long offsetValue, String consumerGroup,
                        String deliverySemantic) {
        this.messageKey = messageKey;
        this.messageValue = messageValue;
        this.topic = topic;
        this.partitionId = partitionId;
        this.offsetValue = offsetValue;
        this.consumerGroup = consumerGroup;
        this.deliverySemantic = deliverySemantic;
        this.createdAt = LocalDateTime.now();
    }

    // Getters and Setters
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public void setMessageKey(String messageKey) {
        this.messageKey = messageKey;
    }

    public String getMessageValue() {
        return messageValue;
    }

    public void setMessageValue(String messageValue) {
        this.messageValue = messageValue;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(Integer partitionId) {
        this.partitionId = partitionId;
    }

    public Long getOffsetValue() {
        return offsetValue;
    }

    public void setOffsetValue(Long offsetValue) {
        this.offsetValue = offsetValue;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getDeliverySemantic() {
        return deliverySemantic;
    }

    public void setDeliverySemantic(String deliverySemantic) {
        this.deliverySemantic = deliverySemantic;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    @Override
    public String toString() {
        return "KafkaMessage{" +
                "id=" + id +
                ", messageKey='" + messageKey + '\'' +
                ", messageValue='" + messageValue + '\'' +
                ", topic='" + topic + '\'' +
                ", partitionId=" + partitionId +
                ", offsetValue=" + offsetValue +
                ", consumerGroup='" + consumerGroup + '\'' +
                ", deliverySemantic='" + deliverySemantic + '\'' +
                ", createdAt=" + createdAt +
                '}';
    }
}
