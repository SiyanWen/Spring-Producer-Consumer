package com.chuwa.demo.config;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomRoundRobinPartitioner implements Partitioner {

    private final AtomicInteger counter = new AtomicInteger(0);

    // Cache to detect duplicate calls for the same message
    private final ThreadLocal<CachedPartition> cache = ThreadLocal.withInitial(() -> null);

    private static class CachedPartition {
        final String topic;
        final byte[] valueBytes;
        final int partition;

        CachedPartition(String topic, byte[] valueBytes, int partition) {
            this.topic = topic;
            this.valueBytes = valueBytes;
            this.partition = partition;
        }
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {

        int numPartitions = cluster.partitionCountForTopic(topic);

        // If key is provided, use key-based partitioning
        if (keyBytes != null) {
            return Math.abs(key.hashCode()) % numPartitions;
        }

        // Check if this is a duplicate call for the same message
        CachedPartition cached = cache.get();
        if (cached != null
                && cached.topic.equals(topic)
                && Arrays.equals(cached.valueBytes, valueBytes)) {
            // Same message, return cached partition (no increment)
            System.out.println("Custom partitioner (cached): " + cached.partition);
            return cached.partition;
        }

        // New message, compute partition using round-robin
        int partition = Math.abs(counter.getAndIncrement()) % numPartitions;

        // Cache the result for potential duplicate call
        cache.set(new CachedPartition(topic, valueBytes, partition));

        System.out.println("Custom partitioner (new): " + partition);
        return partition;
    }

    @Override
    public void close() {
        cache.remove();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // No custom config needed
    }
}