package com.example.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.StickyPartitionCache;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidParameterException;
import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {

    public static final Logger logger = LoggerFactory.getLogger(CustomPartitioner.class);

    private final StickyPartitionCache stickyPartitionCache = new StickyPartitionCache();

    private String specialKeyName;

    @Override
    public void configure(Map<String, ?> configs) {
        specialKeyName = configs.get("custom.special.key").toString();
    }

    @Override
    public int partition(
            String topic,
            Object key, byte[] keyBytes,
            Object value, byte[] valueBytes,
            Cluster cluster
    ) {
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        int numPartitions = partitionInfos.size();
        int numSpecialPartitions = (int) (numPartitions * 0.5);
        int partitionIndex = 0;

        if (keyBytes == null) {
//            return stickyPartitionCache.partition(topic, cluster);
            throw new InvalidParameterException("keyBytes is null");
        }

        // specialKeyName가 true 경우 0,1번 파티션으로 라우팅
        // 그 외에는 3, 4, 5 라우팅
        if (specialKeyName.equals((String) key)) {
            partitionIndex = Utils.toPositive(Utils.murmur2(keyBytes)) % numSpecialPartitions;
        } else {
            partitionIndex = Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - numSpecialPartitions) + numSpecialPartitions;
        }

        logger.info("key:{} |||| partition:{}", (String)key, partitionIndex);

        return partitionIndex;
    }

    @Override
    public void close() {
        // Cleanup resources if needed
    }
}
