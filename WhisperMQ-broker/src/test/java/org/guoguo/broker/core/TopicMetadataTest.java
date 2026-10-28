package org.guoguo.broker.core;

import org.guoguo.common.config.MqConfigProperties;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 分区元数据（P1 Stage A）：默认 1 分区等价旧行为，多分区稳定映射。
 */
class TopicMetadataTest {

    private TopicMetadata metadata(int defaultPartitions) {
        MqConfigProperties config = new MqConfigProperties();
        config.setDefaultPartitions(defaultPartitions);
        return new TopicMetadata(config);
    }

    @Test
    void singlePartitionKeepsLegacyBehaviour() {
        TopicMetadata metadata = metadata(1);

        assertEquals(0, metadata.partitionOf("T", "key", "1", 0));
        assertEquals(0, metadata.partitionOf("T", null, "2", 9));
        assertEquals(1, metadata.partitionsOf("T"));
    }

    @Test
    void partitionIsStableForSameBizKey() {
        TopicMetadata metadata = metadata(4);

        int first = metadata.partitionOf("T", "order-1", "1", 0);
        for (int i = 0; i < 10; i++) {
            assertEquals(first, metadata.partitionOf("T", "order-1", String.valueOf(i), 0));
        }
        assertTrue(first >= 0 && first < 4);
    }

    @Test
    void differentKeysSpreadAcrossPartitions() {
        TopicMetadata metadata = metadata(4);
        Set<Integer> seen = new HashSet<>();

        for (int i = 0; i < 50; i++) {
            seen.add(metadata.partitionOf("T", "key-" + i, "id-" + i, 0));
        }

        assertTrue(seen.size() >= 2, "50 个不同 key 至少应落到 2 个分区");
    }

    @Test
    void requestedPartitionIsClamped() {
        TopicMetadata metadata = metadata(2);

        assertEquals(1, metadata.partitionOf("T", null, "id", 1));
        assertEquals(1, metadata.partitionOf("T", null, "id", 9));
    }

    @Test
    void partitionsCanBeOverriddenAtRuntime() {
        TopicMetadata metadata = metadata(1);

        metadata.setPartitions("T", 3);

        assertEquals(3, metadata.partitionsOf("T"));
        assertEquals(2, metadata.partitionOf("T", null, "id", 2));
    }
}
