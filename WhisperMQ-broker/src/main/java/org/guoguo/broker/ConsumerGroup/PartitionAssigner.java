package org.guoguo.broker.ConsumerGroup;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 分区分配算法（P1 Stage B）。
 *
 * <p>输入已排序的消费者 ID 列表与分区数，输出 {@code partition → consumerId}。
 * 采用轮询取模，保证：
 * <ul>
 *   <li>同一输入下结果稳定（便于排查、避免无意义抖动）；</li>
 *   <li>分区数 ≥ 消费者数时每个消费者至少分到一个分区；</li>
 *   <li>消费者数 &gt; 分区数时多出的消费者暂时空闲（后续可做粘性/一致哈希优化）。</li>
 * </ul>
 */
public final class PartitionAssigner {

    private PartitionAssigner() {
    }

    public static Map<Integer, String> assign(List<String> sortedConsumerIds, int partitions) {
        Map<Integer, String> assignment = new LinkedHashMap<>();
        if (sortedConsumerIds == null || sortedConsumerIds.isEmpty() || partitions <= 0) {
            return assignment;
        }
        int size = sortedConsumerIds.size();
        for (int partition = 0; partition < partitions; partition++) {
            assignment.put(partition, sortedConsumerIds.get(partition % size));
        }
        return assignment;
    }
}
