package org.guoguo.broker.core;

import lombok.extern.slf4j.Slf4j;
import org.guoguo.common.config.MqConfigProperties;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Topic 分区元数据（P1 Stage A）。
 *
 * <p>这是后续"分区投递 + 再均衡 + 多 Broker 路由"的扩展点：
 * 分区数由配置/管理接口决定，消息按 bizKey（缺省用 messageId）稳定映射到分区。
 * 默认 1 个分区时所有消息分区号都是 0，行为与升级前一致。</p>
 */
@Slf4j
@Component
public class TopicMetadata {

    private final MqConfigProperties config;
    /** 运行期覆盖的分区数（管理接口/测试用）；未设置的 topic 回退到全局默认值 */
    private final Map<String, Integer> partitions = new ConcurrentHashMap<>();

    public TopicMetadata(MqConfigProperties config) {
        this.config = config;
    }

    /** 某个 topic 的分区数，至少为 1 */
    public int partitionsOf(String topic) {
        if (topic == null) {
            return 1;
        }
        return partitions.computeIfAbsent(topic, key -> Math.max(1, config.getDefaultPartitions()));
    }

    /** 预留的管理入口：后续可对接创建 topic / 扩分区 */
    public void setPartitions(String topic, int count) {
        if (topic == null) {
            return;
        }
        int safe = Math.max(1, count);
        Integer previous = partitions.put(topic, safe);
        if (previous != null && previous != safe) {
            log.info("topic {} 分区数调整：{} -> {}", topic, previous, safe);
        }
    }

    /**
     * 计算消息分区。
     *
     * @param requested 生产者显式指定的分区；&lt;=0 表示自动
     * @return 0..partitions-1
     */
    public int partitionOf(String topic, String bizKey, String messageId, int requested) {
        int count = partitionsOf(topic);
        if (count <= 1) {
            return 0;
        }
        if (requested > 0) {
            return Math.min(requested, count - 1);
        }
        String key = (bizKey == null || bizKey.isBlank()) ? String.valueOf(messageId) : bizKey;
        return Math.floorMod(key.hashCode(), count);
    }
}
