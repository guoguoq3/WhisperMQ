// org.WhisperMQ.common.entity.ConsumerGroup.java
package org.guoguo.common.pojo.Entity;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import lombok.Data;
import org.guoguo.common.pojo.DTO.SubscribeReqDTO;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 消费者组实体：作为订阅 Broker 的主体，管理组内消费者和消费位点。
 *
 * <p>P1 改造点：Tag 由"组级配置"下沉为"消费者级配置"。
 * 旧实现里 {@code addConsumerTopicTags} 要求消费者先在线，且 {@code consumerJoinGroup}
 * 会用组级订阅配置覆盖每个消费者的 tag，导致同组内无法让 A 只收 TAG1、B 只收 TAG2。</p>
 */
@Data
public class ConsumerGroup {
    /** 消费者组唯一标识（如 "ORDER_GROUP"） */
    private String groupId;

    /** 组内订阅关系：key=主题，value=该主题最近一次订阅配置（用于组级存在性与默认 tag） */
    private Map<String, SubscribeReqDTO> subscribeMap = new ConcurrentHashMap<>();

    /** 组内消费位点：key=主题，value=该主题的最新消费消息ID（同组共享；分区 0 的兼容视图） */
    private Map<String, String> topicOffsetMap = new ConcurrentHashMap<>();

    /**
     * 按分区保存的消费位点：topic → (partition → 最新消息 ID)。
     * P1 新增；分区 0 同时写入 {@link #topicOffsetMap}，保证旧代码/旧数据可用。
     */
    private Map<String, Map<Integer, String>> topicPartitionOffsetMap = new ConcurrentHashMap<>();

    /** 组内在线消费者通道：key=消费者ID（通道ID），value=Netty通道 */
    private Map<String, io.netty.channel.Channel> onlineConsumers = new ConcurrentHashMap<>();

    /** 主题 → 消费者 → 该消费者订阅的 Tag 列表（消费者级，允许同一组内不同消费者订阅不同 Tag） */
    private Map<String, Map<String, List<String>>> topicConsumerTags = new ConcurrentHashMap<>();

    /**
     * 主题 → 已显式订阅该主题的消费者集合（P1 Stage B）。
     * 与 {@link #topicConsumerTags} 不同：入组时从组配置继承的 Tag 不算"已订阅"，
     * 只有真正发出 GROUP_SUBSCRIBE 的消费者才参与分区分配，避免消息推给"还没订阅"的连接。
     */
    private Map<String, Set<String>> topicSubscribers = new ConcurrentHashMap<>();

    /** 主题 → (partition → consumerId)，分区分配表（P1 Stage B） */
    private Map<String, Map<Integer, String>> partitionAssignment = new ConcurrentHashMap<>();

    /** 分配表版本号：消费者加入/离开/订阅变化时递增，未来多 Broker 用于 fence 旧分配 */
    private volatile long assignmentEpoch = 0;

    /**
     * 读取某个分区的位点：优先查分区表，分区 0 回退到旧的 topicOffsetMap。
     */
    public String getOffset(String topic, int partition) {
        Map<Integer, String> byPartition = topicPartitionOffsetMap.get(topic);
        String value = byPartition == null ? null : byPartition.get(partition);
        if (value != null) {
            return value;
        }
        return partition == 0 ? topicOffsetMap.get(topic) : null;
    }

    /**
     * 更新某个分区的位点；分区 0 同步维护旧视图。
     */
    public void putOffset(String topic, int partition, String messageId) {
        if (topic == null || messageId == null) {
            return;
        }
        topicPartitionOffsetMap.computeIfAbsent(topic, k -> new ConcurrentHashMap<>()).put(partition, messageId);
        if (partition == 0) {
            topicOffsetMap.put(topic, messageId);
        }
    }

    /** 该主题是否已有任何位点（任意分区） */
    public boolean hasOffset(String topic) {
        if (topicOffsetMap.containsKey(topic)) {
            return true;
        }
        Map<Integer, String> byPartition = topicPartitionOffsetMap.get(topic);
        return byPartition != null && !byPartition.isEmpty();
    }

    /** 位点快照，供控制台展示：topic（分区 0）或 topic#partition → offset */
    public Map<String, String> offsetView() {
        Map<String, String> view = new LinkedHashMap<>();
        topicPartitionOffsetMap.forEach((topic, byPartition) ->
                byPartition.forEach((partition, offset) ->
                        view.put(partition == 0 ? topic : topic + "#" + partition, offset)));
        topicOffsetMap.forEach(view::putIfAbsent);
        return view;
    }

    // ------------------------------------------------------------------ 订阅者与分区分配

    /** 记录消费者显式订阅了某个 topic（GROUP_SUBSCRIBE 时调用） */
    public void addSubscriber(String topic, String consumerId) {
        if (topic == null || consumerId == null) {
            return;
        }
        topicSubscribers.computeIfAbsent(topic, k -> ConcurrentHashMap.newKeySet()).add(consumerId);
    }

    /** 消费者是否显式订阅了该 topic（且在线） */
    public boolean isSubscribed(String topic, String consumerId) {
        if (topic == null || consumerId == null || !onlineConsumers.containsKey(consumerId)) {
            return false;
        }
        Set<String> subscribers = topicSubscribers.get(topic);
        return subscribers != null && subscribers.contains(consumerId);
    }

    /** 该 topic 下在线且已订阅的消费者 ID（已排序，保证分配结果稳定） */
    public List<String> onlineSubscribers(String topic) {
        Set<String> subscribers = topicSubscribers.get(topic);
        if (subscribers == null || subscribers.isEmpty()) {
            return Collections.emptyList();
        }
        return subscribers.stream()
                .filter(onlineConsumers::containsKey)
                .sorted()
                .toList();
    }

    /** 当前某个分区的 owner；未分配返回 null */
    public String getPartitionOwner(String topic, int partition) {
        Map<Integer, String> assignment = partitionAssignment.get(topic);
        return assignment == null ? null : assignment.get(partition);
    }

    /** 某个 topic 的分配快照（partition → consumerId） */
    public Map<Integer, String> getAssignment(String topic) {
        Map<Integer, String> assignment = partitionAssignment.get(topic);
        return assignment == null ? Map.of() : Map.copyOf(assignment);
    }

    /** 整体分配快照：topic → (partition → consumerId)，供控制台展示 */
    public Map<String, Map<Integer, String>> assignmentView() {
        Map<String, Map<Integer, String>> view = new LinkedHashMap<>();
        partitionAssignment.forEach((topic, assignment) -> {
            if (!assignment.isEmpty()) {
                view.put(topic, Map.copyOf(assignment));
            }
        });
        return view;
    }

    /** 替换某个 topic 的分配表并递增 epoch */
    public void updateAssignment(String topic, Map<Integer, String> assignment) {
        if (topic == null) {
            return;
        }
        if (assignment == null || assignment.isEmpty()) {
            partitionAssignment.remove(topic);
        } else {
            partitionAssignment.put(topic, Map.copyOf(assignment));
        }
        assignmentEpoch++;
    }

    /** 清空某个 topic 的分配表 */
    public void clearAssignment(String topic) {
        if (topic != null && partitionAssignment.remove(topic) != null) {
            assignmentEpoch++;
        }
    }

    /**
     * 该消费者是否接受这条消息的 Tag（用于分区 owner 的兼容回退）。
     * 未配置 Tag 视为接受全部。
     */
    public boolean accepts(String topic, String consumerId, String messageTag) {
        if (messageTag == null) {
            return true;
        }
        List<String> tags = getConsumerTags(topic, consumerId);
        if (tags == null || tags.isEmpty()) {
            return true;
        }
        return tags.contains("*") || tags.contains(messageTag);
    }

    /**
     * 记录某个消费者对某个主题的 Tag 订阅。
     * 不再要求消费者已在线：订阅可能先于入组到达，先存下来，投递时再按在线状态过滤。
     */
    public void addConsumerTopicTags(String topic, String consumerId, List<String> tags) {
        if (topic == null || consumerId == null) {
            return;
        }
        topicConsumerTags.computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
                .put(consumerId, tags == null ? Collections.emptyList() : List.copyOf(tags));
    }

    /** 查询某个消费者在某个主题上的 Tag 订阅；未订阅返回 null */
    public List<String> getConsumerTags(String topic, String consumerId) {
        Map<String, List<String>> consumerTags = topicConsumerTags.get(topic);
        return consumerTags == null ? null : consumerTags.get(consumerId);
    }

    /**
     * 获取订阅了指定主题和 Tag 的在线消费者。
     * 匹配规则：订阅了 *、精确命中，或该消费者没有指定 tag（视为订阅全部）。
     */
    public List<String> getConsumersByTopicAndTag(String topic, String tag) {
        Map<String, List<String>> consumerTags = topicConsumerTags.get(topic);
        if (consumerTags == null || consumerTags.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> matchedConsumers = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : consumerTags.entrySet()) {
            String consumerId = entry.getKey();
            // 只投递给"在线且显式订阅了该 topic"的消费者（入组继承 Tag 但尚未订阅的连接不参与）
            if (!isSubscribed(topic, consumerId)) {
                continue;
            }
            List<String> subscribedTags = entry.getValue();
            if (subscribedTags == null || subscribedTags.isEmpty()
                    || tag == null || subscribedTags.contains("*") || subscribedTags.contains(tag)) {
                matchedConsumers.add(consumerId);
            }
        }
        return matchedConsumers;
    }

    /** 移除消费者时清理其 Tag 订阅关系 */
    public void removeConsumerTags(String consumerId) {
        for (Map<String, List<String>> consumerTags : topicConsumerTags.values()) {
            consumerTags.remove(consumerId);
        }
    }

    /** 消费者更新自己的订阅标签时调用 */
    public void updateConsumerTopicTags(String topic, String consumerId, List<String> newTags) {
        addConsumerTopicTags(topic, consumerId, newTags);
    }

    /** 组订阅主题时调用：更新组级配置，并登记"当前这个消费者"的 Tag 与订阅关系 */
    public void addSubscribe(SubscribeReqDTO subscribeReq, ChannelHandlerContext ctx) {
        String topic = subscribeReq.getTopic();
        subscribeMap.put(topic, subscribeReq);

        ChannelId channelId = ctx.channel().id();
        String consumerId = channelId.asLongText();
        addConsumerTopicTags(topic, consumerId, subscribeReq.getTags());
        addSubscriber(topic, consumerId);
    }

    /** 移除订阅：组取消订阅时调用 */
    public void removeSubscribe(String topic) {
        subscribeMap.remove(topic);
        topicOffsetMap.remove(topic); // 移除订阅时清空对应位点
        topicPartitionOffsetMap.remove(topic);
        topicConsumerTags.remove(topic);
        topicSubscribers.remove(topic);
        partitionAssignment.remove(topic);
    }

    /** 消费者加入组 */
    public void addConsumer(String consumerId, io.netty.channel.Channel channel) {
        onlineConsumers.put(consumerId, channel);
    }

    /** 消费者离开组（同时清理其 Tag 订阅关系与订阅者登记） */
    public void removeConsumer(String consumerId) {
        onlineConsumers.remove(consumerId);
        removeConsumerTags(consumerId);
        topicSubscribers.values().forEach(subscribers -> subscribers.remove(consumerId));
    }

    /** 判断组是否为空（无在线消费者） */
    public boolean isEmpty() {
        return onlineConsumers.isEmpty();
    }
}
