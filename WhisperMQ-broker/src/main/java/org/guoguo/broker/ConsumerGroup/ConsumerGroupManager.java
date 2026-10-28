package org.guoguo.broker.ConsumerGroup;

import io.micrometer.common.util.StringUtils;
import io.netty.channel.ChannelHandlerContext;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.broker.core.BrokerManager;
import org.guoguo.broker.core.TopicMetadata;
import org.guoguo.broker.util.OffsetPersistUtil;
import org.guoguo.common.config.MqConfigProperties;
import org.guoguo.common.pojo.DTO.SubscribeReqDTO;
import org.guoguo.common.pojo.Entity.ConsumerGroup;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class ConsumerGroupManager {
    /**
     * 存储所有消费者组：key=组ID，value=消费者组实体
     */

    private final Map<String, ConsumerGroup> groupMap = new ConcurrentHashMap<>();
    private final OffsetPersistUtil offsetPersistUtil;
    private final BrokerManager brokerManager;
    private final MqConfigProperties config;
    private final TopicMetadata topicMetadata;
    //这里返回一个不可修改的视图
    public Map<String, ConsumerGroup> getGroupMap() {
        return Collections.unmodifiableMap(groupMap);
    }
    @Autowired
    public ConsumerGroupManager(OffsetPersistUtil offsetPersistUtil, BrokerManager brokerManager,
                                MqConfigProperties config, TopicMetadata topicMetadata) {
        this.offsetPersistUtil = offsetPersistUtil;
        this.brokerManager = brokerManager;
        this.config = config;
        this.topicMetadata = topicMetadata;
    }

    // return new HashMap<>(groupMap); 深拷贝方法

    /**
     * 恢复所有消费者组的位点信息
     */
    private void recoverAllOffset() {
        try {
            Map<String, Map<String, String>> allOffsetData = offsetPersistUtil.recoverAllOffset();
            for (Map.Entry<String, Map<String, String>> groupEntry : allOffsetData.entrySet()) {
                String groupId = groupEntry.getKey();
                Map<String, String> topicOffsetMap = groupEntry.getValue();

                // 获取或创建消费者组
                ConsumerGroup group = getOrCreateConsumerGroup(groupId);

                // 恢复该组的所有topic offset
                for (Map.Entry<String, String> topicEntry : topicOffsetMap.entrySet()) {
                    String topic = topicEntry.getKey();
                    String offset = topicEntry.getValue();
                    group.getTopicOffsetMap().put(topic, offset);
                    log.debug("WhisperMQ==============> 恢复组位点（组：{}，主题：{}，位点：{}）", groupId, topic, offset);
                }
            }
            log.info("WhisperMQ==============> 成功恢复所有消费者组位点，共涉及{}个消费者组", allOffsetData.size());
        } catch (Exception e) {
            log.error("WhisperMQ==============> 恢复所有消费者组位点失败", e);
        }
    }

    //获取创建消费者组 没有就创建
    public ConsumerGroup getOrCreateConsumerGroup(String groupId) {
        //检查groupId是否为null
        if (groupId == null) {
            throw new IllegalArgumentException("GroupId cannot be null");
        }
        //相当于若是不存在就创建的方法
        return groupMap.computeIfAbsent(groupId, k -> {
            ConsumerGroup consumerGroup = new ConsumerGroup();
            consumerGroup.setGroupId(groupId);


            log.info("WhisperMQ=====================>消费者组不存在 创建消费者组：{}", groupId);
            return consumerGroup;
        });
    }

    /**
     * 重新计算某个 topic 的分区分配（P1 Stage B）。
     *
     * <p>只有分区数 &gt; 1 的 topic 才维护分配表；分区数为 1 时沿用旧的路由逻辑
     * （按消息 ID 取模做组内负载均衡），保证升级前行为不变。</p>
     *
     * <p>分配变化不暂停投递：在途消息仍由原 owner 确认，新消息按新分配投递；
     * 位点按分区推进，因此不会因为换 owner 而丢消息或回退。</p>
     */
    public synchronized void rebalance(ConsumerGroup group, String topic) {
        if (group == null || topic == null) {
            return;
        }
        int partitions = topicMetadata.partitionsOf(topic);
        if (partitions <= 1) {
            group.clearAssignment(topic);
            return;
        }
        List<String> candidates = group.onlineSubscribers(topic);
        Map<Integer, String> assignment = PartitionAssigner.assign(candidates, partitions);
        group.updateAssignment(topic, assignment);
        log.info("WhisperMQ Broker 组{} 主题{} 再均衡完成：epoch={}，分区数={}，候选消费者={}，分配={}",
                group.getGroupId(), topic, group.getAssignmentEpoch(), partitions, candidates, assignment);
    }

    /**
     * 取某个分区的 owner；如果分配缺失或 owner 已下线/未订阅，先触发一次再均衡再取。
     */
    public String partitionOwner(ConsumerGroup group, String topic, int partition) {
        String owner = group.getPartitionOwner(topic, partition);
        if (owner != null && group.isSubscribed(topic, owner)) {
            return owner;
        }
        rebalance(group, topic);
        return group.getPartitionOwner(topic, partition);
    }

    /**
     * 消费者订阅主题（组级订阅 ＋ 消费者级 Tag）。
     *
     * <p>P1 改造点：
     * <ul>
     *   <li>返回错误原因（null = 成功），让 Broker 能回给客户端明确的订阅响应或错误；</li>
     *   <li>同一个组重复订阅同一个 topic 不再直接 return，而是更新该消费者的 Tag 并做增量回溯
     *       —— 旧实现会让同组第二个消费者永远登记不上自己的 Tag，从而收不到任何消息；</li>
     *   <li>回溯按"本次订阅者自己的 Tag"过滤，不再用组里最后一次订阅的 tag 代替所有人。</li>
     * </ul>
     *
     * @return null 表示订阅成功，否则返回失败原因
     */
    public String GroupSubscribe(SubscribeReqDTO subscribeReqDTO, ChannelHandlerContext ctx) {
        if (subscribeReqDTO == null) {
            return "订阅请求不能为空";
        }
        String groupId = subscribeReqDTO.getGroupId();
        String topic = subscribeReqDTO.getTopic();
        if (StringUtils.isBlank(groupId)) {
            return "groupId 不能为空";
        }
        if (StringUtils.isBlank(topic)) {
            return "topic 不能为空";
        }
        if (ctx == null || ctx.channel() == null) {
            return "订阅连接不可用";
        }
        List<String> tags = subscribeReqDTO.getTags();
        if (tags == null) {
            subscribeReqDTO.setTags(Collections.emptyList()); // 空标签 = 订阅所有标签
            tags = subscribeReqDTO.getTags();
            log.warn("WhisperMQ Broker 订阅主题{}（组：{}）：标签为 null，按订阅所有标签处理", topic, groupId);
        }

        ConsumerGroup consumerGroup = getOrCreateConsumerGroup(groupId);
        String consumerId = ctx.channel().id().asLongText();
        boolean firstSubscription = !consumerGroup.getSubscribeMap().containsKey(topic);
        if (firstSubscription) {
            //回溯一波位点
            offsetPersistUtil.init(consumerGroup, topic);
        } else {
            log.info("WhisperMQ Broker 组{}重复订阅主题{}：更新消费者{}的Tag并回溯增量消息",
                    groupId, topic, consumerId);
        }

        // 记录组级订阅配置 + 当前消费者自己的 Tag
        consumerGroup.addSubscribe(subscribeReqDTO, ctx);
        // 订阅关系变化后重新分配分区（分区数 > 1 才生效）
        rebalance(consumerGroup, topic);

        boolean hasOffset = consumerGroup.hasOffset(topic);
        if (!hasOffset) {
            if (!config.isNewGroupStartFromEarliest()) {
                log.info("WhisperMQ Broker 组{}订阅主题{}：无历史位点，从最新消息开始", groupId, topic);
                return null;
            }
            // 新组默认按最早消息回溯，避免"订阅生效前发布的消息被永久丢弃"
            log.info("WhisperMQ Broker 组{}订阅主题{}：无历史位点，从最早消息开始回溯", groupId, topic);
        }

        // 回溯：主题匹配 + 该消费者的 Tag 匹配 + 分区位点比较，按消息ID升序
        final boolean replayAll = !hasOffset;
        final List<String> subscribeTags = tags;
        List<Map.Entry<String, MqMessage>> backlog = brokerManager.getMessageMap().entrySet().stream()
                .filter(entry -> topic.equals(entry.getValue().getTopic()))
                .filter(entry -> matchesTag(subscribeTags, entry.getValue().getTag()))
                .filter(entry -> {
                    if (replayAll) {
                        return true;
                    }
                    int partition = entry.getValue().getPartition();
                    String partitionOffset = consumerGroup.getOffset(topic, partition);
                    return partitionOffset == null || compareMessageId(entry.getKey(), partitionOffset) > 0;
                })
                .sorted(Comparator.comparingLong(entry -> parseMessageId(entry.getKey())))
                .toList();

        for (Map.Entry<String, MqMessage> entry : backlog) {
            brokerManager.pushMessageToGroup(consumerGroup, entry.getKey(), entry.getValue(), false);
        }
        log.info("WhisperMQ Broker 消费者组{}订阅主题{}成功（消费者：{}，标签：{}，回溯：{}条）",
                groupId, topic, consumerId, tags, backlog.size());
        return null;
    }

    /**
     * Tag 匹配：订阅了 * 或精确命中；消息无标签、订阅未指定标签时不做过滤。
     */
    private boolean matchesTag(List<String> subscribedTags, String messageTag) {
        if (messageTag == null || subscribedTags == null || subscribedTags.isEmpty()) {
            return true;
        }
        return subscribedTags.contains("*") || subscribedTags.contains(messageTag);
    }

    private long parseMessageId(String messageId) {
        try {
            return Long.parseLong(messageId);
        } catch (NumberFormatException e) {
            return messageId == null ? 0 : Math.abs(messageId.hashCode());
        }
    }

    //消费者加入组
    public void consumerJoinGroup(String groupId, String consumerId, io.netty.channel.Channel channel) {
        if (groupId == null) {
            log.error("GroupId is null in consumerJoinGroup");
            return;
        }
        if (consumerId == null) {
            log.error("ConsumerId is null in consumerJoinGroup");
            return;
        }
        if (channel == null) {
            log.error("Channel is null in consumerJoinGroup");
            return;
        }
        ConsumerGroup consumerGroup = getOrCreateConsumerGroup(groupId);
        consumerGroup.addConsumer(consumerId, channel);

        // 只给"还没有自己 Tag 配置"的消费者补默认值（组级配置），
        // 不能覆盖它自己在 GROUP_SUBSCRIBE 里声明的 Tag。
        for (Map.Entry<String, SubscribeReqDTO> entry : consumerGroup.getSubscribeMap().entrySet()) {
            String topic = entry.getKey();
            if (consumerGroup.getConsumerTags(topic, consumerId) == null) {
                consumerGroup.addConsumerTopicTags(topic, consumerId, entry.getValue().getTags());
                log.info("WhisperMQ Broker 消费者{}加入组{}，按组配置初始化为主题{}的Tag：{}",
                        consumerId, groupId, topic, entry.getValue().getTags());
            }
            // 新连接尚未显式订阅，不会进入候选；这里再均衡一次用于清理已下线 owner
            rebalance(consumerGroup, topic);
        }
    }

    // 消费者组取消订阅主题
    public void groupUnsubscribe(String groupId, String topic) {
        //检查参数是否为null
        if (groupId == null) {
            log.warn("WhisperMQ Broker groupId is null, 取消订阅失败");
            return;
        }
        if (topic == null) {
            log.warn("WhisperMQ Broker topic is null, 取消订阅失败");
            return;
        }
        ConsumerGroup group = groupMap.get(groupId);

        if (group == null) {
            log.warn("WhisperMQ Broker 消费者组{}不存在，取消订阅失败", groupId);
            return;
        }
        group.removeSubscribe(topic);
        // 注意：不能在这里关闭整组的位点 writer。
        // 旧实现在"退订一个 topic"时调用 closeGroup(groupId)，会把同组其他 topic 的
        // 位点写入流一起关掉（后续 ACK 直接静默失败）。组真正销毁时才关闭。
        log.info("WhisperMQ Broker 消费者组{}取消订阅主题{}", groupId, topic);
    }

    //消费者离开组
    public void consumerLeaveGroup(String groupId, String consumerId) {
        //检查参数是否为null
        if (groupId == null) {
            log.warn("WhisperMQ Broker groupId is null, 消费者离开组失败");
            return;
        }
        if (consumerId == null) {
            log.warn("WhisperMQ Broker consumerId is null, 消费者离开组失败");
            return;
        }
        ConsumerGroup consumerGroup = groupMap.get(groupId);
        if (consumerGroup == null) {
            return;
        }
        consumerGroup.removeConsumer(consumerId);
        consumerGroup.removeConsumerTags(consumerId); // 清理Tag订阅
        // 掉线后重新分配分区，把该消费者负责的分区交给其他在线消费者
        for (String topic : consumerGroup.getSubscribeMap().keySet()) {
            rebalance(consumerGroup, topic);
        }
        log.info("WhisperMQ Broker 消费者{}离开组{}，当前组内在线消费者数：{}",
                consumerId, groupId, consumerGroup.getOnlineConsumers().size());
        // 组内无消费者时，自动销毁组（可选：也可保留组配置）
        if (consumerGroup.isEmpty()) {
            groupMap.remove(groupId);
            // 组已销毁：此时才关闭该组的位点写入流（异步排队，不阻塞 IO 线程）
            offsetPersistUtil.closeGroup(groupId);
            log.info("WhisperMQ Broker 消费者组{}无在线消费者，自动销毁", groupId);
        }
    }

    //获取订阅了指定主题的所有消费者组
    public Map<String, ConsumerGroup> getGroupsByTopic(String topic) {
        //检查参数是否为null
        if (topic == null) {
            log.warn("WhisperMQ Broker topic is null in getGroupsByTopic");
            return new ConcurrentHashMap<>();
        }
        Map<String, ConsumerGroup> result = new ConcurrentHashMap<>();
        for (ConsumerGroup group : groupMap.values()) {
            if (group.getSubscribeMap().containsKey(topic)) {
                result.put(group.getGroupId(), group);
            }
        }
        return result;
    }

    // 更新消费者组的消费位点（兼容旧调用：分区 0）
    public void updateGroupOffset(String groupId, String topic, String messageId) {
        updateGroupOffset(groupId, topic, 0, messageId);
    }

    /**
     * 更新消费者组在某个分区上的消费位点。
     * <p>P1：位点按 (group, topic, partition) 维护，避免不同分区的 ACK 互相覆盖；
     * 分区 0 同时写入旧视图，兼容旧代码与旧数据。</p>
     */
    public void updateGroupOffset(String groupId, String topic, int partition, String messageId) {
        //检查参数是否为null
        if (groupId == null) {
            log.warn("WhisperMQ Broker groupId is null, 无法更新位点");
            return;
        }
        if (topic == null) {
            log.warn("WhisperMQ Broker topic is null, 无法更新位点");
            return;
        }
        if (messageId == null) {
            log.warn("WhisperMQ Broker messageId is null, 无法更新位点");
            return;
        }
        ConsumerGroup consumerGroup = groupMap.get(groupId);
        if (consumerGroup == null) {
            log.warn("WhisperMQ Broker 消费者组{}不存在，无法更新位点", groupId);
            return;
        }
        // 仅更新为更大的消息ID（确保位点递增，避免回退）
        String currentOffset = consumerGroup.getOffset(topic, partition);
        if (currentOffset == null || compareMessageId(messageId, currentOffset) > 0) {
            consumerGroup.putOffset(topic, partition, messageId);
            // 位点持久化：内部是单线程有序队列（旧实现用 commonPool 异步写，顺序不可控）
            offsetPersistUtil.writeMessage(groupId, topic, partition, messageId);
            log.info("WhisperMQ Broker 更新组{}的主题{}分区{}消费位点至消息{}",
                    groupId, topic, partition, messageId);
        }
    }

    /** 消息 ID 比较：正常为雪花数字，异常格式退化为字符串比较，避免 NumberFormatException */
    private int compareMessageId(String left, String right) {
        try {
            return Long.compare(Long.parseLong(left), Long.parseLong(right));
        } catch (NumberFormatException e) {
            return left.compareTo(right);
        }
    }

}
