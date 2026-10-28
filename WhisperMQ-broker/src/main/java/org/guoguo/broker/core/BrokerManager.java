package org.guoguo.broker.core;

import com.alibaba.fastjson.JSON;
import io.netty.channel.Channel;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.broker.ConsumerGroup.ConsumerGroupManager;
import org.guoguo.broker.util.DeadLetterPersistUtil;
import org.guoguo.broker.store.SegmentMessageStore;
import org.guoguo.common.config.MqConfigProperties;
import org.guoguo.common.constant.DeadType;
import org.guoguo.common.pojo.DTO.ConsumerAckReqDTO;
import org.guoguo.common.pojo.DTO.DeadLetterDTO;
import org.guoguo.common.pojo.Entity.ConsumerGroup;
import org.guoguo.common.pojo.Entity.DeadLetterQueue;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.guoguo.common.constant.MethodType;
import org.guoguo.common.pojo.DTO.RpcMessageDTO;
import org.guoguo.common.pojo.Entity.MqMessageEnduring;
import org.guoguo.common.pojo.DTO.PushMessageDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Broker 核心：消息存储、按组投递、ACK 与位点、可靠投递（in-flight + 超时重投）、死信。
 */
@Slf4j
@Component
public class BrokerManager {

    // 注入消费者组管理器
    private final ConsumerGroupManager groupManager;

    //提供 messageMap 的 getter（供 FilePersistUtil 恢复消息）
    @Getter
    private final Map<String, MqMessage> messageMap = new ConcurrentHashMap<>();

    /**
     * 在途消息表：key = groupId|messageId。
     * 旧实现没有任何在途记录，推出去就不管了；这里记录"已投递未确认"的消息，
     * 用于 ACK 超时重投、消费者掉线重投以及投递幂等。
     */
    private final Map<String, InFlightMessage> inFlightMap = new ConcurrentHashMap<>();

    /** 背压：consumerId → 暂停截止时间（毫秒） */
    private final Map<String, Long> pausedConsumers = new ConcurrentHashMap<>();

    //注入配置文件
    private final MqConfigProperties config;
    private final SegmentMessageStore messageStore;
    private final DeadLetterPersistUtil deadLetterPersistUtil;
    private final TopicMetadata topicMetadata;
    /** 热数据淘汰顺序（配合 hotCacheSize，避免 messageMap 无限增长） */
    private final java.util.concurrent.ConcurrentLinkedQueue<String> hotOrder = new java.util.concurrent.ConcurrentLinkedQueue<>();

    /** 当前在途（已投递未确认）消息数，供控制台观测 */
    public int getInFlightCount() {
        return inFlightMap.size();
    }

    /** 按消费者组统计在途消息数，供控制台观测 */
    public Map<String, Integer> getInFlightCountByGroup() {
        Map<String, Integer> counts = new ConcurrentHashMap<>();
        for (InFlightMessage message : inFlightMap.values()) {
            counts.merge(message.getGroupId(), 1, Integer::sum);
        }
        return counts;
    }

    // ------------------------------------------------------------------ 消息读取（Stage C：内存只保留热数据）

    /** 存活消息总数（磁盘索引 + 未持久化的热数据） */
    public int getMessageCount() {
        int count = messageStore.liveCount();
        for (String messageId : messageMap.keySet()) {
            if (!messageStore.contains(messageId)) {
                count++;
            }
        }
        return count;
    }

    /** 按消息 ID 读取；优先热缓存，未命中回源 segment */
    public MqMessage getMessage(String messageId) {
        MqMessage hot = messageMap.get(messageId);
        return hot != null ? hot : messageStore.get(messageId);
    }

    /** 最近存活消息（倒序），控制台使用；合并热数据以覆盖非持久化消息 */
    public List<SegmentMessageStore.StoredMessage> getRecentMessages(int limit) {
        Map<String, MqMessage> merged = new java.util.HashMap<>(messageMap);
        for (SegmentMessageStore.StoredMessage stored : messageStore.recentLive(Math.max(limit * 2, limit))) {
            merged.putIfAbsent(stored.messageId(), stored.message());
        }
        return merged.entrySet().stream()
                .sorted((a, b) -> Long.compare(parseMessageId(b.getKey()), parseMessageId(a.getKey())))
                .limit(Math.max(1, limit))
                .map(entry -> new SegmentMessageStore.StoredMessage(entry.getKey(), entry.getValue(), false))
                .toList();
    }

    /** 按 topic 遍历历史消息（升序）；热数据（含非持久化消息）与 segment 合并去重 */
    public void forEachBacklog(String topic, java.util.function.BiConsumer<String, MqMessage> consumer) {
        java.util.TreeMap<Long, String> ids = new java.util.TreeMap<>();
        java.util.Map<String, MqMessage> merged = new java.util.HashMap<>();
        for (Map.Entry<String, MqMessage> entry : messageMap.entrySet()) {
            MqMessage message = entry.getValue();
            if (message != null && topic.equals(message.getTopic())) {
                merged.put(entry.getKey(), message);
                ids.put(parseMessageId(entry.getKey()), entry.getKey());
            }
        }
        messageStore.forEachTopicAfter(topic, null, stored -> {
            if (merged.putIfAbsent(stored.messageId(), stored.message()) == null) {
                ids.put(parseMessageId(stored.messageId()), stored.messageId());
            }
        });
        for (String messageId : ids.values()) {
            consumer.accept(messageId, merged.get(messageId));
        }
    }

    /** 各 topic 的存活消息数 */
    public Map<String, Integer> getTopicMessageCounts() {
        return messageStore.liveCountsByTopic();
    }

    /** 存储统计（segment 数、存活/死信条数、字节数） */
    public Map<String, Object> getStoreStats() {
        return messageStore.stats();
    }

    /** 背压：标记消费者暂停投递，到期自动恢复 */
    public void pauseConsumer(String consumerId, long durationMillis) {
        if (consumerId == null) {
            return;
        }
        long pause = durationMillis > 0 ? durationMillis : Math.max(500, config.getBackpressurePauseMillis());
        pausedConsumers.put(consumerId, System.currentTimeMillis() + pause);
        log.info("WhisperMQ Broker 消费者{}暂停投递 {}ms（背压）", consumerId, pause);
    }

    /** 背压：清除暂停并按组位点补投暂停期间未投递的消息 */
    public void resumeConsumer(String groupId, String consumerId) {
        if (consumerId != null) {
            pausedConsumers.remove(consumerId);
        }
        ConsumerGroup group = groupId == null ? null : groupManager.getGroupMap().get(groupId);
        if (group == null) {
            return;
        }
        for (String topic : group.getSubscribeMap().keySet()) {
            groupManager.replayBacklog(group, topic);
        }
        log.info("WhisperMQ Broker 消费者{}恢复投递，组{}按位点补投", consumerId, groupId);
    }

    /** 该消费者当前是否处于暂停窗口 */
    public boolean isPaused(String consumerId) {
        if (consumerId == null) {
            return false;
        }
        Long until = pausedConsumers.get(consumerId);
        if (until == null) {
            return false;
        }
        if (until <= System.currentTimeMillis()) {
            pausedConsumers.remove(consumerId, until);
            return false;
        }
        return true;
    }

    public int getPausedCount() {
        pausedConsumers.entrySet().removeIf(entry -> entry.getValue() <= System.currentTimeMillis());
        return pausedConsumers.size();
    }

    private void cacheMessage(String messageId, MqMessage message) {        messageMap.put(messageId, message);
        hotOrder.add(messageId);
        int limit = Math.max(0, config.getHotCacheSize());
        while (messageMap.size() > limit) {
            String oldest = hotOrder.poll();
            if (oldest == null) {
                break;
            }
            messageMap.remove(oldest);
        }
    }

    //注入死信队列
    private final DeadLetterQueue deadLetterQueue;

    // ACK 超时扫描器
    private final ScheduledExecutorService redeliveryScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "mq-ack-timeout-scan");
        thread.setDaemon(true);
        return thread;
    });
    private final AtomicBoolean started = new AtomicBoolean(false);

    // 注入配置
    @Autowired
    public BrokerManager(
            SegmentMessageStore messageStore,
            @Lazy ConsumerGroupManager consumerGroupManager,
            MqConfigProperties config,
            DeadLetterQueue deadLetterQueue,
            DeadLetterPersistUtil deadLetterPersistUtil,
            TopicMetadata topicMetadata
    ) {
        this.messageStore = messageStore;
        this.groupManager = consumerGroupManager;
        this.config = config;
        this.deadLetterQueue = deadLetterQueue;
        this.deadLetterPersistUtil = deadLetterPersistUtil;
        this.topicMetadata = topicMetadata;
        log.info("WhisperMQ Broker 死信配置重试次数：{}", config.getMaxDeadLetterRetryCount());
    }

    @PostConstruct
    public void init() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        long ackTimeout = Math.max(1000, config.getAckTimeoutMillis());
        long scanInterval = Math.max(500, ackTimeout / 2);
        redeliveryScheduler.scheduleAtFixedRate(this::scanTimeoutMessages,
                scanInterval, scanInterval, TimeUnit.MILLISECONDS);
        log.info("WhisperMQ Broker 可靠投递已启用：ackTimeout={}ms，扫描间隔={}ms，最大投递次数={}",
                ackTimeout, scanInterval, config.getMaxDeliveryAttempts());
    }

    @PreDestroy
    public void destroy() {
        redeliveryScheduler.shutdownNow();
    }

    /**
     * 处理生产者发送的消息：存储消息并推送给订阅者
     */
    public void handlerMessage(MqMessageEnduring mqMessage, String messageId, Boolean isRetry) {
        boolean retry = Boolean.TRUE.equals(isRetry);
        // P1：确定分区（默认 1 个分区时恒为 0，与升级前行为一致）
        int partition = topicMetadata.partitionOf(mqMessage.getTopic(), mqMessage.getBizKey(),
                messageId, mqMessage.getPartition());
        mqMessage.setPartition(partition);

        cacheMessage(messageId, mqMessage);
        //默认为true，即进行持久化
        if (mqMessage.isEnduring() && !retry) {
            //持久化
            log.info("WhisperMQ Broker 存储消息：ID={}，主题={}，分区={}", messageId, mqMessage.getTopic(), partition);
            //核心持久化时机是在 Broker 接收到生产者的消息后、尚未发送给消费者之前完成存储
            messageStore.append(messageId, mqMessage);
        }
        //将生产者生产的消息推送给订阅者
        String topic = mqMessage.getTopic();
        Map<String, ConsumerGroup> subscribeGroups = groupManager.getGroupsByTopic(topic);
        if (subscribeGroups.isEmpty()) {
            log.info("WhisperMQ Broker 无消费者组订阅主题{}，消息{}暂不推送", topic, messageId);
            return;
        }
        for (ConsumerGroup group : subscribeGroups.values()) {
            pushMessageToGroup(group, messageId, mqMessage, retry);
        }
    }


    /**
     * 向消费者组推送消息（基于Tag筛选消费者，组内负载均衡）
     *
     * <p>与旧实现的差异：
     * <ul>
     *   <li>用 groupId|messageId 做投递幂等，生产端超时重传不会重复推送；</li>
     *   <li>推送成功后写入 in-flight 表，等待 ACK；</li>
     *   <li>选择消费者时排除"刚刚失败/超时"的那个（重投优先换人）。</li>
     * </ul>
     */
    public void pushMessageToGroup(ConsumerGroup group, String messageId, MqMessage message, Boolean isRetry) {
        String groupId = group.getGroupId();
        String topic = message.getTopic();
        int partition = message.getPartition();
        String messageTag = message.getTag();
        boolean retry = Boolean.TRUE.equals(isRetry);
        String inFlightKey = inFlightKey(groupId, messageId);

        if (!retry) {
            // 幂等：同一条消息在同一组内只投递一次
            if (inFlightMap.containsKey(inFlightKey)) {
                log.info("WhisperMQ Broker 消息{}已在组{}的在途表中，跳过重复推送", messageId, groupId);
                return;
            }
            // 检查组消费位点（按分区）：只推送该分区位点之后的消息
            String groupOffset = group.getOffset(topic, partition);
            if (groupOffset != null && compareMessageId(messageId, groupOffset) <= 0) {
                log.info("WhisperMQ Broker 消息{}已在组{}分区{}的消费位点之前，跳过推送", messageId, groupId, partition);
                return;
            }
        }

        if (group.getOnlineConsumers().isEmpty()) {
            log.info("WhisperMQ Broker 组{}无在线消费者，消息{}暂不推送", groupId, messageId);
            return;
        }

        String targetConsumerId = selectTarget(group, message, messageId, null);
        if (targetConsumerId == null) {
            log.info("WhisperMQ Broker 消费者组{}无订阅Tag:{}的在线消费者，消息{}暂不推送",
                    groupId, messageTag, messageId);
            return;
        }
        deliver(group, messageId, message, targetConsumerId, 1);
    }

    /**
     * 实际投递并登记在途记录。
     *
     * @param attempts 本次是第几次投递（含首次），用于判断是否超过最大投递次数
     */
    private boolean deliver(ConsumerGroup group, String messageId, MqMessage message,
                            String targetConsumerId, int attempts) {
        Channel targetChannel = group.getOnlineConsumers().get(targetConsumerId);
        if (targetChannel == null || !targetChannel.isActive()) {
            log.warn("WhisperMQ Broker 目标消费者{}不可用，消息{}投递失败", targetConsumerId, messageId);
            return false;
        }
        try {
            PushMessageDTO pushDto = new PushMessageDTO();
            pushDto.setMessageId(messageId);
            pushDto.setConsumerGroup(group.getGroupId());
            pushDto.setJson(JSON.toJSONString(message));
            RpcMessageDTO rpcDto = new RpcMessageDTO();
            rpcDto.setRequest(false);
            rpcDto.setMethodType(MethodType.B_PUSH_MSG);
            rpcDto.setJson(JSON.toJSONString(pushDto));

            targetChannel.writeAndFlush(JSON.toJSONString(rpcDto) + "\n"); // 带分隔符

            inFlightMap.put(inFlightKey(group.getGroupId(), messageId),
                    new InFlightMessage(group.getGroupId(), message.getTopic(), message.getPartition(),
                            messageId, message, targetConsumerId, attempts, System.currentTimeMillis()));
            log.info("WhisperMQ Broker 向组{}的消费者{}推送消息{}（分区{}，第{}次投递）",
                    group.getGroupId(), targetConsumerId, messageId, message.getPartition(), attempts);
            return true;
        } catch (Exception e) {
            log.error("WhisperMQ Broker 推送消息{}给组{}的消费者{}失败",
                    messageId, group.getGroupId(), targetConsumerId, e);
            return false;
        }
    }

    /**
     * 选择投递目标。
     *
     * <p>分区 topic（分区数 &gt; 1）：优先投给该分区的 owner，保证同一分区落到同一个消费者；
     * 若 owner 的 Tag 过滤不匹配（同组 tag 不一致），回退到 tag 匹配的消费者并告警
     * —— 这种情况下该分区的严格顺序会被放宽（与 docs/p1-roadmap.md §3.1 一致）。</p>
     *
     * <p>单分区 topic：保持旧的按消息 ID 取模的组内负载均衡。</p>
     */
    private String selectTarget(ConsumerGroup group, MqMessage message, String messageId, String excludeConsumerId) {
        String topic = message.getTopic();
        if (topicMetadata.partitionsOf(topic) > 1) {
            String owner = groupManager.partitionOwner(group, topic, message.getPartition());
            if (owner != null) {
                if (group.accepts(topic, owner, message.getTag())) {
                    return owner;
                }
                log.warn("WhisperMQ Broker 分区{}的owner {}不接受tag {}，回退到tag匹配消费者（该分区顺序被放宽）",
                        message.getPartition(), owner, message.getTag());
            }
        }
        return pickConsumer(group, topic, message.getTag(), messageId, excludeConsumerId);
    }

    /**
     * 选择目标消费者：同组内按消息 ID 取模（同一消息稳定落到同一消费者），
     * 重投时优先排除上一次失败的消费者。
     */
    private String pickConsumer(ConsumerGroup group, String topic, String messageTag,
                                String messageId, String excludeConsumerId) {
        Map<String, Channel> onlineConsumers = group.getOnlineConsumers();
        List<String> candidates;
        if (messageTag == null || messageTag.isBlank()) {
            // 消息未打标签：无法按 tag 过滤，投递给组内全部在线消费者候选
            candidates = new ArrayList<>(onlineConsumers.keySet());
            candidates.removeIf(id -> !onlineConsumers.get(id).isActive());
        } else {
            candidates = new ArrayList<>(group.getConsumersByTopicAndTag(topic, messageTag));
            candidates.removeIf(id -> !onlineConsumers.containsKey(id));
        }
        if (candidates.isEmpty()) {
            return null;
        }
        // 背压：处于暂停窗口的消费者不参与本轮选择；全部暂停时返回 null，等 resume 补投
        candidates.removeIf(this::isPaused);
        if (candidates.isEmpty()) {
            return null;
        }
        if (excludeConsumerId != null && candidates.size() > 1) {
            candidates.remove(excludeConsumerId);
        }
        // 稳定取模：保证正常投递时同一消息固定落到同一消费者，便于排查
        int index = (int) Math.floorMod(parseMessageId(messageId), candidates.size());
        return candidates.get(index);
    }

    /** 消息 ID 解析失败时退化为哈希，避免 NumberFormatException 打断投递 */
    private long parseMessageId(String messageId) {
        try {
            return Long.parseLong(messageId);
        } catch (NumberFormatException e) {
            return messageId == null ? 0 : Math.abs(messageId.hashCode());
        }
    }

    private int compareMessageId(String left, String right) {
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }
        try {
            return Long.compare(Long.parseLong(left), Long.parseLong(right));
        } catch (NumberFormatException e) {
            return left.compareTo(right);
        }
    }

    private String inFlightKey(String groupId, String messageId) {
        return groupId + "|" + messageId;
    }

    /** 定时扫描：ACK 超时的消息重新投递，超过最大次数进入死信 */
    private void scanTimeoutMessages() {
        try {
            long now = System.currentTimeMillis();
            long ackTimeout = Math.max(1000, config.getAckTimeoutMillis());
            for (InFlightMessage record : inFlightMap.values()) {
                if (now - record.getLastPushTime() < ackTimeout) {
                    continue;
                }
                // 背压窗口内跳过：消费者队列满不是消费失败，不应累计投递次数（否则会误入死信）
                if (isPaused(record.getConsumerId())) {
                    continue;
                }
                // 原子摘除，避免与 ACK 并发时重复重投
                if (!inFlightMap.remove(inFlightKey(record.getGroupId(), record.getMessageId()), record)) {
                    continue;
                }
                log.warn("WhisperMQ Broker 消息{}在组{}中 ACK 超时（已投递{}次，等待{}ms），触发重投",
                        record.getMessageId(), record.getGroupId(), record.getAttempts(),
                        now - record.getLastPushTime());
                redeliver(record);
            }
        } catch (Exception e) {
            log.error("WhisperMQ Broker ACK 超时扫描异常", e);
        }
    }

    /** 重投（或超过上限转死信） */
    private void redeliver(InFlightMessage record) {
        ConsumerGroup group = groupManager.getGroupMap().get(record.getGroupId());
        if (group == null || group.getOnlineConsumers().isEmpty()) {
            // 组已无消费者：不丢弃消息，保留在 messageMap 中，等组重新订阅时按位点回溯
            log.warn("WhisperMQ Broker 组{}已无在线消费者，消息{}暂不重投，等待重新订阅回溯",
                    record.getGroupId(), record.getMessageId());
            return;
        }
        int nextAttempt = record.getAttempts() + 1;
        if (nextAttempt > Math.max(1, config.getMaxDeliveryAttempts())) {
            log.warn("WhisperMQ Broker 消息{}在组{}中投递{}次仍无 ACK，转入死信",
                    record.getMessageId(), record.getGroupId(), record.getAttempts());
            moveToDeadLetter(record.getGroupId(), record.getTopic(), record.getPartition(),
                    record.getMessageId(), record.getMessage(), DeadType.CONSUME_TIMEOUT, record.getAttempts());
            return;
        }
        String targetConsumerId = selectTarget(group, record.getMessage(), record.getMessageId(),
                record.getConsumerId());
        if (targetConsumerId == null) {
            log.warn("WhisperMQ Broker 组{}暂无可用消费者，消息{}等待下一轮重投",
                    record.getGroupId(), record.getMessageId());
            return;
        }
        if (!deliver(group, record.getMessageId(), record.getMessage(), targetConsumerId, nextAttempt)) {
            // 投递失败（通道刚断）：保留在途记录并刷新时间，避免扫描线程热循环
            record.setLastPushTime(System.currentTimeMillis());
            inFlightMap.put(inFlightKey(record.getGroupId(), record.getMessageId()), record);
        }
    }

    /** 消费者掉线：把它手上未确认的消息重新投递给组内其他消费者 */
    public void onConsumerOffline(String groupId, String consumerId) {
        if (groupId == null || consumerId == null) {
            return;
        }
        pausedConsumers.remove(consumerId);
        for (InFlightMessage record : new ArrayList<>(inFlightMap.values())) {
            if (!groupId.equals(record.getGroupId()) || !consumerId.equals(record.getConsumerId())) {
                continue;
            }
            if (inFlightMap.remove(inFlightKey(record.getGroupId(), record.getMessageId()), record)) {
                log.warn("WhisperMQ Broker 消费者{}掉线，重投其未确认消息{}", consumerId, record.getMessageId());
                redeliver(record);
            }
        }
    }

    /**
     * 处理消费者的 ACK 更新消费状态。
     * ACK_FAIL 不再被丢弃，而是立即触发重投（旧实现直接 return，消息就此卡死）。
     */
    public void handleConsumerAck(ConsumerAckReqDTO ackReq, String consumerId) {
        if (ackReq == null || ackReq.getMessageId() == null || ackReq.getGroupId() == null) {
            log.error("WhisperMQ Broker 消费确认请求参数无效：{}", ackReq);
            return;
        }
        String messageId = ackReq.getMessageId();
        String groupId = ackReq.getGroupId();
        String ackStatus = ackReq.getAckStatus() == null ? "ACK_SUCCESS" : ackReq.getAckStatus();

        InFlightMessage record = inFlightMap.remove(inFlightKey(groupId, messageId));

        if ("ACK_FAIL".equals(ackStatus)) {
            log.warn("WhisperMQ Broker 消费者{}拒绝消息{}（组{}），立即重投", consumerId, messageId, groupId);
            if (record != null) {
                redeliver(record);
            } else {
                log.warn("WhisperMQ Broker 消息{}在组{}中无在途记录，忽略本次 ACK_FAIL", messageId, groupId);
            }
            return;
        }

        MqMessage message = messageMap.get(messageId);
        if (message == null) {
            log.error("WhisperMQ Broker 消息{}不存在，ACK处理失败", messageId);
            return;
        }
        String topic = record != null ? record.getTopic() : message.getTopic();
        int partition = ackReq.getPartition() != null ? ackReq.getPartition()
                : (record != null ? record.getPartition() : message.getPartition());
        groupManager.updateGroupOffset(groupId, topic, partition, messageId);
        log.info("WhisperMQ Broker 收到消费者{}的ACK：消息{}（分区{}），状态{}",
                consumerId, messageId, partition, ackStatus);
    }

    /**
     * 处理消费者返回的死信消息。
     *
     * <p>修复点：
     * <ul>
     *   <li>重试只回到"产生该死信的那个组"，不再广播给所有订阅组；</li>
     *   <li>进入终态前清理在途记录，避免 ACK 超时扫描重复处理；</li>
     *   <li>终态消息推进产生它的组位点，避免重新订阅时被它反复回溯。</li>
     * </ul>
     */
    public void handlerDeadLetter(DeadLetterDTO deadLetterDTO, String originMessageId) {
        log.info("WhisperMQ Broker 收到生产者返回的死信消息：原ID={},死信ID={}",
                originMessageId, deadLetterDTO.getDeadLetterId());

        Integer deadRetryCount = deadLetterDTO.getDeadRetryCount();
        PushMessageDTO pushMessageDTO = null;
        try {
            pushMessageDTO = JSON.parseObject(deadLetterDTO.getJson(), PushMessageDTO.class);
        } catch (Exception e) {
            log.error("WhisperMQ Broker 解析死信消息体失败，死信ID={}", deadLetterDTO.getDeadLetterId(), e);
        }
        String groupId = pushMessageDTO == null ? null : pushMessageDTO.getConsumerGroup();
        MqMessageEnduring mqMessage = null;
        if (pushMessageDTO != null && pushMessageDTO.getJson() != null) {
            try {
                mqMessage = JSON.parseObject(pushMessageDTO.getJson(), MqMessageEnduring.class);
            } catch (Exception e) {
                log.error("WhisperMQ Broker 解析死信原始消息失败，原ID={}", originMessageId, e);
            }
        }

        boolean terminal = deadRetryCount == null || deadRetryCount == -1
                || deadRetryCount >= config.getMaxDeadLetterRetryCount();
        if (terminal) {
            if (mqMessage == null) {
                MqMessage existing = getMessage(originMessageId);
                if (existing instanceof MqMessageEnduring) {
                    mqMessage = (MqMessageEnduring) existing;
                }
            }
            if (mqMessage == null) {
                log.error("WhisperMQ Broker 死信原始消息{}已不存在，无法入死信队列", originMessageId);
                return;
            }
            inFlightMap.remove(inFlightKey(groupId, originMessageId));
            moveToDeadLetter(groupId, mqMessage.getTopic(), mqMessage.getPartition(), originMessageId, mqMessage,
                    deadLetterDTO.getDeadType(), deadRetryCount);
            return;
        }

        if (mqMessage == null) {
            log.error("WhisperMQ Broker 死信原始消息{}解析失败，放弃重试", originMessageId);
            return;
        }
        ConsumerGroup group = groupId == null ? null : groupManager.getGroupMap().get(groupId);
        if (group == null || group.getOnlineConsumers().isEmpty()) {
            log.warn("WhisperMQ Broker 死信原组{}已不存在或无在线消费者，消息{}暂不重试，等待重新订阅回溯",
                    groupId, originMessageId);
            return;
        }
        // 允许重新登记在途记录（原投递记录可能还在）
        inFlightMap.remove(inFlightKey(groupId, originMessageId));
        String targetConsumerId = selectTarget(group, mqMessage, originMessageId, null);
        if (targetConsumerId == null) {
            log.warn("WhisperMQ Broker 死信消息{}暂无可用消费者", originMessageId);
            return;
        }
        if (deliver(group, originMessageId, mqMessage, targetConsumerId, 1)) {
            log.info("WhisperMQ Broker 重新发送死信消息{}至组{}，死信原因：{}，死信次数：{}",
                    originMessageId, groupId, deadLetterDTO.getDeadType(), deadRetryCount);
        }
    }

    /**
     * 重放一条死信：从内存队列原子摘除、写重放标记、重新投递给原消费组。
     *
     * @return null 表示重放成功；否则返回失败原因
     */
    public String replayDeadLetter(String deadLetterId) {
        if (deadLetterId == null || deadLetterId.isBlank()) {
            return "死信ID不能为空";
        }
        DeadLetterDTO deadLetter = deadLetterQueue.get(deadLetterId);
        if (deadLetter == null) {
            return "死信不存在或已被重放：" + deadLetterId;
        }
        PushMessageDTO pushMessageDTO;
        try {
            pushMessageDTO = JSON.parseObject(deadLetter.getJson(), PushMessageDTO.class);
        } catch (Exception e) {
            log.error("解析死信{}的消息体失败", deadLetterId, e);
            return "死信消息体损坏：" + deadLetterId;
        }
        if (pushMessageDTO == null || pushMessageDTO.getJson() == null) {
            return "死信消息体为空：" + deadLetterId;
        }
        String groupId = pushMessageDTO.getConsumerGroup();
        ConsumerGroup group = groupId == null ? null : groupManager.getGroupMap().get(groupId);
        if (group == null) {
            return "原消费组不在线，无法重放：" + groupId;
        }
        if (group.getOnlineConsumers().isEmpty()) {
            return "原消费组无在线消费者，无法重放：" + groupId;
        }
        MqMessageEnduring message;
        try {
            message = JSON.parseObject(pushMessageDTO.getJson(), MqMessageEnduring.class);
        } catch (Exception e) {
            log.error("解析死信{}的原始消息失败", deadLetterId, e);
            return "死信原始消息解析失败：" + deadLetterId;
        }
        if (message == null) {
            return "死信原始消息为空：" + deadLetterId;
        }
        // 原子摘除：并发重放同一条死信时只有一个请求能成功
        if (deadLetterQueue.removeById(deadLetterId) == null) {
            return "死信已被并发重放：" + deadLetterId;
        }
        // 先落"已重放"标记，再投递：宁可投递失败也不要重启后重复加载
        deadLetterPersistUtil.writeReplayedMarker(deadLetterId);
        messageMap.putIfAbsent(deadLetter.getMessageId(), message);
        // isRetry=true：跳过位点判断，直接把死信重新推给原组
        pushMessageToGroup(group, deadLetter.getMessageId(), message, true);
        log.info("WhisperMQ Broker 死信{}已重放：原消息{} -> 组{}",
                deadLetterId, deadLetter.getMessageId(), groupId);
        return null;
    }

    /**
     * 消息进入死信终态：标记持久化状态、入内存队列、落盘、推进组位点。
     */
    private void moveToDeadLetter(String groupId, String topic, int partition, String messageId, MqMessage message,
                                  DeadType deadType, Integer deadRetryCount) {
        DeadLetterDTO deadLetter = new DeadLetterDTO();
        deadLetter.setMessageId(messageId);
        deadLetter.setDeadType(deadType == null ? DeadType.BUSINESS_ERROR : deadType);
        deadLetter.setDeadRetryCount(deadRetryCount == null ? 0 : deadRetryCount);

        PushMessageDTO pushMessageDTO = new PushMessageDTO();
        pushMessageDTO.setMessageId(messageId);
        pushMessageDTO.setConsumerGroup(groupId);
        pushMessageDTO.setJson(JSON.toJSONString(message));
        deadLetter.setJson(JSON.toJSONString(pushMessageDTO));

        String deadLetterId = deadLetter.getDeadLetterId();
        try {
            messageStore.markDead(messageId);
            deadLetterQueue.put(deadLetter);
            deadLetterPersistUtil.writeDeadLetter(deadLetter);
            log.info("WhisperMQ Broker 死信消息{}已入队并持久化，死信原因：{}，组：{}",
                    deadLetterId, deadLetter.getDeadType(), groupId);
        } catch (Exception e) {
            log.error("处理死信消息时发生异常，死信ID: {}", deadLetterId, e);
        }

        messageMap.remove(messageId);
        inFlightMap.remove(inFlightKey(groupId, messageId));
        // 该消息已是终态：推进产生它的组位点，避免重新订阅时被这条消息反复回溯
        if (groupId != null && topic != null) {
            groupManager.updateGroupOffset(groupId, topic, partition, messageId);
        }
    }

}
