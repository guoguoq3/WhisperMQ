package org.guoguo.console.service;

import com.alibaba.fastjson2.JSON;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.broker.ConsumerGroup.ConsumerGroupManager;
import org.guoguo.broker.core.BrokerManager;
import org.guoguo.broker.core.MqBroker;
import org.guoguo.broker.core.TopicMetadata;
import org.guoguo.broker.store.SegmentMessageStore;
import org.guoguo.common.config.MqConfigProperties;
import org.guoguo.common.pojo.DTO.DeadLetterDTO;
import org.guoguo.common.pojo.DTO.PushMessageDTO;
import org.guoguo.common.pojo.DTO.SubscribeReqDTO;
import org.guoguo.common.pojo.Entity.ConsumerGroup;
import org.guoguo.common.pojo.Entity.DeadLetterQueue;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.guoguo.common.pojo.Entity.MqMessageEnduring;
import org.guoguo.consumer.service.impl.MqConsumerManager;
import org.guoguo.console.config.ConsoleProperties;
import org.guoguo.producer.pojo.Result;
import org.guoguo.producer.service.impl.MqProducer;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 控制台核心服务：聚合一帧 Broker 状态快照，并管理页面上的生产者 / 消费者。
 *
 * <p>这里创建的生产者/消费者都是真实客户端，走 TCP 协议连本进程内嵌的 Broker，
 * 所以页面看到的行为与真实使用者一致（含 in-flight 重投、位点、死信、Tag 路由）。</p>
 */
@Slf4j
@Service
public class ConsoleService {

    private final MqConfigProperties config;
    private final ConsoleProperties consoleProperties;
    private final BrokerManager brokerManager;
    private final ConsumerGroupManager groupManager;
    private final DeadLetterQueue deadLetterQueue;
    private final TopicMetadata topicMetadata;

    /**
     * 仅用于保证 Bean 初始化顺序：ConsoleService 依赖 MqBroker 之后，
     * Broker 的 @PostConstruct 已经完成端口绑定，生产者才能一次连上。
     */
    private final MqBroker broker;

    private final Map<String, ConsumerSession> sessions = new ConcurrentHashMap<>();
    private final AtomicInteger consumerSequence = new AtomicInteger();

    private MqProducer producer;

    public ConsoleService(MqConfigProperties config,
                          ConsoleProperties consoleProperties,
                          BrokerManager brokerManager,
                          ConsumerGroupManager groupManager,
                          DeadLetterQueue deadLetterQueue,
                          TopicMetadata topicMetadata,
                          MqBroker broker) {
        this.config = config;
        this.consoleProperties = consoleProperties;
        this.brokerManager = brokerManager;
        this.groupManager = groupManager;
        this.deadLetterQueue = deadLetterQueue;
        this.topicMetadata = topicMetadata;
        this.broker = broker;
    }

    @PostConstruct
    public void init() {
        this.producer = new MqProducer(config);
        this.producer.start();
        log.info("控制台已启动：Broker 端口={}，Web 控制台 http://127.0.0.1:8080", config.getBrokerPort());
    }

    @PreDestroy
    public void shutdown() {
        sessions.values().forEach(ConsumerSession::close);
        sessions.clear();
        if (producer != null) {
            producer.close();
        }
    }

    // ------------------------------------------------------------------ 生产者

    public Map<String, Object> send(String topic, String tag, String payload, Boolean enduring, String bizKey) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic 不能为空");
        }
        if (topic.length() > 200) {
            throw new IllegalArgumentException("topic 太长（最多 200 字符）");
        }
        MqMessageEnduring message = new MqMessageEnduring();
        message.setTopic(topic.trim());
        message.setTag(tag == null || tag.isBlank() ? null : tag.trim());
        message.setPayload(payload == null ? "" : payload);
        message.setBizKey(bizKey);
        message.setEnduring(enduring == null || enduring);

        Result<String> result = producer.send(message);

        Map<String, Object> out = new LinkedHashMap<>();
        boolean ok = result.getCode() != null && result.getCode() == 200;
        out.put("ok", ok);
        out.put("code", result.getCode());
        out.put("messageId", result.getMessageId());
        out.put("message", result.getMessage());
        out.put("enduring", message.isEnduring());
        if (ok) {
            out.put("partition", topicMetadata.partitionOf(topic, message.getBizKey(),
                    result.getMessageId(), message.getPartition()));
        }
        return out;
    }

    /**
     * 批量发送（控制台压测/演示）：串行发送 count 条，返回耗时与吞吐。
     * payload 为 "{序号}-" + 指定长度的填充串，便于在消息列表里辨认。
     */
    public Map<String, Object> batchSend(String topic, String tag, int count, int payloadSize) {
        if (count < 1 || count > 20000) {
            throw new IllegalArgumentException("批量条数需在 1~20000 之间");
        }
        int size = Math.max(1, Math.min(64 * 1024, payloadSize));
        String padding = "x".repeat(size);
        long start = System.currentTimeMillis();
        int success = 0;
        String error = null;
        int batchSize = Math.max(1, Math.min(config.getMaxBatchSize(), 1000));
        for (int offset = 0; offset < count; offset += batchSize) {
            int chunkSize = Math.min(batchSize, count - offset);
            java.util.List<MqMessageEnduring> messages = new java.util.ArrayList<>(chunkSize);
            for (int i = 0; i < chunkSize; i++) {
                MqMessageEnduring message = new MqMessageEnduring();
                message.setTopic(topic);
                message.setTag(tag == null || tag.isBlank() ? null : tag.trim());
                message.setPayload((offset + i) + "-" + padding);
                message.setBizKey("batch-" + (offset + i));
                message.setEnduring(true);
                messages.add(message);
            }
            org.guoguo.producer.pojo.Result<String> result = producer.sendBatch(messages);
            if (result.getCode() != null && result.getCode() == 200) {
                success += chunkSize;
            } else {
                error = result.getMessage();
                break;
            }
        }
        long elapsed = Math.max(1, System.currentTimeMillis() - start);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", success == count);
        out.put("success", success);
        out.put("count", count);
        out.put("elapsedMillis", elapsed);
        out.put("throughputPerSecond", Math.round(success * 1000.0 / elapsed));
        out.put("error", error);
        return out;
    }

    // ------------------------------------------------------------------ 消费者

    public ConsumerSession createConsumer(String groupId, String topic, List<String> tags, String mode) {
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("消费者组不能为空");
        }
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic 不能为空");
        }
        List<String> safeTags = (tags == null || tags.isEmpty()) ? List.of("*") : List.copyOf(tags);
        String id = "c" + consumerSequence.incrementAndGet() + "-" + UUID.randomUUID().toString().substring(0, 4);

        MqConsumerManager consumer = new MqConsumerManager(config);
        ConsumerSession session = new ConsumerSession(id, groupId.trim(), topic.trim(), safeTags, mode, consumer);
        try {
            consumer.start();
            consumer.joinGroup(groupId.trim());

            SubscribeReqDTO subscribeReq = new SubscribeReqDTO();
            subscribeReq.setTopic(topic.trim());
            subscribeReq.setTags(safeTags);
            consumer.groupSubscribe(subscribeReq, session::onMessage);
        } catch (RuntimeException e) {
            consumer.close();
            throw new IllegalArgumentException("创建消费者失败：" + e.getMessage(), e);
        }
        sessions.put(id, session);
        log.info("控制台创建消费者 {}：group={}，topic={}，tags={}，mode={}",
                id, groupId, topic, safeTags, session.getMode());
        return session;
    }

    public void closeConsumer(String id) {
        ConsumerSession session = sessions.remove(id);
        if (session == null) {
            throw new IllegalArgumentException("消费者不存在：" + id);
        }
        session.close();
    }

    public void setConsumerMode(String id, String mode) {
        ConsumerSession session = sessions.get(id);
        if (session == null) {
            throw new IllegalArgumentException("消费者不存在：" + id);
        }
        session.setMode(mode);
    }

    // ------------------------------------------------------------------ 观测

    /** 健康检查：存储写失败进入只读时返回 ok=false */
    public Map<String, Object> health() {
        Map<String, Object> store = brokerManager.getStoreStats();
        boolean readOnly = Boolean.TRUE.equals(store.get("readOnly"));
        Map<String, Object> health = new LinkedHashMap<>();
        health.put("ok", !readOnly);
        health.put("readOnly", readOnly);
        health.put("lastError", store.get("lastError"));
        health.put("store", store);
        return health;
    }

    public Map<String, Object> overview() {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("broker", brokerInfo());
        root.put("config", configInfo());
        root.put("topics", topicInfos());
        root.put("groups", groupInfos());
        root.put("consumers", sessions.values().stream()
                .sorted(Comparator.comparing(ConsumerSession::getId))
                .map(ConsumerSession::snapshot)
                .toList());
        root.put("deadLetters", deadLetters());
        root.put("recentMessages", recentMessages());
        return root;
    }

    /** 统一指标端点：Broker/存储/消费端背压与去重指标 */
    public Map<String, Object> stats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("broker", brokerInfo());
        stats.put("store", brokerManager.getStoreStats());
        stats.put("topics", topicInfos());
        stats.put("consumers", sessions.values().stream()
                .sorted(Comparator.comparing(ConsumerSession::getId))
                .map(session -> {
                    Map<String, Object> item = new LinkedHashMap<>();
                    Map<String, Object> snapshot = session.snapshot();
                    item.put("id", snapshot.get("id"));
                    item.put("group", snapshot.get("group"));
                    item.put("paused", snapshot.get("paused"));
                    item.put("queued", snapshot.get("queued"));
                    item.put("queueCapacity", snapshot.get("queueCapacity"));
                    item.put("dedupHits", snapshot.get("dedupHits"));
                    item.put("dispatcher", snapshot.get("dispatcher"));
                    return item;
                })
                .toList());
        return stats;
    }

    private Map<String, Object> brokerInfo() {        Map<String, Object> info = new LinkedHashMap<>();
        info.put("host", config.getBrokerHost());
        info.put("port", config.getBrokerPort());
        info.put("messages", brokerManager.getMessageCount());
        info.put("store", brokerManager.getStoreStats());
        info.put("paused", brokerManager.getPausedCount());
        info.put("protocolVersion", org.guoguo.common.pojo.DTO.RpcMessageDTO.PROTOCOL_VERSION);
        info.put("authEnabled", config.isAuthEnabled());
        info.put("hotCacheSize", config.getHotCacheSize());
        info.put("retentionMillis", config.getRetentionMillis());
        info.put("groups", groupManager.getGroupMap().size());
        info.put("deadLetters", deadLetterQueue.size());
        info.put("inFlight", brokerManager.getInFlightCount());
        info.put("onlineConsumers", groupManager.getGroupMap().values().stream()
                .mapToInt(group -> group.getOnlineConsumers().size()).sum());
        return info;
    }

    private Map<String, Object> configInfo() {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("ackTimeoutMillis", config.getAckTimeoutMillis());
        info.put("maxDeliveryAttempts", config.getMaxDeliveryAttempts());
        info.put("maxDeadLetterRetryCount", config.getMaxDeadLetterRetryCount());
        info.put("newGroupStartFromEarliest", config.isNewGroupStartFromEarliest());
        info.put("subscribeTimeoutMillis", config.getSubscribeTimeoutMillis());
        info.put("heartbeatIntervalMillis", config.getHeartbeatIntervalMillis());
        info.put("reconnectMaxDelayMillis", config.getReconnectMaxDelayMillis());
        return info;
    }

    private List<Map<String, Object>> topicInfos() {
        Map<String, Integer> counts = new TreeMap<>(brokerManager.getTopicMessageCounts());
        for (ConsumerGroup group : groupManager.getGroupMap().values()) {
            for (String topic : group.getSubscribeMap().keySet()) {
                counts.putIfAbsent(topic, 0);
            }
        }
        List<Map<String, Object>> list = new ArrayList<>();
        counts.forEach((topic, count) -> {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("topic", topic);
            item.put("messages", count);
            list.add(item);
        });
        return list;
    }

    private List<Map<String, Object>> groupInfos() {
        Map<String, Integer> inFlightByGroup = brokerManager.getInFlightCountByGroup();
        List<Map<String, Object>> list = new ArrayList<>();
        for (ConsumerGroup group : groupManager.getGroupMap().values()) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("groupId", group.getGroupId());
            item.put("onlineConsumers", group.getOnlineConsumers().size());
            item.put("consumerIds", new ArrayList<>(group.getOnlineConsumers().keySet()));
            item.put("subscriptions", new ArrayList<>(group.getSubscribeMap().keySet()));
            item.put("offsets", new LinkedHashMap<>(group.offsetView()));
            item.put("tags", new LinkedHashMap<>(group.getTopicConsumerTags()));
            item.put("inFlight", inFlightByGroup.getOrDefault(group.getGroupId(), 0));
            Map<String, Integer> partitions = new LinkedHashMap<>();
            for (String topic : group.getSubscribeMap().keySet()) {
                partitions.put(topic, topicMetadata.partitionsOf(topic));
            }
            item.put("partitions", partitions);
            item.put("assignment", group.assignmentView());
            item.put("assignmentEpoch", group.getAssignmentEpoch());
            list.add(item);
        }
        list.sort(Comparator.comparing(item -> String.valueOf(item.get("groupId"))));
        return list;
    }

    public List<Map<String, Object>> deadLetters() {
        List<Map<String, Object>> list = new ArrayList<>();
        for (DeadLetterDTO dto : deadLetterQueue.list()) {
            list.add(toDeadLetterMap(dto, false));
            if (list.size() >= 100) {
                break;
            }
        }
        return list;
    }

    /** 死信详情（完整 payload / 原始 json），供页面点击查看 */
    public Map<String, Object> deadLetterDetail(String deadLetterId) {
        DeadLetterDTO dto = deadLetterQueue.get(deadLetterId);
        return dto == null ? null : toDeadLetterMap(dto, true);
    }

    /** 消息详情：优先热缓存，未命中回源 segment；已被死信清理的消息按原 ID 去死信队列里找 */
    public Map<String, Object> messageDetail(String messageId) {
        MqMessage message = brokerManager.getMessage(messageId);
        if (message != null) {
            Map<String, Object> item = messageFields(messageId, message, true);
            item.put("status", "IN_MEMORY");
            return item;
        }
        for (DeadLetterDTO dto : deadLetterQueue.list()) {
            if (messageId.equals(dto.getMessageId())) {
                Map<String, Object> item = toDeadLetterMap(dto, true);
                item.put("status", "DEAD_LETTER");
                return item;
            }
        }
        return null;
    }

    private Map<String, Object> toDeadLetterMap(DeadLetterDTO dto, boolean full) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("id", dto.getDeadLetterId());
        item.put("messageId", dto.getMessageId());
        item.put("deadType", dto.getDeadType() == null ? null : dto.getDeadType().name());
        item.put("deadTime", dto.getDeadTime() == null ? null : dto.getDeadTime().toString());
        item.put("retryCount", dto.getDeadRetryCount());
        try {
            PushMessageDTO push = JSON.parseObject(dto.getJson(), PushMessageDTO.class);
            if (push != null) {
                item.put("groupId", push.getConsumerGroup());
                MqMessage message = JSON.parseObject(push.getJson(), MqMessage.class);
                if (message != null) {
                    item.putAll(messageFields(dto.getMessageId(), message, full));
                }
            }
        } catch (Exception e) {
            item.put("payload", "<消息体解析失败>");
        }
        if (full) {
            item.put("deadLetterJson", dto.getJson());
        }
        return item;
    }

    private Map<String, Object> messageFields(String messageId, MqMessage message, boolean full) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("messageId", messageId);
        item.put("topic", message.getTopic());
        item.put("tag", message.getTag());
        item.put("bizKey", message.getBizKey());
        item.put("payload", full ? message.getPayload() : truncate(message.getPayload(), 300));
        item.put("payloadLength", message.getPayload() == null ? 0 : message.getPayload().length());
        item.put("partition", message.getPartition());
        item.put("enduring", !(message instanceof MqMessageEnduring) || ((MqMessageEnduring) message).isEnduring());
        return item;
    }

    public Map<String, Object> replayDeadLetter(String deadLetterId) {
        String error = brokerManager.replayDeadLetter(deadLetterId);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", error == null);
        out.put("error", error);
        return out;
    }

    private List<Map<String, Object>> recentMessages() {
        int limit = Math.max(1, consoleProperties.getMaxRecentMessages());
        List<Map<String, Object>> list = new ArrayList<>();
        for (SegmentMessageStore.StoredMessage stored : brokerManager.getRecentMessages(limit)) {
            list.add(messageFields(stored.messageId(), stored.message(), false));
        }
        return list;
    }

    /** 查询某组某主题的当前位点（分区 0 的兼容视图） */
    public String offsetOf(String groupId, String topic) {
        return offsetOf(groupId, topic, 0);
    }

    /** 查询某组某主题某分区的当前位点 */
    public String offsetOf(String groupId, String topic, int partition) {
        ConsumerGroup group = groupManager.getGroupMap().get(groupId);
        return group == null ? null : group.getOffset(topic, partition);
    }

    public ConsumerSession sessionOf(String id) {
        ConsumerSession session = sessions.get(id);
        if (session == null) {
            throw new IllegalArgumentException("消费者不存在：" + id);
        }
        return session;
    }

    private long parseId(String messageId) {
        try {
            return Long.parseLong(messageId);
        } catch (NumberFormatException e) {
            return messageId == null ? 0 : Math.abs(messageId.hashCode());
        }
    }

    private String truncate(String value, int max) {
        if (value == null) {
            return "";
        }
        return value.length() <= max ? value : value.substring(0, max) + "…";
    }
}
