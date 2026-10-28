package org.guoguo.console.service;

import lombok.extern.slf4j.Slf4j;
import org.guoguo.common.constant.DeadType;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.guoguo.consumer.service.impl.MqConsumerManager;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 控制台里的一个"消费者实例"：真实的 {@link MqConsumerManager}（TCP 连接 + 组订阅）+
 * 可切换的业务行为（成功 / 失败 / 失败一次 / 拒绝重试 / 抛异常 / 慢消费），
 * 用来在页面上演示 MQ 的各种投递语义。
 */
@Slf4j
public class ConsumerSession {

    /** 页面可选的行为模式 */
    public static final List<String> MODES = List.of(
            "SUCCESS", "FAIL", "FAIL_ONCE", "REJECT_RETRY", "THROW", "SLOW");

    private static final int MAX_RECENT = 20;

    private final String id;
    private final String groupId;
    private final String topic;
    private final List<String> tags;
    private final AtomicReference<String> mode;
    private final MqConsumerManager consumer;

    private final AtomicInteger received = new AtomicInteger();
    private final AtomicInteger success = new AtomicInteger();
    private final AtomicInteger failed = new AtomicInteger();
    private final AtomicBoolean failOnceTriggered = new AtomicBoolean(false);
    private final ConcurrentLinkedDeque<Map<String, Object>> recent = new ConcurrentLinkedDeque<>();
    private final long startedAt = System.currentTimeMillis();

    private volatile String lastThread;
    private volatile String lastError;

    public ConsumerSession(String id, String groupId, String topic, List<String> tags,
                           String mode, MqConsumerManager consumer) {
        this.id = id;
        this.groupId = groupId;
        this.topic = topic;
        this.tags = List.copyOf(tags);
        this.mode = new AtomicReference<>(normalizeMode(mode));
        this.consumer = consumer;
    }

    private static String normalizeMode(String mode) {
        if (mode == null || mode.isBlank()) {
            return "SUCCESS";
        }
        String upper = mode.trim().toUpperCase();
        return MODES.contains(upper) ? upper : "SUCCESS";
    }

    /** 由 MqConsumerManager 的业务线程池回调 */
    public boolean onMessage(MqMessage message, String messageId) {
        received.incrementAndGet();
        lastThread = Thread.currentThread().getName();

        Map<String, Object> item = new LinkedHashMap<>();
        item.put("messageId", messageId);
        item.put("topic", message.getTopic());
        item.put("tag", message.getTag());
        item.put("payload", truncate(message.getPayload(), 300));
        item.put("at", System.currentTimeMillis());
        recent.addFirst(item);
        while (recent.size() > MAX_RECENT) {
            recent.pollLast();
        }

        String currentMode = mode.get();
        try {
            switch (currentMode) {
                case "FAIL":
                    failed.incrementAndGet();
                    return false;
                case "FAIL_ONCE":
                    if (failOnceTriggered.compareAndSet(false, true)) {
                        failed.incrementAndGet();
                        return false;
                    }
                    success.incrementAndGet();
                    return true;
                case "REJECT_RETRY":
                    // 业务主动拒绝并走死信重试链路（Manager 会跳过 ACK_FAIL，避免双重重投）
                    consumer.reject(message, messageId, DeadType.BUSINESS_ERROR, true);
                    failed.incrementAndGet();
                    return false;
                case "THROW":
                    failed.incrementAndGet();
                    throw new IllegalStateException("模拟业务异常（线程 " + Thread.currentThread().getName() + "）");
                case "SLOW":
                    Thread.sleep(500);
                    success.incrementAndGet();
                    return true;
                case "SUCCESS":
                default:
                    success.incrementAndGet();
                    return true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failed.incrementAndGet();
            return false;
        } catch (RuntimeException e) {
            lastError = e.getMessage();
            throw e;
        }
    }

    private static String truncate(String value, int max) {
        if (value == null) {
            return "";
        }
        return value.length() <= max ? value : value.substring(0, max) + "…";
    }

    public void close() {
        try {
            consumer.close();
        } catch (Exception e) {
            log.warn("关闭消费者 {} 失败", id, e);
        }
    }

    public Map<String, Object> snapshot() {
        Map<String, Object> snap = new LinkedHashMap<>();
        snap.put("id", id);
        snap.put("groupId", groupId);
        snap.put("topic", topic);
        snap.put("tags", tags);
        snap.put("mode", mode.get());
        snap.put("received", received.get());
        snap.put("success", success.get());
        snap.put("failed", failed.get());
        snap.put("startedAt", startedAt);
        snap.put("lastThread", lastThread);
        snap.put("lastError", lastError);
        snap.put("alive", consumer.getDispatcher() != null);
        snap.put("paused", consumer.isPaused());
        snap.put("dedupHits", consumer.getDedupHits());
        snap.put("queued", consumer.getQueued());
        snap.put("queueCapacity", consumer.getQueueCapacity());
        snap.put("dispatcher", Map.of(
                "submitted", consumer.getDispatcher().getSubmitted(),
                "completed", consumer.getDispatcher().getCompleted(),
                "failed", consumer.getDispatcher().getFailed(),
                "rejected", consumer.getDispatcher().getRejected()));
        snap.put("recent", new ArrayList<>(recent));
        return snap;
    }

    public String getId() {
        return id;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getTopic() {
        return topic;
    }

    public List<String> getTags() {
        return tags;
    }

    public String getMode() {
        return mode.get();
    }

    public void setMode(String mode) {
        this.mode.set(normalizeMode(mode));
    }

    public int getReceived() {
        return received.get();
    }

    public int getSuccess() {
        return success.get();
    }

    public int getFailed() {
        return failed.get();
    }

    public MqConsumerManager getConsumer() {
        return consumer;
    }
}
