package org.guoguo.consumer.dispatch;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 消费端去重缓存（P1 Stage D）。
 *
 * <p>at-least-once 语义下 Broker 可能重复投递（ACK 丢失、超时重投、再均衡换 owner）。
 * 这里用容量受限的 LRU 记录"已成功处理"的 messageId：重复消息直接 ACK，不再执行业务。
 * 只缓存成功处理的消息，失败/重投的消息不会被误判为重复。</p>
 */
public class DedupCache {

    private final int capacity;
    private final boolean enabled;
    private final LinkedHashMap<String, Boolean> seen;
    private long hits;

    public DedupCache(int capacity) {
        this.enabled = capacity > 0;
        this.capacity = Math.max(1, capacity);
        this.seen = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > DedupCache.this.capacity;
            }
        };
    }

    /** 是否已经成功处理过；命中会刷新 LRU 位置并累加命中数 */
    public synchronized boolean isDuplicate(String messageId) {
        if (!enabled || messageId == null) {
            return false;
        }
        if (seen.containsKey(messageId)) {
            seen.get(messageId);
            hits++;
            return true;
        }
        return false;
    }

    /** 业务处理成功后登记 */
    public synchronized void mark(String messageId) {
        if (enabled && messageId != null) {
            seen.put(messageId, Boolean.TRUE);
        }
    }

    public synchronized long getHits() {
        return hits;
    }

    public synchronized int size() {
        return seen.size();
    }

    public int getCapacity() {
        return enabled ? capacity : 0;
    }
}
