package org.guoguo.broker.store;

import org.guoguo.common.config.MqConfigProperties;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 分段存储（P1 Stage C）：追加/读取、重启恢复、按 topic 回溯、死信跳过、保留策略。
 */
class SegmentMessageStoreTest {

    @TempDir
    Path temp;

    private MqConfigProperties config() {
        MqConfigProperties config = new MqConfigProperties();
        config.setPersistPath(temp.toString());
        config.setMaxFileSize(2048);
        config.setHotCacheSize(10);
        config.setRetentionMillis(0);
        config.setRetentionMaxBytes(0);
        config.setRetentionCheckIntervalMillis(60_000);
        return config;
    }

    private MqMessage message(String topic, String payload) {
        MqMessage message = new MqMessage();
        message.setTopic(topic);
        message.setTag("TAG");
        message.setPayload(payload);
        return message;
    }

    @Test
    void appendAndReadAfterRestart() {
        SegmentMessageStore store = new SegmentMessageStore(config());
        store.init();
        store.append("100", message("T", "p100"));
        store.append("101", message("T", "p101"));
        assertEquals("p100", store.get("100").getPayload());
        assertEquals(2, store.liveCount());
        store.close();

        SegmentMessageStore reopened = new SegmentMessageStore(config());
        reopened.init();
        try {
            assertEquals("p101", reopened.get("101").getPayload());
            assertEquals(2, reopened.liveCount());
            assertTrue(((Number) reopened.stats().get("bytes")).longValue() > 0,
                    "重启后应统计已有 segment 的字节数");
        } finally {
            reopened.close();
        }
    }

    @Test
    void replayByTopicIsOrderedAndRespectsStartOffset() {
        SegmentMessageStore store = new SegmentMessageStore(config());
        store.init();
        try {
            store.append("1", message("T", "a"));
            store.append("2", message("U", "b"));
            store.append("3", message("T", "c"));

            List<String> ids = new ArrayList<>();
            store.forEachTopicAfter("T", null, stored -> ids.add(stored.messageId()));
            assertEquals(List.of("1", "3"), ids);

            List<String> after = new ArrayList<>();
            store.forEachTopicAfter("T", "1", stored -> after.add(stored.messageId()));
            assertEquals(List.of("3"), after);
        } finally {
            store.close();
        }
    }

    @Test
    void deadMessageIsSkippedAndStaysDeadAfterRestart() {
        SegmentMessageStore store = new SegmentMessageStore(config());
        store.init();
        store.append("10", message("T", "dead"));
        store.append("11", message("T", "alive"));
        assertTrue(store.markDead("10"));
        assertEquals(1, store.liveCount());
        store.close();

        SegmentMessageStore reopened = new SegmentMessageStore(config());
        reopened.init();
        try {
            List<String> ids = new ArrayList<>();
            reopened.forEachTopicAfter("T", null, stored -> ids.add(stored.messageId()));
            assertEquals(List.of("11"), ids);
            assertEquals(1, reopened.liveCount());
        } finally {
            reopened.close();
        }
    }

    @Test
    void retentionDeletesOnlyOldSegments() throws Exception {
        MqConfigProperties config = config();
        config.setRetentionMillis(1);
        SegmentMessageStore store = new SegmentMessageStore(config);
        store.init();
        try {
            for (int i = 0; i < 30; i++) {
                store.append(String.valueOf(1000 + i), message("T", "x".repeat(200)));
            }
            assertTrue(((Number) store.stats().get("segments")).intValue() > 1, "应产生多个 segment");
            int before = store.liveCount();

            Thread.sleep(10);
            store.runRetention();

            assertEquals(1, ((Number) store.stats().get("segments")).intValue(), "只应保留当前 segment");
            assertTrue(store.liveCount() < before, "旧 segment 的消息应被清理");
        } finally {
            store.close();
        }
    }
}
