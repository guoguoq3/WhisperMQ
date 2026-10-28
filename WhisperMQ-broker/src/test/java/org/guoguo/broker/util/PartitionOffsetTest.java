package org.guoguo.broker.util;

import org.guoguo.common.config.MqConfigProperties;
import org.guoguo.common.pojo.Entity.ConsumerGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 分区位点（P1）：内存按分区维护、旧视图兼容、落盘/恢复兼容旧格式。
 */
class PartitionOffsetTest {

    @TempDir
    Path tempDir;

    private MqConfigProperties config() {
        MqConfigProperties config = new MqConfigProperties();
        config.setOffsetPersistPath(tempDir.toString());
        config.setWindowSizeMillis(600_000);
        config.setFlushThreshold(1_000_000);
        config.setFlushIntervalMillis(600_000);
        config.setMaxFileSize(10L * 1024 * 1024);
        return config;
    }

    @Test
    void groupKeepsOffsetsPerPartitionAndLegacyView() {
        ConsumerGroup group = new ConsumerGroup();
        group.setGroupId("G");

        group.putOffset("T", 0, "100");
        group.putOffset("T", 1, "200");

        assertEquals("100", group.getOffset("T", 0));
        assertEquals("200", group.getOffset("T", 1));
        assertTrue(group.hasOffset("T"));
        // 旧视图（分区 0）仍然可读
        assertEquals("100", group.getTopicOffsetMap().get("T"));
        assertTrue(group.offsetView().containsKey("T"));
        assertTrue(group.offsetView().containsKey("T#1"));
    }

    @Test
    void offsetsSurviveRestartPerPartition() throws Exception {
        MqConfigProperties config = config();
        OffsetPersistUtil first = new OffsetPersistUtil(config);
        ConsumerGroup group = new ConsumerGroup();
        group.setGroupId("G1");
        first.init(group, "T");
        first.writeMessage("G1", "T", 0, "10");
        first.writeMessage("G1", "T", 1, "20");
        first.close();

        List<String> lines = Files.readAllLines(tempDir.resolve("G1-offset.log"));
        assertTrue(lines.contains("G1:T|10"), "分区 0 应使用旧格式");
        assertTrue(lines.contains("G1:T#1|20"), "分区 1 应使用 #partition 扩展格式");

        OffsetPersistUtil second = new OffsetPersistUtil(config);
        ConsumerGroup recovered = new ConsumerGroup();
        recovered.setGroupId("G1");
        second.init(recovered, "T");
        try {
            assertEquals("10", recovered.getOffset("T", 0));
            assertEquals("20", recovered.getOffset("T", 1));
        } finally {
            second.close();
        }
    }

    @Test
    void legacyOffsetFileStillLoads() throws Exception {
        Files.writeString(tempDir.resolve("G2-offset.log"), "G2:T|42\n");

        OffsetPersistUtil util = new OffsetPersistUtil(config());
        ConsumerGroup group = new ConsumerGroup();
        group.setGroupId("G2");
        util.init(group, "T");
        try {
            assertEquals("42", group.getOffset("T", 0));
            assertEquals("42", group.getTopicOffsetMap().get("T"));
        } finally {
            util.close();
        }
    }

    @Test
    void topicPrefixDoesNotMatchLongerTopicNames() throws Exception {
        Files.writeString(tempDir.resolve("G3-offset.log"), "G3:T|1\nG3:T2|2\n");

        OffsetPersistUtil util = new OffsetPersistUtil(config());
        ConsumerGroup group = new ConsumerGroup();
        group.setGroupId("G3");
        util.init(group, "T");
        try {
            assertEquals("1", group.getOffset("T", 0));
            // 不应把 G3:T2 的位点误读成 T 的位点
            assertEquals("1", group.getTopicOffsetMap().get("T"));
        } finally {
            util.close();
        }
    }
}
