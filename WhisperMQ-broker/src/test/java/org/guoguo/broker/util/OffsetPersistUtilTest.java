package org.guoguo.broker.util;

import org.guoguo.common.config.MqConfigProperties;
import org.guoguo.common.pojo.Entity.ConsumerGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 位点持久化（P0 修复）：单线程串行、单调不回退、writer 复用、重启恢复。
 */
class OffsetPersistUtilTest {

    @TempDir
    Path tempDir;

    private MqConfigProperties config() {
        MqConfigProperties config = new MqConfigProperties();
        config.setOffsetPersistPath(tempDir.toString());
        config.setWindowSizeMillis(600_000);   // 测试期间不要触发高低频切换
        config.setFlushThreshold(1_000_000);   // 保持低频：一条一刷
        config.setFlushIntervalMillis(600_000);
        config.setMaxFileSize(10L * 1024 * 1024);
        return config;
    }

    private ConsumerGroup group(String groupId) {
        ConsumerGroup group = new ConsumerGroup();
        group.setGroupId(groupId);
        return group;
    }

    private List<String> linesOf(String fileName) throws Exception {
        return Files.readAllLines(tempDir.resolve(fileName));
    }

    @Test
    void writesInOrderAndNeverMovesOffsetBackwards() throws Exception {
        OffsetPersistUtil util = new OffsetPersistUtil(config());
        util.init(group("G1"), "T1");

        util.writeMessage("G1", "T1", "100");
        util.writeMessage("G1", "T1", "99");   // 迟到的小位点，必须被丢弃
        util.writeMessage("G1", "T1", "101");
        util.close();                          // 排空写队列并关闭文件

        assertEquals(List.of("G1:T1|100", "G1:T1|101"), linesOf("G1-offset.log"));
    }

    @Test
    void secondTopicReusesWriterAndBothTopicsArePersisted() throws Exception {
        OffsetPersistUtil util = new OffsetPersistUtil(config());
        ConsumerGroup group = group("G2");
        util.init(group, "T1");
        util.init(group, "T2");   // 旧实现这里会重开 writer 并覆盖 map

        util.writeMessage("G2", "T1", "1");
        util.writeMessage("G2", "T2", "2");
        util.close();

        List<String> lines = linesOf("G2-offset.log");
        assertEquals(2, lines.size());
        assertTrue(lines.contains("G2:T1|1"));
        assertTrue(lines.contains("G2:T2|2"));
    }

    @Test
    void recoversLatestOffsetAfterRestart() throws Exception {
        OffsetPersistUtil first = new OffsetPersistUtil(config());
        ConsumerGroup written = group("G3");
        first.init(written, "T1");
        first.writeMessage("G3", "T1", "10");
        first.writeMessage("G3", "T1", "12");
        first.close();

        OffsetPersistUtil second = new OffsetPersistUtil(config());
        ConsumerGroup recovered = group("G3");
        try {
            second.init(recovered, "T1");
            assertEquals("12", recovered.getTopicOffsetMap().get("T1"));
        } finally {
            second.close();
        }
    }

    @Test
    void concurrentWritesProduceNoCorruptedLines() throws Exception {
        OffsetPersistUtil util = new OffsetPersistUtil(config());
        util.init(group("G4"), "T1");

        int threads = 4;
        int perThread = 50;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int t = 0; t < threads; t++) {
            final int base = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        // 故意乱序写入
                        util.writeMessage("G4", "T1", String.valueOf(1000 + ((base * 37 + i * 13) % 400)));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        util.writeMessage("G4", "T1", "9999");
        util.close();

        List<String> lines = linesOf("G4-offset.log");
        assertTrue(lines.size() > 1, "应写入多条位点");
        for (String line : lines) {
            assertTrue(line.matches("G4:T1\\|\\d+"), "位点行被写坏：" + line);
        }
        assertEquals("G4:T1|9999", lines.get(lines.size() - 1), "最后一行必须是最新位点");
    }
}
