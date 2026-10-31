package org.guoguo.broker.util;

import com.alibaba.fastjson2.JSON;
import org.guoguo.common.config.MqConfigProperties;
import org.guoguo.common.constant.DeadType;
import org.guoguo.common.pojo.DTO.DeadLetterDTO;
import org.guoguo.common.pojo.DTO.PushMessageDTO;
import org.guoguo.common.pojo.Entity.DeadLetterQueue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 死信持久化（P0 修复）：写入、竖线解析、重启恢复、重放标记不再加载。
 *
 * <p>注意：{@link DeadLetterPersistUtil} 只负责文件；入内存队列由 BrokerManager 完成，
 * 所以这里不直接断言内存队列有值，而是通过"重新 init 一个实例"验证恢复。</p>
 */
class DeadLetterPersistUtilTest {

    @TempDir
    Path tempDir;

    private MqConfigProperties config() {
        MqConfigProperties config = new MqConfigProperties();
        config.setPersistPath(tempDir.toString());
        config.setMaxFileSize(10L * 1024 * 1024);
        return config;
    }

    private DeadLetterPersistUtil util(DeadLetterQueue queue) {
        DeadLetterPersistUtil util = new DeadLetterPersistUtil(config(), queue);
        util.init();
        return util;
    }

    private DeadLetterDTO deadLetter(String payload) {
        DeadLetterDTO dto = new DeadLetterDTO();
        dto.setMessageId("origin-1");
        dto.setDeadType(DeadType.CONSUME_TIMEOUT);
        dto.setDeadRetryCount(8);

        PushMessageDTO push = new PushMessageDTO();
        push.setConsumerGroup("G1");
        push.setJson("{\"topic\":\"T1\",\"payload\":\"" + payload + "\"}");
        dto.setJson(JSON.toJSONString(push));
        return dto;
    }

    /** 找到当前写入的死信文件 */
    private Path deadLetterFile() throws IOException {
        Path dir = tempDir.resolve("deadletters");
        try (Stream<Path> files = Files.list(dir)) {
            return files.filter(p -> p.getFileName().toString().startsWith("WhisperMQ-deadletter-"))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("没有死信文件"));
        }
    }

    @Test
    void persistedDeadLetterIsRecoveredWithPayloadContainingPipe() throws Exception {
        DeadLetterQueue queue = new DeadLetterQueue();
        DeadLetterPersistUtil util = util(queue);
        DeadLetterDTO dto = deadLetter("a|b|c");   // payload 里带竖线，不能被 split 截断
        util.writeDeadLetter(dto);
        util.close();

        List<String> lines = Files.readAllLines(deadLetterFile());
        assertTrue(lines.get(0).contains("a|b|c"), "payload 中的竖线应被保留：" + lines.get(0));

        DeadLetterQueue recovered = new DeadLetterQueue();
        util(recovered).close();

        assertEquals(1, recovered.size(), "重启后应从文件恢复死信");
        DeadLetterDTO back = recovered.get(dto.getDeadLetterId());
        assertNotNull(back);
        assertEquals(DeadType.CONSUME_TIMEOUT, back.getDeadType());
        assertEquals(8, back.getDeadRetryCount());
        assertNotNull(back.getDeadTime());
        assertTrue(back.getJson().contains("a|b|c"));
    }

    @Test
    void replayedDeadLetterIsNotLoadedAgainAfterRestart() {
        DeadLetterQueue queue = new DeadLetterQueue();
        DeadLetterPersistUtil util = util(queue);
        DeadLetterDTO dto = deadLetter("x");
        util.writeDeadLetter(dto);

        // 重放成功后 Broker 会写这个标记
        util.writeReplayedMarker(dto.getDeadLetterId());
        util.close();

        DeadLetterQueue afterRestart = new DeadLetterQueue();
        util(afterRestart).close();

        assertEquals(0, afterRestart.size(), "已重放的死信不应在重启后再次出现");
    }

    @Test
    void multipleDeadLettersAreAllRecovered() {
        DeadLetterQueue queue = new DeadLetterQueue();
        DeadLetterPersistUtil util = util(queue);
        DeadLetterDTO first = deadLetter("1");
        DeadLetterDTO second = deadLetter("2");
        util.writeDeadLetter(first);
        util.writeDeadLetter(second);
        util.close();

        DeadLetterQueue recovered = new DeadLetterQueue();
        util(recovered).close();

        assertEquals(2, recovered.size());
        assertNotNull(recovered.get(first.getDeadLetterId()));
        assertNotNull(recovered.get(second.getDeadLetterId()));
    }
}
