package org.guoguo.consumer;

import org.guoguo.broker.BrokerApplication;
import org.guoguo.common.config.MqConfigProperties;
import org.guoguo.common.pojo.Entity.MqMessageEnduring;
import org.guoguo.producer.pojo.Result;
import org.guoguo.producer.service.impl.MqProducer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 单条 vs 批量发送对比（P1）。
 *
 * <p>不挂消费者，只测生产者确认路径：2000 条 256B，单条 send vs 100 条/批的 sendBatch。</p>
 *
 * <p>类名以 IT 结尾，默认 mvn test 不执行；显式运行：</p>
 * <pre>mvn -o -pl WhisperMQ-consumer -am test -Dtest=BatchBenchmarkIT -DfailIfNoTests=false</pre>
 */
public class BatchBenchmarkIT {

    private static final int PORT = 18899;
    private static final int TOTAL = 2000;
    private static final int BATCH_SIZE = 100;
    private static final int PAYLOAD_SIZE = 256;

    @Test
    void compareSingleAndBatch() throws Exception {
        String dir = Paths.get(System.getProperty("java.io.tmpdir"),
                "wq-batch-bench-" + System.nanoTime()).toString();
        ConfigurableApplicationContext broker = new SpringApplicationBuilder(BrokerApplication.class).run(
                "--whisper-mq.broker-port=" + PORT,
                "--whisper-mq.broker-host=127.0.0.1",
                "--whisper-mq.persist-path=" + dir,
                "--whisper-mq.offset-persist-path=" + dir + "/offsets",
                "--whisper-mq.flush-mode=ASYNC");
        try {
            MqConfigProperties config = new MqConfigProperties();
            config.setBrokerHost("127.0.0.1");
            config.setBrokerPort(PORT);
            config.setProducerRetryCountLimit(1);
            config.setProducerRetryTimeCoefficient(100);

            MqProducer producer = new MqProducer(config);
            producer.start();
            try {
                for (int i = 0; i < 100; i++) {
                    producer.send(message("BATCH_BENCH_TOPIC", "warmup-" + i));
                }

                long singleStart = System.nanoTime();
                for (int i = 0; i < TOTAL; i++) {
                    Result<String> result = producer.send(message("BATCH_BENCH_TOPIC", "single-" + i));
                    assertEquals(200, result.getCode(), "单条发送失败：" + result.getMessage());
                }
                long singleElapsed = System.nanoTime() - singleStart;

                long batchStart = System.nanoTime();
                for (int offset = 0; offset < TOTAL; offset += BATCH_SIZE) {
                    List<MqMessageEnduring> messages = new ArrayList<>(BATCH_SIZE);
                    for (int i = 0; i < BATCH_SIZE; i++) {
                        messages.add(message("BATCH_BENCH_TOPIC", "batch-" + (offset + i)));
                    }
                    Result<String> result = producer.sendBatch(messages);
                    assertEquals(200, result.getCode(), "批量发送失败：" + result.getMessage());
                }
                long batchElapsed = System.nanoTime() - batchStart;

                double singleThroughput = TOTAL * 1_000_000_000.0 / singleElapsed;
                double batchThroughput = TOTAL * 1_000_000_000.0 / batchElapsed;
                System.out.printf(
                        "BENCH_BATCH total=%d payload=%dB singleThroughput=%.0f msg/s "
                                + "batchThroughput=%.0f msg/s speedup=%.2fx%n",
                        TOTAL, PAYLOAD_SIZE, singleThroughput, batchThroughput,
                        batchThroughput / singleThroughput);
            } finally {
                producer.close();
            }
        } finally {
            broker.close();
        }
    }

    private static MqMessageEnduring message(String topic, String id) {
        MqMessageEnduring message = new MqMessageEnduring();
        message.setTopic(topic);
        message.setTag("TAG1");
        message.setPayload(id + "-" + "x".repeat(PAYLOAD_SIZE));
        message.setEnduring(true);
        return message;
    }
}
