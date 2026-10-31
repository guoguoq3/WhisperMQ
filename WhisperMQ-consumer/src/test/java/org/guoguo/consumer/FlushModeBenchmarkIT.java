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
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 三档刷盘对比（P0）：ASYNC / INTERVAL / SYNC。
 *
 * <p>每个模式起一个独立的 Broker（不同端口/数据目录），发 1000 条 256B 持久化消息，
 * 只统计生产者同步 send 的吞吐与 P50/P99（不挂消费者，隔离消费处理的影响）。</p>
 *
 * <p>类名以 IT 结尾，默认 mvn test 不执行；显式运行：</p>
 * <pre>mvn -o -pl WhisperMQ-consumer -am test -Dtest=FlushModeBenchmarkIT -DfailIfNoTests=false</pre>
 */
public class FlushModeBenchmarkIT {

    private static final int N = 1000;
    private static final int PAYLOAD_SIZE = 256;
    private static final int BASE_PORT = 18890;

    @Test
    void compareFlushModes() throws Exception {
        List<String> modes = List.of("ASYNC", "INTERVAL", "SYNC");
        for (int index = 0; index < modes.size(); index++) {
            String mode = modes.get(index);
            int port = BASE_PORT + index;
            String dir = Paths.get(System.getProperty("java.io.tmpdir"),
                    "wq-flush-" + mode + "-" + System.nanoTime()).toString();

            ConfigurableApplicationContext broker = new SpringApplicationBuilder(BrokerApplication.class).run(
                    "--whisper-mq.broker-port=" + port,
                    "--whisper-mq.broker-host=127.0.0.1",
                    "--whisper-mq.persist-path=" + dir,
                    "--whisper-mq.offset-persist-path=" + dir + "/offsets",
                    "--whisper-mq.flush-mode=" + mode,
                    "--whisper-mq.store-flush-interval-millis=200",
                    "--whisper-mq.ack-timeout-millis=5000");

            try {
                MqConfigProperties config = new MqConfigProperties();
                config.setBrokerHost("127.0.0.1");
                config.setBrokerPort(port);
                config.setProducerRetryCountLimit(1);
                config.setProducerRetryTimeCoefficient(100);
                config.setHeartbeatIntervalMillis(5000);

                MqProducer producer = new MqProducer(config);
                producer.start();
                try {
                    // 预热，避免把连接建立算进第一批
                    for (int i = 0; i < 50; i++) {
                        producer.send(message("FLUSH_TOPIC", "warmup-" + i));
                    }

                    long[] latencies = new long[N];
                    long start = System.nanoTime();
                    for (int i = 0; i < N; i++) {
                        long begin = System.nanoTime();
                        Result<String> result = producer.send(message("FLUSH_TOPIC", "flush-" + i));
                        latencies[i] = System.nanoTime() - begin;
                        assertEquals(200, result.getCode(), "发送失败：" + result.getMessage());
                    }
                    long elapsed = System.nanoTime() - start;

                    Arrays.sort(latencies);
                    System.out.printf(
                            "BENCH_FLUSH mode=%s messages=%d throughput=%.0f msg/s "
                                    + "sendAvg=%.3fms sendP50=%.3fms sendP99=%.3fms%n",
                            mode, N, N * 1_000_000_000.0 / elapsed,
                            Arrays.stream(latencies).average().orElse(0) / 1_000_000.0,
                            latencies[N / 2] / 1_000_000.0,
                            latencies[(int) (N * 0.99)] / 1_000_000.0);
                } finally {
                    producer.close();
                }
            } finally {
                broker.close();
            }
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
