package org.guoguo.consumer;

import org.guoguo.broker.BrokerApplication;
import org.guoguo.common.config.MqConfigProperties;
import org.guoguo.common.pojo.DTO.SubscribeReqDTO;
import org.guoguo.common.pojo.Entity.MqMessageEnduring;
import org.guoguo.consumer.service.impl.MqConsumerManager;
import org.guoguo.producer.pojo.Result;
import org.guoguo.producer.service.impl.MqProducer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 单机性能基准（P1 Stage D 之后）。
 *
 * <p>不是稳定性测试，只用于给出"本机大致性能"：2000 条 256B 持久化消息，
 * 统计生产者同步 send 的 p50/p99 延迟、发送吞吐与端到端消费吞吐。</p>
 *
 * <p>类名以 IT 结尾，默认 {@code mvn test} 不会执行；需要显式运行：</p>
 * <pre>mvn -o -pl WhisperMQ-consumer -am test -Dtest=BenchmarkIT -DfailIfNoTests=false</pre>
 */
public class BenchmarkIT {

    private static final int PORT = 18888;
    private static final int N = 2000;
    private static final int PAYLOAD_SIZE = 256;
    private static String dataDir;
    private static MqConfigProperties clientConfig;
    private static ConfigurableApplicationContext broker;

    @BeforeAll
    static void startBroker() {
        dataDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "wq-bench-" + System.nanoTime()).toString();
        broker = new SpringApplicationBuilder(BrokerApplication.class).run(
                "--whisper-mq.broker-port=" + PORT,
                "--whisper-mq.broker-host=127.0.0.1",
                "--whisper-mq.persist-path=" + dataDir,
                "--whisper-mq.offset-persist-path=" + dataDir + "/offsets",
                "--whisper-mq.ack-timeout-millis=2000",
                "--whisper-mq.max-delivery-attempts=3",
                "--whisper-mq.hot-cache-size=1000");

        clientConfig = new MqConfigProperties();
        clientConfig.setBrokerHost("127.0.0.1");
        clientConfig.setBrokerPort(PORT);
        clientConfig.setProducerRetryCountLimit(2);
        clientConfig.setProducerRetryTimeCoefficient(100);
        clientConfig.setHeartbeatIntervalMillis(5000);
        clientConfig.setReconnectInitialDelayMillis(200);
        clientConfig.setReconnectMaxDelayMillis(1000);
        clientConfig.setAckTimeoutMillis(2000);
        clientConfig.setPersistPath(dataDir);
        clientConfig.setOffsetPersistPath(dataDir + "/offsets");
    }

    @AfterAll
    static void stopBroker() {
        if (broker != null) {
            broker.close();
        }
    }

    @Test
    void measureThroughputAndLatency() throws Exception {
        String topic = "BENCH_TOPIC";
        String group = "BENCH_GROUP";

        CountDownLatch received = new CountDownLatch(N);
        MqConsumerManager consumer = new MqConsumerManager(clientConfig);
        consumer.start();
        consumer.joinGroup(group);
        SubscribeReqDTO subscribe = new SubscribeReqDTO();
        subscribe.setTopic(topic);
        subscribe.setTags(List.of("*"));
        consumer.groupSubscribe(subscribe, (message, messageId) -> {
            received.countDown();
            return true;
        });

        MqProducer producer = new MqProducer(clientConfig);
        producer.start();

        // 预热：让连接、线程池、存储段进入稳定状态
        for (int i = 0; i < 100; i++) {
            producer.send(message(topic, "warmup-" + i));
        }
        received.await(10, TimeUnit.SECONDS);

        long[] latencies = new long[N];
        long sendStart = System.nanoTime();
        for (int i = 0; i < N; i++) {
            long start = System.nanoTime();
            Result<String> result = producer.send(message(topic, "bench-" + i));
            latencies[i] = System.nanoTime() - start;
            assertEquals(200, result.getCode(), "发送失败：" + result.getMessage());
        }
        long sendElapsed = System.nanoTime() - sendStart;

        assertTrue(received.await(60, TimeUnit.SECONDS), "端到端消费超时");
        long e2eElapsed = System.nanoTime() - sendStart;

        Arrays.sort(latencies);
        double avgMs = Arrays.stream(latencies).average().orElse(0) / 1_000_000.0;
        double p50Ms = latencies[N / 2] / 1_000_000.0;
        double p99Ms = latencies[(int) (N * 0.99)] / 1_000_000.0;

        System.out.printf(
                "BENCH messages=%d payload=%dB producerThroughput=%.0f msg/s "
                        + "sendAvg=%.3fms sendP50=%.3fms sendP99=%.3fms e2eThroughput=%.0f msg/s%n",
                N, PAYLOAD_SIZE,
                N * 1_000_000_000.0 / sendElapsed,
                avgMs, p50Ms, p99Ms,
                N * 1_000_000_000.0 / e2eElapsed);

        producer.close();
        consumer.close();
    }

    private static MqMessageEnduring message(String topic, String id) {
        MqMessageEnduring message = new MqMessageEnduring();
        message.setTopic(topic);
        message.setTag("TAG1");
        message.setPayload(id + "-" + "x".repeat(PAYLOAD_SIZE));
        return message;
    }
}
