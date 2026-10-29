package org.guoguo.consumer;

import com.alibaba.fastjson.JSON;
import org.guoguo.broker.BrokerApplication;
import org.guoguo.broker.ConsumerGroup.ConsumerGroupManager;
import org.guoguo.broker.core.TopicMetadata;
import org.guoguo.common.config.MqConfigProperties;
import org.guoguo.common.constant.MethodType;
import org.guoguo.common.pojo.DTO.DeadLetterDTO;
import org.guoguo.common.pojo.DTO.RpcMessageDTO;
import org.guoguo.common.pojo.DTO.SubscribeReqDTO;
import org.guoguo.common.pojo.Entity.ConsumerGroup;
import org.guoguo.common.pojo.Entity.DeadLetterQueue;
import org.guoguo.common.pojo.Entity.ErrorResponse;
import org.guoguo.common.pojo.Entity.MqMessageEnduring;
import org.guoguo.consumer.service.IMessageListener;
import org.guoguo.consumer.service.impl.MqConsumerManager;
import org.guoguo.producer.pojo.Result;
import org.guoguo.producer.service.impl.MqProducer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P0 改造的端到端冒烟测试：进程内启动真实 Broker（验证非阻塞启动），
 * 再用真实的 Producer/Consumer 客户端验证投递、ACK、重投、死信、重放、错误响应、断线重连。
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SmokeIT {

    private static final int PORT = 19999;
    private static String dataDir;
    private static MqConfigProperties clientConfig;
    private static ConfigurableApplicationContext broker;

    @BeforeAll
    static void startBroker() {
        dataDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "wq-smoke-" + System.nanoTime()).toString();
        broker = launchBroker();

        clientConfig = new MqConfigProperties();
        clientConfig.setBrokerHost("127.0.0.1");
        clientConfig.setBrokerPort(PORT);
        clientConfig.setProducerRetryCountLimit(2);
        clientConfig.setProducerRetryTimeCoefficient(200);
        clientConfig.setHeartbeatIntervalMillis(2000);
        clientConfig.setReconnectInitialDelayMillis(200);
        clientConfig.setReconnectMaxDelayMillis(1000);
        clientConfig.setAckTimeoutMillis(800);
        clientConfig.setMaxDeliveryAttempts(2);
        clientConfig.setMaxDeadLetterRetryCount(2);
        clientConfig.setPersistPath(dataDir);
        clientConfig.setOffsetPersistPath(dataDir + "/offsets");
    }

    private static ConfigurableApplicationContext launchBroker() {
        return new SpringApplicationBuilder(BrokerApplication.class).run(
                "--whisper-mq.broker-port=" + PORT,
                "--whisper-mq.broker-host=127.0.0.1",
                "--whisper-mq.persist-path=" + dataDir,
                "--whisper-mq.offset-persist-path=" + dataDir + "/offsets",
                "--whisper-mq.ack-timeout-millis=800",
                "--whisper-mq.max-delivery-attempts=2",
                "--whisper-mq.max-dead-letter-retry-count=2");
    }

    @AfterAll
    static void stopBroker() {
        if (broker != null) {
            broker.close();
        }
    }

    // ---------------------------------------------------------------- helpers

    private static MqConsumerManager newConsumer(String groupId) {
        MqConsumerManager consumer = new MqConsumerManager(clientConfig);
        consumer.start();
        consumer.joinGroup(groupId);
        return consumer;
    }

    private static void subscribe(MqConsumerManager consumer, String topic, IMessageListener listener) {
        subscribe(consumer, topic, List.of("*"), listener);
    }

    private static void subscribe(MqConsumerManager consumer, String topic, List<String> tags,
                                  IMessageListener listener) {
        SubscribeReqDTO req = new SubscribeReqDTO();
        req.setTopic(topic);
        req.setTags(tags);
        consumer.groupSubscribe(req, listener);
    }

    private static MqMessageEnduring tagged(String topic, String tag, String payload) {
        MqMessageEnduring message = new MqMessageEnduring();
        message.setTopic(topic);
        message.setTag(tag);
        message.setPayload(payload);
        return message;
    }

    private static MqMessageEnduring message(String topic, String payload) {
        MqMessageEnduring message = new MqMessageEnduring();
        message.setTopic(topic);
        message.setTag("TAG1");
        message.setPayload(payload);
        return message;
    }

    private static String offsetOf(String groupId, String topic) {
        ConsumerGroupManager manager = broker.getBean(ConsumerGroupManager.class);
        var group = manager.getGroupMap().get(groupId);
        return group == null ? null : group.getTopicOffsetMap().get(topic);
    }

    private static void awaitTrue(java.util.function.BooleanSupplier condition, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        assertTrue(condition.getAsBoolean(), "等待条件超时（" + timeoutMillis + "ms）");
    }

    // ------------------------------------------------------------------ tests

    @Test
    @Order(1)
    void brokerStartsWithoutBlockingSpringContext() {
        // 旧实现把 closeFuture().sync() 放在 @PostConstruct，run() 永远不返回，这个断言会卡死
        assertTrue(broker.isActive(), "Broker 容器应完成 refresh");
        assertNotNull(broker.getBean(ConsumerGroupManager.class));
    }

    @Test
    @Order(2)
    void sendConsumeAckAdvancesOffset() throws Exception {
        String group = "SMOKE_BASIC";
        String topic = "SMOKE_TOPIC_BASIC";
        MqConsumerManager consumer = newConsumer(group);
        CountDownLatch latch = new CountDownLatch(3);
        subscribe(consumer, topic, (message, messageId) -> {
            latch.countDown();
            return true;
        });

        MqProducer producer = new MqProducer(clientConfig);
        producer.start();
        String lastId = null;
        for (int i = 0; i < 3; i++) {
            Result<String> result = producer.send(message(topic, "hello-" + i));
            assertEquals(200, result.getCode(), "发送应成功：" + result.getMessage());
            lastId = result.getMessageId();
        }
        assertTrue(latch.await(10, TimeUnit.SECONDS), "3 条消息应在 10s 内被消费");

        String expectedId = lastId;
        awaitTrue(() -> expectedId.equals(offsetOf(group, topic)), 5000);

        producer.close();
        consumer.close();
    }

    @Test
    @Order(3)
    void ackFailTriggersImmediateRedelivery() throws Exception {
        String group = "SMOKE_ACK_FAIL";
        String topic = "SMOKE_TOPIC_ACK_FAIL";
        MqConsumerManager consumer = newConsumer(group);
        AtomicInteger deliveries = new AtomicInteger();
        CountDownLatch secondDelivery = new CountDownLatch(1);
        subscribe(consumer, topic, (message, messageId) -> {
            if (deliveries.incrementAndGet() == 1) {
                return false; // 第一次失败 -> ACK_FAIL -> Broker 立即重投
            }
            secondDelivery.countDown();
            return true;
        });

        MqProducer producer = new MqProducer(clientConfig);
        producer.start();
        assertEquals(200, producer.send(message(topic, "retry-me")).getCode());
        assertTrue(secondDelivery.await(10, TimeUnit.SECONDS), "ACK_FAIL 后应收到第二次投递");
        assertEquals(2, deliveries.get());

        producer.close();
        consumer.close();
    }

    @Test
    @Order(4)
    void exhaustedDeliveryMovesMessageToDeadLetterAndReplayWorks() throws Exception {
        String group = "SMOKE_DEAD";
        String topic = "SMOKE_TOPIC_DEAD";
        MqConsumerManager consumer = newConsumer(group);
        AtomicInteger deliveries = new AtomicInteger();
        CountDownLatch replayed = new CountDownLatch(1);
        subscribe(consumer, topic, (message, messageId) -> {
            int count = deliveries.incrementAndGet();
            // 前两次都失败，让 Broker 达到最大投递次数并转入死信
            if (count <= 2) {
                return false;
            }
            // 重放后的投递成功
            replayed.countDown();
            return true;
        });

        MqProducer producer = new MqProducer(clientConfig);
        producer.start();
        Result<String> result = producer.send(message(topic, "will-be-dead"));
        assertEquals(200, result.getCode());

        DeadLetterQueue deadLetterQueue = broker.getBean(DeadLetterQueue.class);
        awaitTrue(() -> deadLetterQueue.size() >= 1, 10000);

        DeadLetterDTO deadLetter = deadLetterQueue.list().get(0);
        awaitTrue(() -> result.getMessageId().equals(offsetOf(group, topic)), 5000);

        // 重放：Broker 重新投递给原组，第 3 次投递应成功
        consumer.replayDeadLetter(deadLetter.getDeadLetterId());
        assertTrue(replayed.await(10, TimeUnit.SECONDS), "死信重放后应再次收到消息");
        assertEquals(3, deliveries.get());
        assertEquals(0, deadLetterQueue.size(), "重放后死信队列应清空");

        producer.close();
        consumer.close();
    }

    @Test
    @Order(5)
    void brokerReturnsStructuredErrorResponse() throws Exception {
        try (Socket socket = new Socket("127.0.0.1", PORT)) {
            socket.setSoTimeout(5000);
            OutputStream out = socket.getOutputStream();
            out.write("{this-is-not-json}\n".getBytes(StandardCharsets.UTF_8));
            out.flush();

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            String line = reader.readLine();
            assertNotNull(line, "Broker 应返回错误响应而不是静默丢弃");
            RpcMessageDTO response = JSON.parseObject(line, RpcMessageDTO.class);
            assertEquals(MethodType.ERROR, response.getMethodType());
            ErrorResponse error = JSON.parseObject(response.getJson(), ErrorResponse.class);
            assertNotNull(error);
            assertNotNull(error.getJson(), "错误原因应可解析");
        }
    }

    @Test
    @Order(6)
    void clientReconnectsAndConsumerResubscribesAfterBrokerRestart() throws Exception {
        String group = "SMOKE_RECONNECT";
        String topic = "SMOKE_TOPIC_RECONNECT";
        MqConsumerManager consumer = newConsumer(group);
        CountDownLatch latch = new CountDownLatch(1);
        subscribe(consumer, topic, (message, messageId) -> {
            latch.countDown();
            return true;
        });
        MqProducer producer = new MqProducer(clientConfig);
        producer.start();

        // 重启 Broker：模拟真实故障
        broker.close();
        Thread.sleep(300);
        dataDir = dataDir; // 复用同一数据目录
        broker = launchBroker();

        // 客户端应自动重连，消费者应自动重新入组 + 重新订阅
        long deadline = System.currentTimeMillis() + 15000;
        Result<String> result = null;
        while (System.currentTimeMillis() < deadline && latch.getCount() > 0) {
            result = producer.send(message(topic, "after-restart"));
            if (latch.await(1000, TimeUnit.MILLISECONDS)) {
                break;
            }
        }
        assertTrue(latch.getCount() == 0, "Broker 重启后消息应最终被消费，最后一次发送结果："
                + (result == null ? "null" : result.getCode() + "/" + result.getMessage()));

        producer.close();
        consumer.close();
    }

    @Test
    @Order(7)
    void subscribeReturnsOnlyAfterBrokerAck() {
        String group = "SMOKE_SUB_ACK";
        String topic = "SMOKE_TOPIC_SUB_ACK";
        MqConsumerManager consumer = newConsumer(group);
        subscribe(consumer, topic, List.of("*"), (message, messageId) -> true);

        // groupSubscribe 返回即代表 Broker 已处理完订阅（旧实现是 fire-and-forget，这里可能还是 null）
        ConsumerGroup groupEntity = broker.getBean(ConsumerGroupManager.class).getGroupMap().get(group);
        assertNotNull(groupEntity);
        assertTrue(groupEntity.getSubscribeMap().containsKey(topic), "订阅确认返回时 Broker 应已记录订阅");
        assertFalse(groupEntity.getTopicConsumerTags().get(topic).isEmpty(), "消费者级 Tag 应已登记");
        consumer.close();
    }

    @Test
    @Order(8)
    void perConsumerTagsAreIsolatedWithinOneGroup() throws Exception {
        String group = "SMOKE_TAG_GROUP";
        String topic = "SMOKE_TOPIC_TAG";
        MqConsumerManager consumerA = newConsumer(group);
        AtomicInteger countA = new AtomicInteger();
        CountDownLatch receivedA = new CountDownLatch(1);
        subscribe(consumerA, topic, List.of("TAG_A"), (message, messageId) -> {
            countA.incrementAndGet();
            receivedA.countDown();
            return true;
        });

        MqConsumerManager consumerB = newConsumer(group);
        AtomicInteger countB = new AtomicInteger();
        CountDownLatch receivedB = new CountDownLatch(1);
        subscribe(consumerB, topic, List.of("TAG_B"), (message, messageId) -> {
            countB.incrementAndGet();
            receivedB.countDown();
            return true;
        });

        MqProducer producer = new MqProducer(clientConfig);
        producer.start();
        assertEquals(200, producer.send(tagged(topic, "TAG_A", "for-a")).getCode());
        assertEquals(200, producer.send(tagged(topic, "TAG_B", "for-b")).getCode());

        assertTrue(receivedA.await(10, TimeUnit.SECONDS), "订阅 TAG_A 的消费者应收到 TAG_A 消息");
        assertTrue(receivedB.await(10, TimeUnit.SECONDS), "订阅 TAG_B 的消费者应收到 TAG_B 消息");
        Thread.sleep(500);
        assertEquals(1, countA.get(), "TAG_A 消费者不应收到 TAG_B 消息");
        assertEquals(1, countB.get(), "TAG_B 消费者不应收到 TAG_A 消息");

        producer.close();
        consumerA.close();
        consumerB.close();
    }

    @Test
    @Order(9)
    void listenerRunsOnBusinessThreadInsteadOfIoThread() throws Exception {
        String group = "SMOKE_THREAD";
        String topic = "SMOKE_TOPIC_THREAD";
        MqConsumerManager consumer = newConsumer(group);
        AtomicReference<String> threadName = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        subscribe(consumer, topic, List.of("*"), (message, messageId) -> {
            threadName.set(Thread.currentThread().getName());
            latch.countDown();
            return true;
        });

        MqProducer producer = new MqProducer(clientConfig);
        producer.start();
        assertEquals(200, producer.send(message(topic, "x")).getCode());
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertTrue(threadName.get().startsWith("mq-consumer-worker-"),
                "监听器应运行在业务线程，实际：" + threadName.get());

        producer.close();
        consumer.close();
    }

    @Test
    @Order(10)
    void unsubscribeActuallyStopsDelivery() throws Exception {
        String group = "SMOKE_UNSUB";
        String topic = "SMOKE_TOPIC_UNSUB";
        MqConsumerManager consumer = newConsumer(group);
        AtomicInteger deliveries = new AtomicInteger();
        subscribe(consumer, topic, List.of("*"), (message, messageId) -> {
            deliveries.incrementAndGet();
            return true;
        });

        SubscribeReqDTO req = new SubscribeReqDTO();
        req.setTopic(topic);
        req.setTags(List.of("*"));
        consumer.groupUnsubscribe(req);

        ConsumerGroup groupEntity = broker.getBean(ConsumerGroupManager.class).getGroupMap().get(group);
        assertNotNull(groupEntity);
        assertFalse(groupEntity.getSubscribeMap().containsKey(topic), "退订后 Broker 不应再保留该订阅");

        MqProducer producer = new MqProducer(clientConfig);
        producer.start();
        assertEquals(200, producer.send(message(topic, "after-unsub")).getCode());
        Thread.sleep(800);
        assertEquals(0, deliveries.get(), "退订后不应再收到消息（旧实现退订请求字段被包错层，永远无效）");

        producer.close();
        consumer.close();
    }

    @Test
    @Order(11)
    void partitionedTopicKeepsOrderAndDoesNotLoseMessagesAcrossRebalance() throws Exception {
        String group = "SMOKE_PARTITION_GROUP";
        String topic = "SMOKE_PARTITION_TOPIC";
        TopicMetadata metadata = broker.getBean(TopicMetadata.class);
        metadata.setPartitions(topic, 2);

        // 分开挑出落在两个分区的 bizKey（分区只由 bizKey 决定）
        List<String> keysP0 = new ArrayList<>();
        List<String> keysP1 = new ArrayList<>();
        for (int i = 0; keysP0.size() < 5 || keysP1.size() < 5; i++) {
            String key = "pkey-" + i;
            int partition = metadata.partitionOf(topic, key, "0", 0);
            if (partition == 0 && keysP0.size() < 5) {
                keysP0.add(key);
            } else if (partition == 1 && keysP1.size() < 5) {
                keysP1.add(key);
            }
        }

        Map<String, Integer> receivedTimes = new ConcurrentHashMap<>();
        Map<String, String> consumerOfPayload = new ConcurrentHashMap<>();
        Map<String, List<String>> orderByConsumer = new ConcurrentHashMap<>();
        Map<String, Integer> partitionOfPayload = new ConcurrentHashMap<>();
        AtomicInteger sequence = new AtomicInteger();

        MqConsumerManager consumerA = newConsumer(group);
        subscribe(consumerA, topic, List.of("*"),
                partitionRecorder("a", receivedTimes, consumerOfPayload, orderByConsumer));

        MqProducer producer = new MqProducer(clientConfig);
        producer.start();
        sendPartitioned(producer, metadata, topic, keysP0, keysP1, partitionOfPayload, sequence);

        // 第二个消费者加入并订阅 -> 触发再均衡，两个消费者各分担一个分区
        MqConsumerManager consumerB = newConsumer(group);
        subscribe(consumerB, topic, List.of("*"),
                partitionRecorder("b", receivedTimes, consumerOfPayload, orderByConsumer));
        sendPartitioned(producer, metadata, topic, keysP0, keysP1, partitionOfPayload, sequence);

        awaitTrue(() -> receivedTimes.size() == 20, 20_000);
        Thread.sleep(300);

        assertEquals(20, receivedTimes.size(), "20 条消息都应被消费");
        receivedTimes.forEach((payload, times) -> assertEquals(1, times, "消息被重复消费：" + payload));
        assertTrue(consumerOfPayload.containsValue("b"), "再均衡后新消费者应接管部分分区");

        for (Map.Entry<String, List<String>> entry : orderByConsumer.entrySet()) {
            List<String> order;
            synchronized (entry.getValue()) {
                order = new ArrayList<>(entry.getValue());
            }
            Map<Integer, Integer> lastIndexByPartition = new HashMap<>();
            for (String payload : order) {
                int index = Integer.parseInt(payload.substring(1));
                int partition = partitionOfPayload.get(payload);
                Integer previous = lastIndexByPartition.put(partition, index);
                if (previous != null) {
                    assertTrue(index > previous, "消费者 " + entry.getKey() + " 在分区 " + partition
                            + " 上的顺序错乱：" + previous + " -> " + index);
                }
            }
        }

        producer.close();
        consumerA.close();
        consumerB.close();
    }

    private static IMessageListener partitionRecorder(String consumerId,
                                                      Map<String, Integer> receivedTimes,
                                                      Map<String, String> consumerOfPayload,
                                                      Map<String, List<String>> orderByConsumer) {
        return (message, messageId) -> {
            String payload = message.getPayload();
            receivedTimes.merge(payload, 1, Integer::sum);
            consumerOfPayload.put(payload, consumerId);
            orderByConsumer.computeIfAbsent(consumerId, k -> Collections.synchronizedList(new ArrayList<>()))
                    .add(payload);
            return true;
        };
    }

    private static void sendPartitioned(MqProducer producer, TopicMetadata metadata, String topic,
                                        List<String> keysP0, List<String> keysP1,
                                        Map<String, Integer> partitionOfPayload, AtomicInteger sequence) {
        List<String> keys = new ArrayList<>(keysP0);
        keys.addAll(keysP1);
        for (String bizKey : keys) {
            String payload = "m" + sequence.getAndIncrement();
            MqMessageEnduring message = new MqMessageEnduring();
            message.setTopic(topic);
            message.setTag("T");
            message.setBizKey(bizKey);
            message.setPayload(payload);
            Result<String> result = producer.send(message);
            assertEquals(200, result.getCode(), "发送失败：" + result.getMessage());
            partitionOfPayload.put(payload, metadata.partitionOf(topic, bizKey, result.getMessageId(), 0));
        }
    }

    /** 供本地排查用：打印临时数据目录 */
    @SuppressWarnings("unused")
    private static Path dataPath() {
        return Paths.get(dataDir);
    }
}
