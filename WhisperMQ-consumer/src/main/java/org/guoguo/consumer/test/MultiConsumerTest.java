package org.guoguo.consumer.test;

import lombok.extern.slf4j.Slf4j;
import org.guoguo.common.pojo.DTO.SubscribeReqDTO;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.guoguo.common.util.SnowflakeIdGeneratorUtil;
import org.guoguo.consumer.service.IMessageListener;
import org.guoguo.consumer.service.impl.MqConsumerManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 多消费者测试：覆盖同组负载均衡、不同组广播消费场景
 */
@Slf4j
@SpringBootTest(classes = org.guoguo.consumer.ConsumerApplication.class)
@TestPropertySource(locations = "classpath:application.properties")
public class MultiConsumerTest {

    // 注：MqConsumer是抽象类，需注入其实现类MqConsumerManager（Spring无法直接注入抽象类）
    @Autowired
    private MqConsumerManager consumer1; // 消费者1
    @Autowired
    private MqConsumerManager consumer2; // 消费者2
    @Autowired
    private MqConsumerManager consumer3; // 消费者3

    // 阻塞等待器：控制测试时长（避免进程提前退出）
    private final CountDownLatch globalLatch = new CountDownLatch(1);


    /**
     * 场景1：同组多消费者（负载均衡）
     * - 2个消费者加入同一组 ORDER_GROUP，订阅同一主题 TEST_TOPIC
     * - 预期：消息轮询分配给2个消费者，同一消息仅1个消费者接收
     */
    @Test
    public void testSameGroupLoadBalance() throws InterruptedException {
        log.info("=== 【同组多消费者测试】开始 ===");

        // -------------------------- 消费者1：加入ORDER_GROUP，订阅TEST_TOPIC-TAG1 --------------------------
        consumer1.joinGroup("ORDER_GROUP");
        SubscribeReqDTO req1 = new SubscribeReqDTO();
        req1.setTopic("TEST_TOPIC");
        req1.setTags(Arrays.asList("TAG1"));
        req1.setGroupId("ORDER_GROUP");
        consumer1.groupSubscribe(req1, new IMessageListener() {
            @Override
            public boolean onMessage(MqMessage message) {
                log.info("【同组-消费者1】收到消息 | 组：ORDER_GROUP | 主题：{} | 内容：{} | 消息ID：{}",
                        message.getTopic(), message.getPayload(), message.getTags());
                return true; // 处理成功，发送ACK
            }
        });
        log.info("【同组-消费者1】初始化完成，等待消息...");

        // -------------------------- 消费者2：加入ORDER_GROUP，订阅TEST_TOPIC-TAG1 --------------------------
        consumer2.joinGroup("ORDER_GROUP");
        SubscribeReqDTO req2 = new SubscribeReqDTO();
        req2.setTopic("TEST_TOPIC");
        req2.setTags(Arrays.asList("TAG1"));
        req2.setGroupId("ORDER_GROUP");
        consumer2.groupSubscribe(req2, new IMessageListener() {
            @Override
            public boolean onMessage(MqMessage message) {
                log.info("【同组-消费者2】收到消息 | 组：ORDER_GROUP | 主题：{} | 内容：{} | 消息ID：{}",
                        message.getTopic(), message.getPayload(), message.getTags());
                return true; // 处理成功，发送ACK
            }
        });
        log.info("【同组-消费者2】初始化完成，等待消息...");

        // 阻塞测试进程（10分钟，足够观察多消息分配情况）
        globalLatch.await(10, TimeUnit.MINUTES);
        log.info("=== 【同组多消费者测试】结束 ===");
    }


    /**
     * 场景2：不同组多消费者（广播消费）
     * - 消费者1加入ORDER_GROUP，消费者2加入PAY_GROUP，订阅同一主题 TEST_TOPIC
     * - 预期：同一消息会广播给两个组，每个组各有一个消费者接收
     */
    @Test
    public void testDifferentGroupBroadcast() throws InterruptedException {
        log.info("=== 【不同组多消费者测试】开始 ===");

        // -------------------------- 消费者1：加入ORDER_GROUP，订阅TEST_TOPIC-TAG1 --------------------------
        consumer1.joinGroup("ORDER_GROUP");
        SubscribeReqDTO req1 = new SubscribeReqDTO();
        req1.setTopic("TEST_TOPIC");
        req1.setTags(Arrays.asList("TAG1"));
        consumer1.groupSubscribe(req1, new IMessageListener() {
            @Override
            public boolean onMessage(MqMessage message) {
                log.info("【不同组-消费者1】收到消息 | 组：ORDER_GROUP | 主题：{} | 内容：{} | 消息ID：{}",
                        message.getTopic(), message.getPayload(), message.getTags());
                return true;
            }
        });
        log.info("【不同组-消费者1】初始化完成，等待消息...");

        // -------------------------- 消费者2：加入PAY_GROUP，订阅TEST_TOPIC-TAG1 --------------------------
        consumer2.joinGroup("PAY_GROUP");
        SubscribeReqDTO req2 = new SubscribeReqDTO();
        req2.setTags(Arrays.asList("TAG1"));
        consumer2.groupSubscribe(req2, new IMessageListener() {
            @Override
            public boolean onMessage(MqMessage message) {
                log.info("【不同组-消费者2】收到消息 | 组：PAY_GROUP | 主题：{} | 内容：{} | 消息ID：{}",
                        message.getTopic(), message.getPayload(), message.getTags());
                return true;
            }
        });
        log.info("【不同组-消费者2】初始化完成，等待消息...");

        // 阻塞测试进程（10分钟）
        globalLatch.await(10, TimeUnit.MINUTES);
        log.info("=== 【不同组多消费者测试】结束 ===");
    }


    /**
     * 场景3：混合场景（3个消费者：2个同组+1个不同组）
     * - 消费者1、2加入ORDER_GROUP（负载均衡），消费者3加入PAY_GROUP（广播）
     * - 预期：消息先广播给ORDER_GROUP和PAY_GROUP，ORDER_GROUP内2个消费者轮询接收
     */
//    @Test
//    public void testMixedGroupScenario() throws InterruptedException {
//        log.info("=== 【混合组多消费者测试】开始 ===");
//
//        // 注入第三个消费者（需确保Spring容器中存在多个MqConsumerManager实例，可通过@Scope("prototype")实现）
//        @Autowired
//        private MqConsumerManager consumer3;
//
//        // -------------------------- 消费者1、2：同组ORDER_GROUP --------------------------
//        consumer1.joinGroup("ORDER_GROUP");
//        consumer2.joinGroup("ORDER_GROUP");
//        SubscribeReqDTO reqSameGroup = new SubscribeReqDTO();
//        reqSameGroup.setTopic("TEST_TOPIC");
//        reqSameGroup.setTags(Arrays.asList("TAG1"));
//
//        // 消费者1监听器
//        consumer1.groupSubscribe(reqSameGroup, message -> {
//            log.info("【混合组-消费者1】收到消息 | 组：ORDER_GROUP | 消息ID：{} | 内容：{}",
//                    message.getMessageId(), message.getPayload());
//            return true;
//        });
//
//        // 消费者2监听器
//        consumer2.groupSubscribe(reqSameGroup, message -> {
//            log.info("【混合组-消费者2】收到消息 | 组：ORDER_GROUP | 消息ID：{} | 内容：{}",
//                    message.getMessageId(), message.getPayload());
//            return true;
//        });
//
//        // -------------------------- 消费者3：不同组PAY_GROUP --------------------------
//        consumer3.joinGroup("PAY_GROUP");
//        SubscribeReqDTO reqDiffGroup = new SubscribeReqDTO();
//        reqDiffGroup.setTopic("TEST_TOPIC");
//        reqDiffGroup.setTags(Arrays.asList("TAG1"));
//
//        consumer3.groupSubscribe(reqDiffGroup, message -> {
//            log.info("【混合组-消费者3】收到消息 | 组：PAY_GROUP | 消息ID：{} | 内容：{}",
//                    message.getMessageId(), message.getPayload());
//            return true;
//        });
//
//        log.info("【混合组】3个消费者初始化完成，等待消息...");
//        globalLatch.await(10, TimeUnit.MINUTES);
//        log.info("=== 【混合组多消费者测试】结束 ===");
//    }
}