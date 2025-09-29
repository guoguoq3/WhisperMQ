package org.guoguo.consumer.test;

import lombok.extern.slf4j.Slf4j;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.guoguo.common.pojo.DTO.SubscribeReqDTO;
import org.guoguo.common.util.SnowflakeIdGeneratorUtil;
import org.guoguo.consumer.service.IMessageListener;
import org.guoguo.consumer.service.IMqConsumer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 消费者测试：订阅主题并接收消息
 */
@Slf4j
@SpringBootTest(classes = org.guoguo.consumer.ConsumerApplication.class) // 指定消费者启动类
@TestPropertySource(locations = "classpath:application.properties") // 加载配置
public class MqConsumerTest {

    @Autowired
    private IMqConsumer mqConsumer; // 注入消费者实例

    // 用于同步测试：等待消息接收后再结束测试
    private final CountDownLatch latch = new CountDownLatch(1);


    @Test
    public void testReceiveMessage() throws InterruptedException {
        // 1. 定义订阅的主题和标签
        SubscribeReqDTO subscribeReq = new SubscribeReqDTO();
        subscribeReq.setTopic("TEST_TOPIC"); // 订阅主题：TEST_TOPIC
        subscribeReq.setTags(Arrays.asList("TAG1")); // 只接收标签为 TAG1 的消息

        // 2. 订阅主题并设置消息监听器（收到消息后处理）
//        mqConsumer.subscribe(subscribeReq, new IMessageListener() {
//            @Override
//            public void onMessage(MqMessage message) {
//                log.info("【消费者测试】收到消息：主题={}，内容={}，标签={}",
//                        message.getTopic(), message.getPayload(), message.getTags());
//
//                // 验证消息内容（实际测试可根据业务场景添加断言）
//                if ("Hello, WhisperMQ!".equals(message.getPayload())) {
//                    log.info("【消费者测试】消息内容验证通过");
//                } else {
//                    log.error("【消费者测试】消息内容验证失败");
//                }
//
//                // 消息处理完成，计数器减1（释放等待的测试线程）
//                latch.countDown();
//            }
//        });

        // 3. 阻塞等待消息接收（最多等10秒，避免无限阻塞）
        latch.await(10, TimeUnit.MINUTES);
        log.info("【消费者测试】测试结束");
    }
}
