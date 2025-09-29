// org.WhisperMQ.consumer.test.ConsumerGroupTest.java
package org.guoguo.consumer.test;

import lombok.extern.slf4j.Slf4j;
import org.guoguo.common.pojo.DTO.SubscribeReqDTO;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.guoguo.common.util.SnowflakeIdGeneratorUtil;
import org.guoguo.consumer.service.IMessageListener;
import org.guoguo.consumer.service.impl.MqConsumer;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
@SpringBootTest(classes = org.guoguo.consumer.ConsumerApplication.class)
@TestPropertySource(locations = "classpath:application.properties")
public class ConsumerGroupTest {

    @Autowired
    private MqConsumer mqConsumer;

    private final CountDownLatch latch = new CountDownLatch(1);


    @Test
    public void testConsumerGroup() throws InterruptedException {
        // 1. 加入消费者组 ORDER_GROUP
        mqConsumer.joinGroup("ORDER_GROUP");

        // 2. 组订阅主题 TEST_TOPIC（标签 TAG1）
        SubscribeReqDTO subscribeReq = new SubscribeReqDTO();
        subscribeReq.setTopic("TEST_TOPIC");
        subscribeReq.setTags(Arrays.asList("TAG1"));

        // 3. 绑定消息监听器
        mqConsumer.groupSubscribe(subscribeReq, new IMessageListener() {
            @Override
            public boolean onMessage(MqMessage message) {
                log.info("【消费者组测试】收到消息：主题={}，内容={}，标签={}",
                        message.getTopic(), message.getPayload(), message.getTags());
                return true; // 处理成功，触发ACK
            }
        });

        // 阻塞等待消息（1分钟）
        latch.await(60, TimeUnit.SECONDS);
    }
}