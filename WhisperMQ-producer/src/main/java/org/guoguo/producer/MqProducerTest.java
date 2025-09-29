package org.guoguo.producer;

import lombok.extern.slf4j.Slf4j;

import org.guoguo.common.pojo.Entity.MqMessage;
import org.guoguo.common.pojo.Entity.MqMessageEnduring;
import org.guoguo.producer.pojo.Result;

import org.guoguo.producer.service.IMqProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.Arrays;

/**
 * 生产者测试：发送消息并验证结果
 */
@Slf4j
@SpringBootTest(classes = org.guoguo.producer.ProducerApplication.class) // 指定生产者启动类
@TestPropertySource(locations = "classpath:application.properties") // 加载配置
public class MqProducerTest {

    @Autowired
    private IMqProducer mqProducer1; // 注入生产者实例
    @Autowired
    private IMqProducer mqProducer2; // 注入生产者实例
    @Autowired
    private IMqProducer mqProducer3; // 注入生产者实例


    @Test
    public void testSendMessage() throws InterruptedException {


        Thread thread1 = new Thread(() -> {
            try {
                for (int i = 0; i < 100; i++) {
                    MqMessageEnduring msg1 = new MqMessageEnduring();
                    msg1.setTopic("TEST_TOPIC");
                    msg1.setTags(Arrays.asList("TAG1"));
                    msg1.setPayload("组测试消息1");
                    Result<String> send1 = mqProducer1.send(msg1);

                    log.info("【生产者测试thread1】发送结果：{}，消息ID：{}", send1.getData(), send1.getMessageId());
                    if (send1.getCode()==200) {
                        log.info("【生产者测试thread1】消息发送成功");
                    } else {
                        log.error("【生产者测试thread1】消息发送失败");
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        },"t1");

        Thread thread2 = new Thread(() -> {
            try {
                for (int i = 0; i < 100; i++){
                    MqMessageEnduring msg2 = new MqMessageEnduring();
                    msg2.setTopic("TEST_TOPIC");
                    msg2.setTags(Arrays.asList("TAG1"));
                    msg2.setPayload("组测试消息2");
                    Result<String> send2 = mqProducer2.send(msg2);

                    log.info("【生产者测试threa2】发送结果：{}，消息ID：{}", send2.getData(), send2.getMessageId());
                    if (send2.getCode()==200) {
                        log.info("【生产者测试thread2】消息发送成功");
                    } else {
                        log.error("【生产者测试thread2】消息发送失败");
                    }}
            } catch (Exception e) {
                e.printStackTrace();
            }
        },"t2");

        Thread thread3 = new Thread(() -> {
            try {
                for (int i = 0; i <=100; i++) {
                    MqMessageEnduring msg3 = new MqMessageEnduring();
                    msg3.setTopic("TEST_TOPIC");
                    msg3.setTags(Arrays.asList("TAG1"));
                    msg3.setPayload("组测试消息3");
                    Result<String> send3 = mqProducer3.send(msg3);

                    log.info("【生产者测试thread3】发送结果：{}，消息ID：{}", send3.getData(), send3.getMessageId());
                    if (send3.getCode()==200) {
                        log.info("【生产者测试thread3】消息发送成功");
                    } else {
                        log.error("【生产者测试thread3】消息发送失败");
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        },"t3");

        log.info("【生产者测试thread1】开始发送消息");
        thread1.start();
        log.info("【生产者测试thread2】开始发送消息");
        thread2.start();
        log.info("【生产者测试thread3】开始发送消息");
        thread3.start();
        log.info("【生产者测试】等待所有线程执行完毕");
        thread1.join();
        thread2.join();
        thread3.join();
        log.info("【生产者测试】所有线程执行完毕");






//        MqMessageEnduring msg1 = new MqMessageEnduring();
//        msg1.setTopic("TEST_TOPIC");
//        msg1.setTags(Arrays.asList("TAG1"));
//        msg1.setPayload("组测试消息1");
//
//        MqMessageEnduring msg2 = new MqMessageEnduring();
//        msg2.setTopic("TEST_TOPIC");
//        msg2.setTags(Arrays.asList("TAG1"));
//        msg2.setPayload("组测试消息2");
//
//        //message.setEnduring(false);// 是否持久化消息(可选，默认为ture)
//
//        // 2. 发送消息
//        Result<String> send = mqProducer1.send(msg1);
//        Result<String> send1 = mqProducer1.send(msg2);
//
//        // 3. 验证发送结果
//        log.info("【生产者测试】发送结果：{}，消息ID：{}", send.getData(), send.getMessageId());
//        if (send.getCode()==200) {
//            log.info("【生产者测试】消息发送成功");
//        } else {
//            log.error("【生产者测试】消息发送失败");
//        }
//        log.info("【生产者测试】发送结果：{}，消息ID：{}", send1.getData(), send1.getMessageId());
//        if (send1.getCode()==200) {
//            log.info("【生产者测试】消息发送成功");
//        } else {
//            log.error("【生产者测试】消息发送失败");
//        }
//
//


    }
}
