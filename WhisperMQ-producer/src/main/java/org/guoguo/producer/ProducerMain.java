package org.guoguo.producer;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.guoguo.common.pojo.Entity.MqMessageEnduring;
import org.guoguo.producer.pojo.Result;
import org.guoguo.producer.service.impl.MqProducer;

@RequiredArgsConstructor
@Slf4j
public class ProducerMain {
    private final MqProducer producer;
    public static void main(String[] args) {
//        MqProducer producer = new MqProducer();
//        producer.start();
//
//        MqMessage message = new MqMessage();
//        message.setTopic("TEST_TOPIC");
//        message.setTags(Arrays.asList("TAG1"));
//        message.setPayload("Hello, MQ!");
//
//        Result<String> result = producer.send(message);
//        System.out.println("发送结果：" + result.getMessage() + "，消息ID：" + result.getMessageId());
        // 发送消息

    }
    public void sendTestMessage() {
        MqMessageEnduring message = new MqMessageEnduring();
        message.setTopic("TEST_TOPIC");
        message.setPayload("Hello, Spring + WhisperMQ!");
        Result<String> result = producer.send(message);
        log.info("发送结果：{}", result);
    }
}