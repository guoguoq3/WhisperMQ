// org.WhisperMQ.common.entity.ConsumerGroup.java
package org.guoguo.common.pojo.Entity;

import lombok.Data;
import org.guoguo.common.pojo.DTO.SubscribeReqDTO;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 消费者组实体：作为订阅 Broker 的主体，管理组内消费者和消费位点
 */
@Data
public class ConsumerGroup {
    /** 消费者组唯一标识（如 "ORDER_GROUP"） */
    private String groupId;

    /** 组内订阅关系：key=主题，value=该主题的订阅配置（标签等） */
    private Map<String, SubscribeReqDTO> subscribeMap = new ConcurrentHashMap<>();

    /** 组内消费位点：key=主题，value=该主题的最新消费消息ID（同组共享） */
    private Map<String, String> topicOffsetMap = new ConcurrentHashMap<>();

    /** 组内在线消费者通道：key=消费者ID（通道ID），value=Netty通道 */
    private Map<String, io.netty.channel.Channel> onlineConsumers = new ConcurrentHashMap<>();

    // 新增订阅：组订阅主题时调用
    public void addSubscribe(SubscribeReqDTO subscribeReq) {
        String topic = subscribeReq.getTopic();
        subscribeMap.put(topic, subscribeReq);
    }

    // 移除订阅：组取消订阅时调用
    public void removeSubscribe(String topic) {
        subscribeMap.remove(topic);
        topicOffsetMap.remove(topic); // 移除订阅时清空对应位点
    }

    // 消费者加入组
    public void   addConsumer(String consumerId, io.netty.channel.Channel channel) {
        onlineConsumers.put(consumerId, channel);
    }

    // 消费者离开组（断开连接时）
    public void removeConsumer(String consumerId) {
        onlineConsumers.remove(consumerId);
    }

    // 判断组是否为空（无在线消费者）
    public boolean isEmpty() {
        return onlineConsumers.isEmpty();
    }
}