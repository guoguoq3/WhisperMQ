// org.WhisperMQ.common.entity.ConsumerGroup.java
package org.guoguo.common.pojo.Entity;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import lombok.Data;
import org.guoguo.common.pojo.DTO.SubscribeReqDTO;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

    // 主题-消费者的Tag订阅关系（key: topic，value: {consumerId: [tags]}）
    // 仅包含组内在线消费者的订阅关系
    private Map<String, Map<String, List<String>>> topicConsumerTags = new ConcurrentHashMap<>();

    // 新增：添加消费者对主题的Tag订阅
    public void addConsumerTopicTags(String topic, String consumerId, List<String> tags) {
        // 仅当消费者在线时才维护其Tag订阅关系
        if (onlineConsumers.containsKey(consumerId)) {
            topicConsumerTags.computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
                    .put(consumerId, tags);
        }

        System.out.println("消费者 " + consumerId + " 订阅了主题 " + topic + " 的标签 " + tags);
    }

    // 新增：获取订阅了指定主题和Tag的消费者ID
    public List<String> getConsumersByTopicAndTag(String topic, String tag) {
        // 1. 获取该主题下所有消费者的Tag订阅关系
        Map<String, List<String>> consumerTags = topicConsumerTags.get(topic);
        if (consumerTags == null || consumerTags.isEmpty()) {
            System.out.println("没有找到主题 " + topic + " 的消费者标签订阅关系");
            return Collections.emptyList();
        }
        // 2. 筛选出订阅了目标Tag的消费者（支持通配符*）
        List<String> matchedConsumers = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : consumerTags.entrySet()) {
            String consumerId = entry.getKey();
            // 确保消费者在线
            if (!onlineConsumers.containsKey(consumerId)) {
                continue;
            }
            List<String> subscribedTags = entry.getValue();

            // 匹配规则：订阅了* 或 包含该Tag
            if (subscribedTags.contains("*") || subscribedTags.contains(tag)) {
                matchedConsumers.add(consumerId);
            }
        }
        return matchedConsumers;
    }

    // 新增：移除消费者时清理其Tag订阅关系
    public void removeConsumerTags(String consumerId) {
        for (Map<String, List<String>> consumerTags : topicConsumerTags.values()) {
            consumerTags.remove(consumerId);
        }
    }
    // 新增：当消费者更新订阅标签时调用
    public void updateConsumerTopicTags(String topic, String consumerId, List<String> newTags) {
        if (onlineConsumers.containsKey(consumerId)) {
            topicConsumerTags.computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
                    .put(consumerId, newTags);
        }
    }



    // 新增订阅：组订阅主题时调用
    public void addSubscribe(SubscribeReqDTO subscribeReq, ChannelHandlerContext ctx) {
        String topic = subscribeReq.getTopic();
        subscribeMap.put(topic, subscribeReq);

        ChannelId channelId = ctx.channel().id();
        String consumerId = channelId.asLongText();
        // 初始化消费者对所有已订阅主题的Tag订阅（默认继承组的订阅配置）
        List<String> tags = subscribeReq.getTags();
        addConsumerTopicTags(topic, consumerId, tags);//TODO 由于其实只需要放入当前自己的即可
        System.out.println("消费者 " + consumerId + " 订阅了主题 " + topic + " 的标签 " + tags);



    }

    // 移除订阅：组取消订阅时调用
    public void removeSubscribe(String topic) {
        subscribeMap.remove(topic);
        topicOffsetMap.remove(topic); // 移除订阅时清空对应位点
        // 同时清理该主题下的Tag订阅关系
        topicConsumerTags.remove(topic);
    }

    // 消费者加入组（同时初始化其Tag订阅关系）
    public void addConsumer(String consumerId, io.netty.channel.Channel channel) {
        onlineConsumers.put(consumerId, channel);
    }

    // 消费者离开组（同时清理其Tag订阅关系）
    public void removeConsumer(String consumerId) {
        onlineConsumers.remove(consumerId);
        removeConsumerTags(consumerId);
    }

    // 判断组是否为空（无在线消费者）
    public boolean isEmpty() {
        return onlineConsumers.isEmpty();
    }
}