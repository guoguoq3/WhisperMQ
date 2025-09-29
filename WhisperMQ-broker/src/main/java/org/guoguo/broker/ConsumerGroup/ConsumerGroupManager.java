package org.guoguo.broker.ConsumerGroup;

import io.micrometer.common.util.StringUtils;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.broker.core.BrokerManager;
import org.guoguo.broker.util.OffsetPersistUtil;
import org.guoguo.common.pojo.DTO.SubscribeReqDTO;
import org.guoguo.common.pojo.Entity.ConsumerGroup;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@Component
public class ConsumerGroupManager {
    /**
     * 存储所有消费者组：key=组ID，value=消费者组实体
     */

    private final Map<String, ConsumerGroup> groupMap = new ConcurrentHashMap<>();
    private final OffsetPersistUtil offsetPersistUtil;
    private final BrokerManager brokerManager;
    //这里返回一个不可修改的视图
    public Map<String, ConsumerGroup> getGroupMap() {
        return Collections.unmodifiableMap(groupMap);
    }
    @Autowired
    public ConsumerGroupManager(OffsetPersistUtil offsetPersistUtil,  BrokerManager brokerManager) {
        this.offsetPersistUtil = offsetPersistUtil;
        this.brokerManager = brokerManager;
    }

    // return new HashMap<>(groupMap); 深拷贝方法

    /**
     * 恢复所有消费者组的位点信息
     */
    private void recoverAllOffset() {
        try {
            Map<String, Map<String, String>> allOffsetData = offsetPersistUtil.recoverAllOffset();
            for (Map.Entry<String, Map<String, String>> groupEntry : allOffsetData.entrySet()) {
                String groupId = groupEntry.getKey();
                Map<String, String> topicOffsetMap = groupEntry.getValue();

                // 获取或创建消费者组
                ConsumerGroup group = getOrCreateConsumerGroup(groupId);

                // 恢复该组的所有topic offset
                for (Map.Entry<String, String> topicEntry : topicOffsetMap.entrySet()) {
                    String topic = topicEntry.getKey();
                    String offset = topicEntry.getValue();
                    group.getTopicOffsetMap().put(topic, offset);
                    log.debug("WhisperMQ==============> 恢复组位点（组：{}，主题：{}，位点：{}）", groupId, topic, offset);
                }
            }
            log.info("WhisperMQ==============> 成功恢复所有消费者组位点，共涉及{}个消费者组", allOffsetData.size());
        } catch (Exception e) {
            log.error("WhisperMQ==============> 恢复所有消费者组位点失败", e);
        }
    }

    //获取创建消费者组 没有就创建
    public ConsumerGroup getOrCreateConsumerGroup(String groupId) {
        //检查groupId是否为null
        if (groupId == null) {
            throw new IllegalArgumentException("GroupId cannot be null");
        }
        //相当于若是不存在就创建的方法
        return groupMap.computeIfAbsent(groupId, k -> {
            ConsumerGroup consumerGroup = new ConsumerGroup();
            consumerGroup.setGroupId(groupId);


            log.info("WhisperMQ=====================>消费者组不存在 创建消费者组：{}", groupId);
            return consumerGroup;
        });
    }

    //消费者订阅主题  其实直接调用这个方法调用就行不必再创建消费者组
    public void GroupSubscribe(SubscribeReqDTO subscribeReqDTO) {
        //检查subscribeReqDTO是否为null
        if (subscribeReqDTO == null) {
            log.error("SubscribeReqDTO is null");
            return;
        }
        String groupId = subscribeReqDTO.getGroupId();
        String topic = subscribeReqDTO.getTopic();
        List<String> tags = subscribeReqDTO.getTags();

        if (StringUtils.isBlank(groupId)) {
            log.error("WhisperMQ Broker 订阅失败：消费者组 ID（groupId）不能为 null/空");
            return;
        }
        if (StringUtils.isBlank(topic)) {
            log.error("WhisperMQ Broker 订阅失败：主题（topic）不能为 null/空（组：{}）", groupId);
            return;
        }
        if (tags == null) {
            subscribeReqDTO.setTags(Collections.emptyList()); // 空标签视为“订阅所有标签”
            log.warn("WhisperMQ Broker 订阅主题{}（组：{}）：标签为 null，自动调整为订阅所有标签", topic, groupId);
        }

        //检查groupId是否为null
        if (subscribeReqDTO.getGroupId() == null) {
            log.error("GroupId is null in SubscribeReqDTO: {}", subscribeReqDTO);
            return;
        }
        ConsumerGroup consumerGroup = getOrCreateConsumerGroup(subscribeReqDTO.getGroupId());

        if (consumerGroup.getSubscribeMap().containsKey(topic)) {
            log.warn("WhisperMQ Broker 订阅关系已存在，请勿重复订阅");
            return;
        }

        //回溯一波位点 todo： 这块只有持久化文件中的位点 第二种就好全量恢复
        offsetPersistUtil.init(consumerGroup, topic);

        String lastOffset = consumerGroup.getTopicOffsetMap().get(topic);
        if (lastOffset == null){
           log.info("WhisperMQ Broker 组{}订阅主题{}：无历史位点，位点为最新的消息", groupId, topic);
            consumerGroup.addSubscribe(subscribeReqDTO);
       }else {

        //记录组的订阅关系
        consumerGroup.addSubscribe(subscribeReqDTO);
        log.info("WhisperMQ Broker 消费者组{}订阅主题{}（标签：{}）",
                subscribeReqDTO.getGroupId(), topic, subscribeReqDTO.getTags());

           //获取在消费者位点之后的消息 两个条件一个是大于lastOffset 一个是topic相同  todo：可能还会有tag的事情
            List<Map.Entry<String, MqMessage>> filteredMessagesStream = brokerManager.getMessageMap().entrySet().stream()
                    .filter(entry -> topic.equals(entry.getValue().getTopic())) // 主题匹配
                    .filter(entry -> Long.parseLong(entry.getKey()) > Long.parseLong(lastOffset)) // ID大于lastOffset
                    .toList();


            for (Map.Entry<String, MqMessage> entry : filteredMessagesStream) {
            brokerManager.pushMessageToGroup(consumerGroup, entry.getKey(), entry.getValue());
        }
        }
    }

    //消费者加入组
    public void consumerJoinGroup(String groupId, String consumerId, io.netty.channel.Channel channel) {
        //检查参数是否为null
        if (groupId == null) {
            log.error("GroupId is null in consumerJoinGroup");
            return;
        }
        if (consumerId == null) {
            log.error("ConsumerId is null in consumerJoinGroup");
            return;
        }
        if (channel == null) {
            log.error("Channel is null in consumerJoinGroup");
            return;
        }
        ConsumerGroup consumerGroup = getOrCreateConsumerGroup(groupId);
        consumerGroup.addConsumer(consumerId, channel);
    }


    // 消费者组取消订阅主题
    public void groupUnsubscribe(String groupId, String topic) {
        //检查参数是否为null
        if (groupId == null) {
            log.warn("WhisperMQ Broker groupId is null, 取消订阅失败");
            return;
        }
        if (topic == null) {
            log.warn("WhisperMQ Broker topic is null, 取消订阅失败");
            return;
        }
        ConsumerGroup group = groupMap.get(groupId);

        if (group == null) {
            log.warn("WhisperMQ Broker 消费者组{}不存在，取消订阅失败", groupId);
            return;
        }
        group.removeSubscribe(topic);
        offsetPersistUtil.closeGroup(groupId);
        log.info("WhisperMQ Broker 消费者组{}取消订阅主题{}", groupId, topic);
    }

    //消费者离开组
    public void consumerLeaveGroup(String groupId, String consumerId) {
        //检查参数是否为null
        if (groupId == null) {
            log.warn("WhisperMQ Broker groupId is null, 消费者离开组失败");
            return;
        }
        if (consumerId == null) {
            log.warn("WhisperMQ Broker consumerId is null, 消费者离开组失败");
            return;
        }
        ConsumerGroup consumerGroup = groupMap.get(groupId);
        if (consumerGroup == null) {
            return;
        }
        consumerGroup.removeConsumer(consumerId);
        log.info("WhisperMQ Broker 消费者{}离开组{}，当前组内在线消费者数：{}",
                consumerId, groupId, consumerGroup.getOnlineConsumers().size());
        // 组内无消费者时，自动销毁组（可选：也可保留组配置）
        if (consumerGroup.isEmpty()) {
            groupMap.remove(groupId);
            log.info("WhisperMQ Broker 消费者组{}无在线消费者，自动销毁", groupId);
        }
    }

    //获取订阅了指定主题的所有消费者组
    public Map<String, ConsumerGroup> getGroupsByTopic(String topic) {
        //检查参数是否为null
        if (topic == null) {
            log.warn("WhisperMQ Broker topic is null in getGroupsByTopic");
            return new ConcurrentHashMap<>();
        }
        Map<String, ConsumerGroup> result = new ConcurrentHashMap<>();
        for (ConsumerGroup group : groupMap.values()) {
            if (group.getSubscribeMap().containsKey(topic)) {
                result.put(group.getGroupId(), group);
            }
        }
        return result;
    }

    // 更新消费者组的消费位点
    public void updateGroupOffset(String groupId, String topic, String messageId) {
        //检查参数是否为null
        if (groupId == null) {
            log.warn("WhisperMQ Broker groupId is null, 无法更新位点");
            return;
        }
        if (topic == null) {
            log.warn("WhisperMQ Broker topic is null, 无法更新位点");
            return;
        }
        if (messageId == null) {
            log.warn("WhisperMQ Broker messageId is null, 无法更新位点");
            return;
        }
        ConsumerGroup consumerGroup = groupMap.get(groupId);
        if (consumerGroup == null) {
            log.warn("WhisperMQ Broker 消费者组{}不存在，无法更新位点", groupId);
            return;
        }
        // 仅更新为更大的消息ID（确保位点递增，避免回退）
        String currentOffset = consumerGroup.getTopicOffsetMap().get(topic);
        if (currentOffset == null || Long.parseLong(messageId) > Long.parseLong(currentOffset)) {
            consumerGroup.getTopicOffsetMap().put(topic, messageId);
            // 异步持久化
            CompletableFuture.runAsync(() ->
                    offsetPersistUtil.writeMessage(groupId, topic, messageId)
            );
            log.info("WhisperMQ Broker 更新组{}的主题{}消费位点至消息{}",
                    groupId, topic, messageId);
        }
    }

}
