package org.guoguo.broker.core;

import com.alibaba.fastjson.JSON;
import io.netty.channel.Channel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.broker.ConsumerGroup.ConsumerGroupManager;
import org.guoguo.broker.util.FilePersistUtil;
import org.guoguo.common.pojo.DTO.ConsumerAckReqDTO;
import org.guoguo.common.pojo.Entity.ConsumerGroup;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.guoguo.common.constant.MethodType;
import org.guoguo.common.pojo.DTO.RpcMessageDTO;
import org.guoguo.common.pojo.Entity.MqMessageEnduring;
import org.guoguo.common.pojo.DTO.PushMessageDTO;
import org.guoguo.common.util.SnowflakeIdGeneratorUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
@Slf4j
@Component
public class BrokerManager {

    // 注入消费者组管理器
    private final ConsumerGroupManager groupManager;

    //提供 messageMap 的 getter（供 FilePersistUtil 恢复消息）
    // 存储消息（简化：内存存储，实际应持久化到文件/数据库）todo: 做持久化
    @Getter
    private final Map<String, MqMessage> messageMap = new ConcurrentHashMap<>();

    // 单个消费者的确认状态（避免同组内重复推送）：key=消息ID+消费者ID，value=状态
    private final Map<String, String> consumerAckMap = new ConcurrentHashMap<>();


    private final FilePersistUtil filePersistUtil;
    @Autowired
    public BrokerManager(@Lazy FilePersistUtil filePersistUtil,@Lazy ConsumerGroupManager consumerGroupManager) {
        this.filePersistUtil = filePersistUtil;
        this.groupManager = consumerGroupManager;
    }

      /**
     * 处理生产者发送的消息：存储消息并推送给订阅者
     */
    public void handlerMessage(MqMessageEnduring mqMessage, String messageId){
        messageMap.put(messageId, mqMessage);
        //默认为true，即进行持久化
        if(mqMessage.isEnduring()){
            //持久化
            log.info("WhisperMQ Broker 存储消息：ID={}，主题={}", messageId, mqMessage.getTopic());
            //核心持久化时机是在 Broker 接收到生产者的消息后、尚未发送给消费者之前完成存储
            filePersistUtil.writeMessage(messageId, mqMessage);
        }
        //将生产者生产的消息推送给订阅者
        String topic = mqMessage.getTopic();
        Map<String, ConsumerGroup> subscribeGroups = groupManager.getGroupsByTopic(topic);
        if (subscribeGroups.isEmpty()) {
            log.info("WhisperMQ Broker 无消费者组订阅主题{}，消息{}暂不推送", topic, messageId);
            return;
        }
        //推送前检查ack状态
        for (ConsumerGroup group : subscribeGroups.values()) {
            pushMessageToGroup(group, messageId, mqMessage);
        }
    }


    /**
     * 向消费者组推送消息（组内负载均衡：轮询分配给在线消费者）
     */
    public void pushMessageToGroup(ConsumerGroup group, String messageId, MqMessage message) {
        String groupId = group.getGroupId();
        String topic = message.getTopic();
        Map<String, Channel> onlineConsumers = group.getOnlineConsumers();

        if (onlineConsumers.isEmpty()){
            log.info("WhisperMQ Broker 无消费者组订阅主题{}，消息{}暂不推送", topic, messageId);
            return;
        }

        // 检查组的消费位点：只推送位点之后的消息
        String groupOffset = group.getTopicOffsetMap().get(topic);
        if (groupOffset != null && Long.parseLong(messageId) <= Long.parseLong(groupOffset)) {
            log.info("WhisperMQ Broker 消息{}已在组{}的消费位点之前，跳过推送", messageId, groupId);
            return;
        }

        //组内负载均衡：轮询选择一个消费者（简化实现）
        List<String> consumerIds = new ArrayList<>(onlineConsumers.keySet());
        // 按消息ID取模，确保同消息分配给同消费者
        int index = (int) (Long.parseLong(messageId) % consumerIds.size());
        String targetConsumerId = consumerIds.get(index);
        Channel targetChannel = onlineConsumers.get(targetConsumerId);

        //检查该消费者是否已确认过此消息（避免重复推送）
        String ackKey=messageId+":"+targetConsumerId;
        if (consumerAckMap.containsKey(ackKey)) {
            log.info("WhisperMQ Broker 消费者{}已确认消息{}，跳过推送", targetConsumerId, messageId);
            return;
        }
        // 4. 推送消息给目标消费者
        try {
            if (targetChannel.isActive()) {
                PushMessageDTO pushDto = new PushMessageDTO();
                pushDto.setMessageId(messageId);
                pushDto.setJson(JSON.toJSONString(message));
                RpcMessageDTO rpcDto = new RpcMessageDTO();
                rpcDto.setRequest(false);
                rpcDto.setMethodType(MethodType.B_PUSH_MSG);
                rpcDto.setJson(JSON.toJSONString(pushDto));
                System.out.println(onlineConsumers);
                targetChannel.writeAndFlush(JSON.toJSONString(rpcDto) + "\n"); // 带分隔符
                log.info("WhisperMQ Broker 向组{}的消费者{}推送消息{}", groupId, targetConsumerId, messageId);
            }
        } catch (Exception e) {
            log.error("WhisperMQ Broker 推送消息{}给组{}的消费者{}失败", messageId, groupId, targetConsumerId, e);
            // todo:失败时可重试分配给其他消费者
        }


    }

    /**
     * 处理消费者的 ACK 更新消费状态
     */
    public void handleConsumerAck(ConsumerAckReqDTO ackReq, String consumerId) {
        String messageId=ackReq.getMessageId();
        String consumerGroup = ackReq.getGroupId();

        //校验
        if (messageId==null || consumerId==null){
            log.error("WhisperMQ Broker 消费确认请求参数无效：messageId={}，consumerId={}", messageId, consumerId);
            return;
        }

        MqMessage message = messageMap.get(messageId);
        if (message == null) {
            log.error("WhisperMQ Broker 消息{}不存在，ACK处理失败", messageId);
            return;
        }

        // 从消息中获取主题
        String topic = messageMap.get(messageId).getTopic();
        String groupKey = topic + ":" + consumerGroup;

        consumerAckMap.put(groupKey, ackReq.getAckStatus());
        groupManager.updateGroupOffset(consumerGroup, topic, messageId);
        log.info("WhisperMQ Broker 收到消费者{}的ACK：消息{}，状态{}", consumerId, messageId, ackReq.getAckStatus());

    }



}
