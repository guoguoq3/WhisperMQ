package org.guoguo.consumer.service.impl;

import com.alibaba.fastjson.JSON;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.common.config.MqConfigProperties;
import org.guoguo.common.constant.MethodType;
import org.guoguo.common.pojo.DTO.ConsumerAckReqDTO;
import org.guoguo.common.pojo.DTO.RpcMessageDTO;
import org.guoguo.common.pojo.DTO.SubscribeReqDTO;
import org.guoguo.common.pojo.Entity.ConsumerThings;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.guoguo.common.util.SnowflakeIdGeneratorUtil;
import org.guoguo.consumer.service.IMessageListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Scope("prototype")
public class MqConsumerManager extends MqConsumer {
    // 仅注入配置，调用父类构造（父类已处理配置依赖）
    @Autowired
    public MqConsumerManager(MqConfigProperties config) {
        super(config); // 调用父类 MqConsumer 的构造函数，传入配置
    }

    // 消费者所属的组（支持多组，简化为单组）
    private String currentGroupId;




    @Override
    public void joinGroup(String groupId) {
        if (channel == null || !channel.isActive()) {
            throw new RuntimeException("WhisperMQ 消费者未连接Broker，无法加入组");
        }
        if (currentGroupId != null) {
            log.warn("WhisperMQ 消费者已在组{}中，自动退出旧组", currentGroupId);
            leaveGroup(currentGroupId);
        }

        // 发送“加入组”请求给Broker
        RpcMessageDTO rpcDto = new RpcMessageDTO();
        rpcDto.setRequest(true);
        ConsumerThings consumerThings = new ConsumerThings();
        consumerThings.setJson(groupId);// 直接传组ID字符串
        rpcDto.setJson(JSON.toJSONString(consumerThings));
        rpcDto.setMethodType(MethodType.CONSUMER_JOIN_GROUP);

        channel.writeAndFlush(JSON.toJSONString(rpcDto) + "\n");
        this.currentGroupId = groupId;
        log.info("WhisperMQ 消费者请求加入组{}", groupId);
    }

    @Override
    public void leaveGroup(String groupId) {
        if (channel == null || !channel.isActive()) {
            log.warn("WhisperMQ 消费者未连接Broker，跳过离开组{}", groupId);
            return;
        }

        // 发送“离开组”请求给Broker
        RpcMessageDTO rpcDto = new RpcMessageDTO();
        rpcDto.setRequest(true);
        ConsumerThings consumerThings = new ConsumerThings();
        consumerThings.setJson(groupId);// 直接传组ID字符串
        rpcDto.setJson(JSON.toJSONString(consumerThings));
        rpcDto.setMethodType(MethodType.CONSUMER_LEAVE_GROUP);

        channel.writeAndFlush(JSON.toJSONString(rpcDto) + "\n");
        if (groupId.equals(currentGroupId)) {
            this.currentGroupId = null;
        }
        log.info("WhisperMQ 消费者请求离开组{}", groupId);
    }

    @Override
    public void groupSubscribe(SubscribeReqDTO subscribeReq, IMessageListener listener) {
        if (currentGroupId == null) {
            throw new RuntimeException("WhisperMQ 消费者未加入任何组，无法发起组订阅");
        }
        if (channel == null || !channel.isActive()) {
            throw new RuntimeException("WhisperMQ 消费者未连接Broker，无法发起组订阅");
        }

        // 绑定主题-监听器（处理后续推送的消息）
        String topic = subscribeReq.getTopic();
        topicListenerMap.put(topic, listener);
        System.out.println(topicListenerMap);
        // 填充组ID（确保订阅请求关联当前组）
        subscribeReq.setGroupId(currentGroupId);
        // 发送“组订阅”请求给Broker
        RpcMessageDTO rpcDto = new RpcMessageDTO();
        rpcDto.setRequest(true);
        rpcDto.setJson(JSON.toJSONString(subscribeReq));
        rpcDto.setMethodType(MethodType.GROUP_SUBSCRIBE);

        channel.writeAndFlush(JSON.toJSONString(rpcDto) + "\n");
        log.info("WhisperMQ 消费者请求组{}订阅主题{}", currentGroupId, topic);
    }

    @Override
    public void groupUnsubscribe(SubscribeReqDTO subscribeReq) {
        if (currentGroupId == null) {
            throw new RuntimeException("WhisperMQ 消费者未加入任何组，无法发起组取消订阅");
        }
        if (channel == null || !channel.isActive()) {
            throw new RuntimeException("WhisperMQ 消费者未连接Broker，无法发起组取消订阅");
        }

        // 填充组ID
        subscribeReq.setGroupId(currentGroupId);
        String topic = subscribeReq.getTopic();

        // 发送“组取消订阅”请求给Broker
        RpcMessageDTO rpcDto = new RpcMessageDTO();
        rpcDto.setRequest(true);
        ConsumerThings consumerThings = new ConsumerThings();
        consumerThings.setJson(JSON.toJSONString(subscribeReq));
        rpcDto.setJson(JSON.toJSONString(consumerThings));
        rpcDto.setMethodType(MethodType.GROUP_UNSUBSCRIBE);

        channel.writeAndFlush(JSON.toJSONString(rpcDto) + "\n");
        // 移除监听器
        topicListenerMap.remove(topic);
        log.info("WhisperMQ 消费者请求组{}取消订阅主题{}", currentGroupId, topic);
    }
    /**
     * 处理Broker推送的消息（供Handler调用）
     */
    public boolean handlePushMessage(String messageId, String topic, MqMessage message) {
        System.out.println(111111);
        System.out.println(topicListenerMap);
        IMessageListener listener = topicListenerMap.get(topic);
        if (listener == null) {
            log.error("WhisperMQ 消费者无主题{}的监听器，消息{}处理失败", topic, messageId);
            return false;
        }

        try {
            // 调用用户自定义监听器
            return listener.onMessage(message);
        } catch (Exception e) {
            log.error("WhisperMQ 消费者处理消息{}异常", messageId, e);
            return false;
        }
    }

    /**
     * 发送ACK给Broker（供Handler调用）
     */
    public void sendAck(String messageId, String ackStatus) {
        if (currentGroupId == null) {
            log.error("WhisperMQ 消费者未加入任何组，无法发送ACK：消息{}", messageId);
            return;
        }
        if (channel == null || !channel.isActive()) {
            log.error("WhisperMQ 消费者未连接Broker，无法发送ACK：消息{}", messageId);
            return;
        }

        // 构造ACK请求（携带组ID）
        ConsumerAckReqDTO ackReq = new ConsumerAckReqDTO();
        ackReq.setMessageId(messageId);
        ackReq.setGroupId(currentGroupId);
        ackReq.setAckStatus(ackStatus);

        // 发送ACK
        RpcMessageDTO rpcDto = new RpcMessageDTO();
        rpcDto.setRequest(true);
        rpcDto.setMethodType(MethodType.C_ACK_MSG);
        rpcDto.setJson(JSON.toJSONString(ackReq));

        channel.writeAndFlush(JSON.toJSONString(rpcDto) + "\n");
        log.info("WhisperMQ 消费者向组{}发送ACK：消息{}，状态{}", currentGroupId, messageId, ackStatus);
    }
}
