package org.guoguo.consumer.service.impl;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.common.config.MqConfigProperties;
import org.guoguo.common.constant.DeadType;
import org.guoguo.common.constant.MethodType;
import org.guoguo.common.pojo.DTO.ConsumerAckReqDTO;
import org.guoguo.common.pojo.DTO.DeadLetterDTO;
import org.guoguo.common.pojo.DTO.DeadLetterReplayReqDTO;
import org.guoguo.common.pojo.DTO.PushMessageDTO;
import org.guoguo.common.pojo.DTO.RpcMessageDTO;
import org.guoguo.common.pojo.DTO.SubscribeReqDTO;
import org.guoguo.common.pojo.Entity.ConsumerThings;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.guoguo.consumer.service.IMessageListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 消费者业务实现：入组、组订阅、ACK、拒绝/死信、断线重连后的状态恢复。
 *
 * <p>P1 改造点：
 * <ul>
 *   <li>入组/订阅/退订都等待 Broker 的确认响应，返回即代表 Broker 已处理（旧实现 fire-and-forget，
 *       客户端无法知道订阅何时生效）；</li>
 *   <li>消息处理交给业务线程池，按 topic 串行，不再占用 Netty IO 线程。</li>
 * </ul>
 */
@Slf4j
@Component
@Scope("prototype")
public class MqConsumerManager extends MqConsumer {

    private static final String ACK_SUCCESS = "ACK_SUCCESS";
    private static final String ACK_FAIL = "ACK_FAIL";

    // 消费者所属的组（支持多组，简化为单组）
    private String currentGroupId;
    private final Map<String, DeadLetterDTO> deadLetterMap = new ConcurrentHashMap<>();
    // 已订阅的请求（重连后按此恢复订阅，含 tags）
    private final Map<String, SubscribeReqDTO> subscriptionMap = new ConcurrentHashMap<>();
    // 本轮投递中已经调用过 reject() 的消息，用于避免 ACK_FAIL 与死信重试双重重投
    private final Set<String> rejectedMessageIds = ConcurrentHashMap.newKeySet();

    // 仅注入配置，调用父类构造（父类已处理配置依赖）
    @Autowired
    public MqConsumerManager(MqConfigProperties config) {
        super(config); // 调用父类 MqConsumer 的构造函数，传入配置
    }

    // ------------------------------------------------------------------ 入组 / 退组

    @Override
    public void joinGroup(String groupId) {
        ensureConnected("加入组");
        if (currentGroupId != null && !currentGroupId.equals(groupId)) {
            log.warn("WhisperMQ 消费者已在组{}中，自动退出旧组", currentGroupId);
            leaveGroup(currentGroupId);
        }

        ConsumerThings request = new ConsumerThings();
        request.setJson(groupId);// 直接传组ID字符串
        String traceId = request.getMessageId();

        CompletableFuture<Void> future = expectResponse(traceId);
        if (!sendFrame(buildRpc(MethodType.CONSUMER_JOIN_GROUP, request))) {
            throw new RuntimeException("WhisperMQ 消费者未连接Broker，无法加入组");
        }
        if (!awaitResponse(traceId, future, config.getSubscribeTimeoutMillis(), "加入组")) {
            throw new RuntimeException("WhisperMQ 加入组超时：" + groupId);
        }
        this.currentGroupId = groupId;
        log.info("WhisperMQ 消费者加入组{}成功", groupId);
    }

    /** 发送入组请求（重连恢复用，不等待响应：不能阻塞 IO 线程） */
    private void sendJoinGroup(String groupId) {
        ConsumerThings request = new ConsumerThings();
        request.setJson(groupId);
        sendFrame(buildRpc(MethodType.CONSUMER_JOIN_GROUP, request));
    }

    @Override
    public void leaveGroup(String groupId) {
        if (channel == null || !channel.isActive()) {
            log.warn("WhisperMQ 消费者未连接Broker，跳过离开组{}", groupId);
            return;
        }
        ConsumerThings request = new ConsumerThings();
        request.setJson(groupId);
        String traceId = request.getMessageId();
        CompletableFuture<Void> future = expectResponse(traceId);
        if (sendFrame(buildRpc(MethodType.CONSUMER_LEAVE_GROUP, request))) {
            // 退组不是关键路径：超时只告警，不抛异常
            awaitResponse(traceId, future, config.getSubscribeTimeoutMillis(), "离开组");
        }

        if (groupId.equals(currentGroupId)) {
            this.currentGroupId = null;
            subscriptionMap.clear();
            // 离开组后不再接收该组的推送，监听器一并清理（重新入组后需重新订阅）
            topicListenerMap.clear();
        }
        log.info("WhisperMQ 消费者离开组{}", groupId);
    }

    // ------------------------------------------------------------------ 订阅 / 退订

    @Override
    public void groupSubscribe(SubscribeReqDTO subscribeReq, IMessageListener listener) {
        ensureConnected("组订阅");
        if (currentGroupId == null) {
            throw new RuntimeException("WhisperMQ 消费者未加入任何组，无法发起组订阅");
        }
        String topic = subscribeReq.getTopic();
        // 必须先把监听器登记好：Broker 会先推回溯消息、后回订阅响应
        topicListenerMap.put(topic, listener);
        subscribeReq.setGroupId(currentGroupId);
        subscriptionMap.put(topic, subscribeReq);

        String traceId = subscribeReq.getMessageId();
        CompletableFuture<Void> future = expectResponse(traceId);
        if (!sendFrame(buildRpc(MethodType.GROUP_SUBSCRIBE, subscribeReq))) {
            throw new RuntimeException("WhisperMQ 消费者未连接Broker，无法发起组订阅");
        }
        if (!awaitResponse(traceId, future, config.getSubscribeTimeoutMillis(), "订阅主题 " + topic)) {
            throw new RuntimeException("WhisperMQ 订阅主题超时：" + topic);
        }
        log.info("WhisperMQ 消费者组{}订阅主题{}成功", currentGroupId, topic);
    }

    /** 发送组订阅请求（重连恢复用，不等待响应） */
    private void sendSubscribe(SubscribeReqDTO subscribeReq) {
        sendFrame(buildRpc(MethodType.GROUP_SUBSCRIBE, subscribeReq));
    }

    @Override
    public void groupUnsubscribe(SubscribeReqDTO subscribeReq) {
        ensureConnected("组取消订阅");
        if (currentGroupId == null) {
            throw new RuntimeException("WhisperMQ 消费者未加入任何组，无法发起组取消订阅");
        }
        subscribeReq.setGroupId(currentGroupId);
        String topic = subscribeReq.getTopic();

        // 注意：GroupUnsubscribe 在 Broker 侧直接按 SubscribeReqDTO 解析，
        // 旧实现多包了一层 ConsumerThings，导致 Broker 解析出 groupId/topic 都是 null（退订永远无效）。
        String traceId = subscribeReq.getMessageId();
        CompletableFuture<Void> future = expectResponse(traceId);
        if (sendFrame(buildRpc(MethodType.GROUP_UNSUBSCRIBE, subscribeReq))) {
            awaitResponse(traceId, future, config.getSubscribeTimeoutMillis(), "取消订阅 " + topic);
        }

        // 移除监听器与本地订阅记录
        topicListenerMap.remove(topic);
        subscriptionMap.remove(topic);
        log.info("WhisperMQ 消费者组{}取消订阅主题{}", currentGroupId, topic);
    }

    /**
     * 重连成功后恢复会话：Broker 侧用通道 ID 作为 consumerId，
     * 重连后是新连接，必须重新入组并重新订阅，才能继续收消息。
     * <p>本方法在 IO 线程执行，只发送不等待确认。</p>
     */
    @Override
    protected void onReconnected() {
        if (currentGroupId == null) {
            log.info("WhisperMQ 消费者重连完成，尚未入组，无需恢复");
            return;
        }
        sendJoinGroup(currentGroupId);
        for (SubscribeReqDTO req : subscriptionMap.values()) {
            req.setGroupId(currentGroupId);
            sendSubscribe(req);
        }
        log.info("WhisperMQ 消费者重连完成，已恢复组{}的{}个订阅", currentGroupId, subscriptionMap.size());
    }

    // ------------------------------------------------------------------ 消息处理

    /**
     * 把推送消息交给业务线程池（由 Handler 在 IO 线程调用，立即返回）。
     * 线程池满时不 ACK，让 Broker 的 ACK 超时重投兜底。
     */
    public void dispatchPush(String messageId, String topic, MqMessage message) {
        boolean accepted = dispatcher.submit(topic, () -> processPush(messageId, topic, message));
        if (!accepted) {
            log.warn("WhisperMQ 业务线程池已满，消息{}暂不处理（不 ACK，等待 Broker 超时重投）", messageId);
        }
    }

    /** 在业务线程上执行：调用监听器、根据结果回 ACK */
    private void processPush(String messageId, String topic, MqMessage message) {
        // 每次投递开始清除上一轮留下的拒绝标记
        rejectedMessageIds.remove(messageId);

        boolean success = handlePushMessage(messageId, topic, message);
        if (success) {
            // 成功消费后清理死信重试计数，避免按消息泄漏
            deadLetterMap.remove(messageId);
            sendAck(messageId, ACK_SUCCESS, message.getPartition());
        } else if (wasRejected(messageId)) {
            // 监听器已经通过 reject() 走死信重试链路，不需要再发 ACK_FAIL 造成双重重投
            log.info("WhisperMQ 消费者已通过 reject 处理消息{}，跳过 ACK_FAIL", messageId);
        } else {
            sendAck(messageId, ACK_FAIL, message.getPartition());
            log.warn("WhisperMQ 消费者处理消息{}失败，发送FAIL ACK，等待 Broker 重投", messageId);
        }
    }

    /**
     * 处理 Broker 推送的消息（调用业务监听器）。
     * 保留为 public 便于单测直接驱动，正常链路由 {@link #dispatchPush} 调用。
     */
    public boolean handlePushMessage(String messageId, String topic, MqMessage message) {
        IMessageListener listener = topicListenerMap.get(topic);
        if (listener == null) {
            log.error("WhisperMQ 消费者无主题{}的监听器，消息{}处理失败", topic, messageId);
            return false;
        }
        try {
            return listener.onMessage(message, messageId);
        } catch (Exception e) {
            log.error("WhisperMQ 消费者处理消息{}异常", messageId, e);
            return false;
        }
    }

    /** 该消息本轮是否已经通过 reject() 进入死信链路 */
    public boolean wasRejected(String messageId) {
        return rejectedMessageIds.contains(messageId);
    }

    /** 发送ACK给Broker（兼容旧调用：分区 0） */
    public void sendAck(String messageId, String ackStatus) {
        sendAck(messageId, ackStatus, 0);
    }

    /** 发送ACK给Broker（携带分区，Broker 据此推进对应分区的位点） */
    public void sendAck(String messageId, String ackStatus, int partition) {
        if (currentGroupId == null) {
            log.error("WhisperMQ 消费者未加入任何组，无法发送ACK：消息{}", messageId);
            return;
        }
        if (channel == null || !channel.isActive()) {
            log.error("WhisperMQ 消费者未连接Broker，无法发送ACK：消息{}", messageId);
            return;
        }

        ConsumerAckReqDTO ackReq = new ConsumerAckReqDTO();
        ackReq.setMessageId(messageId);
        ackReq.setGroupId(currentGroupId);
        ackReq.setAckStatus(ackStatus);
        ackReq.setPartition(Math.max(0, partition));

        sendFrame(buildRpc(MethodType.C_ACK_MSG, ackReq));
        log.info("WhisperMQ 消费者向组{}发送ACK：消息{}（分区{}），状态{}",
                currentGroupId, messageId, partition, ackStatus);
    }

    // ------------------------------------------------------------------ 死信

    /**
     * 提供关于死信队列的消费者端拒绝方法
     */
    public void reject(MqMessage message, String messageId, DeadType deadType, boolean isRetry) {
        rejectedMessageIds.add(messageId);

        //还原功能层DTO
        PushMessageDTO pushMessageDTO = new PushMessageDTO();
        pushMessageDTO.setMessageId(messageId);
        pushMessageDTO.setJson(JSON.toJSONString(message));
        pushMessageDTO.setConsumerGroup(currentGroupId);
        //构建死信DTO
        DeadLetterDTO deadLetter = new DeadLetterDTO();
        deadLetter.setMessageId(messageId);
        deadLetter.setDeadType(deadType);
        deadLetter.setJson(JSON.toJSONString(pushMessageDTO));

        if (!isRetry) {
            deadLetter.setDeadRetryCount(-1); // 设置为-1表示不重试
            log.info("WhisperMQ 死信队列消费者拒绝消息{}，死信类型:{}，不重试", messageId, deadType);
            sendDeadLetter(deadLetter);
            return;
        }

        //从重试map中获取死信dto
        DeadLetterDTO deadLetterDTO = deadLetterMap.get(messageId);
        if (deadLetterDTO == null) {
            deadLetter.setDeadRetryCount(0);
            deadLetterMap.put(messageId, deadLetter);
            log.info("WhisperMQ 死信队列消费者拒绝消息{}，死信类型:{}，重试次数:{}",
                    messageId, deadType, deadLetter.getDeadRetryCount());
            sendDeadLetter(deadLetter);
            return;
        }

        deadLetterDTO.setDeadRetryCount(deadLetterDTO.getDeadRetryCount() + 1);
        deadLetterMap.put(messageId, deadLetterDTO);
        if (deadLetterDTO.getDeadRetryCount() <= config.getMaxDeadLetterRetryCount()) {
            log.info("WhisperMQ 死信队列消费者拒绝消息{}，死信类型:{}，重试次数:{}",
                    messageId, deadType, deadLetterDTO.getDeadRetryCount());
            sendDeadLetter(deadLetterDTO);
            return;
        }
        // 次数已超上限：Broker 在 deadRetryCount == max 时已经完成入死信并移除了消息，
        // 这里不能再发，否则会产生重复死信。
        log.info("WhisperMQ 死信队列消费者拒绝消息{}，死信类型:{}，重试次数已达上限，停止重试",
                messageId, deadType);
        deadLetterMap.remove(messageId);
    }

    private void sendDeadLetter(DeadLetterDTO deadLetter) {
        sendFrame(buildRpc(MethodType.C_DEAD_MSG, deadLetter));
    }

    /**
     * 重放一条死信：Broker 会把它重新投递回原消费组。
     * 死信 ID 可以从 Broker 日志（"死信消息xxx已入队"）或死信文件获得。
     */
    public void replayDeadLetter(String deadLetterId) {
        if (deadLetterId == null || deadLetterId.isBlank()) {
            log.error("WhisperMQ 重放死信失败：deadLetterId 不能为空");
            return;
        }
        DeadLetterReplayReqDTO replayReq = new DeadLetterReplayReqDTO();
        replayReq.setDeadLetterId(deadLetterId);
        if (!sendFrame(buildRpc(MethodType.DEAD_LETTER_REPLAY, replayReq))) {
            log.error("WhisperMQ 重放死信失败：未连接 Broker");
            return;
        }
        log.info("WhisperMQ 消费者请求重放死信{}", deadLetterId);
    }

    // ------------------------------------------------------------------ 工具方法

    private void ensureConnected(String action) {
        if (channel == null || !channel.isActive()) {
            throw new RuntimeException("WhisperMQ 消费者未连接Broker，无法" + action);
        }
    }

    private String buildRpc(String methodType, Object body) {
        RpcMessageDTO rpcDto = new RpcMessageDTO();
        rpcDto.setRequest(true);
        rpcDto.setMethodType(methodType);
        rpcDto.setJson(JSON.toJSONString(body));
        return JSON.toJSONString(rpcDto);
    }
}
