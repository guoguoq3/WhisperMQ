package org.guoguo.broker.handler;

import com.alibaba.fastjson2.JSON;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.broker.ConsumerGroup.ConsumerGroupManager;
import org.guoguo.broker.core.BrokerManager;
import org.guoguo.common.config.MqConfigProperties;
import org.guoguo.common.pojo.DTO.*;
import org.guoguo.common.pojo.Entity.*;
import org.guoguo.common.constant.MethodType;
import org.guoguo.common.util.SnowflakeIdGeneratorUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
//每次有新的连接调用这个 都会新建一个实例 以前不报错 是因为每次我都新建new一个
@ChannelHandler.Sharable
public class MqBrokerHandler extends SimpleChannelInboundHandler<String> {
    // 获取Broker管理器实例
    private final BrokerManager brokerManager;
    private final ConsumerGroupManager groupManager;
    private final MqConfigProperties config;

    @Autowired
    public MqBrokerHandler(BrokerManager brokerManager, ConsumerGroupManager groupManager,
                           MqConfigProperties config) {
        this.brokerManager = brokerManager;
        this.groupManager = groupManager;
        this.config = config;
    }
    /**
     * 接收消息将rpc消息转为message对象
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) {

        // 解析RPC消息（去除换行符，避免JSON解析错误）
        String cleanMsg = msg.trim();
        if (cleanMsg.isEmpty()) {
            log.warn("WhisperMQ Broker 收到空消息");
            return;
        }
        RpcMessageDTO rpcDto ;
        try {
            rpcDto = JSON.parseObject(cleanMsg, RpcMessageDTO.class);
        } catch (Exception e) {
            log.error("WhisperMQ Broker 解析RPC消息失败: {}", cleanMsg, e);
            sendErrorResponse(ctx, null, "消息格式错误：" + e.getMessage());
            return;
        }
        if (rpcDto == null) {
            log.error("WhisperMQ Broker RPC消息解析结果为null: {}", cleanMsg);
            sendErrorResponse(ctx, null, "消息格式错误");
            return;
        }
        // P1：协议版本校验。旧客户端不传时按 1 处理；高于服务端支持版本直接拒绝。
        Integer protocolVersion = rpcDto.getVersion() == null ? 1 : rpcDto.getVersion();
        if (protocolVersion > RpcMessageDTO.PROTOCOL_VERSION) {
            log.warn("WhisperMQ Broker 收到不支持的协议版本：{}（支持 <= {}）",
                    protocolVersion, RpcMessageDTO.PROTOCOL_VERSION);
            sendErrorResponse(ctx, null, "不支持的协议版本：" + protocolVersion
                    + "，服务端支持 <= " + RpcMessageDTO.PROTOCOL_VERSION);
            return;
        }
        // P1 Stage D：token 鉴权（默认关闭；开启后不匹配直接断开，避免未授权客户端占用连接）
        if (config.isAuthEnabled() && !authPassed(rpcDto.getToken())) {
            log.warn("WhisperMQ Broker 鉴权失败，关闭连接：{}", ctx.channel().remoteAddress());
            sendErrorResponse(ctx, null, "鉴权失败");
            ctx.close();
            return;
        }
        String methodType = rpcDto.getMethodType();
        if (methodType == null) {
            log.error("WhisperMQ Broker 方法类型为null: {}", rpcDto);
            sendErrorResponse(ctx, null, "缺少方法类型");
            return;
        }
        FunctionEntity funcDto;
        try {
            funcDto = JSON.parseObject(rpcDto.getJson(), FunctionEntity.class);
        } catch (Exception e) {
            log.error("WhisperMQ Broker 解析FunctionEntity失败: {}", rpcDto.getJson(), e);
            sendErrorResponse(ctx, null, "FunctionEntity格式错误：" + e.getMessage());
            return;
        }
        if (funcDto == null) {
            log.error("WhisperMQ Broker FunctionEntity解析结果为null: {}", rpcDto.getJson());
            sendErrorResponse(ctx, null, "FunctionEntity格式错误");
            return;
        }

        String traceId = funcDto.getMessageId();
        log.info("WhisperMQ Broker 收到请求：method={}，traceId={}", methodType, traceId);

        // 消费者ID：用通道ID标识（唯一）
        //这个ChannelId是一个对象
        ChannelId channelId = ctx.channel().id();
        String consumerId = channelId.asLongText();

        try {
            // 根据方法类型处理不同请求
            switch (methodType) {

                //处理生产者发送消息 并告知生产者消息已收到
                case MethodType.P_SEND_MSG:
                    ProducerMessageDTO pmsDto=JSON.parseObject(rpcDto.getJson(), ProducerMessageDTO.class);
                    MqMessageEnduring mqMessage=JSON.parseObject(pmsDto.getJson(), MqMessageEnduring.class);
                    brokerManager.handlerMessage(mqMessage,funcDto.getMessageId(),false);
                    sendSuccessResponse(ctx,traceId,pmsDto.getCurrentVersion());
                    break;

                //处理消费者订阅消息并告知消费者消息已收到
                case MethodType.GROUP_SUBSCRIBE:
                    SubscribeReqDTO subscribeReqDTO = JSON.parseObject(rpcDto.getJson(), SubscribeReqDTO.class);
                    // 订阅失败时把原因回给客户端，而不是一律 SUCCESS
                    String subscribeError = groupManager.GroupSubscribe(subscribeReqDTO, ctx);
                    if (subscribeError == null) {
                        sendControlResponse(ctx, traceId, MethodType.B_SUBSCRIBE_RESPONSE);
                    } else {
                        sendErrorResponse(ctx, traceId, subscribeError);
                    }
                    break;

                //处理消费者组取消订阅主题
                case MethodType.GROUP_UNSUBSCRIBE:
                    SubscribeReqDTO subscribeReq = JSON.parseObject(rpcDto.getJson(), SubscribeReqDTO.class);
                    groupManager.groupUnsubscribe(subscribeReq.getGroupId(), subscribeReq.getTopic());
                    sendControlResponse(ctx, traceId, MethodType.B_CONTROL_RESPONSE);
                    break;

                //处理消费者加入消费者组
                case MethodType.CONSUMER_JOIN_GROUP:
                    //这里的rpcDto.getJson()是ConsumerThings类，需要再调用一下包装类，而funcDto.getJson()是直接传的组字符串，不需要类型来接收
                    groupManager.consumerJoinGroup(funcDto.getJson(), consumerId, ctx.channel());
                    sendControlResponse(ctx, traceId, MethodType.B_CONTROL_RESPONSE);
                    break;

                //处理消费者离开消费者组
                case MethodType.CONSUMER_LEAVE_GROUP:
                    //这里的funcDto.getJson()是直接传的组字符串，不需要类型来接收
                    groupManager.consumerLeaveGroup(funcDto.getJson(), consumerId);
                    sendControlResponse(ctx, traceId, MethodType.B_CONTROL_RESPONSE);
                    break;

                // 背压：暂停投递（fire-and-forget）
                case MethodType.C_PAUSE:
                    ConsumerFlowControlDTO pauseReq = JSON.parseObject(rpcDto.getJson(), ConsumerFlowControlDTO.class);
                    brokerManager.pauseConsumer(consumerId, pauseReq == null ? 0 : pauseReq.getDurationMillis());
                    break;

                // 背压：恢复投递，Broker 按组位点补投暂停期间的消息
                case MethodType.C_RESUME:
                    ConsumerFlowControlDTO resumeReq = JSON.parseObject(rpcDto.getJson(), ConsumerFlowControlDTO.class);
                    brokerManager.resumeConsumer(resumeReq == null ? null : resumeReq.getGroupId(), consumerId);
                    break;

                //处理消费者返回ack确认消息
                case MethodType.C_ACK_MSG:
                    ConsumerAckReqDTO ackReq = JSON.parseObject(rpcDto.getJson(), ConsumerAckReqDTO.class);
                    brokerManager.handleConsumerAck(ackReq, consumerId);
                    break;

                case MethodType.C_DEAD_MSG:
                    DeadLetterDTO deadLetter = JSON.parseObject(rpcDto.getJson(), DeadLetterDTO.class);
                    log.info("Broker 收到死信消息：{}", deadLetter.getMessageId());
                    brokerManager.handlerDeadLetter(deadLetter, funcDto.getMessageId());
                    break;

                // 心跳：刷新连接活跃状态，原样回 PONG
                case MethodType.HEARTBEAT_PING:
                    sendPong(ctx, traceId);
                    break;

                // 死信重放：把指定死信重新投递给原消费组
                case MethodType.DEAD_LETTER_REPLAY:
                    DeadLetterReplayReqDTO replayReq = JSON.parseObject(rpcDto.getJson(), DeadLetterReplayReqDTO.class);
                    String replayError = brokerManager.replayDeadLetter(
                            replayReq == null ? null : replayReq.getDeadLetterId());
                    if (replayError == null) {
                        sendSuccessResponse(ctx, traceId);
                    } else {
                        sendErrorResponse(ctx, traceId, replayError);
                    }
                    break;

                default:
                    log.error("Broker收到未知请求：" + rpcDto);
                    // 这里后续会添加消息存储和推送逻辑
            }
        }catch (Exception e){
            log.error("WhisperMQ Broker 处理消息异常", e);
            sendErrorResponse(ctx, traceId, "处理失败：" + e.getMessage());
        }

    }

    //通道断开 自动移除组内的消费者
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        String consumerId = ctx.channel().id().asLongText();
        log.info("WhisperMQ Broker 检测到通道已断开：{}（consumerId={}）", ctx.channel().remoteAddress(), consumerId);
        // 修复：旧实现命中第一个组就 break，且最后还多调用了一次 consumerLeaveGroup(consumerId, consumerId)。
        // 现在遍历全部消费者组，把该连接从所有组中摘除（同一连接理论上只属于一个组，
        // 但遍历全部才能保证脏数据/重复加入时不残留）。
        for (Map.Entry<String, ConsumerGroup> entry : groupManager.getGroupMap().entrySet()) {
            ConsumerGroup group = entry.getValue();
            if (group.getOnlineConsumers().containsKey(consumerId)) {
                groupManager.consumerLeaveGroup(entry.getKey(), consumerId);
                // 该消费者手上未确认的消息重新投递给组内其他消费者
                brokerManager.onConsumerOffline(entry.getKey(), consumerId);
            }
        }
    }

    // 心跳/空闲检测：读空闲说明对端已失联（或未按约定发心跳），主动关闭连接释放资源
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent idleEvent && idleEvent.state() == IdleState.READER_IDLE) {
            log.warn("WhisperMQ Broker 连接读空闲超时，关闭连接：{}", ctx.channel().remoteAddress());
            ctx.close();
            return;
        }
        super.userEventTriggered(ctx, evt);
    }

    /** 心跳响应 */
    private void sendPong(ChannelHandlerContext ctx, String traceId) {
        ConsumerThings consumerThings = new ConsumerThings();
        consumerThings.setMessageId(traceId);
        consumerThings.setJson(MethodType.HEARTBEAT_PONG);
        RpcMessageDTO response = new RpcMessageDTO();
        response.setRequest(false);
        response.setMethodType(MethodType.HEARTBEAT_PONG);
        response.setJson(JSON.toJSONString(consumerThings));
        ctx.writeAndFlush(JSON.toJSONString(response) + "\n");
    }

    // 发送成功响应
    private void sendSuccessResponse(ChannelHandlerContext ctx, String traceId) {
        ConsumerThings consumerThings = new ConsumerThings();
        consumerThings.setMessageId(traceId);
        consumerThings.setJson("SUCCESS77");
        RpcMessageDTO response = new RpcMessageDTO();
        response.setRequest(false);
        response.setJson(JSON.toJSONString(consumerThings));
        ctx.writeAndFlush(JSON.toJSONString(response) + "\n");
    }

    /**
     * 控制类请求（入组/退组/订阅）的确认响应。
     * 带上原始 traceId，客户端据此唤醒 groupSubscribe / joinGroup 的等待者。
     */
    private void sendControlResponse(ChannelHandlerContext ctx, String traceId, String methodType) {
        ConsumerThings consumerThings = new ConsumerThings();
        consumerThings.setMessageId(traceId);
        consumerThings.setJson(methodType);
        RpcMessageDTO response = new RpcMessageDTO();
        response.setRequest(false);
        response.setMethodType(methodType);
        response.setJson(JSON.toJSONString(consumerThings));
        ctx.writeAndFlush(JSON.toJSONString(response) + "\n");
    }

    //发送成功响应给生产者确认
    private void sendSuccessResponse(ChannelHandlerContext ctx, String traceId,long currentVersion) {
        RpcMessageDTO response = new RpcMessageDTO();
        ProducerMessageDTO pmsDto= new ProducerMessageDTO();
        pmsDto.setMessageId(traceId);
        pmsDto.setCurrentVersion(currentVersion);
        response.setRequest(false);
        response.setJson(JSON.toJSONString(pmsDto));
        response.setMethodType(MethodType.P_CONFIRM_MSG);
        ctx.writeAndFlush(JSON.toJSONString(response) + "\n");
    }

    /**
     * 发送统一错误响应。
     *
     * <p>旧实现把 json 赋值两次（ErrorResponse 的 JSON 被 "ERROR:xxx" 覆盖），
     * 而且没有 methodType，客户端把它当正常 JSON 解析必然抛异常，
     * 最终表现为"客户端超时"而不是一个明确的错误。</p>
     *
     * @param traceId 原始请求的 messageId，客户端据此把错误关联到等待中的 send()
     */
    private void sendErrorResponse(ChannelHandlerContext ctx, String traceId, String msg) {
        ErrorResponse errorResponse = new ErrorResponse();
        errorResponse.setMessageId(traceId);
        errorResponse.setJson(msg == null ? "unknown error" : msg);

        RpcMessageDTO response = new RpcMessageDTO();
        response.setRequest(false);
        response.setMethodType(MethodType.ERROR);
        response.setJson(JSON.toJSONString(errorResponse));
        ctx.writeAndFlush(JSON.toJSONString(response) + "\n");
    }

    /** 常量时间比较 token，避免时序侧信道 */
    private boolean authPassed(String token) {
        String expected = config.getAuthToken();
        if (expected == null || expected.isEmpty() || token == null) {
            return false;
        }
        return java.security.MessageDigest.isEqual(
                token.getBytes(StandardCharsets.UTF_8),
                expected.getBytes(StandardCharsets.UTF_8));
    }

}