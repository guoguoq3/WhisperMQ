package org.guoguo.broker.handler;

import com.alibaba.fastjson.JSON;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.broker.ConsumerGroup.ConsumerGroupManager;
import org.guoguo.broker.core.BrokerManager;
import org.guoguo.common.pojo.DTO.ConsumerAckReqDTO;
import org.guoguo.common.pojo.DTO.ProducerMessageDTO;
import org.guoguo.common.pojo.Entity.*;
        import org.guoguo.common.pojo.DTO.RpcMessageDTO;
import org.guoguo.common.constant.MethodType;
import org.guoguo.common.pojo.DTO.SubscribeReqDTO;
import org.guoguo.common.util.SnowflakeIdGeneratorUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Slf4j
@Component
//每次有新的连接调用这个 都会新建一个实例 以前不报错 是因为每次我都新建new一个
@ChannelHandler.Sharable
public class MqBrokerHandler extends SimpleChannelInboundHandler<String> {
    // 获取Broker管理器实例
    private final BrokerManager brokerManager;
    private final ConsumerGroupManager groupManager;


    @Autowired
    public MqBrokerHandler(BrokerManager brokerManager, ConsumerGroupManager groupManager) {
        this.brokerManager = brokerManager;
        this.groupManager = groupManager;
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
            sendErrorResponse(ctx, "消息格式错误：" + e.getMessage());
            return;
        }
        if (rpcDto == null) {
            log.error("WhisperMQ Broker RPC消息解析结果为null: {}", cleanMsg);
            sendErrorResponse(ctx, "消息格式错误");
            return;
        }
        String methodType = rpcDto.getMethodType();
        if (methodType == null) {
            log.error("WhisperMQ Broker 方法类型为null: {}", rpcDto);
            sendErrorResponse(ctx, "缺少方法类型");
            return;
        }
        FunctionEntity funcDto;
        try {
            funcDto = JSON.parseObject(rpcDto.getJson(), FunctionEntity.class);
        } catch (Exception e) {
            log.error("WhisperMQ Broker 解析FunctionEntity失败: {}", rpcDto.getJson(), e);
            sendErrorResponse(ctx, "FunctionEntity格式错误：" + e.getMessage());
            return;
        }
        if (funcDto == null) {
            log.error("WhisperMQ Broker FunctionEntity解析结果为null: {}", rpcDto.getJson());
            sendErrorResponse(ctx, "FunctionEntity格式错误");
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
                //TODO 处理生产者重传的逻辑判断，避免重复消费和消息丢失
                case MethodType.P_SEND_MSG:
                    ProducerMessageDTO pmsDto=JSON.parseObject(rpcDto.getJson(), ProducerMessageDTO.class);
                    MqMessageEnduring mqMessage=JSON.parseObject(pmsDto.getJson(), MqMessageEnduring.class);
                    brokerManager.handlerMessage(mqMessage,funcDto.getMessageId());
                    sendSuccessResponse(ctx,traceId,pmsDto.getCurrentVersion());
                    break;

                //处理消费者订阅消息并告知消费者消息已收到
                case MethodType.GROUP_SUBSCRIBE:
                    SubscribeReqDTO subscribeReqDTO = JSON.parseObject(rpcDto.getJson(), SubscribeReqDTO.class);
                    groupManager.GroupSubscribe(subscribeReqDTO);
                    //前面是消费者消息回溯 这里回溯完返回订阅响应结果
                    sendSuccessResponse(ctx,traceId);
                    break;

                //处理消费者组取消订阅主题
                case MethodType.GROUP_UNSUBSCRIBE:
                    SubscribeReqDTO subscribeReq = JSON.parseObject(rpcDto.getJson(), SubscribeReqDTO.class);
                    groupManager.groupUnsubscribe(subscribeReq.getGroupId(), subscribeReq.getTopic());
                    sendSuccessResponse(ctx,traceId);
                    break;

                //处理消费者加入消费者组
                case MethodType.CONSUMER_JOIN_GROUP:
                    //这里的rpcDto.getJson()是ConsumerThings类，需要再调用一下包装类，而funcDto.getJson()是直接传的组字符串，不需要类型来接收
                    groupManager.consumerJoinGroup(funcDto.getJson(), consumerId, ctx.channel());
                    sendSuccessResponse(ctx,traceId);
                    break;

                //处理消费者离开消费者组
                case MethodType.CONSUMER_LEAVE_GROUP:
                    //这里的funcDto.getJson()是直接传的组字符串，不需要类型来接收
                    groupManager.consumerLeaveGroup(funcDto.getJson(), consumerId);
                    sendSuccessResponse(ctx, traceId);
                    break;

                //处理消费者返回ack确认消息
                case MethodType.C_ACK_MSG:
                    ConsumerAckReqDTO ackReq = JSON.parseObject(rpcDto.getJson(), ConsumerAckReqDTO.class);
                    brokerManager.handleConsumerAck(ackReq,ackReq.getGroupId());
                    break;

                default:
                    log.error("Broker收到未知请求：" + rpcDto);
                    // 这里后续会添加消息存储和推送逻辑
            }
        }catch (Exception e){
            log.error("WhisperMQ Broker 处理消息异常", e);
            sendErrorResponse(ctx, "处理失败：" + e.getMessage());
        }

    }

    //通道断开 自动移除组内的消费者
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        String consumerId = ctx.channel().id().asLongText();
        log.info("WhisperMQ Broker 检测到消费者通道已断开：{}", ctx.channel().remoteAddress());
        for (Map.Entry<String, ConsumerGroup> entry : groupManager.getGroupMap().entrySet()) {
            ConsumerGroup group = entry.getValue();
            if (group.getOnlineConsumers().containsKey(consumerId)){
                groupManager.consumerLeaveGroup(entry.getKey(),consumerId);
                break;
            }
        }
        groupManager.consumerLeaveGroup(ctx.channel().id().asLongText(), ctx.channel().id().asLongText());
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
    private void sendSuccessResponse(ChannelHandlerContext ctx, String traceId, String methodType) {
        ConsumerThings  consumerThings = new ConsumerThings();
        consumerThings.setMessageId(traceId);
        consumerThings.setJson("SUCCESS77");
        RpcMessageDTO response = new RpcMessageDTO();
        response.setRequest(false);
        response.setMethodType(methodType);

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

    // 发送错误响应
    private void sendErrorResponse(ChannelHandlerContext ctx, String msg) {
        RpcMessageDTO response = new RpcMessageDTO();
        response.setRequest(false);
        ErrorResponse errorResponse = new ErrorResponse();
        errorResponse.setMessageId(String.valueOf(System.currentTimeMillis()));
        response.setJson(JSON.toJSONString(errorResponse));
        response.setJson("ERROR:" + msg);
        ctx.writeAndFlush(JSON.toJSONString(response) + "\n");
    }

}