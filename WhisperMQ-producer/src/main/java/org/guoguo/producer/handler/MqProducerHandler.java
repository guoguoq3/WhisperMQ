package org.guoguo.producer.handler;

import com.alibaba.fastjson2.JSON;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.common.constant.MethodType;
import org.guoguo.common.pojo.Entity.ConsumerThings;
import org.guoguo.common.pojo.DTO.RpcMessageDTO;
import org.guoguo.common.pojo.Entity.ErrorResponse;
import org.guoguo.producer.service.impl.MqProducer;
import org.guoguo.common.pojo.DTO.ProducerMessageDTO;
/*
用于处理从消息代理（broker）返回的响应消息 当通道接收到数据时，Netty会自动调用channelRead0方法
Broker发送响应消息
   ↓
Netty接收数据并解码为字符串
   ↓
调用MqProducerHandler.channelRead0()
   ↓
解析JSON消息为RpcMessageDTO对象
   ↓
按 methodType 分派：ERROR / PONG / 确认
 */
@Slf4j
public class MqProducerHandler extends SimpleChannelInboundHandler<String> {
    private final MqProducer producer;

    public MqProducerHandler(MqProducer producer) {
        this.producer = producer;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) {
        String cleanMsg = msg.trim();
        if (cleanMsg.isEmpty()) {
            return;
        }
        RpcMessageDTO rpcDto;
        try {
            rpcDto = JSON.parseObject(cleanMsg, RpcMessageDTO.class);
        } catch (Exception e) {
            log.error("生产者解析 Broker 响应失败：{}", cleanMsg, e);
            return;
        }
        if (rpcDto == null) {
            return;
        }
        String methodType = rpcDto.getMethodType();

        // 1. 统一错误响应：结构化解析，并把失败原因回传给等待中的 send()
        if (MethodType.ERROR.equals(methodType)) {
            String traceId = null;
            String reason = rpcDto.getJson();
            try {
                ErrorResponse error = JSON.parseObject(rpcDto.getJson(), ErrorResponse.class);
                if (error != null) {
                    traceId = error.getMessageId();
                    reason = error.getJson();
                }
            } catch (Exception ignored) {
                // 解析失败就按原始字符串处理
            }
            log.error("生产者收到 Broker 错误响应：traceId={}，原因={}", traceId, reason);
            producer.onErrorMessage(traceId, reason);
            return;
        }

        // 2. 心跳响应
        if (MethodType.HEARTBEAT_PONG.equals(methodType)) {
            log.debug("生产者收到 Broker 心跳响应");
            return;
        }

        // 3. 业务确认
        if (rpcDto.isRequest()) {
            return;
        }
        ProducerMessageDTO producerDto = JSON.parseObject(rpcDto.getJson(), ProducerMessageDTO.class);
        if (producerDto == null) {
            log.warn("生产者收到无法解析的确认响应：{}", cleanMsg);
            return;
        }
        producer.setResponse(producerDto.getMessageId(), methodType, producerDto.getCurrentVersion());
    }

    /** 写空闲：向 Broker 发送心跳，避免连接被中间设备/服务端空闲检测回收 */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent idleEvent && idleEvent.state() == IdleState.WRITER_IDLE) {
            ConsumerThings things = new ConsumerThings();
            things.setJson(MethodType.HEARTBEAT_PING);
            RpcMessageDTO ping = new RpcMessageDTO();
            ping.setRequest(true);
            ping.setMethodType(MethodType.HEARTBEAT_PING);
            ping.setJson(JSON.toJSONString(things));
            ctx.writeAndFlush(JSON.toJSONString(ping) + "\n");
            log.debug("生产者发送心跳");
            return;
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("生产者通道异常，关闭连接等待重连", cause);
        ctx.close();
    }
}
