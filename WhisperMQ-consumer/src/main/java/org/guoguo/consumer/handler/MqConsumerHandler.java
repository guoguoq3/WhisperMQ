package org.guoguo.consumer.handler;

import com.alibaba.fastjson2.JSON;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.common.constant.MethodType;
import org.guoguo.common.pojo.DTO.PushMessageDTO;
import org.guoguo.common.pojo.DTO.RpcMessageDTO;
import org.guoguo.common.pojo.Entity.ConsumerThings;
import org.guoguo.common.pojo.Entity.ErrorResponse;
import org.guoguo.common.pojo.Entity.FunctionEntity;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.guoguo.consumer.service.impl.MqConsumer;
import org.guoguo.consumer.service.impl.MqConsumerManager;

/**
 * 消费者网络层 Handler。
 *
 * <p>P1 改造点：
 * <ul>
 *   <li>不再在 IO 线程上执行业务监听器：收到 B_PUSH_MSG 后只做解析，然后交给
 *       {@link MqConsumerManager#dispatchPush} 的业务线程池；</li>
 *   <li>识别 B_SUBSCRIBE_RESPONSE / B_CONTROL_RESPONSE / ERROR，按 traceId 唤醒
 *       正在等待订阅确认的调用线程。</li>
 * </ul>
 *
 * <p>注意：本类由 {@link MqConsumer#start()} 手工 new（每个消费者一个实例），
 * 重连时会把同一个实例加入新连接的 pipeline，因此必须标注 {@link ChannelHandler.Sharable}；
 * 但不能标注 @Component —— 否则 Spring 会为它构造一个“幽灵” MqConsumerManager，
 * 多开一条不参与消费的连接。</p>
 */
@Slf4j
@ChannelHandler.Sharable
public class MqConsumerHandler extends SimpleChannelInboundHandler<String> {
    private final MqConsumer consumer;

    public MqConsumerHandler(MqConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) {

        try {
            String cleanMsg = msg.trim();
            if (cleanMsg.isEmpty()) {
                return;
            }
            RpcMessageDTO rpcDto = JSON.parseObject(cleanMsg, RpcMessageDTO.class);
            if (rpcDto == null) {
                log.warn("消费者收到无法解析的消息：{}", cleanMsg);
                return;
            }
            String methodType = rpcDto.getMethodType();

            // 1. 统一错误响应：唤醒等待该 traceId 的调用线程（入组/订阅），而不是只打日志
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
                    // 保留原始字符串
                }
                log.error("WhisperMQ 消费者收到 Broker 错误响应：traceId={}，原因={}", traceId, reason);
                consumer.completeResponse(traceId, new RuntimeException(String.valueOf(reason)));
                return;
            }

            // 2. 心跳响应
            if (MethodType.HEARTBEAT_PONG.equals(methodType)) {
                log.debug("消费者收到 Broker 心跳响应");
                return;
            }

            // 3. 控制类请求的确认（入组/退组/订阅）：按 traceId 唤醒等待者
            if (MethodType.B_SUBSCRIBE_RESPONSE.equals(methodType)
                    || MethodType.B_CONTROL_RESPONSE.equals(methodType)) {
                FunctionEntity ack = JSON.parseObject(rpcDto.getJson(), FunctionEntity.class);
                String traceId = ack == null ? null : ack.getMessageId();
                log.info("WhisperMQ 消费者收到控制响应：methodType={}，traceId={}", methodType, traceId);
                consumer.completeResponse(traceId, null);
                return;
            }

            // 4. Broker 推送消息：解析后交给业务线程池，立即返回，不阻塞 IO 线程
            if (MethodType.B_PUSH_MSG.equals(methodType)) {
                PushMessageDTO pushMsg = JSON.parseObject(rpcDto.getJson(), PushMessageDTO.class);
                if (pushMsg == null || pushMsg.getJson() == null) {
                    log.warn("消费者收到格式非法的推送消息：{}", cleanMsg);
                    return;
                }
                String messageId = pushMsg.getMessageId();
                MqMessage message = JSON.parseObject(pushMsg.getJson(), MqMessage.class);
                ((MqConsumerManager) consumer).dispatchPush(messageId, message.getTopic(), message);
                return;
            }

            // 5. 其它响应（兼容老 Broker 的无 methodType 响应）
            FunctionEntity functionEntity = JSON.parseObject(rpcDto.getJson(), FunctionEntity.class);
            if (!rpcDto.isRequest() && functionEntity != null) {
                String response = functionEntity.getJson();
                if (response != null && response.startsWith("ERROR")) {
                    log.error("WhisperMQ 消费者收到Broker错误响应：{}", response);
                } else {
                    log.info("WhisperMQ 消费者收到Broker响应：{}", response);
                }
            }

        } catch (Exception e) {
            log.error("WhisperMQ 消费者处理消息异常", e);
        }

    }

    /** 写空闲：发送心跳保活 */
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
            log.debug("消费者发送心跳");
            return;
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("WhisperMQ 消费者通道异常", cause);
        ctx.close(); // 异常时关闭通道，由 MqConsumer 的重连逻辑接管
    }

    /**
     * 通道断开：只记录日志。
     * 旧实现这里调用 close()，会把 EventLoopGroup 一起关掉，导致永远无法重连。
     * 重连由 MqConsumer 注册的 closeFuture 监听器统一触发。
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.warn("WhisperMQ 消费者与Broker断开连接，等待自动重连");
    }

}
