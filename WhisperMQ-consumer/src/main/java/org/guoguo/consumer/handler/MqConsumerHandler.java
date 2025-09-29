package org.guoguo.consumer.handler;

import com.alibaba.fastjson.JSON;
import io.netty.channel.*;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.common.constant.MethodType;
import org.guoguo.common.pojo.DTO.RpcMessageDTO;
import org.guoguo.common.pojo.Entity.FunctionEntity;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.guoguo.common.pojo.DTO.PushMessageDTO;
import org.guoguo.consumer.service.impl.MqConsumer;
import org.guoguo.consumer.service.impl.MqConsumerManager;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@ChannelHandler.Sharable
public class MqConsumerHandler extends SimpleChannelInboundHandler<String> {
    // 仅依赖 MqConsumerManager（包含所有业务逻辑，无需依赖父类 MqConsumer）
   // private final MqConsumerManager mqConsumerManager;

    private MqConsumer consumer;


    // 注入 MqConsumerManager，移除对 MqConsumer 的冗余依赖
//    @Autowired
//    public MqConsumerHandler(@Lazy MqConsumerManager mqConsumerManager) {
//        this.mqConsumerManager = mqConsumerManager;
//    }

    public MqConsumerHandler(MqConsumer consumer) {
        this.consumer = consumer;
    }
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) {

       try {
           // 解析Broker消息（去除换行符）
           String cleanMsg = msg.trim();
           // 解析 Broker 发送的消息
           RpcMessageDTO rpcDto = JSON.parseObject(msg, RpcMessageDTO.class);
           String methodType = rpcDto.getMethodType();
           FunctionEntity functionEntity = JSON.parseObject(rpcDto.getJson(), FunctionEntity.class);
           log.info("消费者收到消息：{}", functionEntity.getJson());

          //监听各处传来的消息
           if (MethodType.B_PUSH_MSG.equals(methodType)) {
               // 处理消息推送（原逻辑不变）
               PushMessageDTO pushMsg = JSON.parseObject(rpcDto.getJson(), PushMessageDTO.class);
               String messageId = pushMsg.getMessageId();
               MqMessage message = JSON.parseObject(pushMsg.getJson(), MqMessage.class);
               String topic = message.getTopic();


               boolean handleSuccess = ((MqConsumerManager)consumer).handlePushMessage(messageId, topic, message); // 调用监听器，返回处理结果
               // 2. 处理成功则发送 ACK 给 Broker
               if (handleSuccess) {
                   ((MqConsumerManager)consumer).sendAck(messageId, "ACK_SUCCESS");
               } else {
                   // 处理失败可发送FAIL，后续Broker可重试（可选扩展）
                   ((MqConsumerManager)consumer).sendAck(messageId, "FAIL");
                   log.warn("WhisperMQ 消费者处理消息{}失败，发送FAIL ACK", messageId);
               }


               // 处理Broker的响应（如加入组成功、订阅成功）统一返回
           }else if (!rpcDto.isRequest()) {
                   String response = functionEntity.getJson();
                   if (response.startsWith("ERROR")) {
                       log.error("WhisperMQ 消费者收到Broker错误响应：{}", response);
                   } else {
                       log.info("WhisperMQ 消费者收到Broker成功响应：{}", response);
                   }
           }

       }catch (Exception e){
           log.error("WhisperMQ 消费者处理消息异常", e);
       }

    }

//    /**
//     * 向 Broker 发送消费确认（ACK）
//     *
//     * @param channel   消费者与 Broker 的连接通道
//     * @param messageId 要确认的消息 ID
//     */
//    private void sendAckToBroker(Channel channel, String messageId,String consumerGroup) {
//        if (!channel.isActive()) {
//            log.error("WhisperMQ 消费者通道已断开，无法发送 ACK：消息 ID={}", messageId);
//            return;
//        }
//        // 1. 生成消费者唯一标识（用通道 ID，确保同一消费者的唯一性）
//        ChannelId channelId = channel.id();
//        String consumerId = channelId.asLongText(); // 通道的长文本唯一 ID，避免重复
//        // 2. 构造 ACK 请求 DTO
//        ConsumerAckReqDTO ackReq = new ConsumerAckReqDTO();
//        ackReq.setMessageId(messageId);
//        ackReq.setConsumerId(consumerId);
//        ackReq.setAckStatus("SUCCESS"); // 处理成功，状态为 SUCCESS
//        ackReq.setGroupId(consumerGroup);
//        // 3. 封装为 RPC 消息，发送给 Broker
//        RpcMessageDTO rpcDto = new RpcMessageDTO();
//        rpcDto.setRequest(true); // 这是消费者向 Broker 的请求
//        rpcDto.setTraceId(String.valueOf(System.currentTimeMillis())); // 生成临时 traceId
//        rpcDto.setMethodType(MethodType.C_ACK_MSG); // 协议类型：消费确认
//        rpcDto.setJson(JSON.toJSONString(ackReq));
//        // 发送 ACK 消息
//        channel.writeAndFlush(JSON.toJSONString(rpcDto) + "\n");
//        log.info("WhisperMQ 消费者向 Broker 发送 ACK：消息 ID={}，消费者 ID={}", messageId, consumerId);
//    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error ("WhisperMQ 消费者通道异常", cause);
        ctx.close (); // 异常时关闭通道
    }


//    public boolean handlerMessage(String topic, MqMessage message) {
//        IMessageListener listener =consumer.getTopicListenerMap().get(topic);
//        if (listener == null) {
//            log.error("WhisperMQ 消费者无主题{}的监听器，消息处理失败", topic);
//            return false;
//        }
//        try {
//            // 调用用户自定义的监听器逻辑，返回处理结果
//            return listener.onMessage(message);
//        } catch (Exception e) {
//            log.error("WhisperMQ 消费者监听器处理消息异常：主题={}", topic, e);
//            return false;
//        }
//    }

    /**
     * 通道断开时：触发消费者关闭（避免资源泄漏）
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.warn("WhisperMQ 消费者与Broker断开连接");
        ((MqConsumerManager)consumer).close();
    }

}
