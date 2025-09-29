package org.guoguo.producer.handler;

import com.alibaba.fastjson.JSON;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.guoguo.common.pojo.DTO.ProducerMessageDTO;
import org.guoguo.common.pojo.DTO.RpcMessageDTO;
import org.guoguo.producer.service.impl.MqProducer;
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
判断为响应消息(!isRequest)
   ↓
调用producer.setResponse()将结果返回给生产者
 */
public class MqProducerHandler extends SimpleChannelInboundHandler<String> {
    private final MqProducer producer;

    public MqProducerHandler(MqProducer producer) {
        this.producer = producer;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) {
        RpcMessageDTO rpcDto = JSON.parseObject(msg, RpcMessageDTO.class);
        if (!rpcDto.isRequest()) {
            //如果是响应
            //返回id和消息类型
            ProducerMessageDTO producerDto = JSON.parseObject(rpcDto.getJson(), ProducerMessageDTO.class);
            producer.setResponse(producerDto.getMessageId(),rpcDto.getMethodType(),producerDto.getCurrentVersion());
        }
    }
}