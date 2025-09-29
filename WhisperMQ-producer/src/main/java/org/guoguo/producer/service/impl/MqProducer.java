// org.guoguo.producer.service.impl.MqProducer.java
package org.guoguo.producer.service.impl;

import com.alibaba.fastjson.JSON;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

import org.guoguo.common.config.MqConfigProperties;
import org.guoguo.common.constant.MethodType;
import org.guoguo.common.pojo.DTO.ProducerMessageDTO;
import org.guoguo.common.pojo.DTO.RpcMessageDTO;
import org.guoguo.common.pojo.Entity.MqMessageEnduring;
import org.guoguo.common.pojo.Entity.VersionedLatch;
import org.guoguo.producer.constant.ResultCodeEnum;
import org.guoguo.producer.handler.MqProducerHandler;
import org.guoguo.producer.pojo.Result;

import org.guoguo.common.util.SnowflakeIdGeneratorUtil;
import org.guoguo.producer.service.IMqProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component // 交给 Spring 管理
public class MqProducer implements IMqProducer {
    private final MqConfigProperties config;
    private Channel channel;
    private EventLoopGroup group;

    // 注入配置
    @Autowired
    public MqProducer(MqConfigProperties config) {
        this.config = config;
    }

    // 初始化时连接 Broker
    @PostConstruct
    public void start() {
        group = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<>() {
                        @Override
                        protected void initChannel(Channel ch) {
                            // 1. 添加分隔符处理器：以换行符 \n 作为消息结束标志
                            // 最大帧长度 1024*1024（1MB），超过则抛异常
                            ByteBuf delimiter = Unpooled.copiedBuffer("\n".getBytes());
                            ch.pipeline()
                                    .addLast(new DelimiterBasedFrameDecoder(1024 * 1024, delimiter))
                                    .addLast(new StringDecoder())
                                    .addLast(new StringEncoder())
                                    .addLast(new MqProducerHandler(MqProducer.this));
                        }
                    });

            // 从配置中获取 Broker 地址和端口
            ChannelFuture future = bootstrap.connect(config.getBrokerHost(), config.getBrokerPort()).sync();
            this.channel = future.channel();
            log.info("生产者连接 Broker 成功：{}:{}", config.getBrokerHost(), config.getBrokerPort());
        } catch (Exception e) {
            log.error("生产者连接 Broker 失败", e);
        }
    }


    /*//使用ConcurrentHashMap存储traceId与latch等待器的映射
    private final ConcurrentHashMap<String, CountDownLatch> traceIdLatchMap = new ConcurrentHashMap<>();*/

    // 存储结构改为：traceId -> 带版本的Latch
    private final ConcurrentHashMap<String, VersionedLatch> traceIdLatchMap = new ConcurrentHashMap<>();

    //使用AckConfirmHashMap存储生产者已被确认的消息
    private final ConcurrentHashMap<String,MqMessageEnduring> ackConfirmHashMap = new ConcurrentHashMap<>();

    @Override
    //使用MqMessageEnduring类封装MqMessage消息实体类，添加持久化选择字段
    public Result<String> send(MqMessageEnduring message) {
        try {
            //封装消息功能层DTO
            ProducerMessageDTO producerMessageDTO = new ProducerMessageDTO();
            producerMessageDTO.setJson(JSON.toJSONString(message));
            //为每个消息通过雪花算法创建独立id
            String currentTraceId = producerMessageDTO.getMessageId();
            RpcMessageDTO rpcMessageDTO = new RpcMessageDTO();
            rpcMessageDTO.setRequest(true);
            rpcMessageDTO.setMethodType(MethodType.P_SEND_MSG);

            boolean await;
            //超时重传机制
            for (long i = 0; i <= config.getProducerRetryCountLimit(); i++){

                long startTime = System.currentTimeMillis();
                // 为每个消息创建独立的等待器
                VersionedLatch latch = new VersionedLatch(1);
                //获得此等待器版本
                long currentVersion = latch.getVersion();
                //设置并封装当前消息版本，使得返回确认消息时携带
                producerMessageDTO.setCurrentVersion(currentVersion);
                rpcMessageDTO.setJson(JSON.toJSONString(producerMessageDTO));
                // 使用ConcurrentHashMap存储traceId与latch的映射
                traceIdLatchMap.put(currentTraceId, latch);
                // 发送消息
                channel.writeAndFlush(JSON.toJSONString(rpcMessageDTO) + "\n");
                // 等待确认或超时
                log.info("等待生产者发送消息，重试次数为：{}，消息Id：{}",i,currentTraceId);
                await = latch.await((long) (config.getProducerRetryTimeCoefficient()*Math.pow(2L,i)), TimeUnit.MILLISECONDS);
                // 5. 校验唤醒是否有效（必须是当前版本的latch被唤醒）
                VersionedLatch storedLatch = traceIdLatchMap.get(currentTraceId);


                //解除等待返回true，超时则返回false，校验唤醒是否有效
                if(await&&storedLatch!=null&&storedLatch.getVersion()==currentVersion){
                        //确认是当前版本的latch被正常唤醒（非超时、非错误唤醒），返回确认响应
                    //存储生产者已被确认的消息
                    ackConfirmHashMap.put(currentTraceId,message);
                    return Result.ok("消息发送成功", currentTraceId);
                }
                // 清理映射关系
                traceIdLatchMap.remove(currentTraceId);
                //获取当前时间
                long endTime = System.currentTimeMillis();
                log.info("生产者第{}次发送失败，本次等待时间应该为：{} ，实际等待时间为：{},消息id:{}"
                        ,i,(long)config.getProducerRetryTimeCoefficient()*Math.pow(2L, i),endTime-startTime,currentTraceId);
            }
            //超时返回失败
            log.info("消息发送超时，返回失败");
            // 清理映射关系
            traceIdLatchMap.remove(currentTraceId);
            return Result.build("消息发送超时", ResultCodeEnum.FAILED, currentTraceId);

        } catch (Exception e) {
            return Result.build("消息发送失败", ResultCodeEnum.FAILED,null);
        }
    }
/*
 *
 * @author jjs
 * date 2025/9/12 22:20
 * @param null
 * description:目前超时重传仍存在的问题（优化点）
 *
 * CountDownLatch 的并发问题
如果在latch.await()返回前，有其他线程调用了该 latch 的countDown()，可能导致错误唤醒
重试时创建新的 latch 并覆盖 map 中的值，但旧的 latch 可能还在等待，会造成内存泄漏
异常处理仍不完善
channel.writeAndFlush()可能抛出异常，但当前代码会直接进入 catch 块返回失败
异常情况下currentTraceId可能为 null，导致traceIdLatchMap.remove(currentTraceId)抛出 NPE
消息重复发送风险
如果服务端已经收到消息并返回确认，但由于网络原因客户端没收到，会触发重试导致消息重复
Map 清理时机问题
每次循环结束都清理 map，但如果在latch.await()之后、remove之前发生异常，仍可能导致内存泄漏

改进建议：

统一使用Math.pow()进行幂运算，避免日志与实际行为不一致
考虑使用ThreadLocal存储 latch，避免 map 的并发问题
完善异常处理，确保各种异常情况下都能正确清理资源
添加消息去重机制，比如基于currentTraceId的幂等性设计
可以使用try-finally块确保资源清理代码一定会执行
 *
 */







    // 关闭资源
    @PreDestroy
    public void close() {
        traceIdLatchMap.clear();
        ackConfirmHashMap.clear();
        if (group != null) group.shutdownGracefully();
        log.info("生产者已关闭");
    }

    public void setResponse(String traceId, String methodType, long version) {
        // 根据traceId获取对应的latch并释放
        VersionedLatch latch = traceIdLatchMap.get(traceId);
        if (latch != null&&latch.getVersion() == version) {
            // 验证确认类型
            if (MethodType.P_CONFIRM_MSG.equals(methodType)) {
                latch.countDown(version);
            }
        }
    }

}