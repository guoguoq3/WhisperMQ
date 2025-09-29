// org.guoguo.consumer.service.impl.MqConsumer.java
package org.guoguo.consumer.service.impl;

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
import org.awaitility.reflect.FieldAnnotationMatcherStrategy;
import org.guoguo.common.config.MqConfigProperties;
import org.guoguo.consumer.handler.MqConsumerHandler;
import org.guoguo.consumer.service.IMessageListener;
import org.guoguo.consumer.service.IMqConsumer;
import org.guoguo.common.util.SnowflakeIdGeneratorUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component // 交给 Spring 管理
public abstract class MqConsumer implements IMqConsumer {
    private final MqConfigProperties config;
    protected Channel channel;
    private EventLoopGroup group;

    protected final Map<String, IMessageListener> topicListenerMap = new HashMap<>();


    // 延迟注入 Handler（由 Spring 管理，避免构造时依赖）
    private MqConsumerHandler mqConsumerHandler;
    // 注入配置
    @Autowired
    public MqConsumer( MqConfigProperties config) {
        this.config = config;

    }


    // 初始化时连接 Broker
    @PostConstruct
    @Override
    public void start() {
        mqConsumerHandler = new MqConsumerHandler(this);
        group = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) {
                            // 1. 添加分隔符处理器：以换行符 \n 作为消息结束标志
                            // 最大帧长度 1024*1024（1MB），超过则抛异常
                            ByteBuf delimiter = Unpooled.copiedBuffer("\n".getBytes());
                            ch.pipeline()
                                    .addLast(new DelimiterBasedFrameDecoder(1024 * 1024, delimiter))
                                    .addLast(new StringDecoder())
                                    .addLast(new StringEncoder())
                                    .addLast(mqConsumerHandler);
                        }
                    });

            // 从配置中获取 Broker 地址和端口
            ChannelFuture future = bootstrap.connect(config.getBrokerHost(), config.getBrokerPort()).sync();
            this.channel = future.channel();
            log.info("消费者连接 Broker 成功：{}:{}", config.getBrokerHost(), config.getBrokerPort());
        } catch (Exception e) {
            log.error("消费者连接 Broker 失败", e);
        }
    }

    public Map<String, IMessageListener> getTopicListenerMap() {
        return Collections.unmodifiableMap(topicListenerMap);
    }


    @PreDestroy
    public void close() {
        if (group != null) {
            group.shutdownGracefully();
            log.info("WhisperMQ 消费者关闭，释放资源");
        }
    }
}