// org.guoguo.broker.core.MqBroker.java
package org.guoguo.broker.core;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.broker.ConsumerGroup.ConsumerGroupManager;
import org.guoguo.broker.handler.MqBrokerHandler;
import org.guoguo.broker.util.FilePersistUtil;
import org.guoguo.broker.util.OffsetPersistUtil;
import org.guoguo.common.config.MqConfigProperties;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component // 交给 Spring 管理
public class MqBroker {
    private final MqConfigProperties config;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private FilePersistUtil filePersistUtil;



   private final MqBrokerHandler mqBrokerHandler;
//   private final OffsetPersistUtil offsetPersistUtil;


    // 构造器注入配置
    @Autowired
    public MqBroker(MqConfigProperties config, MqBrokerHandler mqBrokerHandler,BrokerManager brokerManager, FilePersistUtil filePersistUtil) {
        this.config = config;
        this.mqBrokerHandler=mqBrokerHandler;
        this.filePersistUtil=filePersistUtil;
//        this.offsetPersistUtil=offsetPersistUtil;

    }

    // 启动时自动执行（初始化 Netty 服务）
    @PostConstruct
    public void start() throws InterruptedException {
        System.out.println(222222);
        //需要时创建
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<Channel>() {

                        @Override
                        protected void initChannel(Channel channel) {
                            // 1. 添加分隔符处理器：以换行符 \n 作为消息结束标志
                            // 最大帧长度 1024*1024（1MB），超过则抛异常
                            ByteBuf delimiter = Unpooled.copiedBuffer("\n".getBytes());
                            channel.pipeline()
                                    .addLast(new DelimiterBasedFrameDecoder(1024 * 1024, delimiter))
                                    .addLast(new StringDecoder())
                                    .addLast(new StringEncoder())
                                    .addLast(mqBrokerHandler);
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            // 从配置中获取端口
            ChannelFuture future = bootstrap.bind(config.getBrokerPort()).sync();
            log.info("Broker 启动成功，监听端口: {}", config.getBrokerPort());
            future.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    // 销毁时关闭资源
    @PreDestroy
    public void stop() {
        if (bossGroup != null) bossGroup.shutdownGracefully();
        if (workerGroup != null) workerGroup.shutdownGracefully();
//        offsetPersistUtil.close();
        log.info("Broker 已关闭");
    }
}