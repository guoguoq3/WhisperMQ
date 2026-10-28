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
import io.netty.handler.timeout.IdleStateHandler;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.broker.handler.MqBrokerHandler;
import org.guoguo.broker.store.SegmentMessageStore;
import org.guoguo.common.config.MqConfigProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Broker 网络入口。
 *
 * <p>P0 改造点：启动不再阻塞 Spring 容器线程。
 * 旧实现把 {@code closeFuture().sync()} 写在 {@code @PostConstruct} 里，导致
 * ApplicationContext.refresh() 永远无法返回：ApplicationReadyEvent 不触发，
 * MqBroker 之后定义的 Bean 不会初始化，容器生命周期（优雅关闭、健康检查）全部失效。</p>
 */
@Slf4j
@Component // 交给 Spring 管理
public class MqBroker {
    private final MqConfigProperties config;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    // 保留该依赖：确保消息存储先于 Broker 完成索引加载（@PostConstruct 顺序）
    private final SegmentMessageStore messageStore;
    private final MqBrokerHandler mqBrokerHandler;

    // 构造器注入配置
    @Autowired
    public MqBroker(MqConfigProperties config, MqBrokerHandler mqBrokerHandler,
                    SegmentMessageStore messageStore) {
        this.config = config;
        this.mqBrokerHandler = mqBrokerHandler;
        this.messageStore = messageStore;
    }

    // 启动时自动执行（初始化 Netty 服务），非阻塞
    @PostConstruct
    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        // 连接空闲检测：客户端每 heartbeatIntervalMillis 发一次心跳，
        // 这里按 3 个心跳周期判定读空闲，清理半开/僵尸连接。
        int readerIdleSeconds = (int) Math.max(5, config.getHeartbeatIntervalMillis() * 3 / 1000);

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel channel) {
                        // 1. 分隔符处理器：以换行符 \n 作为消息结束标志，最大帧 1MB
                        ByteBuf delimiter = Unpooled.copiedBuffer("\n".getBytes(StandardCharsets.UTF_8));
                        channel.pipeline()
                                .addLast(new IdleStateHandler(readerIdleSeconds, 0, 0, TimeUnit.SECONDS))
                                .addLast(new DelimiterBasedFrameDecoder(1024 * 1024, delimiter))
                                .addLast(new StringDecoder())
                                .addLast(new StringEncoder())
                                .addLast(mqBrokerHandler);
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true);

        // 绑定端口后立即返回，由 Netty 自己的 EventLoop 提供服务
        serverChannel = bootstrap.bind(config.getBrokerPort()).sync().channel();
        log.info("Broker 启动成功，监听端口: {}（已异步启动，Spring 容器可正常完成 refresh）",
                config.getBrokerPort());
    }

    // 销毁时关闭资源（现在能真正被 Spring 调用了）
    @PreDestroy
    public void stop() {
        log.info("Broker 开始关闭");
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        log.info("Broker 已关闭");
    }
}
