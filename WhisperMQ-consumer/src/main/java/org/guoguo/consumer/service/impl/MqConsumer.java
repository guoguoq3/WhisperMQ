// org.guoguo.consumer.service.impl.MqConsumer.java
package org.guoguo.consumer.service.impl;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.common.config.MqConfigProperties;
import org.guoguo.consumer.dispatch.MessageDispatcher;
import org.guoguo.consumer.handler.MqConsumerHandler;
import org.guoguo.consumer.service.IMessageListener;
import org.guoguo.consumer.service.IMqConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 消费者客户端基类。
 *
 * <p>P0：断线指数退避重连、重连后 {@link #onReconnected()} 恢复会话、心跳、断线不再销毁 EventLoopGroup。</p>
 * <p>P1：消息处理交给 {@link MessageDispatcher}（按 topic 串行、不占用 IO 线程）；
 * 入组/订阅通过 traceId 等待 Broker 的确认响应（{@link #expectResponse}/{@link #awaitResponse}）。</p>
 */
@Slf4j
@Component // 交给 Spring 管理
public abstract class MqConsumer implements IMqConsumer {
    protected final MqConfigProperties config;
    protected volatile Channel channel;
    private EventLoopGroup group;
    private Bootstrap bootstrap;

    /** 业务线程池：监听器不再跑在 Netty IO 线程上 */
    protected final MessageDispatcher dispatcher;

    private final ScheduledExecutorService reconnectScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "consumer-reconnect");
        thread.setDaemon(true);
        return thread;
    });
    private final AtomicInteger reconnectAttempt = new AtomicInteger(0);
    private volatile boolean closed = false;
    private volatile boolean reconnectScheduled = false;

    /** 等待 Broker 控制响应的请求：traceId -> future */
    private final Map<String, CompletableFuture<Void>> pendingResponses = new ConcurrentHashMap<>();

    // 主题 -> 监听器，IO 线程读、业务线程写，必须并发安全
    protected final Map<String, IMessageListener> topicListenerMap = new ConcurrentHashMap<>();

    // 延迟注入 Handler（由 Spring 管理，避免构造时依赖）
    private MqConsumerHandler mqConsumerHandler;

    // 注入配置
    @Autowired
    public MqConsumer(MqConfigProperties config) {
        this.config = config;
        int workers = config.getConsumerWorkerThreads() > 0
                ? config.getConsumerWorkerThreads()
                : Math.max(2, Runtime.getRuntime().availableProcessors());
        this.dispatcher = new MessageDispatcher(workers, config.getConsumerQueueCapacity());
    }

    // 初始化时连接 Broker
    @PostConstruct
    @Override
    public void start() {
        mqConsumerHandler = new MqConsumerHandler(this);
        group = new NioEventLoopGroup(1);
        bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        ByteBuf delimiter = Unpooled.copiedBuffer("\n".getBytes(StandardCharsets.UTF_8));
                        ch.pipeline()
                                .addLast(new IdleStateHandler(0,
                                        (int) Math.max(1, config.getHeartbeatIntervalMillis() / 1000), 0,
                                        TimeUnit.SECONDS))
                                .addLast(new DelimiterBasedFrameDecoder(1024 * 1024, delimiter))
                                .addLast(new StringDecoder())
                                .addLast(new StringEncoder())
                                .addLast(mqConsumerHandler);
                    }
                });
        connectBlocking();
    }

    private void connectBlocking() {
        try {
            ChannelFuture future = bootstrap.connect(config.getBrokerHost(), config.getBrokerPort()).sync();
            this.channel = future.channel();
            reconnectAttempt.set(0);
            log.info("消费者连接 Broker 成功：{}:{}", config.getBrokerHost(), config.getBrokerPort());
            watchClose(future.channel());
        } catch (Exception e) {
            log.error("消费者连接 Broker 失败：{}:{}，将进入自动重连",
                    config.getBrokerHost(), config.getBrokerPort(), e);
            scheduleReconnect();
        }
    }

    private void watchClose(Channel ch) {
        ch.closeFuture().addListener(f -> {
            if (!closed) {
                log.warn("消费者与 Broker 的连接已断开，准备重连");
                channel = null;
                scheduleReconnect();
            }
        });
    }

    private void scheduleReconnect() {
        if (closed || reconnectScheduled) {
            return;
        }
        reconnectScheduled = true;
        long delay = nextDelay();
        log.warn("消费者将在 {}ms 后尝试重连 Broker", delay);
        reconnectScheduler.schedule(this::reconnect, delay, TimeUnit.MILLISECONDS);
    }

    private long nextDelay() {
        long initial = Math.max(200, config.getReconnectInitialDelayMillis());
        long max = Math.max(initial, config.getReconnectMaxDelayMillis());
        int attempt = Math.min(reconnectAttempt.getAndIncrement(), 10);
        return Math.min(max, initial * (1L << attempt));
    }

    private void reconnect() {
        reconnectScheduled = false;
        if (closed || (channel != null && channel.isActive())) {
            return;
        }
        log.info("消费者尝试重连 Broker：{}:{}", config.getBrokerHost(), config.getBrokerPort());
        bootstrap.connect(config.getBrokerHost(), config.getBrokerPort()).addListener((ChannelFuture f) -> {
            if (f.isSuccess()) {
                this.channel = f.channel();
                reconnectAttempt.set(0);
                log.info("消费者重连 Broker 成功：{}:{}", config.getBrokerHost(), config.getBrokerPort());
                watchClose(f.channel());
                try {
                    onReconnected();
                } catch (Exception e) {
                    log.error("消费者重连后的状态恢复失败", e);
                }
            } else {
                log.warn("消费者重连 Broker 失败", f.cause());
                scheduleReconnect();
            }
        });
    }

    /**
     * 子类在重连成功后恢复业务状态（重新入组、重新订阅）。
     * <p>注意：本方法运行在 Netty IO 线程上，禁止阻塞等待 Broker 响应。</p>
     */
    protected void onReconnected() {
        // 默认无状态
    }

    public Map<String, IMessageListener> getTopicListenerMap() {
        return Collections.unmodifiableMap(topicListenerMap);
    }

    /** 获取业务线程池（测试/诊断用） */
    public MessageDispatcher getDispatcher() {
        return dispatcher;
    }

    /** 向当前连接发送一帧（连接不可用时返回 false，由调用方决定是否报错） */
    protected boolean sendFrame(String json) {
        Channel current = this.channel;
        if (current == null || !current.isActive()) {
            return false;
        }
        current.writeAndFlush(json + "\n");
        return true;
    }

    // ------------------------------------------------------- 控制请求的确认等待

    /**
     * 注册一个等待 Broker 响应的请求。必须在发送请求帧【之前】调用，
     * 否则响应可能先于注册到达而被丢弃。
     */
    protected CompletableFuture<Void> expectResponse(String traceId) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        if (traceId != null) {
            pendingResponses.put(traceId, future);
        }
        return future;
    }

    /**
     * 等待响应。
     *
     * @return true=收到成功响应；false=超时
     * @throws RuntimeException Broker 返回了错误响应
     */
    protected boolean awaitResponse(String traceId, CompletableFuture<Void> future,
                                    long timeoutMillis, String what) {
        try {
            future.get(Math.max(1, timeoutMillis), TimeUnit.MILLISECONDS);
            return true;
        } catch (TimeoutException e) {
            log.warn("{} 等待 Broker 确认超时（{}ms），traceId={}", what, timeoutMillis, traceId);
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw new RuntimeException(what + " 被 Broker 拒绝：" + cause.getMessage(), cause);
        } finally {
            if (traceId != null) {
                pendingResponses.remove(traceId, future);
            }
        }
    }

    /** 收到 Broker 响应时调用（由 Handler 在 IO 线程上触发） */
    public void completeResponse(String traceId, Throwable error) {
        if (traceId == null) {
            return;
        }
        CompletableFuture<Void> future = pendingResponses.remove(traceId);
        if (future == null) {
            return;
        }
        if (error == null) {
            future.complete(null);
        } else {
            future.completeExceptionally(error);
        }
    }

    /** 关闭前钩子：子类可在此刷新未发送的 ACK 等 */
    protected void onBeforeClose() {
    }

    @PreDestroy
    public void close() {
        closed = true;
        // 先让业务线程把已接收的消息处理完（期间仍可发送 ACK），再断连接
        dispatcher.close();
        onBeforeClose();
        reconnectScheduler.shutdownNow();
        if (channel != null) {
            channel.close();
        }
        if (group != null) {
            group.shutdownGracefully();
        }
        log.info("WhisperMQ 消费者关闭，释放资源");
    }
}
