// org.guoguo.producer.service.impl.MqProducer.java
package org.guoguo.producer.service.impl;

import com.alibaba.fastjson.JSON;
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
import org.guoguo.common.constant.MethodType;
import org.guoguo.common.pojo.DTO.ProducerMessageDTO;
import org.guoguo.common.pojo.DTO.RpcMessageDTO;
import org.guoguo.common.pojo.Entity.MqMessageEnduring;
import org.guoguo.common.pojo.Entity.VersionedLatch;
import org.guoguo.producer.constant.ResultCodeEnum;
import org.guoguo.producer.handler.MqProducerHandler;
import org.guoguo.producer.pojo.Result;

import org.guoguo.producer.service.IMqProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 生产者客户端。
 *
 * <p>P0 改造点：
 * <ol>
 *   <li>连接断开后按指数退避自动重连（旧实现只在 @PostConstruct 连一次）；</li>
 *   <li>连接不可用时快速失败并返回明确的 Result，而不是抛 NPE 被 catch 吞掉；</li>
 *   <li>增加写空闲心跳，配合 Broker 的读空闲检测清理半开连接；</li>
 *   <li>Broker 返回 ERROR 时立即失败并返回原因，不再空等到超时。</li>
 * </ol>
 */
@Slf4j
@Component // 交给 Spring 管理
public class MqProducer implements IMqProducer {
    private final MqConfigProperties config;
    private volatile Channel channel;
    private EventLoopGroup group;
    private Bootstrap bootstrap;

    private final ScheduledExecutorService reconnectScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "producer-reconnect");
        thread.setDaemon(true);
        return thread;
    });
    private final AtomicInteger reconnectAttempt = new AtomicInteger(0);
    private volatile boolean closed = false;
    private volatile boolean reconnectScheduled = false;

    // 注入配置
    @Autowired
    public MqProducer(MqConfigProperties config) {
        this.config = config;
    }

    // 初始化时连接 Broker
    @PostConstruct
    public void start() {
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
                                .addLast(new MqProducerHandler(MqProducer.this));
                    }
                });
        connectBlocking();
    }

    /** 首次连接：保持同步语义（启动后即可发送），失败则进入后台重连 */
    private void connectBlocking() {
        try {
            ChannelFuture future = bootstrap.connect(config.getBrokerHost(), config.getBrokerPort()).sync();
            this.channel = future.channel();
            reconnectAttempt.set(0);
            log.info("生产者连接 Broker 成功：{}:{}", config.getBrokerHost(), config.getBrokerPort());
            watchClose(future.channel());
        } catch (Exception e) {
            log.error("生产者连接 Broker 失败：{}:{}，将进入自动重连",
                    config.getBrokerHost(), config.getBrokerPort(), e);
            scheduleReconnect();
        }
    }

    /** 连接关闭后统一从这里触发重连（channelInactive 与 closeFuture 只保留这一条路径，避免重复连接） */
    private void watchClose(Channel ch) {
        ch.closeFuture().addListener(f -> {
            if (!closed) {
                log.warn("生产者与 Broker 的连接已断开，准备重连");
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
        log.warn("生产者将在 {}ms 后尝试第 {} 次重连 Broker", delay, reconnectAttempt.get() + 1);
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
        log.info("生产者尝试重连 Broker：{}:{}", config.getBrokerHost(), config.getBrokerPort());
        bootstrap.connect(config.getBrokerHost(), config.getBrokerPort()).addListener((ChannelFuture f) -> {
            if (f.isSuccess()) {
                this.channel = f.channel();
                reconnectAttempt.set(0);
                log.info("生产者重连 Broker 成功：{}:{}", config.getBrokerHost(), config.getBrokerPort());
                watchClose(f.channel());
            } else {
                log.warn("生产者重连 Broker 失败：{}", f.cause() == null ? "unknown" : f.cause().getMessage());
                scheduleReconnect();
            }
        });
    }

    // 存储结构：traceId -> 带版本的Latch
    private final ConcurrentHashMap<String, VersionedLatch> traceIdLatchMap = new ConcurrentHashMap<>();

    // Broker 明确返回 ERROR 时，按 traceId 记录失败原因，让 send() 立即失败
    private final ConcurrentHashMap<String, String> traceIdErrorMap = new ConcurrentHashMap<>();

    @Override
    //使用MqMessageEnduring类封装MqMessage消息实体类，添加持久化选择字段
    public Result<String> send(MqMessageEnduring message) {
        //封装消息功能层DTO
        ProducerMessageDTO producerMessageDTO = new ProducerMessageDTO();
        producerMessageDTO.setJson(JSON.toJSONString(message));
        //为每个消息通过雪花算法创建独立id
        String traceId = producerMessageDTO.getMessageId();

        Channel currentChannel = this.channel;
        if (currentChannel == null || !currentChannel.isActive()) {
            log.warn("生产者发送失败：与 Broker 的连接不可用，消息ID：{}", traceId);
            return fail("消息发送失败：Broker 未连接", traceId);
        }

        RpcMessageDTO rpcMessageDTO = new RpcMessageDTO();
        rpcMessageDTO.setRequest(true);
        rpcMessageDTO.setMethodType(MethodType.P_SEND_MSG);
        rpcMessageDTO.setToken(config.getAuthToken());

        try {
            //超时重传机制
            for (long i = 0; i <= config.getProducerRetryCountLimit(); i++) {
                // 为每次尝试创建独立等待器，避免旧版本信号串扰
                VersionedLatch latch = new VersionedLatch(1);
                long currentVersion = latch.getVersion();
                producerMessageDTO.setCurrentVersion(currentVersion);
                rpcMessageDTO.setJson(JSON.toJSONString(producerMessageDTO));
                traceIdLatchMap.put(traceId, latch);
                traceIdErrorMap.remove(traceId);

                // 系数/次数可能未配置（默认 0）：至少等待 1ms，避免"消息到了 Broker 但本地已判定失败"
                long waitMillis = Math.max(1L,
                        (long) (Math.max(1, config.getProducerRetryTimeCoefficient()) * Math.pow(2L, i)));
                currentChannel.writeAndFlush(JSON.toJSONString(rpcMessageDTO) + "\n");
                log.info("等待生产者发送消息，重试次数为：{}，消息Id：{}，本次等待{}ms", i, traceId, waitMillis);

                boolean await = latch.await(waitMillis, TimeUnit.MILLISECONDS);
                String brokerError = traceIdErrorMap.remove(traceId);
                traceIdLatchMap.remove(traceId);

                if (brokerError != null) {
                    // Broker 已明确拒绝，重试没有意义
                    log.error("Broker 拒绝消息，不再重试。消息ID：{}，原因：{}", traceId, brokerError);
                    return fail("消息发送失败：" + brokerError, traceId);
                }
                if (await && latch.getVersion() == currentVersion) {
                    return Result.ok("消息发送成功", traceId);
                }
                log.warn("生产者第{}次发送等待超时（{}ms），消息id：{}", i, waitMillis, traceId);
            }
            log.info("消息发送超时，返回失败");
            return fail("消息发送超时", traceId);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            traceIdLatchMap.remove(traceId);
            return fail("消息发送被中断", traceId);
        } catch (Exception e) {
            traceIdLatchMap.remove(traceId);
            log.error("消息发送异常，消息ID：{}", traceId, e);
            return fail("消息发送失败", traceId);
        }
    }

    /**
     * 构造失败结果。
     * 注意：{@code Result.build(body, ResultCodeEnum, messageId)} 会用枚举自带的 "发送失败"
     * 覆盖具体原因，这里显式传 code/message，便于控制台和调用方看到真正失败原因。
     */
    private Result<String> fail(String message, String traceId) {
        return Result.build(message, ResultCodeEnum.FAILED.getCode(), message, traceId);
    }

    // 关闭资源
    @PreDestroy
    public void close() {
        closed = true;
        reconnectScheduler.shutdownNow();
        traceIdLatchMap.clear();
        traceIdErrorMap.clear();
        if (group != null) {
            group.shutdownGracefully();
        }
        log.info("生产者已关闭");
    }

    /** Broker 确认消息 */
    public void setResponse(String traceId, String methodType, long version) {
        VersionedLatch latch = traceIdLatchMap.get(traceId);
        if (latch != null && latch.getVersion() == version) {
            if (MethodType.P_CONFIRM_MSG.equals(methodType)) {
                latch.countDown(version);
            }
        }
    }

    /** Broker 返回 ERROR 时调用：记录原因并唤醒等待线程 */
    public void onErrorMessage(String traceId, String error) {
        if (traceId == null) {
            return;
        }
        VersionedLatch latch = traceIdLatchMap.get(traceId);
        if (latch == null) {
            // 该消息已经超时/结束，忽略迟到的错误响应，避免 errorMap 泄漏
            log.debug("收到迟到/未知的消息错误响应，traceId={}，原因={}", traceId, error);
            return;
        }
        traceIdErrorMap.put(traceId, error);
        latch.countDown(latch.getVersion());
    }
}
