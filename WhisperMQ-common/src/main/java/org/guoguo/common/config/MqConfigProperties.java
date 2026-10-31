// org.guoguo.common.config.MqConfigProperties.java
package org.guoguo.common.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * MQ 统一配置类，绑定 application.properties 中的配置
 */
@Data
//@Component
@ConfigurationProperties(prefix = "whisper-mq") // 配置前缀
public class MqConfigProperties {
    //初始化链接相关配置
    /** Broker 地址（默认：127.0.0.1） */
    private String brokerHost ;
    /** Broker 端口（默认：9999） */
    private int brokerPort ;
    /** 生产者超时时间（毫秒，默认：4000） */
    private int producerTimeout ;
    /** 消费者连接超时时间（毫秒，默认：5000） */
    private int consumerTimeout ;

    //持久化、位点相关
    /** 位点刷盘时间（默认：5秒） */
    private long flushIntervalMillis=5000;
    /** 持久化文件大小阈值（默认：100条，超过则开启刷盘定时任务） */
    private  long flushThreshold=100;
    /** 窗口大小（毫秒，默认：10秒） */
    private  long windowSizeMillis=10000;
    //flush的超时时间
    private long  getFlushTimeoutMillis;
    /** 消息持久化文件路径（默认：项目根目录下的 WhisperMQ-data 文件夹） */
    private String persistPath = System.getProperty("user.dir") + "/WhisperMQ-data";
    /** 消费者位点持久化文件路径（默认：项目根目录下的 WhisperMQ-offset 文件夹） */
    private String offsetPersistPath = System.getProperty("user.dir") + "/WhisperMQ-offset";
    /** 单个持久化文件最大大小（默认：10MB，超过则创建新文件） */
    private long maxFileSize = 1024 * 1024 * 10; // 10MB

    //生产者重试配置
    /** 生产者最大重试次数（默认：3） */
    private int producerRetryCountLimit = 3;
    /** 消费者重试时间系数（默认：500） */
    private int producerRetryTimeCoefficient = 500;

    //死信队列配置
    //死信队列最大重试次数（默认为5）
    private int maxDeadLetterRetryCount=5;

    //可靠投递 / 连接保活相关配置
    /** 消息推送给消费者后等待 ACK 的超时时间（毫秒，超时触发重投，默认：10秒） */
    private long ackTimeoutMillis = 10000;
    /** 单条消息向同一消费组投递的最大次数（含首次，超过后进入死信，默认：3） */
    private int maxDeliveryAttempts = 3;
    /** 客户端心跳发送间隔（毫秒，默认：15秒） */
    private long heartbeatIntervalMillis = 15000;
    /** 客户端断线重连的初始退避时间（毫秒，默认：1秒） */
    private long reconnectInitialDelayMillis = 1000;
    /** 客户端断线重连的最大退避时间（毫秒，默认：30秒） */
    private long reconnectMaxDelayMillis = 30000;
    /** 新消费组首次订阅时是否从最早消息开始回溯（默认 true，避免订阅生效前发布的消息被丢弃） */
    private boolean newGroupStartFromEarliest = true;

    /**
     * 每个 topic 的默认分区数（默认 1）。
     * <p>1 表示与升级前完全一致；大于 1 属于 P1 Stage B 的实验能力，
     * 在分区投递/再均衡完成前不要在生产使用。</p>
     */
    private int defaultPartitions = 1;

    //存储与保留（P1 Stage C）
    /** 启动后内存中保留的热消息条数（默认 1000；0 表示不缓存消息体） */
    private int hotCacheSize = 1000;
    /** 消息保留时长（毫秒，默认 0 表示不按时间清理） */
    private long retentionMillis = 0;
    /** 消息日志总容量上限（字节，默认 0 表示不限） */
    private long retentionMaxBytes = 0;
    /** 保留策略检查间隔（毫秒，默认 60 秒） */
    private long retentionCheckIntervalMillis = 60000;

    //背压、去重、鉴权（P1 Stage D）
    /** 是否启用消费者背压暂停投递 */
    private boolean backpressureEnabled = true;
    /** 队列满时暂停投递的时长（毫秒） */
    private long backpressurePauseMillis = 3000;
    /** 队列回落到该比例以下时恢复投递（0~1） */
    private double backpressureResumeThreshold = 0.5;
    /** 消费端是否对 messageId 去重 */
    private boolean dedupEnabled = true;
    /** 去重缓存条数（0 表示关闭） */
    private int dedupCacheSize = 1000;
    /** 是否开启 token 鉴权（默认关闭，兼容旧客户端） */
    private boolean authEnabled = false;
    /** 共享鉴权 token */
    private String authToken = "";

    //存储可靠性（P0：刷盘与停机）
    /** 刷盘模式：ASYNC（只写页缓存）/ INTERVAL（定时 force）/ SYNC（每条 force） */
    private String flushMode = "ASYNC";
    /** INTERVAL 模式的刷盘间隔（毫秒） */
    private long storeFlushIntervalMillis = 1000;
    /** 优雅停机时等待在途消息的最长时间（毫秒） */
    private long shutdownDrainMillis = 3000;
    /** 批量发送单次最大消息数（P1） */
    private int maxBatchSize = 1000;

    //消费端业务线程池 / 控制请求确认（P1）
    /** 消费者业务线程数（0 = 按 CPU 核数自动，默认：0） */
    private int consumerWorkerThreads = 0;
    /** 每个业务线程的消息队列容量（默认：10000，满了不 ACK、由 Broker 超时重投） */
    private int consumerQueueCapacity = 10000;
    /** 入组/订阅等待 Broker 确认的超时时间（毫秒，默认：5秒） */
    private long subscribeTimeoutMillis = 5000;
}