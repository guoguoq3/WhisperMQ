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
    /** Broker 地址（默认：127.0.0.1） */
    private String brokerHost ;
    /** Broker 端口（默认：9999） */
    private int brokerPort ;
    /** 生产者超时时间（毫秒，默认：4000） */
    private int producerTimeout ;
    /** 消费者连接超时时间（毫秒，默认：5000） */
    private int consumerTimeout ;
    /** 生产者最大重试次数（默认：3） */
    private int producerRetryCountLimit ;
    /** 消费者重试时间系数（默认：500） */
    private int producerRetryTimeCoefficient ;
    /** 位点刷盘时间（默认：5秒） */
    private long flushIntervalMillis;
    /** 持久化文件大小阈值（默认：100条，超过则开启刷盘定时任务） */
    private  long flushThreshold;
    /** 窗口大小（毫秒，默认：10秒） */
    private  long windowSizeMillis;
    //flush的超时时间
    private long  getFlushTimeoutMillis;

    //新增持久化配置
    /** 消息持久化文件路径（默认：项目根目录下的 WhisperMQ-data 文件夹） */
    private String persistPath = System.getProperty("user.dir") + "/WhisperMQ-data";

    private String offsetPersistPath = System.getProperty("user.dir") + "/WhisperMQ-offset";
    /** 单个持久化文件最大大小（默认：10MB，超过则创建新文件） */
    private long maxFileSize = 1024 * 1024 * 10; // 10MB
}