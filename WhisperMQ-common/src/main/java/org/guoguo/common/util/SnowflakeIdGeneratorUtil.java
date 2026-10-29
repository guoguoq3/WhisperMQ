package org.guoguo.common.util;

import org.springframework.stereotype.Component;

@Component
public class SnowflakeIdGeneratorUtil {
    // 静态内部类持有单例实例
    private static class SingletonHolder {
        private static volatile SnowflakeIdGeneratorUtil INSTANCE;

        // 初始化方法，由Spring调用
        private static void initInstance(long workerId, long dataCenterId) {
            INSTANCE = new SnowflakeIdGeneratorUtil(workerId, dataCenterId);
        }
    }

    // 起始时间戳，2020-01-01 00:00:00
    private final long startTimeStamp = 1577836800000L;
    // 机器ID所占位数
    private final long workerIdBits = 5L;
    // 数据中心ID所占位数
    private final long dataCenterIdBits = 5L;
    // 序列号所占位数
    private final long sequenceBits = 12L;

    // 机器ID最大值 31
    private final long maxWorkerId = -1L ^ (-1L << workerIdBits);
    // 数据中心ID最大值 31
    private final long maxDataCenterId = -1L ^ (-1L << dataCenterIdBits);

    // 机器ID向左移位数
    private final long workerIdShift = sequenceBits;
    // 数据中心ID向左移位数
    private final long dataCenterIdShift = sequenceBits + workerIdBits;
    // 时间戳向左移位数
    private final long timestampShift = sequenceBits + workerIdBits + dataCenterIdBits;
    // 序列号掩码 4095
    private final long sequenceMask = -1L ^ (-1L << sequenceBits);

    private final long workerId;
    private final long dataCenterId;
    private volatile long sequence = 0L;
    private long lastTimestamp = -1L;

    // 私有构造函数，防止外部实例化
    private SnowflakeIdGeneratorUtil() {
        this(1L, 1L);
    }

    // 私有构造函数，由静态内部类调用
    private SnowflakeIdGeneratorUtil(long workerId, long dataCenterId) {
        if (workerId > maxWorkerId || workerId < 0) {
            throw new IllegalArgumentException("Worker ID 不能大于 " + maxWorkerId + " 或小于 0");
        }
        if (dataCenterId > maxDataCenterId || dataCenterId < 0) {
            throw new IllegalArgumentException("数据中心 ID 不能大于 " + maxDataCenterId + " 或小于 0");
        }
        this.workerId = workerId;
        this.dataCenterId = dataCenterId;
    }

    // Spring初始化时调用，用于设置workerId和dataCenterId
    public void init() {
        // 实际项目中可以从配置文件获取
        long workerId = 1L;
        long dataCenterId = 1L;
        SingletonHolder.initInstance(workerId, dataCenterId);
    }

    // 静态方法获取单例实例
    public static SnowflakeIdGeneratorUtil getInstance() {
        if (SingletonHolder.INSTANCE == null) {
            synchronized (SnowflakeIdGeneratorUtil.class) {
                if (SingletonHolder.INSTANCE == null) {
                    // 如果Spring还未初始化，使用默认值
                    SingletonHolder.INSTANCE = new SnowflakeIdGeneratorUtil();
                }
            }
        }
        return SingletonHolder.INSTANCE;
    }

    // 生成下一个ID
    public synchronized long nextId() {
        long currentTimestamp = System.currentTimeMillis();
        // 处理时钟回拨问题
        if (currentTimestamp < lastTimestamp) {
            throw new RuntimeException("时钟回拨，拒绝生成ID " + (lastTimestamp - currentTimestamp) + " 毫秒");
        }
        if (currentTimestamp == lastTimestamp) {
            sequence = (sequence + 1) & sequenceMask;
            if (sequence == 0) {
                // 序列号用尽，等待下一毫秒
                currentTimestamp = waitNextMillis(lastTimestamp);
            }
        } else {
            // 时间戳改变，重置序列号
            sequence = 0L;
        }
        lastTimestamp = currentTimestamp;

        return ((currentTimestamp - startTimeStamp) << timestampShift) |
                (dataCenterId << dataCenterIdShift) |
                (workerId << workerIdShift) |
                sequence;
    }

    // 等待下一毫秒
    private long waitNextMillis(long lastTimestamp) {
        long timestamp = System.currentTimeMillis();
        while (timestamp <= lastTimestamp) {
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }
}
