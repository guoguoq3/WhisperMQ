package org.guoguo.common.pojo.Entity;

import lombok.Data;

/**
 * 消息实体。
 *
 * <p>P1：新增 {@link #partition}。默认 0，旧数据（JSON 中没有该字段）反序列化后也是 0，
 * 因此与升级前行为完全一致；分区数 > 1 时由 Broker 的 TopicMetadata 计算。</p>
 */
@Data
public class MqMessage {
    /** 主题 */
    private String topic;
    /** 标签 */
    private String tag;
    /** 消息内容 */
    private String payload;
    /** 业务标识（可用于分区 hash / 幂等去重） */
    private String bizKey;
    /** 分区号（默认 0；旧数据缺省即 0） */
    private int partition = 0;
}
