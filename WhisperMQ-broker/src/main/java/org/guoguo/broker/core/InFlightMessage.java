package org.guoguo.broker.core;

import lombok.Getter;
import lombok.Setter;
import org.guoguo.common.pojo.Entity.MqMessage;

/**
 * 一条"已投递给消费者、尚未收到 ACK"的消息记录。
 *
 * <p>Broker 用它实现可靠投递：ACK 超时/失败/消费者掉线时按 {@link #attempts} 重投，
 * 超过上限后转入死信。key 为 {@code groupId|messageId}，与"组内一条消息只处理一次"的语义对齐。</p>
 */
@Getter
public class InFlightMessage {

    /** 消费者组 */
    private final String groupId;
    /** 主题 */
    private final String topic;
    /** 分区（P1：位点按分区推进；旧消息为 0） */
    private final int partition;
    /** 消息 ID（雪花号） */
    private final String messageId;
    /** 消息体，重投时直接复用，避免 messageMap 被清理后无法重投 */
    private final MqMessage message;

    /** 当前被投递到的消费者（通道 ID） */
    @Setter
    private String consumerId;
    /** 最近一次投递时间（毫秒） */
    @Setter
    private volatile long lastPushTime;
    /** 已投递次数（含首次） */
    @Setter
    private volatile int attempts;

    public InFlightMessage(String groupId, String topic, int partition, String messageId, MqMessage message,
                           String consumerId, int attempts, long lastPushTime) {
        this.groupId = groupId;
        this.topic = topic;
        this.partition = partition;
        this.messageId = messageId;
        this.message = message;
        this.consumerId = consumerId;
        this.attempts = attempts;
        this.lastPushTime = lastPushTime;
    }
}
