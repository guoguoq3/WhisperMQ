package org.guoguo.common.constant;

// 死信原因枚举（标准化原因，避免字符串混乱）
public enum DeadType {
    RETRY_EXHAUSTED("重试次数耗尽"),
    CONSUME_TIMEOUT("消费超时"),
    MESSAGE_EXPIRED("消息过期"),
    INVALID_FORMAT("消息格式错误"),
    BUSINESS_ERROR("业务处理失败");

    private final String desc;

    DeadType(String desc) {
        this.desc = desc;
    }
}