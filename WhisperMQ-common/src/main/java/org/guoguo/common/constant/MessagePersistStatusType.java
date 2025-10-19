package org.guoguo.common.constant;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum MessagePersistStatusType {
    DEAD_LETTER_MESSAGE("1"),
    NORMAL_MESSAGE("0");
    /**
     * 状态值（存储到数据库或序列化时使用）
     */

    private final String status;
}
