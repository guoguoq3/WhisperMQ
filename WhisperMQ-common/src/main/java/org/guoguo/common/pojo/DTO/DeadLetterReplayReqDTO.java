package org.guoguo.common.pojo.DTO;

import lombok.Data;
import org.guoguo.common.pojo.Entity.FunctionEntity;
import org.guoguo.common.util.SnowflakeIdGeneratorUtil;

/**
 * 死信重放请求：客户端指定要重放的死信 ID，Broker 把它重新投递回原消费组。
 */
@Data
public class DeadLetterReplayReqDTO extends FunctionEntity {

    /** 要重放的死信 ID */
    private String deadLetterId;

    public DeadLetterReplayReqDTO() {
        this.messageId = String.valueOf(SnowflakeIdGeneratorUtil.getInstance().nextId());
    }
}
