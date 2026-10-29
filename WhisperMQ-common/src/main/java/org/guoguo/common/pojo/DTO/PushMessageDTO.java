package org.guoguo.common.pojo.DTO;


import lombok.Data;
import lombok.EqualsAndHashCode;
import org.guoguo.common.pojo.Entity.FunctionEntity;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.guoguo.common.util.SnowflakeIdGeneratorUtil;

/**
 * 推送给消费者的消息
 */

@EqualsAndHashCode(callSuper = true)
@Data
public class PushMessageDTO extends FunctionEntity {

    /** 消费者组 */
    private String consumerGroup;

    public PushMessageDTO() {
        this.messageId = String.valueOf(SnowflakeIdGeneratorUtil.getInstance().nextId());
    }
}