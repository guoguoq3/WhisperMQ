package org.guoguo.common.pojo.DTO;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.guoguo.common.pojo.Entity.FunctionEntity;

/**
 * 消费者消费确认请求 DTO
 * 消费者发送给broker
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ConsumerAckReqDTO extends FunctionEntity {
    /** 消费者标识（避免不同消费者重复确认，用通道ID或自定义唯一ID） */
    private String consumerId;
    /** 确认状态（SUCCESS=处理成功，FAIL=处理失败，后续可扩展重试） */
    private String ackStatus = "SUCCESS";
    //消费者分组
    private String groupId;
}