package org.guoguo.common.pojo.DTO;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.guoguo.common.pojo.Entity.FunctionEntity;

/**
 * 消费者流控请求（P1 Stage D 背压）。
 *
 * <p>消费者业务队列满时发送 {@code C_PAUSE}，Broker 暂停向该连接投递；
 * 队列回落到水位以下后发送 {@code C_RESUME}，Broker 清除暂停并按组位点补投。</p>
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ConsumerFlowControlDTO extends FunctionEntity {
    private String groupId;
    private String consumerId;
    /** 暂停时长（毫秒），仅 PAUSE 有效；0 表示由 Broker 使用默认值 */
    private long durationMillis = 0;
    /** 触发原因，便于排查 */
    private String reason;
}
