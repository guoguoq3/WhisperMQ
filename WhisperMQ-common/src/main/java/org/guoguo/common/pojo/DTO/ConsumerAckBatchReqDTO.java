package org.guoguo.common.pojo.DTO;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.guoguo.common.pojo.Entity.FunctionEntity;

import java.util.List;

/**
 * 批量消费确认（P1）。
 * 一条 RPC 携带多条 ACK，Broker 逐条摘除在途记录并推进分区位点。
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ConsumerAckBatchReqDTO extends FunctionEntity {
    private String groupId;
    private List<ConsumerAckReqDTO> acks;
}
