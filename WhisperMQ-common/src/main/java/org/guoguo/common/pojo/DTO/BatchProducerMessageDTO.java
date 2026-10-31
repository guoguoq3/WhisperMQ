package org.guoguo.common.pojo.DTO;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.guoguo.common.pojo.Entity.FunctionEntity;
import org.guoguo.common.pojo.Entity.MqMessageEnduring;
import org.guoguo.common.util.SnowflakeIdGeneratorUtil;

import java.util.List;

/**
 * 生产者批量发送 DTO（P1）。
 * 一条 RPC 携带多条消息，Broker 逐条生成消息 ID 并落盘。
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class BatchProducerMessageDTO extends FunctionEntity {
    private List<MqMessageEnduring> messages;
    private long currentVersion;
    private Integer retryCount;

    public BatchProducerMessageDTO() {
        this.messageId = String.valueOf(SnowflakeIdGeneratorUtil.getInstance().nextId());
        this.retryCount = 0;
    }
}
