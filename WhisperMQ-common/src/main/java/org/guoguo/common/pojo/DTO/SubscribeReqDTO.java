package org.guoguo.common.pojo.DTO;

import lombok.Data;
import org.guoguo.common.pojo.Entity.FunctionEntity;
import org.guoguo.common.util.SnowflakeIdGeneratorUtil;

import java.util.List;

/**
 * 消费者订阅请求
 */
@Data
public class SubscribeReqDTO extends FunctionEntity {
    /** 订阅的主题 */
    private String topic;
    /** 标签过滤（如 ["TAG1", "TAG2"]，暂时简单处理为精确匹配） */
    private List<String> tags;

    private String groupId;

    public SubscribeReqDTO() {
        this.messageId = String.valueOf(SnowflakeIdGeneratorUtil.getInstance().nextId());
    }

}