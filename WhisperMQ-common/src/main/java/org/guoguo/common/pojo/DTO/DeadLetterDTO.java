package org.guoguo.common.pojo.DTO;

import lombok.Data;
import org.guoguo.common.constant.DeadType;
import org.guoguo.common.pojo.Entity.FunctionEntity;
import org.guoguo.common.util.SnowflakeIdGeneratorUtil;

/**
 * @author 荆锦硕
 * @date 2025年10月09日 12:08
 * <p>
 * description:
 */
@Data
public class DeadLetterDTO extends FunctionEntity {
    //死信ID
    private String deadLetterId;
    //死信类型
    private DeadType deadType;
    //死信产生时间戳
    private Long deadTime;
    //死信重试次数
    private Integer deadRetryCount;

    //由于继承了FunctionEntity，所以不需要再定义originMessageId和json，改为原有的messageId和json
    /*//原消息id标识
    private String originMessageId;
    //原消息
    private String json;*/

    public DeadLetterDTO() {
        this.deadLetterId = String.valueOf(SnowflakeIdGeneratorUtil.getInstance().nextId());
        this.deadTime = System.currentTimeMillis();
        this.deadRetryCount = -1;
    }
}


