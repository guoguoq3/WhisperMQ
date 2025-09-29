package org.guoguo.common.pojo.DTO;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.guoguo.common.pojo.Entity.FunctionEntity;
import org.guoguo.common.util.SnowflakeIdGeneratorUtil;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author 荆锦硕
 * @date 2025年09月12日 20:41
 * <p>
 * description:Producer——>Broker传输消息的功能DTO
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ProducerMessageDTO extends FunctionEntity {

    /**重试次数*/
    private Integer retryCount;
    /** latch发送的版本号*/
    private long currentVersion;


    //构造函数，初始化消息ID，三端共享
    public ProducerMessageDTO(){
        this.messageId = String.valueOf(SnowflakeIdGeneratorUtil.getInstance().nextId());
        //重试次数初始化为0
        this.retryCount=0;

    }

}
