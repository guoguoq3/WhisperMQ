package org.guoguo.common.pojo.Entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

import org.guoguo.common.util.SnowflakeIdGeneratorUtil;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author 荆锦硕
 * @date 2025年09月13日 15:21
 * <p>
 * description:
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ConsumerThings extends FunctionEntity{

    public ConsumerThings() {
        this.messageId = String.valueOf(SnowflakeIdGeneratorUtil.getInstance().nextId());
    }
}
