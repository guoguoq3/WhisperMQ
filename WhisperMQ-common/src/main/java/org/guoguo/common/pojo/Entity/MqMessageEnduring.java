package org.guoguo.common.pojo.Entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author 荆锦硕
 * @date 2025年09月04日 20:57
 * <p>
 * description:
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class MqMessageEnduring extends MqMessage{
    //持久化字段，默认持久化
    private boolean enduring=true;
}
