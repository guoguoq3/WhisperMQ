package org.guoguo.common.pojo.Entity;

import lombok.Data;

/**
 * @author 荆锦硕
 * @date 2025年09月13日 19:39
 * <p>
 * description:功能层父类，实现最基本的消息ID和消息内容
 * 子类继承后扩展响应功能变量
 */
@Data
public class FunctionEntity {
    //消息ID
    protected String messageId;
    //消息内容
    protected String json;
}
