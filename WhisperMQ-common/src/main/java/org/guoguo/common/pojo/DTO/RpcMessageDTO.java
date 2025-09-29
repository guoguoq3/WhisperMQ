package org.guoguo.common.pojo.DTO;

import lombok.Data;

@Data
public class RpcMessageDTO {
    /** 是否为请求 */
    private boolean request;
    /** 方法类型（如发送消息、订阅等） */
    private String methodType;
    /** 消息内容（JSON） */
    private String json;


}