package org.guoguo.common.pojo.DTO;

import lombok.Data;

/**
 * RPC 信封。
 *
 * <p>P1：新增 {@link #version} 协议版本。规则：客户端随请求携带，服务端只接受
 * {@code version <= PROTOCOL_VERSION}；旧客户端不传时按 1 处理，老服务端忽略该字段。</p>
 */
@Data
public class RpcMessageDTO {

    /** 当前协议版本。新增字段/方法类型时递增，服务端保持对旧版本的兼容。 */
    public static final int PROTOCOL_VERSION = 1;

    /** 协议版本；旧客户端不传时视为 1 */
    private Integer version = PROTOCOL_VERSION;

    /** 是否为请求 */
    private boolean request;
    /** 方法类型（如发送消息、订阅等） */
    private String methodType;
    /** 消息内容（JSON） */
    private String json;

    /** 鉴权 token（P1 Stage D；服务端开启鉴权时必填，关闭时忽略） */
    private String token;
}
