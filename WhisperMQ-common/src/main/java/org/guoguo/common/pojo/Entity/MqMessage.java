package org.guoguo.common.pojo.Entity;

import lombok.Data;

import java.util.List;
@Data
public class MqMessage {
    /** 主题 */
    private String topic;
    /** 标签列表 */
    private List<String> tags;
    /** 消息内容 */
    private String payload;
    /** 业务标识 */
    private String bizKey;



}