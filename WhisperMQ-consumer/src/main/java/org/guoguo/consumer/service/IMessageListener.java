package org.guoguo.consumer.service;

import org.guoguo.common.pojo.Entity.MqMessage;

/**
 * 消息监听器：用户自定义消息处理逻辑
 */
public interface IMessageListener {
    /**
     * 处理消息
     * @param message 收到的消息
     * @return 处理结果（true=成功，false=失败）
     */
    boolean onMessage(MqMessage message);
}