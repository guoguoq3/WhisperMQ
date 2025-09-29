package org.guoguo.consumer.service;

import org.guoguo.common.pojo.DTO.SubscribeReqDTO;

/**
 * 消费者接口
 */
public interface IMqConsumer {
    /**
     * 1. 加入消费者组（必须先加入组才能接收消息）
     * @param groupId 消费者组ID
     */
    void joinGroup(String groupId);

    /**
     * 2. 离开消费者组（主动退出）
     * @param groupId 消费者组ID
     */
    void leaveGroup(String groupId);

    /**
     * 3. 让所属的消费者组订阅主题（组订阅，非单个消费者订阅）
     * @param subscribeReq 订阅请求（含组ID和主题）
     * @param listener 消息处理器
     */
    void groupSubscribe(SubscribeReqDTO subscribeReq, IMessageListener listener);

    /**
     * 4. 让所属的消费者组取消订阅主题
     * @param subscribeReq 订阅请求（含组ID和主题）
     */
    void groupUnsubscribe(SubscribeReqDTO subscribeReq);


    /**
     * 启动消费者（连接Broker）
     */
    void start();

    /**
     * 关闭消费者
     */
    void close();
}