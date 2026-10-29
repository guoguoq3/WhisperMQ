package org.guoguo.consumer.service;

import org.guoguo.common.pojo.DTO.SubscribeReqDTO;

/**
 * 消费者接口
 *
 * <p>P1 起 {@link #joinGroup}、{@link #groupSubscribe}、{@link #groupUnsubscribe}
 * 会阻塞等待 Broker 确认（默认 5s，见 {@code whisper-mq.subscribe-timeout-millis}），
 * 返回即代表 Broker 已处理；失败会抛 {@link RuntimeException}。
 * 因此不要在 Netty IO 线程（例如消息监听器以外的网络回调）里调用它们。</p>
 */
public interface IMqConsumer {
    /**
     * 1. 加入消费者组（必须先加入组才能接收消息），等待 Broker 确认后返回
     * @param groupId 消费者组ID
     */
    void joinGroup(String groupId);

    /**
     * 2. 离开消费者组（主动退出）
     * @param groupId 消费者组ID
     */
    void leaveGroup(String groupId);

    /**
     * 3. 让所属的消费者组订阅主题（组订阅 + 本消费者自己的 Tag），等待 Broker 确认后返回
     * @param subscribeReq 订阅请求（含组ID、主题和 tags）
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