package org.guoguo.common.constant;

public class MethodType {
    /** 生产者发送消息 */
    public static final String P_SEND_MSG = "P_SEND_MSG";
//    /** 消费者订阅 */
//    public static final String C_SUBSCRIBE = "C_SUBSCRIBE";
    // 新增：Broker向消费者推送消息
    public static final String B_PUSH_MSG = "B_PUSH_MSG";
    /** Broker向消费者新增订阅响应类型 */
    public static final String B_SUBSCRIBE_RESPONSE = "B_SUBSCRIBE_RESPONSE";
    // 消费者发送消费确认
    public static final String C_ACK_MSG = "C_ACK_MSG";
    // 组订阅主题
    public static final String GROUP_SUBSCRIBE = "GROUP_SUBSCRIBE";
    // 组取消订阅
    public static final String GROUP_UNSUBSCRIBE = "GROUP_UNSUBSCRIBE";
    // 消费者加入组
    public static final String CONSUMER_JOIN_GROUP = "CONSUMER_JOIN_GROUP";
    // 消费者离开组
    public static final String CONSUMER_LEAVE_GROUP = "CONSUMER_LEAVE_GROUP";
    // Broker向生产者发送确认
    public static final String P_CONFIRM_MSG = "P_CONFIRM_MSG";
}