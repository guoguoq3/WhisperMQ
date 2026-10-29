package org.guoguo.consumer.service;

public class ConsumerMain {
//    public static void main(String[] args) {
//        // 1. 创建消费者并启动（连接Broker）
//        MqConsumer consumer = new MqConsumer();
//        consumer.start();
//
//        // 2. 定义订阅请求：订阅 "TEST_TOPIC" 主题，标签为 "TAG1"
//        SubscribeReqDTO subscribeReq = new SubscribeReqDTO();
//        subscribeReq.setTopic("TEST_TOPIC");
//        subscribeReq.setTags(Arrays.asList("TAG1"));
//
//        // 3. 订阅主题，并设置消息监听器（自定义消息处理逻辑）
//        consumer.subscribe(subscribeReq, new IMessageListener(){
//            @Override
//            public void onMessage(MqMessage  message) {
//                System.out.println("消费者收到消息：主题=" + message.getTopic()
//                        + "，内容=" + message.getPayload()
//                        + "，标签=" + message.getTags());
//            }
//        });
//
//        // 防止程序退出（实际应用中应保持运行）
//        try {
//            Thread.sleep(60_000); // 运行60秒后退出
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        } finally {
//            consumer.close();
//        }
//    }
}