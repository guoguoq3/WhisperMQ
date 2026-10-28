package org.guoguo.common.pojo.Entity;

import io.netty.channel.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * 消费者级 Tag 订阅（P1）：同一组内不同消费者可以订阅不同 Tag。
 */
class ConsumerGroupTagsTest {

    private ConsumerGroup group;

    @BeforeEach
    void setUp() {
        group = new ConsumerGroup();
        group.setGroupId("G1");
    }

    private void online(String consumerId) {
        group.getOnlineConsumers().put(consumerId, mock(Channel.class));
    }

    @Test
    void perConsumerTagsAreIsolated() {
        online("c1");
        online("c2");
        group.addConsumerTopicTags("T", "c1", List.of("TAG1"));
        group.addConsumerTopicTags("T", "c2", List.of("TAG2"));
        group.addSubscriber("T", "c1");
        group.addSubscriber("T", "c2");

        assertEquals(List.of("c1"), group.getConsumersByTopicAndTag("T", "TAG1"));
        assertEquals(List.of("c2"), group.getConsumersByTopicAndTag("T", "TAG2"));
        assertTrue(group.getConsumersByTopicAndTag("T", "TAG3").isEmpty());
    }

    @Test
    void tagsRegisteredBeforeJoinSurviveWhenConsumerGoesOnline() {
        // 订阅可能先于入组到达：先存 Tag/订阅关系，入组后应立即可投递
        group.addConsumerTopicTags("T", "c1", List.of("TAG1"));
        group.addSubscriber("T", "c1");
        assertTrue(group.getConsumersByTopicAndTag("T", "TAG1").isEmpty(), "未在线时不应被选中");

        online("c1");
        assertEquals(List.of("c1"), group.getConsumersByTopicAndTag("T", "TAG1"));
    }

    @Test
    void wildcardAndEmptyTagsMatchEveryMessage() {
        online("wildcard");
        online("empty");
        group.addConsumerTopicTags("T", "wildcard", List.of("*"));
        group.addConsumerTopicTags("T", "empty", List.of());
        group.addSubscriber("T", "wildcard");
        group.addSubscriber("T", "empty");

        assertEquals(2, group.getConsumersByTopicAndTag("T", "ANY_TAG").size());
        assertEquals(2, group.getConsumersByTopicAndTag("T", null).size());
    }

    @Test
    void consumerTagsAreScopedToTheirTopic() {
        online("c1");
        group.addConsumerTopicTags("T1", "c1", List.of("TAG1"));
        group.addConsumerTopicTags("T2", "c1", List.of("TAG2"));
        group.addSubscriber("T1", "c1");
        group.addSubscriber("T2", "c1");

        assertEquals(List.of("c1"), group.getConsumersByTopicAndTag("T1", "TAG1"));
        assertTrue(group.getConsumersByTopicAndTag("T1", "TAG2").isEmpty());
        assertEquals(List.of("TAG2"), group.getConsumerTags("T2", "c1"));
    }

    @Test
    void removeConsumerClearsItsTags() {
        online("c1");
        group.addConsumerTopicTags("T", "c1", List.of("TAG1"));
        group.addSubscriber("T", "c1");

        group.removeConsumer("c1");

        assertNull(group.getConsumerTags("T", "c1"));
        assertTrue(group.getConsumersByTopicAndTag("T", "TAG1").isEmpty());
        assertTrue(group.isEmpty());
    }

    @Test
    void duplicateSubscribeDoesNotEraseOtherConsumersTags() {
        online("c1");
        online("c2");
        group.addSubscribe(subscribeReq("T", List.of("TAG1")), ctxOf("c1"));
        group.addSubscribe(subscribeReq("T", List.of("TAG2")), ctxOf("c2"));

        assertEquals(List.of("c1"), group.getConsumersByTopicAndTag("T", "TAG1"));
        assertEquals(List.of("c2"), group.getConsumersByTopicAndTag("T", "TAG2"));
        assertFalse(group.getSubscribeMap().isEmpty());
    }

    // ---------------------------------------------------------------- helpers

    private org.guoguo.common.pojo.DTO.SubscribeReqDTO subscribeReq(String topic, List<String> tags) {
        org.guoguo.common.pojo.DTO.SubscribeReqDTO req = new org.guoguo.common.pojo.DTO.SubscribeReqDTO();
        req.setTopic(topic);
        req.setTags(tags);
        req.setGroupId("G1");
        return req;
    }

    private io.netty.channel.ChannelHandlerContext ctxOf(String consumerId) {
        io.netty.channel.ChannelHandlerContext ctx = mock(io.netty.channel.ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        io.netty.channel.ChannelId channelId = mock(io.netty.channel.ChannelId.class);
        org.mockito.Mockito.when(ctx.channel()).thenReturn(channel);
        org.mockito.Mockito.when(channel.id()).thenReturn(channelId);
        org.mockito.Mockito.when(channelId.asLongText()).thenReturn(consumerId);
        return ctx;
    }
}
