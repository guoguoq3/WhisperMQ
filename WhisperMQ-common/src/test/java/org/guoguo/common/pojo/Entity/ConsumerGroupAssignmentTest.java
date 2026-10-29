package org.guoguo.common.pojo.Entity;

import io.netty.channel.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * 订阅者集合与分区分配表（P1 Stage B）。
 */
class ConsumerGroupAssignmentTest {

    private ConsumerGroup group;

    @BeforeEach
    void setUp() {
        group = new ConsumerGroup();
        group.setGroupId("G");
    }

    private void online(String consumerId) {
        group.addConsumer(consumerId, mock(Channel.class));
    }

    @Test
    void onlyExplicitSubscribersAreCandidates() {
        online("c1");

        assertFalse(group.isSubscribed("T", "c1"), "仅入组、未订阅的连接不应参与分配");

        group.addSubscriber("T", "c1");
        assertTrue(group.isSubscribed("T", "c1"));
        assertEquals(List.of("c1"), group.onlineSubscribers("T"));
    }

    @Test
    void offlineConsumersAreNotCandidates() {
        group.addSubscriber("T", "c1");
        assertEquals(List.of(), group.onlineSubscribers("T"));

        online("c1");
        assertEquals(List.of("c1"), group.onlineSubscribers("T"));
    }

    @Test
    void assignmentUpdateBumpsEpochAndClears() {
        long before = group.getAssignmentEpoch();

        group.updateAssignment("T", Map.of(0, "c1", 1, "c2"));
        assertEquals(2, group.getAssignment("T").size());
        assertEquals("c2", group.getPartitionOwner("T", 1));
        assertTrue(group.getAssignmentEpoch() > before);

        group.clearAssignment("T");
        assertTrue(group.getAssignment("T").isEmpty());
        assertNull(group.getPartitionOwner("T", 0));
    }

    @Test
    void removingConsumerCleansSubscriberAndTags() {
        online("c1");
        group.addSubscriber("T", "c1");
        group.addConsumerTopicTags("T", "c1", List.of("A"));

        group.removeConsumer("c1");

        assertFalse(group.isSubscribed("T", "c1"));
        assertNull(group.getConsumerTags("T", "c1"));
        assertEquals(List.of(), group.onlineSubscribers("T"));
    }

    @Test
    void acceptsRespectsConsumerTags() {
        online("c1");
        group.addConsumerTopicTags("T", "c1", List.of("A"));
        assertTrue(group.accepts("T", "c1", "A"));
        assertFalse(group.accepts("T", "c1", "B"));
        assertTrue(group.accepts("T", "c1", null));

        online("c2");
        group.addConsumerTopicTags("T", "c2", List.of("*"));
        assertTrue(group.accepts("T", "c2", "B"));

        online("c3");
        group.addConsumerTopicTags("T", "c3", List.of());
        assertTrue(group.accepts("T", "c3", "B"));
    }
}
