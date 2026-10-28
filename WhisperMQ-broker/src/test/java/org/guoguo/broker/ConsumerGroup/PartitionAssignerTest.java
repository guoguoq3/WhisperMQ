package org.guoguo.broker.ConsumerGroup;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 分区分配算法（P1 Stage B）。
 */
class PartitionAssignerTest {

    @Test
    void noConsumersYieldsNoAssignment() {
        assertTrue(PartitionAssigner.assign(List.of(), 4).isEmpty());
        assertTrue(PartitionAssigner.assign(null, 4).isEmpty());
        assertTrue(PartitionAssigner.assign(List.of("a"), 0).isEmpty());
    }

    @Test
    void singleConsumerOwnsEveryPartition() {
        Map<Integer, String> assignment = PartitionAssigner.assign(List.of("a"), 3);

        assertEquals(3, assignment.size());
        assertEquals("a", assignment.get(0));
        assertEquals("a", assignment.get(2));
    }

    @Test
    void roundRobinAcrossConsumers() {
        Map<Integer, String> assignment = PartitionAssigner.assign(List.of("a", "b"), 4);

        assertEquals("a", assignment.get(0));
        assertEquals("b", assignment.get(1));
        assertEquals("a", assignment.get(2));
        assertEquals("b", assignment.get(3));
    }

    @Test
    void moreConsumersThanPartitionsIsAllowed() {
        Map<Integer, String> assignment = PartitionAssigner.assign(List.of("a", "b", "c"), 2);

        assertEquals(2, assignment.size());
        assertEquals("a", assignment.get(0));
        assertEquals("b", assignment.get(1));
    }

    @Test
    void assignmentIsStableForSameInput() {
        List<String> consumers = List.of("c1", "c2", "c3");

        assertEquals(PartitionAssigner.assign(consumers, 7), PartitionAssigner.assign(consumers, 7));
    }
}
