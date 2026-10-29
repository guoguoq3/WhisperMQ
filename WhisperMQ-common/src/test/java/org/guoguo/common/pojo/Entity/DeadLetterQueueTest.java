package org.guoguo.common.pojo.Entity;

import org.guoguo.common.constant.DeadType;
import org.guoguo.common.pojo.DTO.DeadLetterDTO;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 死信队列容器（P0 修复 + P1 能力）：空队列、去重、原子移除、快照。
 */
class DeadLetterQueueTest {

    private DeadLetterDTO newDeadLetter() {
        DeadLetterDTO dto = new DeadLetterDTO();
        dto.setMessageId("origin");
        dto.setDeadType(DeadType.CONSUME_TIMEOUT);
        return dto;
    }

    @Test
    void emptyQueueReturnsNullInsteadOfThrowing() {
        DeadLetterQueue queue = new DeadLetterQueue();
        // 旧实现 deadLetterMap.get(null) 会抛 NPE
        assertNull(queue.get());
        assertNull(queue.remove());
        assertNull(queue.get("not-exist"));
        assertNull(queue.removeById(null));
        assertEquals(0, queue.size());
        assertEquals(0, queue.list().size());
    }

    @Test
    void putThenGetAndRemoveByIdWorks() {
        DeadLetterQueue queue = new DeadLetterQueue();
        DeadLetterDTO dto = newDeadLetter();
        queue.put(dto);

        assertEquals(1, queue.size());
        assertSame(dto, queue.get());
        assertSame(dto, queue.get(dto.getDeadLetterId()));

        assertSame(dto, queue.removeById(dto.getDeadLetterId()));
        assertEquals(0, queue.size());
        assertNull(queue.removeById(dto.getDeadLetterId()), "重复移除应返回 null");
    }

    @Test
    void duplicateIdIsRejected() {
        DeadLetterQueue queue = new DeadLetterQueue();
        DeadLetterDTO dto = newDeadLetter();
        queue.put(dto);
        assertThrows(IllegalArgumentException.class, () -> queue.put(dto));
        assertEquals(1, queue.size());
    }

    @Test
    void listKeepsInsertionOrder() {
        DeadLetterQueue queue = new DeadLetterQueue();
        DeadLetterDTO first = newDeadLetter();
        DeadLetterDTO second = newDeadLetter();
        queue.put(first);
        queue.put(second);

        assertEquals(2, queue.list().size());
        assertSame(first, queue.list().get(0));
        assertSame(second, queue.list().get(1));
        assertNotNull(queue.get());
    }
}
