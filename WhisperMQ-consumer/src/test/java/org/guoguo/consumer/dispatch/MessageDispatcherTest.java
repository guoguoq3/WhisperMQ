package org.guoguo.consumer.dispatch;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务线程池（P1）：同 topic 串行、跨 topic 并行、异常隔离、队列满时的背压语义。
 */
class MessageDispatcherTest {

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void sameTopicKeepsFifoOrderAndRunsOffCallerThread() throws Exception {
        MessageDispatcher dispatcher = new MessageDispatcher(2, 1000);
        try {
            List<Integer> order = Collections.synchronizedList(new ArrayList<>());
            Set<String> threads = ConcurrentHashMap.newKeySet();
            CountDownLatch latch = new CountDownLatch(100);

            for (int i = 0; i < 100; i++) {
                final int value = i;
                boolean accepted = dispatcher.submit("topic-A", () -> {
                    threads.add(Thread.currentThread().getName());
                    order.add(value);
                    latch.countDown();
                });
                assertTrue(accepted);
            }

            assertTrue(latch.await(5, TimeUnit.SECONDS), "100 个任务应在 5s 内执行完");
            assertEquals(100, order.size());
            for (int i = 0; i < 100; i++) {
                assertEquals(i, order.get(i), "同一 topic 必须保持提交顺序");
            }
            for (String thread : threads) {
                assertTrue(thread.startsWith("mq-consumer-worker-"), "不应在调用线程执行：" + thread);
            }
            assertFalse(threads.contains(Thread.currentThread().getName()));
            assertEquals(100, dispatcher.getCompleted());
        } finally {
            dispatcher.close();
        }
    }

    @Test
    void differentTopicsCanUseDifferentWorkers() {
        MessageDispatcher dispatcher = new MessageDispatcher(4, 100);
        try {
            Set<Integer> indexes = new HashSet<>();
            for (int i = 0; i < 50; i++) {
                indexes.add(dispatcher.workerIndex("topic-" + i));
            }
            assertTrue(indexes.size() >= 2, "50 个 topic 至少应分散到 2 个 worker");
        } finally {
            dispatcher.close();
        }
    }

    @Test
    void fullQueueIsRejectedInsteadOfBlockingIoThread() throws Exception {
        MessageDispatcher dispatcher = new MessageDispatcher(1, 1);
        CountDownLatch gate = new CountDownLatch(1);
        try {
            assertTrue(dispatcher.submit("k", () -> await(gate)), "第一个任务应被接受（占住 worker）");
            assertTrue(dispatcher.submit("k", () -> { }), "第二个任务进队列");
            assertFalse(dispatcher.submit("k", () -> { }), "队列满后应拒绝而不是阻塞");
            assertEquals(1, dispatcher.getRejected());
        } finally {
            gate.countDown();
            dispatcher.close();
        }
    }

    @Test
    void taskExceptionDoesNotKillWorker() throws Exception {
        MessageDispatcher dispatcher = new MessageDispatcher(1, 10);
        try {
            CountDownLatch done = new CountDownLatch(1);
            assertTrue(dispatcher.submit("k", () -> {
                throw new IllegalStateException("boom");
            }));
            assertTrue(dispatcher.submit("k", done::countDown));

            assertTrue(done.await(3, TimeUnit.SECONDS), "异常任务之后 worker 仍应可用");
            assertEquals(1, dispatcher.getFailed());
        } finally {
            dispatcher.close();
        }
    }

    @Test
    void submitAfterCloseIsRejected() {
        MessageDispatcher dispatcher = new MessageDispatcher(1, 10);
        dispatcher.close();
        assertFalse(dispatcher.submit("k", () -> { }));
        assertEquals(0, dispatcher.getSubmitted());
    }

    @Test
    void workerIndexIsStableForSameKey() {
        MessageDispatcher dispatcher = new MessageDispatcher(4, 10);
        try {
            assertEquals(dispatcher.workerIndex("same"), dispatcher.workerIndex("same"));
            assertNotEquals(-1, dispatcher.workerIndex(null));
        } finally {
            dispatcher.close();
        }
    }
}
