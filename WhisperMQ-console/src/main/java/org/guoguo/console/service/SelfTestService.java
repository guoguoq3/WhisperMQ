package org.guoguo.console.service;

import lombok.extern.slf4j.Slf4j;
import org.guoguo.common.pojo.DTO.DeadLetterDTO;
import org.guoguo.common.pojo.Entity.DeadLetterQueue;
import org.springframework.stereotype.Service;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/**
 * 一键自检：不依赖 Maven，直接在运行中的 Broker 上跑 4 个端到端场景：
 * <ol>
 *   <li>发送 → 消费 → ACK → 位点前进；</li>
 *   <li>ACK_FAIL → 立即重投（FAIL_ONCE）；</li>
 *   <li>同组内消费者级 Tag 隔离；</li>
 *   <li>投递次数耗尽 → 死信 → 重放。</li>
 * </ol>
 */
@Slf4j
@Service
public class SelfTestService {

    private final ConsoleService console;
    private final DeadLetterQueue deadLetterQueue;
    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r, "console-selftest");
        thread.setDaemon(true);
        return thread;
    });
    private final AtomicReference<TestRun> current = new AtomicReference<>();

    public SelfTestService(ConsoleService console, DeadLetterQueue deadLetterQueue) {
        this.console = console;
        this.deadLetterQueue = deadLetterQueue;
    }

    public synchronized Map<String, Object> start() {
        TestRun running = current.get();
        if (running != null && running.isRunning()) {
            return running.snapshot();
        }
        TestRun run = new TestRun();
        current.set(run);
        executor.submit(() -> execute(run));
        return run.snapshot();
    }

    public Map<String, Object> state() {
        TestRun run = current.get();
        if (run == null) {
            Map<String, Object> idle = new LinkedHashMap<>();
            idle.put("status", "IDLE");
            idle.put("steps", List.of());
            idle.put("logs", List.of());
            return idle;
        }
        return run.snapshot();
    }

    private void execute(TestRun run) {
        run.log("开始自检（4 个场景）");
        try {
            stepBasicSendConsume(run);
            stepAckFailRedelivery(run);
            stepTagIsolation(run);
            stepDeadLetterReplay(run);
        } catch (Throwable t) {
            log.error("自检执行异常", t);
            run.fail("自检执行", "异常：" + t);
        } finally {
            run.finish();
        }
    }

    // ------------------------------------------------------------------ 场景

    private void stepBasicSendConsume(TestRun run) {
        String group = "CONSOLE_SELF_BASIC_" + run.getSuffix();
        String topic = "CONSOLE_SELF_BASIC_TOPIC_" + run.getSuffix();
        ConsumerSession session = console.createConsumer(group, topic, List.of("*"), "SUCCESS");
        try {
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                Map<String, Object> result = console.send(topic, "TAG1", "self-basic-" + i, true, null);
                if (!Boolean.TRUE.equals(result.get("ok"))) {
                    run.fail("发送 → 消费 → ACK → 位点", "发送失败：" + result);
                    return;
                }
                ids.add(String.valueOf(result.get("messageId")));
            }
            boolean received = waitUntil(() -> session.getReceived() >= 3, 10_000);
            String lastId = ids.get(ids.size() - 1);
            boolean offsetOk = waitUntil(() -> lastId.equals(console.offsetOf(group, topic)), 5_000);
            run.step("发送 3 条 → 消费 → ACK → 位点前进", received && offsetOk,
                    "received=" + session.getReceived() + "，offset=" + console.offsetOf(group, topic)
                            + "，期望 offset=" + lastId);
        } finally {
            console.closeConsumer(session.getId());
        }
    }

    private void stepAckFailRedelivery(TestRun run) {
        String group = "CONSOLE_SELF_ACKFAIL_" + run.getSuffix();
        String topic = "CONSOLE_SELF_ACKFAIL_TOPIC_" + run.getSuffix();
        ConsumerSession session = console.createConsumer(group, topic, List.of("*"), "FAIL_ONCE");
        try {
            Map<String, Object> result = console.send(topic, "TAG1", "self-ackfail", true, null);
            if (!Boolean.TRUE.equals(result.get("ok"))) {
                run.fail("ACK_FAIL → 立即重投", "发送失败：" + result);
                return;
            }
            boolean redelivered = waitUntil(() -> session.getReceived() >= 2, 10_000);
            run.step("ACK_FAIL → Broker 立即重投", redelivered,
                    "received=" + session.getReceived() + "（期望 ≥ 2），failed=" + session.getFailed());
        } finally {
            console.closeConsumer(session.getId());
        }
    }

    private void stepTagIsolation(TestRun run) {
        String group = "CONSOLE_SELF_TAG_" + run.getSuffix();
        String topic = "CONSOLE_SELF_TAG_TOPIC_" + run.getSuffix();
        ConsumerSession consumerA = console.createConsumer(group, topic, List.of("TAG_A"), "SUCCESS");
        ConsumerSession consumerB = console.createConsumer(group, topic, List.of("TAG_B"), "SUCCESS");
        try {
            console.send(topic, "TAG_A", "for-a", true, null);
            console.send(topic, "TAG_B", "for-b", true, null);
            boolean bothReceived = waitUntil(
                    () -> consumerA.getReceived() >= 1 && consumerB.getReceived() >= 1, 10_000);
            sleep(500);
            boolean ok = bothReceived && consumerA.getReceived() == 1 && consumerB.getReceived() == 1;
            run.step("同组内消费者级 Tag 隔离", ok,
                    "TAG_A 收到 " + consumerA.getReceived() + " 条，TAG_B 收到 " + consumerB.getReceived() + " 条（各期望 1）");
        } finally {
            console.closeConsumer(consumerA.getId());
            console.closeConsumer(consumerB.getId());
        }
    }

    private void stepDeadLetterReplay(TestRun run) {
        String group = "CONSOLE_SELF_DLQ_" + run.getSuffix();
        String topic = "CONSOLE_SELF_DLQ_TOPIC_" + run.getSuffix();
        ConsumerSession session = console.createConsumer(group, topic, List.of("*"), "FAIL");
        try {
            Map<String, Object> result = console.send(topic, "TAG1", "self-dead-letter", true, null);
            if (!Boolean.TRUE.equals(result.get("ok"))) {
                run.fail("投递耗尽 → 死信 → 重放", "发送失败：" + result);
                return;
            }
            String messageId = String.valueOf(result.get("messageId"));
            DeadLetterDTO deadLetter = waitForDeadLetter(messageId, 20_000);
            if (deadLetter == null) {
                run.fail("投递耗尽 → 死信 → 重放", "等待死信超时（messageId=" + messageId + "）");
                return;
            }
            int before = session.getReceived();
            session.setMode("SUCCESS");
            Map<String, Object> replay = console.replayDeadLetter(deadLetter.getDeadLetterId());
            boolean delivered = waitUntil(() -> session.getReceived() > before, 10_000);
            run.step("投递耗尽 → 死信 → 重放", Boolean.TRUE.equals(replay.get("ok")) && delivered,
                    "死信=" + deadLetter.getDeadLetterId() + "，重放结果=" + replay
                            + "，重放后 received=" + session.getReceived());
        } finally {
            console.closeConsumer(session.getId());
        }
    }

    // ------------------------------------------------------------------ 工具

    private DeadLetterDTO waitForDeadLetter(String messageId, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            for (DeadLetterDTO dto : deadLetterQueue.list()) {
                if (messageId.equals(dto.getMessageId())) {
                    return dto;
                }
            }
            sleep(100);
        }
        return null;
    }

    private boolean waitUntil(BooleanSupplier condition, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            sleep(50);
        }
        return condition.getAsBoolean();
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ------------------------------------------------------------------ 运行状态

    public static class TestRun {
        private final String id = Long.toString(System.currentTimeMillis(), 36);
        private final String suffix = Long.toString(System.nanoTime() % 100_000, 36).toUpperCase();
        private final long startedAt = System.currentTimeMillis();
        private final AtomicInteger passed = new AtomicInteger();
        private final AtomicInteger failed = new AtomicInteger();
        private final List<Map<String, Object>> steps = Collections.synchronizedList(new ArrayList<>());
        private final ConcurrentLinkedDeque<String> logs = new ConcurrentLinkedDeque<>();
        private volatile boolean running = true;
        private volatile long finishedAt;

        public String getSuffix() {
            return suffix;
        }

        void log(String line) {
            logs.addLast("[" + LocalTime.now().withNano(0) + "] " + line);
            while (logs.size() > 300) {
                logs.pollFirst();
            }
        }

        void step(String name, boolean ok, String detail) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("name", name);
            item.put("ok", ok);
            item.put("detail", detail);
            item.put("at", System.currentTimeMillis());
            steps.add(item);
            if (ok) {
                passed.incrementAndGet();
            } else {
                failed.incrementAndGet();
            }
            log((ok ? "PASS " : "FAIL ") + name + " — " + detail);
        }

        void fail(String name, String detail) {
            step(name, false, detail);
        }

        void finish() {
            running = false;
            finishedAt = System.currentTimeMillis();
            log("自检结束：" + passed.get() + " 通过，" + failed.get() + " 失败");
        }

        public boolean isRunning() {
            return running;
        }

        Map<String, Object> snapshot() {
            Map<String, Object> snap = new LinkedHashMap<>();
            snap.put("id", id);
            snap.put("status", running ? "RUNNING" : (failed.get() == 0 ? "PASSED" : "FAILED"));
            snap.put("startedAt", startedAt);
            snap.put("finishedAt", finishedAt);
            snap.put("durationMillis", (finishedAt == 0 ? System.currentTimeMillis() : finishedAt) - startedAt);
            snap.put("passed", passed.get());
            snap.put("failed", failed.get());
            snap.put("steps", new ArrayList<>(steps));
            snap.put("logs", new ArrayList<>(logs));
            return snap;
        }
    }
}
