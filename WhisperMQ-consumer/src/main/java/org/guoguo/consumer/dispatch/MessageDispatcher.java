package org.guoguo.consumer.dispatch;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 消费者业务线程池。
 *
 * <p>P1 改造点：旧实现直接在 Netty IO 线程里调用 {@code listener.onMessage(...)}，
 * 一条慢消息会阻塞该连接的所有读写（包括 ACK、心跳、重连后的订阅响应），
 * 严重时还会触发 Broker 的读空闲检测。现在改为：
 * <ul>
 *   <li>每个 worker 是单线程 + 有界队列；</li>
 *   <li>按 topic 哈希固定分片 → 同一 topic 内严格 FIFO，不同 topic 可并行；</li>
 *   <li>队列满时 {@link #submit} 返回 false，调用方不 ACK，让 Broker 的 ACK 超时重投兜底（背压）。</li>
 * </ul>
 */
@Slf4j
public class MessageDispatcher implements AutoCloseable {

    private final ExecutorService[] workers;
    private final int queueCapacity;
    private final AtomicLong submitted = new AtomicLong();
    private final AtomicLong completed = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();
    private volatile boolean closed = false;

    public MessageDispatcher(int workerCount, int queueCapacity) {
        int n = Math.max(1, workerCount);
        int capacity = Math.max(1, queueCapacity);
        this.queueCapacity = capacity;
        this.workers = new ExecutorService[n];
        for (int i = 0; i < n; i++) {
            final int index = i;
            this.workers[i] = new ThreadPoolExecutor(
                    1, 1, 0L, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(capacity),
                    r -> {
                        Thread thread = new Thread(r, "mq-consumer-worker-" + index);
                        thread.setDaemon(true);
                        return thread;
                    },
                    new ThreadPoolExecutor.AbortPolicy());
        }
        log.info("消费者业务线程池已创建：worker={}，每个 worker 队列容量={}", n, capacity);
    }

    /**
     * 提交一条消息处理任务。
     *
     * @return false 表示线程池已关闭或队列已满，调用方不要 ACK（交给 Broker 超时重投）
     */
    public boolean submit(String key, Runnable task) {
        if (closed || task == null) {
            rejected.incrementAndGet();
            return false;
        }
        ExecutorService worker = workers[workerIndex(key)];
        submitted.incrementAndGet();
        try {
            worker.execute(() -> {
                try {
                    task.run();
                    completed.incrementAndGet();
                } catch (Throwable t) {
                    failed.incrementAndGet();
                    log.error("消费者业务线程执行消息处理任务异常", t);
                }
            });
            return true;
        } catch (RejectedExecutionException e) {
            submitted.decrementAndGet();
            rejected.incrementAndGet();
            log.warn("消费者业务线程池已满（topic={}），本消息暂不处理", key);
            return false;
        }
    }

    /** 稳定分片：同一个 topic 永远落到同一个 worker，保证该 topic 内的处理顺序 */
    public int workerIndex(String key) {
        return Math.floorMod(key == null ? 0 : key.hashCode(), workers.length);
    }

    public int getWorkerCount() {
        return workers.length;
    }

    /** 当前排队中的任务总数（背压判断用） */
    public int queued() {
        int total = 0;
        for (ExecutorService worker : workers) {
            total += ((ThreadPoolExecutor) worker).getQueue().size();
        }
        return total;
    }

    /** 全部 worker 的队列总容量 */
    public int capacity() {
        return workers.length * queueCapacity;
    }

    public long getSubmitted() {
        return submitted.get();
    }

    public long getCompleted() {
        return completed.get();
    }

    public long getFailed() {
        return failed.get();
    }

    public long getRejected() {
        return rejected.get();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (ExecutorService worker : workers) {
            worker.shutdown();
        }
        for (ExecutorService worker : workers) {
            try {
                if (!worker.awaitTermination(3, TimeUnit.SECONDS)) {
                    worker.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                worker.shutdownNow();
            }
        }
        log.info("消费者业务线程池已关闭：submitted={}，completed={}，failed={}，rejected={}",
                submitted.get(), completed.get(), failed.get(), rejected.get());
    }
}
