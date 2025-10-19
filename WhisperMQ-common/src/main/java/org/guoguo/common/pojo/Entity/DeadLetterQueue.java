package org.guoguo.common.pojo.Entity;
import lombok.Data;
import org.guoguo.common.pojo.DTO.DeadLetterDTO;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 线程安全的死信队列实现类
 * - 用ConcurrentLinkedQueue存储死信ID（保证顺序和线程安全）
 * - 用ConcurrentHashMap存储死信实体（保证查询效率和线程安全）
 * - 用ReentrantLock保证put/clean等复合操作的原子性
 */
@Configuration
public class DeadLetterQueue {
    // 存储死信ID的队列（线程安全，FIFO顺序）
    private final Queue<String> deadLetterIdQueue = new ConcurrentLinkedQueue<>();

    // 存储死信实体的Map（线程安全，key为deadLetterId）
    private final Map<String, DeadLetterDTO> deadLetterMap = new ConcurrentHashMap<>();

    // 重入锁：保证put、clean等操作的原子性（避免并发下的数据不一致）
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * 存入死信实体（线程安全）
     * 同时添加到队列（记录顺序）和Map（存储实体）
     * @param deadLetter 死信实体（必须包含有效deadLetterId）
     * @throws IllegalArgumentException 若死信ID为空或已存在则抛出异常
     */
    public void put(DeadLetterDTO deadLetter) {
        // 校验参数非空
        if (deadLetter == null) {
            throw new IllegalArgumentException("死信实体不能为null");
        }
        String deadLetterId = deadLetter.getDeadLetterId();
        if (deadLetterId == null || deadLetterId.trim().isEmpty()) {
            throw new IllegalArgumentException("死信ID不能为空");
        }

        // 加锁保证"检查ID是否存在+添加到队列和Map"的原子性
        lock.lock();
        try {
            // 避免重复添加相同ID的死信
            if (deadLetterMap.containsKey(deadLetterId)) {
                throw new IllegalArgumentException("死信ID已存在：" + deadLetterId);
            }
            // 先添加到Map，再添加到队列（避免队列有ID但Map无实体的中间状态）
            deadLetterMap.put(deadLetterId, deadLetter);
            deadLetterIdQueue.offer(deadLetterId);
        } finally {
            lock.unlock(); // 确保锁释放
        }
    }

    /**
     * 获取队列中最先进入的死信实体（线程安全）
     * @return 队首死信实体，若队列为空则返回null
     */
    public DeadLetterDTO get() {
        // 队列的peek()是线程安全的，直接获取队首ID
        String firstId = deadLetterIdQueue.peek();
        // Map的get()是线程安全的，根据ID查询实体
        return deadLetterMap.get(firstId);
    }

    /**
     * 根据死信ID获取对应的实体（线程安全）
     * @param deadLetterId 死信ID
     * @return 对应的死信实体，若不存在则返回null
     */
    public DeadLetterDTO get(String deadLetterId) {
        // 允许ID为null，此时Map.get()返回null
        return deadLetterMap.get(deadLetterId);
    }

    /**
     * 清除所有死信（线程安全）
     * 清空队列和Map中的所有数据
     */
    public void clean() {
        // 加锁保证"清空队列+清空Map"的原子性
        lock.lock();
        try {
            deadLetterIdQueue.clear();
            deadLetterMap.clear();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 可选：移除并返回队列中最先进入的死信（处理完成后调用）
     * @return 被移除的死信实体，若队列为空则返回null
     */
    public DeadLetterDTO remove() {
        lock.lock();
        try {
            String firstId = deadLetterIdQueue.poll(); // 从队列移除并返回ID
            if (firstId != null) {
                return deadLetterMap.remove(firstId); // 从Map移除并返回实体
            }
            return null;
        } finally {
            lock.unlock();
        }
    }
}