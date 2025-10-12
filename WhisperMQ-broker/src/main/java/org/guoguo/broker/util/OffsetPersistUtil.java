package org.guoguo.broker.util;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.broker.ConsumerGroup.ConsumerGroupManager;
import org.guoguo.common.config.MqConfigProperties;
import org.guoguo.common.pojo.Entity.ConsumerGroup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
消费者组持久化时将 在持久化文件中的消费者会有唯一性
 */
@Slf4j
@Component
public class OffsetPersistUtil {
    private final MqConfigProperties mqConfigProperties;


    // 内存缓存：记录已持久化的唯一键（groupId:topic），避免重复写入（可选优化，减少文件IO） 目前需确保log日志其中键唯一 每次来新消息 就先缓存替换 每一段时间替换文件
    private final Map<String, String> persistedUniqueKeyCache = new ConcurrentHashMap<>();

    //用正则匹配Pattern是线程安全的，通过static final预编译后，可在多线程中共享，避免重复编译带来的性能损耗
    //每秒 10 万 + 条日志
    // 预编译正则：解析 "groupId:topic|offset" 格式
    private static final Pattern OFFSET_PATTERN = Pattern.compile("^(?<uniqueKey>[^|]+)\\|(?<offset>\\d+)$");

    // 每个消费者组的写入流（key=groupId）
    private final Map<String, BufferedWriter> groupWriterMap = new ConcurrentHashMap<>();
    // 每个消费者组的文件映射（避免重复创建流）
    private final Map<String, File> groupFileMap = new ConcurrentHashMap<>();

    //引入一个计数器 当在十秒内 计数器的值大于100 则开始定时执行flush 若十秒钟内峰值小于100 则停止定时任务
    // 用于控制flush任务的Future对象
    private ScheduledFuture<?> flushTaskFuture;

    // 确保任务启停的线程安全
    private final ReentrantLock taskLock = new ReentrantLock();
    // 单个线程池管理所有任务，避免频繁创建销毁
    private final ScheduledExecutorService scheduler;
    private final Map<String, Object> groupLocks=new ConcurrentHashMap<>();
    private final AtomicLong writeCount = new AtomicLong(0); // 写入计数器
    private volatile boolean isFlushTaskRunning = false; // 标记定时任务是否正在运行

    @Autowired
    public OffsetPersistUtil(MqConfigProperties mqConfigProperties) {
        this.mqConfigProperties = mqConfigProperties;



        // 初始化线程池并指定有意义的线程名
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread thread = new Thread(r, "offset-persist-thread");
            //守护线程
            thread.setDaemon(true);
            return thread;
        });

        // 启动监控任务
        scheduler.scheduleAtFixedRate(this::monitorWriteCount,
                0,
                //周期性执行
                mqConfigProperties.getWindowSizeMillis(),
                TimeUnit.MILLISECONDS);    }
    /**
     * 监控写入计数器并动态调整定时任务
     */
    private void monitorWriteCount() {
        try {
            long currentWriteCount = writeCount.getAndSet(0);
            log.debug("当前窗口写入量: {}, 阈值: {}", currentWriteCount, mqConfigProperties.getFlushThreshold());

            if (currentWriteCount >= mqConfigProperties.getFlushThreshold() && !isFlushTaskRunning) {
                log.info("写入量超过阈值（{}），启动定时flush任务", mqConfigProperties.getFlushThreshold());
                startFlushTask();
            } else if (currentWriteCount < mqConfigProperties.getFlushThreshold() && isFlushTaskRunning) {
                log.info("写入量低于阈值（{}），停止定时flush任务", mqConfigProperties.getFlushThreshold());
                stopFlushTask();
            }
        } catch (Exception e) {
            log.error("监控写入计数器失败", e);
        }
    }

    /**
     * 启动定时flush任务
     */
    private void startFlushTask() {
        taskLock.lock();
        try {
            if (!isFlushTaskRunning) {
                // 提交flush任务并保存Future引用
                flushTaskFuture = scheduler.scheduleAtFixedRate(
                        this::batchFlush,
                        0,
                        mqConfigProperties.getFlushIntervalMillis(),  // 使用单独的刷新间隔配置
                        TimeUnit.MILLISECONDS
                );
                isFlushTaskRunning = true;
                log.info("定时flush任务已启动，间隔: {}ms", mqConfigProperties.getFlushIntervalMillis());
            }
        } finally {
            taskLock.unlock();
        }
    }

    /**
     * 停止定时flush任务
     */
    private void stopFlushTask() {
        taskLock.lock();
        try {
            if (isFlushTaskRunning && flushTaskFuture != null) {
                // 取消当前任务但不影响线程池
                boolean cancelled = flushTaskFuture.cancel(false);
                if (cancelled) {
                    isFlushTaskRunning = false;
                    log.info("定时flush任务已停止");
                } else {
                    log.warn("定时flush任务取消失败，可能已在执行中");
                }
            }
        } finally {
            taskLock.unlock();
        }
    }

    /**
     * 批量flush逻辑，增加超时控制
     */
    private void batchFlush() {
        // 使用单独的线程执行实际flush操作，避免阻塞调度线程
        CompletableFuture.runAsync(() -> {
            try {
                // 执行flush并设置超时
                boolean result = CompletableFuture.supplyAsync(() -> {
                    try {
                        boolean anyFlushed = false;
                        for (Map.Entry<String, BufferedWriter> entry : groupWriterMap.entrySet()) {
                            String groupId = entry.getKey();
                            BufferedWriter writer = entry.getValue();

                            if (writer != null) {
                                synchronized (groupLocks.computeIfAbsent(groupId, k -> new Object())) {
                                    writer.flush();
                                }
                                anyFlushed = true;
                                log.info("消费者组[{}]批量flush完成", groupId);
                            }
                        }
                        return anyFlushed;
                    } catch (Exception e) {
                        log.error("flush操作失败", e);
                        return false;
                    }
                }).get(mqConfigProperties.getFlushIntervalMillis(), TimeUnit.MILLISECONDS);

                if (result) {
                    log.info("定时批量flush完成");
                }
            } catch (TimeoutException e) {
                log.error("flush操作超时", e);
                // 可以考虑标记该writer为异常状态
            } catch (Exception e) {
                log.error("批量flush执行失败", e);
            }
        });
    }


    /**
     * 不是启动时执行 而是一个消费者组订阅时调用这个方法 获取是否存在位点 如果有就直接恢复到其消费者组文件中
     */

    /**
     * 消费者组订阅时初始化：为该组创建专属文件并恢复位点
     */
    public void init(ConsumerGroup consumerGroup, String topic) {
        String groupId = consumerGroup.getGroupId();
        try {
            // 1. 创建根目录
            File persistDir = new File(mqConfigProperties.getOffsetPersistPath());
            if (!persistDir.exists() && !persistDir.mkdirs()) {
                throw new RuntimeException("创建位点目录失败：" + persistDir.getAbsolutePath());
            }

            // 2. 为消费者组创建专属文件（如 group1-offset.log）
            File groupFile = getOrCreateGroupFile(persistDir, groupId);
            groupFileMap.put(groupId, groupFile);

            // 3. 初始化该组的写入流
            BufferedWriter writer = Files.newBufferedWriter(
                    groupFile.toPath(),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.APPEND,
                    StandardOpenOption.CREATE
            );
            groupWriterMap.put(groupId, writer);

            // 4. 仅恢复当前组+主题的位点（无需遍历其他组文件）
            recoverOffsetByGroupAndTopic(consumerGroup, topic);

        } catch (Exception e) {
            log.error("初始化消费者组[{}]位点失败", groupId, e);
            throw new RuntimeException("位点初始化失败", e);
        }
    }


    /**
     * 获取或创建消费者组的专属文件（如 group1-offset.log）
     */
    private File getOrCreateGroupFile(File persistDir, String groupId) throws IOException {
        String fileName = groupId + "-offset.log";
        File groupFile = new File(persistDir, fileName);

        // 如果文件已存在，直接返回（无需创建）
        if (groupFile.exists()) {
            log.debug("组[{}]文件已存在：{}", groupId, groupFile.getAbsolutePath());
            return groupFile;
        }
        // 尝试创建文件
        boolean created = groupFile.createNewFile();
        if (created) {
            log.info("组[{}]创建新位点文件：{}", groupId, groupFile.getAbsolutePath());
            return groupFile;
        }
        // 若创建失败，再次检查文件是否存在（处理并发场景）
        if (groupFile.exists()) {
            log.debug("组[{}]文件已被其他线程创建：{}", groupId, groupFile.getAbsolutePath());
            return groupFile;
        }
        // 确实创建失败，抛出详细异常
        throw new IOException(
                String.format("组[%s]文件创建失败：%s。可能原因：权限不足/磁盘满/路径无效",
                        groupId, groupFile.getAbsolutePath())
        );
    }



    /**
     * 写入位点：仅操作当前消费者组的专属文件 todo：lock锁一下 有共享字眼
     */
    public void writeMessage(String groupId, String topic, String messageId) {
        if (!groupWriterMap.containsKey(groupId)) {
            log.error("消费者组[{}]未初始化，无法写入位点", groupId);
            return;
        }

        String uniqueKey = groupId + ":" + topic;
        try {
            // 1. 缓存判断：避免重复写入
            String cachedOffset = persistedUniqueKeyCache.get(uniqueKey);
            if (messageId.equals(cachedOffset)) {
                log.debug("位点无更新，跳过写入（{}）", uniqueKey);
                return;
            }

            // 2. 检查文件大小，超过限制则滚动（仅当前组文件）
            File groupFile = groupFileMap.get(groupId);
            if (groupFile.length() >= mqConfigProperties.getMaxFileSize()) {
                log.info("组[{}]文件超过最大大小，创建新文件", groupId);
                rotateGroupFile(groupId, groupFile);
            }

            // 3. 写入当前组的文件
            BufferedWriter writer = groupWriterMap.get(groupId);
            String line = uniqueKey + "|" + messageId + "\n";
            writer.write(line);

            // 更新写入计数器
            writeCount.incrementAndGet();

            // 4. 更新缓存
            persistedUniqueKeyCache.put(uniqueKey, messageId);
            log.debug("组[{}]位点写入成功：{}", groupId, line.trim());

        } catch (Exception e) {
            log.error("组[{}]位点写入失败（{}）", groupId, uniqueKey, e);
        }
    }


    /**
     * 从所有历史文件中恢复位点到内存
     * 注意在这些消息的恢复过程中都是单线程的 不会涉及到多并发这时系统还未开始正常处理外部请求，不需要并发处理
     * 但是可以 todo：多线程来进行消息恢复
     */
    public void recoverOffsetByGroupAndTopic(ConsumerGroup consumerGroup, String topic) {

        String groupId = consumerGroup.getGroupId();
        String targetUniqueKey = groupId + ":" + topic;
        File groupFile = groupFileMap.get(groupId);


        if (groupFile == null || !groupFile.exists()) {
            log.info("组[{}]无位点文件，无需恢复", groupId);
            return;
        }

        String latestOffset = null;
        //仅遍历当前组的位点文件
        try (BufferedReader reader = Files.newBufferedReader(groupFile.toPath())){
            String line;
            //todo:这块每行都做正则性能上
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                Matcher matcher = OFFSET_PATTERN.matcher(line);
                if (matcher.matches() && matcher.group("uniqueKey").equals(targetUniqueKey)) {
                    latestOffset = matcher.group("offset"); // 后出现的行覆盖旧值
                }
            }
        } catch (Exception e) {
            log.error("读取组[{}]文件失败", groupId, e);
            throw new RuntimeException(e);
        }

        if (latestOffset != null) {
            consumerGroup.getTopicOffsetMap().put(topic, latestOffset);
            //总的放一下
            persistedUniqueKeyCache.put(targetUniqueKey, latestOffset);
            log.info("组[{}]恢复位点成功（{}:{}）", groupId, topic, latestOffset);
        }



    }



    /**
     * 全量恢复：按组遍历所有文件（每个组单独处理）
     */
    public Map<String, Map<String, String>> recoverAllOffset() {
        Map<String, Map<String, String>> result = new HashMap<>();
        File persistDir = new File(mqConfigProperties.getOffsetPersistPath());

        if (!persistDir.exists()) {
            log.info("位点目录不存在，全量恢复为空");
            return result;
        }

        // 遍历所有组的文件（文件名格式：group1-offset.log）
        File[] groupFiles = persistDir.listFiles((dir, name) ->
                name.endsWith("-offset.log")
        );

        if (groupFiles == null || groupFiles.length == 0) {
            log.info("无位点文件，全量恢复为空");
            return result;
        }

        for (File file : groupFiles) {
            // 从文件名提取groupId（如 "group1-offset.log" → "group1"）
            String fileName = file.getName();
            String groupId = fileName.replace("-offset.log", "");

            Map<String, String> topicOffsets = new HashMap<>();
            try (BufferedReader reader = Files.newBufferedReader(file.toPath())) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    Matcher matcher = OFFSET_PATTERN.matcher(line);
                    if (matcher.matches()) {
                        String uniqueKey = matcher.group("uniqueKey");
                        String offset = matcher.group("offset");
                        // 解析出topic（uniqueKey格式：groupId:topic）
                        String[] keyParts = uniqueKey.split(":", 2);
                        if (keyParts.length == 2) {
                            topicOffsets.put(keyParts[1], offset);
                            persistedUniqueKeyCache.put(uniqueKey, offset); // 更新缓存
                        }
                    }
                }
            } catch (Exception e) {
                log.error("全量恢复：读取组[{}]文件失败", groupId, e);
            }

            if (!topicOffsets.isEmpty()) {
                result.put(groupId, topicOffsets);
            }
        }

        log.info("全量恢复完成，共恢复{}个消费者组的位点", result.size());
        return result;
    }

    /**
     * 滚动消费者组的文件（如 group1-offset.log → group1-offset-1699999999.log）
     */
    private void rotateGroupFile(String groupId, File oldFile) throws IOException {
        // 1. 关闭旧流
        BufferedWriter oldWriter = groupWriterMap.get(groupId);
        oldWriter.close();

        // 2. 重命名旧文件（加时间戳后缀）
        String oldFileName = oldFile.getName();
        String newOldFileName = oldFileName.replace(".log", "-" + System.currentTimeMillis() + ".log");
        File newOldFile = new File(oldFile.getParent(), newOldFileName);
        if (!oldFile.renameTo(newOldFile)) {
            log.warn("组[{}]文件重命名失败，直接创建新文件", groupId);
        }

        // 3. 创建新文件并初始化流
        File persistDir = new File(mqConfigProperties.getOffsetPersistPath());
        File newFile = getOrCreateGroupFile(persistDir, groupId);
        BufferedWriter newWriter = Files.newBufferedWriter(
                newFile.toPath(),
                StandardCharsets.UTF_8,
                StandardOpenOption.APPEND,
                StandardOpenOption.CREATE
        );

        // 4. 更新映射关系
        groupFileMap.put(groupId, newFile);
        groupWriterMap.put(groupId, newWriter);
        log.info("组[{}]文件滚动完成，新文件：{}", groupId, newFile.getName());
    }

    /**
     * 关闭指定组的资源（消费者组销毁时调用）
     */
    public void closeGroup(String groupId) {
        try {
            BufferedWriter writer = groupWriterMap.remove(groupId);
            if (writer != null) {
                writer.close();
            }
            groupFileMap.remove(groupId);
            log.info("组[{}]资源已关闭", groupId);
        } catch (IOException e) {
            log.error("组[{}]资源关闭失败", groupId, e);
        }
    }

    /**
     * 关闭所有资源（Broker关闭时调用）
     */
    @PreDestroy
    public void close() {
        groupWriterMap.forEach((groupId, writer) -> {
            try {
                writer.close();
            } catch (IOException e) {
                log.error("组[{}]流关闭失败", groupId, e);
            }
        });
        groupWriterMap.clear();
        groupFileMap.clear();
        log.info("所有位点资源已关闭");
    }
}