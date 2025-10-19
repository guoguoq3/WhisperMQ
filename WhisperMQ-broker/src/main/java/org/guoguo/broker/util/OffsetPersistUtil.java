package org.guoguo.broker.util;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.common.config.MqConfigProperties;
import org.guoguo.common.pojo.Entity.ConsumerGroup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@Component
public class OffsetPersistUtil {
    private final MqConfigProperties mqConfigProperties;
    private final Map<String, String> persistedUniqueKeyCache = new ConcurrentHashMap<>();
    private static final Pattern OFFSET_PATTERN = Pattern.compile("^(?<uniqueKey>[^|]+)\\|(?<offset>\\d+)$");
    private final Map<String, BufferedWriter> groupWriterMap = new ConcurrentHashMap<>();
    private final Map<String, File> groupFileMap = new ConcurrentHashMap<>();
    private ScheduledFuture<?> flushTaskFuture;
    private final ReentrantLock taskLock = new ReentrantLock();
    private final ScheduledExecutorService scheduler;
    private final Map<String, Object> groupLocks = new ConcurrentHashMap<>();
    private final AtomicLong writeCount = new AtomicLong(0);
    private volatile boolean isFlushTaskRunning = false;

    // 新增：标记当前是否为高频写入模式（用于动态切换刷盘策略）
    private volatile boolean isHighFrequencyMode = false;

    @Autowired
    public OffsetPersistUtil(MqConfigProperties mqConfigProperties) {
        this.mqConfigProperties = mqConfigProperties;
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread thread = new Thread(r, "offset-persist-thread");
            thread.setDaemon(true);
            return thread;
        });
        scheduler.scheduleAtFixedRate(this::monitorWriteCount,
                0,
                mqConfigProperties.getWindowSizeMillis(),
                TimeUnit.MILLISECONDS);
    }

    /**
     * 监控写入量，动态切换高频/低频模式
     */
    private void monitorWriteCount() {
        try {
            long currentWriteCount = writeCount.getAndSet(0);
            log.debug("当前窗口写入量: {}, 阈值: {}", currentWriteCount, mqConfigProperties.getFlushThreshold());

            // 高频模式判断：写入量≥阈值，开启定时批量刷盘
            if (currentWriteCount >= mqConfigProperties.getFlushThreshold()) {
                if (!isHighFrequencyMode) {
                    log.info("写入量超过阈值（{}），切换为高频模式（定时批量刷盘）", mqConfigProperties.getFlushThreshold());
                    isHighFrequencyMode = true;
                    startFlushTask(); // 启动定时刷盘
                }
            }
            // 低频模式判断：写入量<阈值，关闭定时刷盘，切换为一条一刷
            else {
                if (isHighFrequencyMode) {
                    log.info("写入量低于阈值（{}），切换为低频模式（一条一刷）", mqConfigProperties.getFlushThreshold());
                    isHighFrequencyMode = false;
                    stopFlushTask(); // 停止定时刷盘
                }
            }
        } catch (Exception e) {
            log.error("监控写入计数器失败", e);
        }
    }

    private void startFlushTask() {
        taskLock.lock();
        try {
            if (!isFlushTaskRunning) {
                flushTaskFuture = scheduler.scheduleAtFixedRate(
                        this::batchFlush,
                        0,
                        mqConfigProperties.getFlushIntervalMillis(),
                        TimeUnit.MILLISECONDS
                );
                isFlushTaskRunning = true;
                log.info("高频模式：定时flush任务已启动，间隔: {}ms", mqConfigProperties.getFlushIntervalMillis());
            }
        } finally {
            taskLock.unlock();
        }
    }

    private void stopFlushTask() {
        taskLock.lock();
        try {
            if (isFlushTaskRunning && flushTaskFuture != null) {
                boolean cancelled = flushTaskFuture.cancel(false);
                if (cancelled) {
                    isFlushTaskRunning = false;
                    log.info("高频模式：定时flush任务已停止");
                } else {
                    log.warn("高频模式：定时flush任务取消失败，可能已在执行中");
                }
            }
        } finally {
            taskLock.unlock();
        }
    }

    private void batchFlush() {
        CompletableFuture.runAsync(() -> {
            try {
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
                                log.debug("高频模式：消费者组[{}]批量flush完成", groupId);
                            }
                        }
                        return anyFlushed;
                    } catch (Exception e) {
                        log.error("高频模式：flush操作失败", e);
                        return false;
                    }
                }).get(mqConfigProperties.getFlushIntervalMillis(), TimeUnit.MILLISECONDS);

                if (result) {
                    log.debug("高频模式：定时批量flush完成");
                }
            } catch (TimeoutException e) {
                log.error("高频模式：flush操作超时", e);
            } catch (Exception e) {
                log.error("高频模式：批量flush执行失败", e);
            }
        });
    }

    public void init(ConsumerGroup consumerGroup, String topic) {
        String groupId = consumerGroup.getGroupId();
        try {
            File persistDir = new File(mqConfigProperties.getOffsetPersistPath());
            if (!persistDir.exists() && !persistDir.mkdirs()) {
                throw new RuntimeException("创建位点目录失败：" + persistDir.getAbsolutePath());
            }

            File groupFile = getOrCreateGroupFile(persistDir, groupId);
            groupFileMap.put(groupId, groupFile);

            BufferedWriter writer = Files.newBufferedWriter(
                    groupFile.toPath(),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.APPEND,
                    StandardOpenOption.CREATE
            );
            groupWriterMap.put(groupId, writer);

            recoverOffsetByGroupAndTopic(consumerGroup, topic);

        } catch (Exception e) {
            log.error("初始化消费者组[{}]位点失败", groupId, e);
            throw new RuntimeException("位点初始化失败", e);
        }
    }

    private File getOrCreateGroupFile(File persistDir, String groupId) throws IOException {
        String fileName = groupId + "-offset.log";
        File groupFile = new File(persistDir, fileName);

        if (groupFile.exists()) {
            log.debug("组[{}]文件已存在：{}", groupId, groupFile.getAbsolutePath());
            return groupFile;
        }
        boolean created = groupFile.createNewFile();
        if (created) {
            log.info("组[{}]创建新位点文件：{}", groupId, groupFile.getAbsolutePath());
            return groupFile;
        }
        if (groupFile.exists()) {
            log.debug("组[{}]文件已被其他线程创建：{}", groupId, groupFile.getAbsolutePath());
            return groupFile;
        }
        throw new IOException(
                String.format("组[%s]文件创建失败：%s。可能原因：权限不足/磁盘满/路径无效",
                        groupId, groupFile.getAbsolutePath())
        );
    }

    /**
     * 写入位点：根据高频/低频模式动态切换刷盘策略
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

            // 2. 检查文件大小，超过限制则滚动
            File groupFile = groupFileMap.get(groupId);
            if (groupFile.length() >= mqConfigProperties.getMaxFileSize()) {
                log.info("组[{}]文件超过最大大小，创建新文件", groupId);
                rotateGroupFile(groupId, groupFile);
            }

            // 3. 写入当前组的文件
            BufferedWriter writer = groupWriterMap.get(groupId);
            String line = uniqueKey + "|" + messageId + "\n";
            writer.write(line);

            // 4. 根据模式决定是否立即刷盘
            if (!isHighFrequencyMode) {
                // 低频模式：一条一刷（同步刷盘，确保数据不丢失）
                synchronized (groupLocks.computeIfAbsent(groupId, k -> new Object())) {
                    writer.flush();
                }
                log.debug("低频模式：组[{}]位点写入并立即刷盘：{}", groupId, line.trim());
            } else {
                // 高频模式：批量定时刷（仅写入缓冲，由定时任务统一刷盘）
                log.debug("高频模式：组[{}]位点写入缓冲：{}", groupId, line.trim());
            }

            // 5. 更新计数器和缓存
            writeCount.incrementAndGet();
            persistedUniqueKeyCache.put(uniqueKey, messageId);

        } catch (Exception e) {
            log.error("组[{}]位点写入失败（{}）", groupId, uniqueKey, e);
        }
    }

    public void recoverOffsetByGroupAndTopic(ConsumerGroup consumerGroup, String topic) {
        String groupId = consumerGroup.getGroupId();
        String targetUniqueKey = groupId + ":" + topic;
        File groupFile = groupFileMap.get(groupId);

        if (groupFile == null || !groupFile.exists()) {
            log.info("组[{}]无位点文件，无需恢复", groupId);
            return;
        }

        String latestOffset = null;
        try (BufferedReader reader = Files.newBufferedReader(groupFile.toPath())) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                Matcher matcher = OFFSET_PATTERN.matcher(line);
                if (matcher.matches() && matcher.group("uniqueKey").equals(targetUniqueKey)) {
                    latestOffset = matcher.group("offset");
                }
            }
        } catch (Exception e) {
            log.error("读取组[{}]文件失败", groupId, e);
            throw new RuntimeException(e);
        }

        if (latestOffset != null) {
            consumerGroup.getTopicOffsetMap().put(topic, latestOffset);
            persistedUniqueKeyCache.put(targetUniqueKey, latestOffset);
            log.info("组[{}]恢复位点成功（{}:{}）", groupId, topic, latestOffset);
        }
    }

    public Map<String, Map<String, String>> recoverAllOffset() {
        Map<String, Map<String, String>> result = new HashMap<>();
        File persistDir = new File(mqConfigProperties.getOffsetPersistPath());

        if (!persistDir.exists()) {
            log.info("位点目录不存在，全量恢复为空");
            return result;
        }

        File[] groupFiles = persistDir.listFiles((dir, name) -> name.endsWith("-offset.log"));
        if (groupFiles == null || groupFiles.length == 0) {
            log.info("无位点文件，全量恢复为空");
            return result;
        }

        for (File file : groupFiles) {
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
                        String[] keyParts = uniqueKey.split(":", 2);
                        if (keyParts.length == 2) {
                            topicOffsets.put(keyParts[1], offset);
                            persistedUniqueKeyCache.put(uniqueKey, offset);
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

    private void rotateGroupFile(String groupId, File oldFile) throws IOException {
        BufferedWriter oldWriter = groupWriterMap.get(groupId);
        oldWriter.close();

        String oldFileName = oldFile.getName();
        String newOldFileName = oldFileName.replace(".log", "-" + System.currentTimeMillis() + ".log");
        File newOldFile = new File(oldFile.getParent(), newOldFileName);
        if (!oldFile.renameTo(newOldFile)) {
            log.warn("组[{}]文件重命名失败，直接创建新文件", groupId);
        }

        File persistDir = new File(mqConfigProperties.getOffsetPersistPath());
        File newFile = getOrCreateGroupFile(persistDir, groupId);
        BufferedWriter newWriter = Files.newBufferedWriter(
                newFile.toPath(),
                StandardCharsets.UTF_8,
                StandardOpenOption.APPEND,
                StandardOpenOption.CREATE
        );

        groupFileMap.put(groupId, newFile);
        groupWriterMap.put(groupId, newWriter);
        log.info("组[{}]文件滚动完成，新文件：{}", groupId, newFile.getName());
    }

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
        scheduler.shutdown();
        log.info("所有位点资源已关闭");
    }
}