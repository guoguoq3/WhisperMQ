package org.guoguo.broker.util;

import com.alibaba.fastjson.JSON;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.common.config.MqConfigProperties;
import org.guoguo.common.pojo.DTO.DeadLetterDTO;
import org.guoguo.common.pojo.Entity.DeadLetterQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 死信队列持久化。
 *
 * <p>P0 改造点：
 * <ol>
 *   <li>启用启动恢复：旧实现把 {@code recoverDeadLettersFromFile} 整段注释掉了，
 *       重启后内存死信队列为空，磁盘上的死信变成"只写不读"的黑洞；</li>
 *   <li>新增"已重放"标记文件：重放成功的死信在重启后不会被再次加载，避免重复重放；</li>
 *   <li>修复滚动建目录路径笔误（{@code /deadletter} → {@code /deadletters}）。</li>
 * </ol>
 */
@Component
@Slf4j
public class DeadLetterPersistUtil {

    /** 已重放死信标记文件（故意不带 WhisperMQ-deadletter- 前缀，避免被当成正常死信文件加载） */
    private static final String REPLAYED_FILE_NAME = "replayed-ids.log";

    private final MqConfigProperties mqConfigProperties;
    private final DeadLetterQueue deadLetterQueue;

    // 当前正在写入的死信持久化文件
    private File currentDeadLetterFile;
    // 文件写入流
    private BufferedWriter writer;
    // 已经重放过的死信 ID
    private final Set<String> replayedIds = ConcurrentHashMap.newKeySet();
    private File replayedMarkerFile;

    @Autowired
    public DeadLetterPersistUtil(MqConfigProperties mqConfigProperties, DeadLetterQueue deadLetterQueue) {
        this.mqConfigProperties = mqConfigProperties;
        this.deadLetterQueue = deadLetterQueue;
    }

    /**
     * Broker启动时自动执行：初始化死信持久化目录、恢复历史死信消息
     */
    @PostConstruct
    public void init() {
        try {
            // 创建死信持久化目录
            File persistDir = new File(mqConfigProperties.getPersistPath() + "/deadletters");
            if (!persistDir.exists()) {
                boolean mkdirSuccess = persistDir.mkdirs();
                if (mkdirSuccess) {
                    log.info("WhisperMQ==============> 创建死信持久化目录成功 ：{}", persistDir.getAbsolutePath());
                } else {
                    log.info("WhisperMQ==============> 创建死信持久化目录失败{}", persistDir.getAbsolutePath());
                    throw new RuntimeException("死信持久化目录创建失败，Broker 启动异常");
                }
            }

            // 查找最新的死信持久化文件
            currentDeadLetterFile = findLatestDeadLetterFile(persistDir);
            if (currentDeadLetterFile == null) {
                currentDeadLetterFile = createNewDeadLetterFile(persistDir);
            }

            // 创建文件写入流
            writer = Files.newBufferedWriter(currentDeadLetterFile.toPath(), StandardCharsets.UTF_8, StandardOpenOption.APPEND);

            // 先加载"已重放"标记，再恢复历史死信，避免把重放过的死信又加载回来
            replayedMarkerFile = new File(persistDir, REPLAYED_FILE_NAME);
            loadReplayedIds();

            // 从历史文件中恢复死信消息到内存
            recoverDeadLettersFromFile(persistDir);

        } catch (Exception e) {
            log.error("WhisperMQ异常，初始化死信持久化目录失败======================>", e);
            throw new RuntimeException("初始化死信持久化目录失败，Broker 启动异常", e);
        }
    }

    /**
     * 写入死信消息到持久化文件
     *
     * @param deadLetterDTO 死信消息
     */
    public synchronized void writeDeadLetter(DeadLetterDTO deadLetterDTO) {
        try {
            // 检查当前文件是否超过最大大小，超过则创建新文件
            if (currentDeadLetterFile.length() >= mqConfigProperties.getMaxFileSize()) {
                log.info("WhisperMQ==============> 当前死信文件已超过最大大小，创建新的文件");
                // 关闭流
                writer.flush();
                writer.close();
                // 创建新文件（修复：原路径少了一个 s，滚动时必然失败）
                currentDeadLetterFile = createNewDeadLetterFile(new File(mqConfigProperties.getPersistPath() + "/deadletters"));
                // 创建文件写入流
                writer = Files.newBufferedWriter(currentDeadLetterFile.toPath(), StandardCharsets.UTF_8, StandardOpenOption.APPEND);
            }

            // 进行消息封装
            String messageStr = "死信ID:" + deadLetterDTO.getDeadLetterId() + "|" + "消息原ID:" + deadLetterDTO.getMessageId() + "|" + "死信创建时间:" + deadLetterDTO.getDeadTime() + "|" + JSON.toJSONString(deadLetterDTO);
            // 写入文件
            writer.write(messageStr);
            // 换行
            writer.newLine();
            // 强制刷新
            writer.flush();

            log.info("WhisperMQ 死信消息持久化成功：文件={}，原ID={}，死信ID={}", currentDeadLetterFile.getName(), deadLetterDTO.getMessageId(), deadLetterDTO.getDeadLetterId());

        } catch (Exception e) {
            log.error("WhisperMQ 死信消息持久化失败，原ID={}，死信ID={}", deadLetterDTO.getMessageId(), deadLetterDTO.getDeadLetterId(), e);
        }
    }

    /**
     * 记录"已重放"标记：追加一行死信 ID。重启加载时跳过这些 ID。
     */
    public synchronized void writeReplayedMarker(String deadLetterId) {
        if (deadLetterId == null) {
            return;
        }
        try {
            if (replayedMarkerFile == null) {
                replayedMarkerFile = new File(mqConfigProperties.getPersistPath() + "/deadletters", REPLAYED_FILE_NAME);
            }
            Files.writeString(replayedMarkerFile.toPath(),
                    deadLetterId + System.lineSeparator(),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            replayedIds.add(deadLetterId);
            log.info("WhisperMQ 记录死信重放标记：{}", deadLetterId);
        } catch (Exception e) {
            log.error("记录死信重放标记失败：{}", deadLetterId, e);
        }
    }

    private void loadReplayedIds() {
        if (replayedMarkerFile == null || !replayedMarkerFile.exists()) {
            return;
        }
        try (BufferedReader reader = Files.newBufferedReader(replayedMarkerFile.toPath(), StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty()) {
                    replayedIds.add(line);
                }
            }
            log.info("WhisperMQ 已加载{}条死信重放标记", replayedIds.size());
        } catch (Exception e) {
            log.error("加载死信重放标记失败", e);
        }
    }

    /**
     * 从历史文件恢复死信到内存队列。
     * 行格式：{@code 死信ID:xxx|消息原ID:yyy|死信创建时间:zzz|<DeadLetterDTO JSON>}
     */
    private void recoverDeadLettersFromFile(File persistDir) {
        File[] files = persistDir.listFiles((dir, name) ->
                name.startsWith("WhisperMQ-deadletter-") && name.endsWith(".log"));
        if (files == null || files.length == 0) {
            log.info("WhisperMQ 无历史死信文件，无需恢复");
            return;
        }

        int recovered = 0;
        int skipped = 0;
        int failed = 0;
        for (File file : files) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                    new FileInputStream(file), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) {
                        continue;
                    }
                    DeadLetterDTO dto = parseDeadLetterLine(line);
                    if (dto == null) {
                        failed++;
                        continue;
                    }
                    if (replayedIds.contains(dto.getDeadLetterId())) {
                        skipped++;
                        continue;
                    }
                    try {
                        deadLetterQueue.put(dto);
                        recovered++;
                    } catch (IllegalArgumentException duplicate) {
                        // 同一个死信 ID 在日志里出现多次（例如重试导致的重复写入），只保留一条
                        skipped++;
                    }
                }
            } catch (Exception e) {
                log.error("WhisperMQ 从死信文件 {} 恢复失败", file.getName(), e);
            }
        }
        log.info("WhisperMQ 死信恢复完成：恢复{}条，跳过{}条（已重放/重复），解析失败{}条，当前积压{}条",
                recovered, skipped, failed, deadLetterQueue.size());
    }

    /**
     * 解析一行死信记录。用 limit=4 保留 JSON 中原有的 {@code |}（payload 里可能包含竖线）。
     */
    private DeadLetterDTO parseDeadLetterLine(String line) {
        try {
            String[] parts = line.split("\\|", 4);
            if (parts.length < 4) {
                log.warn("WhisperMQ 死信记录格式非法，跳过：{}", line);
                return null;
            }
            DeadLetterDTO dto = JSON.parseObject(parts[3], DeadLetterDTO.class);
            if (dto == null || dto.getDeadLetterId() == null) {
                return null;
            }
            // 兼容：以 JSON 内的 ID 为准，前缀只做人类可读展示
            return dto;
        } catch (Exception e) {
            log.warn("WhisperMQ 死信记录解析失败，跳过：{}", line, e);
            return null;
        }
    }

    /**
     * 查找最新的死信持久化文件
     */
    private File findLatestDeadLetterFile(File persistDir) {
        // 获取persistDir目录下所有以"WhisperMQ-deadletter-"开头且以".log"结尾的文件列表
        File[] files = persistDir.listFiles((dir, name) -> name.startsWith("WhisperMQ-deadletter-") && name.endsWith(".log"));
        if (files == null || files.length == 0) {
            return null;
        }

        // 按照修改日期降序排列
        File latestFile = files[0];
        for (int i = 1; i < files.length; i++) {
            if (files[i].lastModified() > latestFile.lastModified()) {
                latestFile = files[i];
            }
        }
        log.info("WhisperMQ==============> 找到最新的死信持久化文件：{}", latestFile.getAbsolutePath());
        return latestFile;
    }

    /**
     * 创建新的死信持久化文件
     */
    private File createNewDeadLetterFile(File persistDir) {
        String fileName = "WhisperMQ-deadletter-" + System.currentTimeMillis() + ".log";
        File newFile = new File(persistDir, fileName);
        try {
            boolean createSuccess = newFile.createNewFile();
            if (createSuccess) {
                log.info("WhisperMQ==============> 新死信持久化文件创建成功：{}", newFile.getAbsolutePath());
                return newFile;
            } else {
                log.info("WhisperMQ==============> 新死信持久化文件创建失败：{}", newFile.getAbsolutePath());
                throw new IOException("新死信持久化文件创建失败");
            }
        } catch (Exception e) {
            log.error("WhisperMQ 新死信持久化文件创建失败", e);
            throw new RuntimeException("新死信持久化文件创建失败", e);
        }
    }

    /**
     * 关闭资源
     */
    public void close() {
        try {
            if (writer != null) {
                writer.flush();
                writer.close();
            }
            log.info("死信持久化资源已关闭");
        } catch (IOException e) {
            log.error("关闭死信持久化资源失败", e);
        }
    }
}
