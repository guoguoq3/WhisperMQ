package org.guoguo.broker.util;

import com.alibaba.fastjson.JSON;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.common.config.MqConfigProperties;
import org.guoguo.common.pojo.DTO.DeadLetterDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * 死信队列持久化工具类：负责死信消息写入文件、Broker启动时恢复死信消息
 */
@Component
@Slf4j
public class DeadLetterPersistUtil {

    private final MqConfigProperties mqConfigProperties;
    // 死信队列内存存储
    private final Map<String, DeadLetterDTO> deadLetterMap = new ConcurrentHashMap<>();
    // 当前正在写入的死信持久化文件
    private File currentDeadLetterFile;
    // 文件写入流
    private BufferedWriter writer;

    @Autowired
    public DeadLetterPersistUtil(MqConfigProperties mqConfigProperties) {
        this.mqConfigProperties = mqConfigProperties;
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
/*

            // 从历史文件中恢复死信消息到内存
            recoverDeadLettersFromFile(persistDir);
*/

        } catch (Exception e) {
            log.error("WhisperMQ异常，初始化死信持久化目录失败======================>", e);
            throw new RuntimeException("初始化死信持久化目录失败，Broker 启动异常");
        }
    }

    /**
     * 写入死信消息到持久化文件
     * @param deadLetterDTO 死信消息
     */
    public synchronized void writeDeadLetter(DeadLetterDTO deadLetterDTO) {
        try {
            // 检查当前文件是否超过最大大小，超过则创建新文件
            if(currentDeadLetterFile.length() >= mqConfigProperties.getMaxFileSize()){
                log.info("WhisperMQ==============> 当前死信文件已超过最大大小，创建新的文件");
                // 关闭流
                writer.close();
                // 创建新文件
                currentDeadLetterFile = createNewDeadLetterFile(new File(mqConfigProperties.getPersistPath() + "/deadletter"));
                // 创建文件写入流
                writer = Files.newBufferedWriter(currentDeadLetterFile.toPath(), StandardCharsets.UTF_8, StandardOpenOption.APPEND);
            }

            // 进行消息封装
            String messageStr = deadLetterDTO.getDeadLetterId() + "|" + JSON.toJSONString(deadLetterDTO);
            // 写入文件
            writer.write(messageStr);
            // 换行
            writer.newLine();
            // 强制刷新
            writer.flush();
            
            // 存储到内存
            deadLetterMap.put(deadLetterDTO.getDeadLetterId(), deadLetterDTO);
            
            log.info("WhisperMQ 死信消息持久化成功：文件={}，死信ID={}", currentDeadLetterFile.getName(), deadLetterDTO.getDeadLetterId());

        } catch (Exception e) {
            log.error("WhisperMQ 死信消息持久化失败，死信ID={}", deadLetterDTO.getDeadLetterId(), e);
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
/*

    */
/**
     * 从所有历史文件中恢复死信消息到内存
     *//*

    private void recoverDeadLettersFromFile(File persistDir) {
        File[] files = persistDir.listFiles((dir, name) -> name.startsWith("WhisperMQ-deadletter-") && name.endsWith(".log"));
        if (files == null || files.length == 0) {
            log.info("WhisperMQ 无历史死信持久化文件，无需恢复消息");
            return;
        }

        List<String> recoverFailedIds = new ArrayList<>();
        int recoverCount = 0;

        // 遍历所有死信持久化文件，按行读取消息
        for (File file : files) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                    new FileInputStream(file), StandardCharsets.UTF_8
            ))) {
                String line;
                // 按行读取（每行一个消息）
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) {
                        continue;
                    }

                    // 解析"死信ID|死信JSON"
                    String[] parts = line.split("\\|", 2);
                    if (parts.length != 2) {
                        log.warn("WhisperMQ 死信持久化文件解析异常，无效行：{}", line);
                        continue;
                    }

                    String deadLetterId = parts[0];
                    String deadLetterJson = parts[1];
                    
                    try {
                        // 反序列化为 DeadLetterDTO
                        DeadLetterDTO deadLetter = JSON.parseObject(deadLetterJson, DeadLetterDTO.class);
                        // 恢复到内存
                        deadLetterMap.put(deadLetterId, deadLetter);
                        recoverCount++;
                    } catch (Exception e) {
                        recoverFailedIds.add(deadLetterId);
                        log.error("WhisperMQ 文件内恢复死信消息序列化失败，文件：{} 死信ID: {}", file.getName(), deadLetterId, e);
                    }
                }
                log.info("WhisperMQ 从文件 {} 恢复死信消息 {} 条", file.getName(), recoverCount);
            } catch (Exception e) {
                log.error("WhisperMQ 从文件 {} 恢复死信消息失败", file.getName(), e);
            }
        }

        log.info("WhisperMQ 死信消息恢复完成，共恢复 {} 条死信消息，失败 {} 条",
                recoverCount, recoverFailedIds.size());
        if (!recoverFailedIds.isEmpty()) {
            log.warn("WhisperMQ 恢复失败的死信ID：{}", recoverFailedIds);
        }
    }

    */
/**
     * 获取所有死信消息
     *//*

    public Map<String, DeadLetterDTO> getDeadLetterMap() {
        return deadLetterMap;
    }

    */
/**
     * 根据ID获取死信消息
     *//*

    public DeadLetterDTO getDeadLetter(String deadLetterId) {
        return deadLetterMap.get(deadLetterId);
    }
*/

    /**
     * 关闭资源
     */
    public void close() {
        try {
            if (writer != null) {
                writer.close();
            }
            log.info("死信持久化资源已关闭");
        } catch (IOException e) {
            log.error("关闭死信持久化资源失败", e);
        }
    }
}
