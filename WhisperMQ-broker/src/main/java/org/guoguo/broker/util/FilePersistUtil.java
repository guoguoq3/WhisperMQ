package org.guoguo.broker.util;

import com.alibaba.fastjson.JSON;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.broker.core.BrokerManager;
import org.guoguo.common.config.MqConfigProperties;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.guoguo.common.pojo.Entity.MqMessageEnduring;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;


/**
 * 消息持久化工具类：负责消息写入文件、Broker 启动时恢复消息
 */
@Component
@Slf4j
public class FilePersistUtil {

    private final MqConfigProperties mqConfigProperties;
    private final BrokerManager brokerManager;
    //当前正在写入的持久化文件
    private File currentPersistFile;
    //文件写入流
    private BufferedWriter writer;




    /*
    构造函数注入 spring官方推荐 避免对象为空 防止在运行时出现NPE 不可变性 更易发现循环注入
     */
    @Autowired
    public FilePersistUtil(MqConfigProperties mqConfigProperties, BrokerManager brokerManager) {
        this.mqConfigProperties = mqConfigProperties;
        this.brokerManager = brokerManager;
    }

    /**
     * 1. Broker 启动时自动执行：初始化持久化目录、恢复历史消息
     */
    @PostConstruct
    public void init() {
        try {
            //若是持久化目录不存在就创建一个
            File persistDir = new File(mqConfigProperties.getPersistPath());
            if (!persistDir.exists()) {
                boolean mkdirSuccess = persistDir.mkdirs();
                if (mkdirSuccess) {
                    log.info("WhisperMQ==============> 创建持久化目录成功 ：{}", persistDir.getAbsolutePath());
                } else {
                    log.info("WhisperMQ==============> 创建持久化目录失败{}", persistDir.getAbsolutePath());
                    throw new RuntimeException("持久化目录创建失败，Broker 启动异常");
                }
            }
            //查找最新的持久化文件 如是有历史文件 优先用最新的 没有就创建文件
            currentPersistFile = findLatestPersistFile(persistDir);
            if (currentPersistFile == null) {
                currentPersistFile = createNewPersistFile(persistDir);
            }

            //创建文件写入流 BufferedWriter OutputStreamWriter(...) - 将字节流转换为字符流new FileOutputStream(currentPersistFile, true) - 创建文件输出流
//            writer = new  BufferedWriter(new  OutputStreamWriter(
//                    new FileOutputStream(currentPersistFile,true),  //true为追加写入
//                    StandardCharsets.UTF_8
//            ));
            //用这种 nio实现的是对传统 IO 的优化实现
            //第二种是传统 IO 的嵌套写法，涉及更多的对象层级转换 todo：亮点
            writer = Files.newBufferedWriter(currentPersistFile.toPath(), StandardCharsets.UTF_8, StandardOpenOption.APPEND);

            // 1.4 从历史文件中恢复消息到内存（messageMap）

            /*
            关于前面已经找到最新的持久化文件了吗 为啥还要在恢复历史消息时传入目录currentPersistFile 只是 “当前正在写入的文件”，而目录中可能有多个历史文件（如 WhisperMQ-persist-1.log、WhisperMQ-persist-2.log 等）；
            这些历史文件中存储了更早的消息，如果只恢复最新文件，会丢失旧文件中的消息。
            保证 无论消息存储在哪个文件中，Broker 重启后都能完整恢复
             */
            recoverMessagesFromFile(persistDir);

        } catch (Exception e) {
            log.error("WhisperMQ异常，初始化持久化目录失败======================>", e);
            throw new RuntimeException("初始化持久化目录失败，Broker 启动异常");

        }
    }

    /**
     * 2. 写入消息到持久化文件
     * @param messageId 消息ID
     * @param message 消息内容
     */
    public synchronized void writeMessage(String messageId, MqMessage message) {
       try {
           //监测当前文件是否超过最大大小 否则创建新的文件 （在broker启动时恢复的）
           log.info("设置文件大小为：{},实际大小为：{}",mqConfigProperties.getMaxFileSize(),currentPersistFile.length());
           if(currentPersistFile.length()>=mqConfigProperties.getMaxFileSize()){
                log.info("WhisperMQ==============> 当前文件已超过最大大小，创建新的文件");
                //关闭流
               writer.close();
               //创建新文件
               currentPersistFile=createNewPersistFile(new File(mqConfigProperties.getPersistPath()));
               //创建文件写入流 之后直接写就可以了  这里变了是因为换文件了
               //不能完全保持流不变，因为BufferedWriter是与特定文件关联的，当文件切换时，必须关闭旧的流并创建新的流。
               writer=Files.newBufferedWriter(currentPersistFile.toPath(), StandardCharsets.UTF_8, StandardOpenOption.APPEND);
           }


           //进行消息封装
           String messageStr = messageId + "|" +JSON.toJSONString(message);
           //写入文件
           writer.write(messageStr);
           //换行 方便后续按行读取
           writer.newLine();
           //强制刷新 立即写入
           writer.flush();
           log.info("WhisperMQ 消息持久化成功：文件={}，消息ID={}", currentPersistFile.getName(), messageId);

       }catch (Exception e){
           log.error("WhisperMQ 消息持久化失败，消息ID={}", messageId, e);
           //todo：失败重试的机制

       }
    }


    /**
     * 查找最新的持久化文件
     */
    private File findLatestPersistFile(File persistDir) {
        //获取persistDir目录下所有以"WhisperMQ-persist-"开头且以".log"结尾的文件列表
        File[] files = persistDir.listFiles((dir, name) -> name.startsWith("WhisperMQ-persist-") && name.endsWith(".log"));
        if (files == null || files.length == 0) {
            return null;
        }

        //按照修改日期降序排列
        File latestFile = files[0];
        for (int i = 1; i < files.length; i++) {
            if (files[i].lastModified() > latestFile.lastModified()) {
                latestFile = files[i];
            }
        }
        log.info("WhisperMQ==============> 找到最新的持久化文件：{}", latestFile.getAbsolutePath());
        return latestFile;
    }

    /**
     * 创建新的持久化文件
     */
    private File createNewPersistFile(File persistDir) {
        String fileName = "WhisperMQ-persist-" + System.currentTimeMillis() + ".log";
        File newFile = new File(persistDir, fileName);
        try {
            boolean createSuccess = newFile.createNewFile();
            if (createSuccess) {
                log.info("WhisperMQ==============> 新持久化文件创建成功：{}", newFile.getAbsolutePath());
                return newFile;
            } else {
                log.info("WhisperMQ==============> 新持久化文件创建失败：{}", newFile.getAbsolutePath());
                throw new IOException("新持久化文件创建失败");
            }
        } catch (Exception e) {
            log.error("WhisperMQ 新持久化文件创建失败", e);
            throw new RuntimeException("新持久化文件创建失败", e);
        }

    }

    /**
     * 从所有历史文件中恢复消息到内存（messageMap）
     * 注意在这些消息的恢复过程中都是单线程的 不会涉及到多并发这时系统还未开始正常处理外部请求，不需要并发处理
     * 但是可以 todo：多线程来进行消息恢复
     */
    private void recoverMessagesFromFile(File persistDir) {
        File[] files = persistDir.listFiles((dir, name) -> name.startsWith("WhisperMQ-persist-") && name.endsWith(".log"));
        if (files == null || files.length == 0) {
            log.info("WhisperMQ 无历史持久化文件，无需恢复消息");
            return;
        }

        List<String> recoverFailedIds = new ArrayList<>();
        int recoverCount = 0;

        // 遍历所有持久化文件，按行读取消息
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

                    // 解析“消息ID|消息JSON”（之前写入时用|分隔）
                    String[] parts = line.split("\\|", 2); // 分割为2部分：消息ID + 消息JSON
                    if (parts.length != 2) {
                        log.warn("WhisperMQ 持久化文件解析异常，无效行：{}", line);
                        continue;
                    }

                    String messageId = parts[0];
                    String messageJson = parts[1];
                  //以前想着用外部引用 做this处理 但是这样是最优良的
                  try {
                      // 反序列化为 MqMessage
                      MqMessageEnduring message = JSON.parseObject(messageJson, MqMessageEnduring.class);
                      // 恢复到 BrokerManager 的内存（messageMap）
                      brokerManager.getMessageMap().put(messageId, message);
                      recoverCount++;
                  }catch (Exception e){
                      recoverFailedIds.add(messageId);
                      log.error("WhisperMQ 文件内恢复消息序列化失败，文件：{} 消息ID: {}", file.getName(),messageId, e);
                  }


                }
                log.info("WhisperMQ 从文件 {} 恢复消息 {} 条", file.getName(), recoverCount);
            } catch (Exception e) {
                log.error("WhisperMQ 从文件 {} 恢复消息失败", file.getName(), e);
            }
        }

        log.info("WhisperMQ 消息恢复完成，共恢复 {} 条消息，失败 {} 条",
                recoverCount, recoverFailedIds.size());
        if (!recoverFailedIds.isEmpty()) {
            log.warn("WhisperMQ 恢复失败的消息ID：{}", recoverFailedIds);
        }
    }

}
