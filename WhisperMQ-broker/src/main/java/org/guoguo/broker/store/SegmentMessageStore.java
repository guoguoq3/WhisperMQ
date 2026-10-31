package org.guoguo.broker.store;

import com.alibaba.fastjson2.JSON;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.common.config.MqConfigProperties;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * 分段消息存储（P1 Stage C）。
 *
 * <p>布局（沿用旧文件名，旧数据可直接作为 segment 读取）：</p>
 * <pre>
 * persistPath/messages/
 *   WhisperMQ-persist-&lt;createdAt&gt;.log   消息行：id|json|status
 *   WhisperMQ-persist-&lt;createdAt&gt;.idx   索引行：id|offset|length|base64(topic)
 *   dead-ids.log                         死信墓碑（append-only，重启后据此标记 dead）
 * </pre>
 *
 * <p>启动时只加载索引（不加载消息体），消息体按需从 segment 读取；
 * 保留策略按 TTL / 总容量整段删除。</p>
 */
@Slf4j
@Component
public class SegmentMessageStore {

    public record StoredMessage(String messageId, MqMessage message, boolean dead) {
    }

    public static final class IndexEntry {
        final String messageId;
        final String topic;
        final String segment;
        final long offset;
        final int length;
        volatile boolean dead;

        IndexEntry(String messageId, String topic, String segment, long offset, int length, boolean dead) {
            this.messageId = messageId;
            this.topic = topic;
            this.segment = segment;
            this.offset = offset;
            this.length = length;
            this.dead = dead;
        }
    }

    private static final String LOG_PREFIX = "WhisperMQ-persist-";
    private static final String LOG_SUFFIX = ".log";
    private static final String IDX_SUFFIX = ".idx";
    private static final String TOMBSTONE = "dead-ids.log";

    private final MqConfigProperties config;
    private final Object lock = new Object();

    private Path dir;
    private final Map<String, IndexEntry> byId = new HashMap<>();
    private final NavigableMap<Long, IndexEntry> ordered = new TreeMap<>();
    private final Map<String, NavigableMap<Long, IndexEntry>> byTopic = new HashMap<>();
    private final Map<String, Set<String>> segmentIds = new HashMap<>();
    private final Map<String, Long> segmentCreated = new HashMap<>();
    private final Map<String, RandomAccessFile> readers = new HashMap<>();
    private final Set<String> deadIds = ConcurrentHashMap.newKeySet();

    private RandomAccessFile writer;
    private RandomAccessFile indexWriter;
    private String currentSegment;
    private long currentSize;
    private long totalBytes;
    private int liveCount;
    private ScheduledExecutorService retentionScheduler;
    private ScheduledExecutorService flushScheduler;
    /** 写失败（磁盘满/IO 错误）后进入只读模式，拒绝新写入 */
    private volatile boolean readOnly;
    private volatile String lastError;

    public SegmentMessageStore(MqConfigProperties config) {
        this.config = config;
    }

    // ------------------------------------------------------------------ 生命周期

    @PostConstruct
    public void init() {
        try {
            dir = Paths.get(config.getPersistPath(), "messages");
            Files.createDirectories(dir);
            loadTombstones();

            List<Path> logs = new ArrayList<>();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, LOG_PREFIX + "*" + LOG_SUFFIX)) {
                stream.forEach(logs::add);
            }
            logs.sort(Comparator.comparing(path -> path.getFileName().toString()));
            for (Path log : logs) {
                loadSegment(log);
            }
            if (currentSegment == null) {
                synchronized (lock) {
                    rollLocked();
                }
            } else {
                synchronized (lock) {
                    openWriter(currentSegment);
                }
            }
            scheduleRetention();
            scheduleFlush();
            log.info("消息存储初始化完成：目录={}，segment={}，消息={}，存活={}，字节={}，刷盘模式={}",
                    dir, segmentIds.size(), byId.size(), liveCount, totalBytes, flushMode());
        } catch (Exception e) {
            throw new IllegalStateException("消息存储初始化失败：" + dir, e);
        }
    }

    @PreDestroy
    public void close() {
        if (retentionScheduler != null) {
            retentionScheduler.shutdownNow();
        }
        if (flushScheduler != null) {
            flushScheduler.shutdownNow();
        }
        synchronized (lock) {
            // 关闭前强制落盘，保证已确认消息在进程退出后仍可恢复
            forceLocked();
            closeQuietly(writer);
            closeQuietly(indexWriter);
            writer = null;
            indexWriter = null;
            readers.values().forEach(this::closeQuietly);
            readers.clear();
        }
    }

    // ------------------------------------------------------------------ 写入

    /** 追加一条消息（同步写 segment + 索引），并更新内存索引与存活计数 */
    public void append(String messageId, MqMessage message) {
        if (messageId == null || message == null) {
            return;
        }
        if (readOnly) {
            throw new IllegalStateException("Broker 处于只读模式，拒绝写入：" + lastError);
        }
        byte[] body = (messageId + "|" + JSON.toJSONString(message) + "|0\n")
                .getBytes(StandardCharsets.UTF_8);
        synchronized (lock) {
            try {
                if (writer == null) {
                    rollLocked();
                }
                if (currentSize >= Math.max(1024, config.getMaxFileSize())) {
                    rollLocked();
                }
                long offset = currentSize;
                writeBody(offset, body);
                currentSize += body.length;

                byte[] idx = (messageId + "|" + offset + "|" + body.length + "|"
                        + encodeTopic(message.getTopic()) + "\n").getBytes(StandardCharsets.UTF_8);
                writeIndex(idx);

                IndexEntry entry = new IndexEntry(messageId, message.getTopic(), currentSegment,
                        offset, body.length, false);
                addEntry(entry);
                totalBytes += body.length + idx.length;
                if ("SYNC".equals(flushMode())) {
                    forceLocked();
                }
            } catch (IOException e) {
                readOnly = true;
                lastError = "写入失败：" + e.getMessage();
                log.error("消息写入失败，Broker 进入只读模式：{}", messageId, e);
                throw new IllegalStateException("消息写入失败，Broker 已进入只读模式：" + messageId, e);
            }
        }
    }

    /** 测试可覆写：模拟磁盘写入失败 */
    protected void writeBody(long offset, byte[] body) throws IOException {
        writer.seek(offset);
        writer.write(body);
    }

    /** 测试可覆写：模拟索引写入失败 */
    protected void writeIndex(byte[] idx) throws IOException {
        indexWriter.seek(indexWriter.length());
        indexWriter.write(idx);
    }

    /** 标记死信：原地把状态位改成 1，并写墓碑（重启后据此恢复 dead 标记） */
    public boolean markDead(String messageId) {
        synchronized (lock) {
            IndexEntry entry = byId.get(messageId);
            if (entry == null || entry.dead) {
                return false;
            }
            try {
                int statusPos = statusPosition(entry);
                if (statusPos >= 0) {
                    // 旧 segment 的 reader 是只读模式，这里单独以 rw 打开做原地状态位改写
                    try (RandomAccessFile raf = new RandomAccessFile(dir.resolve(entry.segment).toFile(), "rw")) {
                        raf.seek(entry.offset + statusPos);
                        raf.write('1');
                    }
                }
                entry.dead = true;
                liveCount--;
                deadIds.add(messageId);
                Files.writeString(dir.resolve(TOMBSTONE), messageId + "\n", StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
                return true;
            } catch (IOException e) {
                log.error("标记死信失败：{}", messageId, e);
                return false;
            }
        }
    }

    // ------------------------------------------------------------------ 读取

    /** 按消息 ID 读取消息体；不存在返回 null */
    public MqMessage get(String messageId) {
        IndexEntry entry;
        synchronized (lock) {
            entry = byId.get(messageId);
        }
        return entry == null ? null : readRecord(entry);
    }

    /** 该消息是否在持久化索引中（用于区分非持久化消息） */
    public boolean contains(String messageId) {
        synchronized (lock) {
            return byId.containsKey(messageId);
        }
    }

    /** 按 topic + 起始消息 ID（不含）顺序遍历，供订阅回溯使用 */
    public void forEachTopicAfter(String topic, String afterMessageId, Consumer<StoredMessage> consumer) {
        long after = afterMessageId == null ? Long.MIN_VALUE : parseId(afterMessageId);
        final int batchSize = 500;
        while (true) {
            // 分批取索引引用，避免大 topic 一次性把整个索引快照进 List
            List<IndexEntry> chunk = new ArrayList<>(batchSize);
            synchronized (lock) {
                NavigableMap<Long, IndexEntry> topicIndex = byTopic.get(topic);
                if (topicIndex == null) {
                    return;
                }
                var iterator = topicIndex.tailMap(after, false).values().iterator();
                while (iterator.hasNext() && chunk.size() < batchSize) {
                    chunk.add(iterator.next());
                }
            }
            if (chunk.isEmpty()) {
                return;
            }
            for (IndexEntry entry : chunk) {
                after = parseId(entry.messageId);
                if (entry.dead) {
                    continue;
                }
                MqMessage message = readRecord(entry);
                if (message != null) {
                    consumer.accept(new StoredMessage(entry.messageId, message, false));
                }
            }
        }
    }

    /** 最近的存活消息（按 ID 倒序），供控制台展示 */
    public List<StoredMessage> recentLive(int limit) {
        List<StoredMessage> result = new ArrayList<>();
        List<IndexEntry> snapshot;
        synchronized (lock) {
            snapshot = new ArrayList<>(ordered.descendingMap().values());
        }
        for (IndexEntry entry : snapshot) {
            if (result.size() >= limit) {
                break;
            }
            if (entry.dead) {
                continue;
            }
            MqMessage message = readRecord(entry);
            if (message != null) {
                result.add(new StoredMessage(entry.messageId, message, false));
            }
        }
        return result;
    }

    // ------------------------------------------------------------------ 指标

    public int liveCount() {
        synchronized (lock) {
            return liveCount;
        }
    }

    public int size() {
        synchronized (lock) {
            return byId.size();
        }
    }

    /** 各 topic 的存活消息数 */
    public Map<String, Integer> liveCountsByTopic() {
        Map<String, Integer> counts = new TreeMap<>();
        synchronized (lock) {
            for (IndexEntry entry : byId.values()) {
                if (!entry.dead) {
                    counts.merge(entry.topic, 1, Integer::sum);
                }
            }
        }
        return counts;
    }

    public Map<String, Object> stats() {
        synchronized (lock) {
            Map<String, Object> stats = new TreeMap<>();
            stats.put("segments", segmentIds.size());
            stats.put("messages", byId.size());
            stats.put("live", liveCount);
            stats.put("dead", byId.size() - liveCount);
            stats.put("bytes", totalBytes);
            stats.put("currentSegment", currentSegment);
            stats.put("retentionMillis", config.getRetentionMillis());
            stats.put("retentionMaxBytes", config.getRetentionMaxBytes());
            stats.put("flushMode", flushMode());
            stats.put("readOnly", readOnly);
            stats.put("lastError", lastError);
            return stats;
        }
    }

    // ------------------------------------------------------------------ 保留策略

    public void runRetention() {
        synchronized (lock) {
            if (segmentIds.size() <= 1) {
                return;
            }
            long now = System.currentTimeMillis();
            List<String> names = new ArrayList<>(segmentIds.keySet());
            names.sort(Comparator.comparing(name -> segmentCreated.getOrDefault(name, 0L)));
            for (String name : names) {
                if (name.equals(currentSegment) || !segmentIds.containsKey(name)) {
                    continue;
                }
                long created = segmentCreated.getOrDefault(name, now);
                boolean expired = config.getRetentionMillis() > 0
                        && now - created > config.getRetentionMillis();
                boolean oversized = config.getRetentionMaxBytes() > 0
                        && totalBytes > config.getRetentionMaxBytes()
                        && segmentIds.size() > 1;
                if (expired || oversized) {
                    deleteSegment(name);
                }
            }
        }
    }

    // ------------------------------------------------------------------ 内部实现

    private void loadSegment(Path logFile) {
        String segment = logFile.getFileName().toString();
        Path idxFile = dir.resolve(segment.substring(0, segment.length() - LOG_SUFFIX.length()) + IDX_SUFFIX);
        try {
            // 崩溃恢复：截断未写完的尾部，并补齐索引落后于日志的部分
            prepareSegment(logFile, idxFile);
            segmentIds.put(segment, new HashSet<>());
            segmentCreated.put(segment, parseCreatedAt(segment));
            totalBytes += Files.size(logFile) + Files.size(idxFile);
            int loaded = 0;
            try (var reader = Files.newBufferedReader(idxFile, StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\\|", 4);
                    if (parts.length < 4) {
                        continue;
                    }
                    IndexEntry entry = new IndexEntry(parts[0], decodeTopic(parts[3]), segment,
                            Long.parseLong(parts[1]), Integer.parseInt(parts[2]), deadIds.contains(parts[0]));
                    addEntry(entry);
                    loaded++;
                }
            }
            currentSegment = segment;
            currentSize = Files.size(logFile);
            log.info("加载消息段 {}：{} 条索引", segment, loaded);
        } catch (Exception e) {
            throw new IllegalStateException("加载消息段失败：" + segment, e);
        }
    }

    private void buildIndex(Path logFile, Path idxFile) throws IOException {
        byte[] data = Files.readAllBytes(logFile);
        String segment = logFile.getFileName().toString();
        int start = 0;
        try (var out = Files.newBufferedWriter(idxFile, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            for (int i = 0; i <= data.length; i++) {
                if (i == data.length || data[i] == '\n') {
                    if (i > start) {
                        String line = new String(data, start, i - start, StandardCharsets.UTF_8);
                        int first = line.indexOf('|');
                        int last = line.lastIndexOf('|');
                        if (first > 0 && last > first) {
                            try {
                                MqMessage message = JSON.parseObject(line.substring(first + 1, last), MqMessage.class);
                                int length = i - start + (i < data.length ? 1 : 0);
                                out.write(line.substring(0, first) + "|" + start + "|" + length + "|"
                                        + encodeTopic(message == null ? null : message.getTopic()) + "\n");
                            } catch (Exception e) {
                                log.warn("构建索引时跳过无法解析的消息行（segment={}，offset={}）", segment, start);
                            }
                        }
                    }
                    start = i + 1;
                }
            }
        }
    }

    /**
     * 崩溃恢复：
     * 1) 若日志尾部有半条消息（没有换行），截断它（该消息未确认，客户端会重试）；
     * 2) 索引缺失/损坏/落后时，重建或补齐索引。
     */
    private void prepareSegment(Path logFile, Path idxFile) throws IOException {
        long validSize = truncateIncompleteTail(logFile);
        if (!Files.exists(idxFile)) {
            buildIndex(logFile, idxFile);
            return;
        }
        long indexedEnd = indexEnd(idxFile);
        if (indexedEnd < 0 || indexedEnd > validSize || (indexedEnd == 0 && validSize > 0)) {
            log.warn("索引与日志不一致，重建索引：{}", idxFile.getFileName());
            Files.deleteIfExists(idxFile);
            buildIndex(logFile, idxFile);
        } else if (indexedEnd < validSize) {
            log.warn("索引落后于日志，补齐尾部索引：{}（{} -> {}）",
                    idxFile.getFileName(), indexedEnd, validSize);
            appendIndexTail(logFile, idxFile, indexedEnd);
        }
    }

    /** 日志最后一行没有换行符时，视为崩溃导致的半条消息并截断 */
    private long truncateIncompleteTail(Path logFile) throws IOException {
        long size = Files.size(logFile);
        if (size == 0) {
            return 0;
        }
        try (RandomAccessFile raf = new RandomAccessFile(logFile.toFile(), "rw")) {
            raf.seek(size - 1);
            if (raf.read() == '\n') {
                return size;
            }
            long pos = size - 1;
            while (pos >= 0) {
                raf.seek(pos);
                if (raf.read() == '\n') {
                    break;
                }
                pos--;
            }
            long newSize = pos + 1;
            raf.setLength(newSize);
            log.warn("检测到未写完的消息尾部，已截断：{}，{} -> {} 字节",
                    logFile.getFileName(), size, newSize);
            return newSize;
        }
    }

    /** 返回索引中最大的 offset+length；索引损坏返回 -1 */
    private long indexEnd(Path idxFile) {
        long maxEnd = 0;
        try (var reader = Files.newBufferedReader(idxFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                String[] parts = line.split("\\|", 4);
                if (parts.length < 4) {
                    return -1;
                }
                long offset = Long.parseLong(parts[1]);
                int length = Integer.parseInt(parts[2]);
                maxEnd = Math.max(maxEnd, offset + length);
            }
            return maxEnd;
        } catch (Exception e) {
            return -1;
        }
    }

    /** 从日志的 startOffset 开始，把缺失的索引行补到 .idx 末尾 */
    private void appendIndexTail(Path logFile, Path idxFile, long startOffset) throws IOException {
        byte[] data = Files.readAllBytes(logFile);
        int start = (int) Math.min(Math.max(0, startOffset), data.length);
        try (var out = Files.newBufferedWriter(idxFile, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            int lineStart = start;
            for (int i = start; i <= data.length; i++) {
                if (i == data.length || data[i] == '\n') {
                    if (i > lineStart) {
                        String line = new String(data, lineStart, i - lineStart, StandardCharsets.UTF_8);
                        int first = line.indexOf('|');
                        int last = line.lastIndexOf('|');
                        if (first > 0 && last > first) {
                            try {
                                MqMessage message = JSON.parseObject(line.substring(first + 1, last), MqMessage.class);
                                int length = i - lineStart + (i < data.length ? 1 : 0);
                                out.write(line.substring(0, first) + "|" + lineStart + "|" + length + "|"
                                        + encodeTopic(message == null ? null : message.getTopic()) + "\n");
                            } catch (Exception e) {
                                log.warn("补齐索引时跳过无法解析的行（offset={}）", lineStart);
                            }
                        }
                    }
                    lineStart = i + 1;
                }
            }
        }
    }

    /** 按配置定时/手动 force */
    public void force() {
        synchronized (lock) {
            forceLocked();
        }
    }

    private void forceLocked() {
        try {
            if (writer != null) {
                writer.getChannel().force(false);
            }
            if (indexWriter != null) {
                indexWriter.getChannel().force(false);
            }
        } catch (IOException e) {
            lastError = "force 失败：" + e.getMessage();
            log.error("刷盘失败", e);
        }
    }

    private void scheduleFlush() {
        if (!"INTERVAL".equals(flushMode())) {
            return;
        }
        long interval = Math.max(100, config.getStoreFlushIntervalMillis());
        flushScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "mq-flush");
            thread.setDaemon(true);
            return thread;
        });
        flushScheduler.scheduleAtFixedRate(this::force, interval, interval, TimeUnit.MILLISECONDS);
        log.info("刷盘模式=INTERVAL，每 {}ms force 一次", interval);
    }

    private String flushMode() {
        String mode = config.getFlushMode() == null ? "ASYNC" : config.getFlushMode().trim().toUpperCase();
        return switch (mode) {
            case "SYNC", "INTERVAL" -> mode;
            default -> "ASYNC";
        };
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public String getLastError() {
        return lastError;
    }

    private void openWriter(String segment) throws IOException {
        Path logPath = dir.resolve(segment);
        Path idx = dir.resolve(segment.substring(0, segment.length() - LOG_SUFFIX.length()) + IDX_SUFFIX);
        writer = new RandomAccessFile(logPath.toFile(), "rw");
        writer.seek(writer.length());
        currentSize = writer.length();
        indexWriter = new RandomAccessFile(idx.toFile(), "rw");
        indexWriter.seek(indexWriter.length());
    }

    private void rollLocked() throws IOException {
        forceLocked();
        closeQuietly(writer);
        closeQuietly(indexWriter);
        String base = LOG_PREFIX + System.currentTimeMillis();
        String segment = base + LOG_SUFFIX;
        int suffix = 1;
        while (Files.exists(dir.resolve(segment))) {
            segment = base + "-" + suffix++ + LOG_SUFFIX;
        }
        Path logPath = dir.resolve(segment);
        Files.createFile(logPath);
        writer = new RandomAccessFile(logPath.toFile(), "rw");
        indexWriter = new RandomAccessFile(
                dir.resolve(segment.substring(0, segment.length() - LOG_SUFFIX.length()) + IDX_SUFFIX).toFile(), "rw");
        currentSegment = segment;
        currentSize = 0;
        segmentIds.put(segment, new HashSet<>());
        segmentCreated.put(segment, System.currentTimeMillis());
        log.info("创建新的消息段：{}", segment);
    }

    private void addEntry(IndexEntry entry) {
        byId.put(entry.messageId, entry);
        ordered.put(parseId(entry.messageId), entry);
        byTopic.computeIfAbsent(entry.topic, key -> new TreeMap<>())
                .put(parseId(entry.messageId), entry);
        segmentIds.computeIfAbsent(entry.segment, key -> new HashSet<>()).add(entry.messageId);
        if (!entry.dead) {
            liveCount++;
        }
    }

    private MqMessage readRecord(IndexEntry entry) {
        try {
            RandomAccessFile raf = reader(entry.segment);
            byte[] buffer = new byte[entry.length];
            synchronized (raf) {
                raf.seek(entry.offset);
                raf.readFully(buffer);
            }
            String line = new String(buffer, StandardCharsets.UTF_8);
            int first = line.indexOf('|');
            int last = line.lastIndexOf('|');
            if (first < 0 || last <= first) {
                return null;
            }
            return JSON.parseObject(line.substring(first + 1, last), MqMessage.class);
        } catch (Exception e) {
            log.error("读取消息失败（segment={}，offset={}）", entry.segment, entry.offset, e);
            return null;
        }
    }

    /** 返回状态位在记录内的字节下标；记录格式 id|json|status\n */
    private int statusPosition(IndexEntry entry) throws IOException {
        RandomAccessFile raf = reader(entry.segment);
        byte[] buffer = new byte[entry.length];
        synchronized (raf) {
            raf.seek(entry.offset);
            raf.readFully(buffer);
        }
        for (int i = buffer.length - 1; i >= 0; i--) {
            if (buffer[i] == '|') {
                return i + 1;
            }
        }
        return -1;
    }

    private RandomAccessFile reader(String segment) throws IOException {
        synchronized (lock) {
            RandomAccessFile raf = readers.get(segment);
            if (raf == null) {
                raf = new RandomAccessFile(dir.resolve(segment).toFile(), "r");
                readers.put(segment, raf);
            }
            return raf;
        }
    }

    private void deleteSegment(String segment) {
        try {
            Set<String> ids = segmentIds.remove(segment);
            if (ids != null) {
                for (String id : ids) {
                    IndexEntry entry = byId.remove(id);
                    if (entry == null) {
                        continue;
                    }
                    ordered.remove(parseId(id));
                    NavigableMap<Long, IndexEntry> topicIndex = byTopic.get(entry.topic);
                    if (topicIndex != null) {
                        topicIndex.remove(parseId(id));
                        if (topicIndex.isEmpty()) {
                            byTopic.remove(entry.topic);
                        }
                    }
                    if (!entry.dead) {
                        liveCount--;
                    }
                }
            }
            RandomAccessFile raf = readers.remove(segment);
            closeQuietly(raf);
            Path logPath = dir.resolve(segment);
            Path idx = dir.resolve(segment.substring(0, segment.length() - LOG_SUFFIX.length()) + IDX_SUFFIX);
            totalBytes -= Files.size(logPath) + (Files.exists(idx) ? Files.size(idx) : 0);
            Files.deleteIfExists(logPath);
            Files.deleteIfExists(idx);
            segmentCreated.remove(segment);
            log.info("保留策略删除消息段：{}", segment);
        } catch (IOException e) {
            log.error("删除消息段失败：{}", segment, e);
        }
    }

    private void loadTombstones() {
        Path file = dir.resolve(TOMBSTONE);
        if (!Files.exists(file)) {
            return;
        }
        try (var reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isBlank()) {
                    deadIds.add(line.trim());
                }
            }
            log.info("加载死信墓碑 {} 条", deadIds.size());
        } catch (IOException e) {
            log.error("加载死信墓碑失败", e);
        }
    }

    private void scheduleRetention() {
        if (config.getRetentionMillis() <= 0 && config.getRetentionMaxBytes() <= 0) {
            return;
        }
        long interval = Math.max(5000, config.getRetentionCheckIntervalMillis());
        retentionScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "mq-retention");
            thread.setDaemon(true);
            return thread;
        });
        retentionScheduler.scheduleAtFixedRate(this::runRetention, interval, interval, TimeUnit.MILLISECONDS);
        log.info("保留策略已启用：ttl={}ms，maxBytes={}，检查间隔={}ms",
                config.getRetentionMillis(), config.getRetentionMaxBytes(), interval);
    }

    private long parseCreatedAt(String segment) {
        String name = segment.substring(LOG_PREFIX.length(), segment.length() - LOG_SUFFIX.length());
        int dash = name.indexOf('-');
        if (dash > 0) {
            name = name.substring(0, dash);
        }
        try {
            return Long.parseLong(name);
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private String encodeTopic(String topic) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(String.valueOf(topic).getBytes(StandardCharsets.UTF_8));
    }

    private String decodeTopic(String encoded) {
        if (encoded == null || encoded.isEmpty()) {
            return "";
        }
        try {
            return new String(Base64.getUrlDecoder().decode(encoded), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            return encoded;
        }
    }

    private long parseId(String messageId) {
        try {
            return Long.parseLong(messageId);
        } catch (NumberFormatException e) {
            return messageId == null ? 0 : Math.abs(messageId.hashCode());
        }
    }

    private void closeQuietly(RandomAccessFile file) {
        if (file == null) {
            return;
        }
        try {
            file.close();
        } catch (IOException ignored) {
            // 关闭失败无需中断流程
        }
    }
}
