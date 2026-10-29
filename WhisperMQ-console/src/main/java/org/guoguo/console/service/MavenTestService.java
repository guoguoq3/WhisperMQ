package org.guoguo.console.service;

import lombok.extern.slf4j.Slf4j;
import org.guoguo.console.config.ConsoleProperties;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicReference;

/**
 * “运行 Maven 测试”按钮：在仓库根目录异步执行 {@code whisper-console.maven-command}
 * （默认 {@code mvn -o test}），把输出实时缓冲下来供页面轮询展示。
 */
@Slf4j
@Service
public class MavenTestService {

    private final ConsoleProperties properties;
    private final AtomicReference<Run> current = new AtomicReference<>();
    private volatile Process process;

    public MavenTestService(ConsoleProperties properties) {
        this.properties = properties;
    }

    public synchronized Map<String, Object> start() {
        Run running = current.get();
        if (running != null && running.isRunning()) {
            return running.snapshot();
        }
        Path repoRoot = detectRepoRoot();
        Run run = new Run(properties.getMavenCommand(), repoRoot.toString());
        current.set(run);
        Thread thread = new Thread(() -> execute(run, repoRoot), "console-maven-test");
        thread.setDaemon(true);
        thread.start();
        return run.snapshot();
    }

    public Map<String, Object> state() {
        Run run = current.get();
        if (run == null) {
            Map<String, Object> idle = new LinkedHashMap<>();
            idle.put("status", "IDLE");
            idle.put("lines", java.util.List.of());
            return idle;
        }
        return run.snapshot();
    }

    public synchronized Map<String, Object> cancel() {
        Process running = this.process;
        if (running != null) {
            running.descendants().forEach(ProcessHandle::destroyForcibly);
            running.destroyForcibly();
            log.warn("已请求终止 Maven 测试进程");
        }
        Run run = current.get();
        return run == null ? state() : run.snapshot();
    }

    private void execute(Run run, Path repoRoot) {
        run.log("执行命令：" + run.getCommand());
        run.log("工作目录：" + repoRoot);
        boolean windows = System.getProperty("os.name", "").toLowerCase().contains("win");
        ProcessBuilder builder = windows
                ? new ProcessBuilder("cmd.exe", "/c", run.getCommand())
                : new ProcessBuilder("sh", "-c", run.getCommand());
        builder.directory(repoRoot.toFile());
        builder.redirectErrorStream(true);

        try {
            process = builder.start();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    run.addLine(line);
                }
            }
            int exitCode = process.waitFor();
            run.finish(exitCode);
        } catch (Exception e) {
            run.log("执行失败：" + e.getMessage());
            run.log("提示：确认 mvn 已在 PATH 中，或修改 whisper-console.maven-command");
            run.finish(-1);
        } finally {
            process = null;
        }
    }

    /** 优先用配置；否则从 user.dir 向上找含 &lt;modules&gt; 的 pom.xml */
    private Path detectRepoRoot() {
        String configured = properties.getRepoRoot();
        if (configured != null && !configured.isBlank()) {
            return Paths.get(configured).toAbsolutePath().normalize();
        }
        Path dir = Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();
        for (int i = 0; i < 5 && dir != null; i++) {
            Path pom = dir.resolve("pom.xml");
            if (Files.isRegularFile(pom)) {
                try {
                    if (Files.readString(pom, StandardCharsets.UTF_8).contains("<modules>")) {
                        return dir;
                    }
                } catch (IOException ignored) {
                    // 继续向上找
                }
            }
            dir = dir.getParent();
        }
        return Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();
    }

    // ------------------------------------------------------------------ 运行状态

    public static class Run {
        private final String command;
        private final String repoRoot;
        private final long startedAt = System.currentTimeMillis();
        private final ConcurrentLinkedDeque<String> lines = new ConcurrentLinkedDeque<>();
        private volatile boolean running = true;
        private volatile int exitCode = Integer.MIN_VALUE;
        private volatile long finishedAt;

        Run(String command, String repoRoot) {
            this.command = command;
            this.repoRoot = repoRoot;
        }

        public String getCommand() {
            return command;
        }

        public String getRepoRoot() {
            return repoRoot;
        }

        public boolean isRunning() {
            return running;
        }

        void addLine(String line) {
            lines.addLast(line);
            while (lines.size() > 500) {
                lines.pollFirst();
            }
        }

        void log(String line) {
            addLine("[" + LocalTime.now().withNano(0) + "] " + line);
        }

        void finish(int exitCode) {
            this.exitCode = exitCode;
            this.running = false;
            this.finishedAt = System.currentTimeMillis();
            log(exitCode == 0 ? "测试通过（exit=0）" : "测试失败（exit=" + exitCode + "）");
        }

        Map<String, Object> snapshot() {
            Map<String, Object> snap = new LinkedHashMap<>();
            snap.put("status", running ? "RUNNING" : (exitCode == 0 ? "PASSED" : "FAILED"));
            snap.put("command", command);
            snap.put("repoRoot", repoRoot);
            snap.put("exitCode", exitCode);
            snap.put("startedAt", startedAt);
            snap.put("finishedAt", finishedAt);
            snap.put("durationMillis", (finishedAt == 0 ? System.currentTimeMillis() : finishedAt) - startedAt);
            snap.put("lines", new ArrayList<>(lines));
            return snap;
        }
    }
}
