package org.guoguo.console.web;

import org.guoguo.console.service.ConsumerSession;
import org.guoguo.console.service.ConsoleService;
import org.guoguo.console.service.MavenTestService;
import org.guoguo.console.service.SelfTestService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 控制台 REST API。
 *
 * <p>所有操作都作用在真实的内嵌 Broker 上：发消息、建消费者、看位点、重放死信、跑测试。</p>
 */
@RestController
@RequestMapping("/api")
public class ConsoleController {

    private final ConsoleService console;
    private final SelfTestService selfTest;
    private final MavenTestService mavenTest;

    public ConsoleController(ConsoleService console, SelfTestService selfTest, MavenTestService mavenTest) {
        this.console = console;
        this.selfTest = selfTest;
        this.mavenTest = mavenTest;
    }

    public record SendRequest(String topic, String tag, String payload, Boolean enduring, String bizKey) {
    }

    public record BatchSendRequest(String topic, String tag, Integer count, Integer payloadSize) {
    }

    public record ConsumerRequest(String group, String topic, List<String> tags, String mode) {
    }

    public record ModeRequest(String mode) {
    }

    // ------------------------------------------------------------------ 观测

    @GetMapping("/health")
    public Map<String, Object> health() {
        return console.health();
    }

    @GetMapping("/overview")
    public Map<String, Object> overview() {
        return console.overview();
    }

    /** 统一指标：Broker / 存储 / 背压 / 去重 */
    @GetMapping("/stats")
    public Map<String, Object> stats() {
        return console.stats();
    }

    // ------------------------------------------------------------------ 生产者

    @PostMapping("/messages")
    public Map<String, Object> send(@RequestBody SendRequest request) {
        return console.send(request.topic(), request.tag(), request.payload(),
                request.enduring(), request.bizKey());
    }

    /** 批量发送（压测/演示）：返回条数、耗时与吞吐 */
    @PostMapping("/messages/batch")
    public Map<String, Object> batchSend(@RequestBody BatchSendRequest request) {
        int count = request.count() == null ? 100 : request.count();
        int payloadSize = request.payloadSize() == null ? 256 : request.payloadSize();
        return console.batchSend(request.topic(), request.tag(), count, payloadSize);
    }

    /** 消息详情（完整 payload）；内存里没有时会到死信队列里按原消息 ID 查找 */
    @GetMapping("/messages/{id}")
    public ResponseEntity<Map<String, Object>> messageDetail(@PathVariable String id) {
        Map<String, Object> detail = console.messageDetail(id);
        return detail == null
                ? ResponseEntity.status(404).body(error("消息不存在或已被清理：" + id))
                : ResponseEntity.ok(detail);
    }

    // ------------------------------------------------------------------ 消费者

    @PostMapping("/consumers")
    public Map<String, Object> createConsumer(@RequestBody ConsumerRequest request) {
        ConsumerSession session = console.createConsumer(request.group(), request.topic(),
                request.tags(), request.mode());
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);
        out.put("consumer", session.snapshot());
        return out;
    }

    @DeleteMapping("/consumers/{id}")
    public Map<String, Object> closeConsumer(@PathVariable String id) {
        console.closeConsumer(id);
        return Map.of("ok", true);
    }

    @PatchMapping("/consumers/{id}/mode")
    public Map<String, Object> changeMode(@PathVariable String id, @RequestBody ModeRequest request) {
        console.setConsumerMode(id, request.mode());
        return Map.of("ok", true, "mode", console.sessionOf(id).getMode());
    }

    // ------------------------------------------------------------------ 死信

    @GetMapping("/deadletters")
    public Map<String, Object> deadLetters() {
        return Map.of("deadLetters", console.deadLetters());
    }

    /** 死信详情（完整 payload + 原始 JSON） */
    @GetMapping("/deadletters/{id}")
    public ResponseEntity<Map<String, Object>> deadLetterDetail(@PathVariable String id) {
        Map<String, Object> detail = console.deadLetterDetail(id);
        return detail == null
                ? ResponseEntity.status(404).body(error("死信不存在或已被重放：" + id))
                : ResponseEntity.ok(detail);
    }

    @PostMapping("/deadletters/{id}/replay")
    public Map<String, Object> replay(@PathVariable String id) {
        return console.replayDeadLetter(id);
    }

    // ------------------------------------------------------------------ 测试

    @PostMapping("/test/self")
    public Map<String, Object> startSelfTest() {
        return selfTest.start();
    }

    @GetMapping("/test/self")
    public Map<String, Object> selfTestState() {
        return selfTest.state();
    }

    @PostMapping("/test/maven")
    public Map<String, Object> startMavenTest() {
        return mavenTest.start();
    }

    @GetMapping("/test/maven")
    public Map<String, Object> mavenTestState() {
        return mavenTest.state();
    }

    @PostMapping("/test/maven/cancel")
    public Map<String, Object> cancelMavenTest() {
        return mavenTest.cancel();
    }

    // ------------------------------------------------------------------ 错误处理

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleBadRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(error(e.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleError(Exception e) {
        return ResponseEntity.internalServerError().body(error(String.valueOf(e.getMessage())));
    }

    private Map<String, Object> error(String message) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ok", false);
        body.put("error", message == null ? "unknown error" : message);
        return body;
    }
}
