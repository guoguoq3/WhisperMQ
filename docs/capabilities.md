# WhisperMQ 能力边界与使用指南

> 适用版本：`v1.0.0-p1`（P1 Stage A–D 完成）
> 更新日期：2026-10-29

WhisperMQ 是一个**单机、轻量、可嵌入**的 Java 消息队列：Netty + JSON 行协议，
支持 Topic/Tag、消费组、ACK 重投、死信、分段存储、分区与背压。
它面向**内网、单机/小规模、教学与内部工具**场景，不是 Kafka/RocketMQ 的替代品。

---

## 1. 能力边界（支持 / 部分支持 / 不支持）

### 1.1 支持

| 能力 | 说明 |
|---|---|
| Topic / Tag | Tag 支持 `*`、`A,B`，且是**消费者级**配置（同组可订阅不同 Tag） |
| 消费组 | 同组共享位点；不同组各自收到全量消息 |
| 投递语义 | **at-least-once**：ACK_SUCCESS 才推进位点；超时/失败/掉线重投 |
| 批量生产 | `P_SEND_BATCH`：一条 RPC 多条消息，100 条/批实测约 10k msg/s |
| ACK 模式 | 监听器返回 `true/false`；也可 `reject()` 直接走死信重试链路 |
| 死信队列 | 投递次数耗尽或业务拒绝后入 DLQ，控制台/协议可重放 |
| 持久化 | segment（`.log`）+ 索引（`.idx`），重启恢复；非持久化消息只走内存 |
| 保留策略 | 按 TTL / 总容量整段删除（默认关闭） |
| 分区 | 静态分区数（`default-partitions`），同分区固定 owner；支持再均衡 |
| 背压 | 消费者队列满时 Pause/Resume，暂停期间不累计投递次数 |
| 去重 | 消费端 messageId LRU（默认 1000 条） |
| 鉴权 | 共享 token（默认关闭）；TLS **未实现** |
| 可观测 | 控制台 + `/api/overview` + `/api/stats`；dispatcher/存储/背压指标 |
| 部署形态 | 独立 Broker 进程，或嵌入到 Spring Boot 应用（控制台即此模式） |

### 1.2 部分支持 / 有前提

| 能力 | 前提与限制 |
|---|---|
| 分区顺序 | 同一分区、同一消费者、Tag 一致时严格 FIFO；同组 Tag 不一致时可能回退到其它消费者，顺序被放宽 |
| 分区扩缩容 | 分区数在运行期由 `TopicMetadata.setPartitions` 调整；**不迁移已有位点**，扩分区后旧消息仍按原分区读 |
| 再均衡 | 排序后轮询分配；消费者上下线会让部分分区换 owner，期间不暂停投递 |
| 保留策略 | 以 segment 为粒度，可能多保留一个段；当前段永不删除 |
| 去重 | 进程内 LRU，重启失效、多实例不共享 |
| 鉴权 | 共享密钥，无 per-client 身份、无轮换；**无 TLS** |
| 存储索引 | 索引常驻内存（每条百字节级）；百万级消息需要后续磁盘索引 |
| 回溯 | 按 topic 线性扫描索引，未建 `(topic, partition)` 二级索引 |

### 1.3 不支持

- **exactly-once / 事务消息**（只有 at-least-once + 消费端幂等辅助）
- **多 Broker 副本、选主、故障转移、跨机房**（单点）
- **TLS / mTLS、Topic 级 ACL、多租户隔离**
- 延迟消息、定时消息、消息压缩、消息清理（compaction）
- 跨分区全局顺序、批量生产协议（控制台批量发送只是循环调用单条）
- 海量堆积（千万级以上）与 Kafka 级吞吐

---

## 2. 性能实测

**测试环境**：Windows 11，JDK 22（target 17），单机回环，单 Broker 单分区，
2000 条 256B **持久化**消息，消费者正常 ACK，生产者同步 `send()`。

| 指标 | 结果 |
|---|---|
| 生产者吞吐 | **≈ 2 540 条/秒** |
| `send()` 平均延迟 | **0.417 ms** |
| `send()` P50 | **0.354 ms** |
| `send()` P99 | **0.960 ms** |
| 端到端消费吞吐 | **≈ 2 539 条/秒** |
| 批量发送（100 条/批） | **≈ 10 345 条/秒** |
| 控制台 HTTP 批量发送 | ≈ 700 条/秒（含 REST/JSON 开销，仅演示用） |

复现命令：

```bash
mvn -o -pl WhisperMQ-consumer -am test -Dtest=BenchmarkIT -DfailIfNoTests=false
```

### 刷盘模式对比（本机，1000 条 256B，不挂消费者）

| 模式 | 吞吐 | P50 | P99 | 适用 |
|---|---|---|---|---|
| `ASYNC` | ≈ 2 100 条/秒 | 0.368 ms | 1.386 ms | 默认；接受断电丢失窗口 |
| `INTERVAL`（200ms） | ≈ 2 300 条/秒 | 0.314 ms | 1.003 ms | 平衡；最多丢一个 force 间隔 |
| `SYNC` | ≈ 138 条/秒 | 6.958 ms | 13.678 ms | 强持久化；每条都等 fsync |

> SYNC 的绝对值取决于磁盘/杀毒软件，本机 fsync 约 7ms；如果业务要“绝不丢已确认消息”，
> 必须接受这个量级的吞吐下降，或改用批量提交（P1 优化项）。

**如何理解这些数字**

- 这是**单连接、同步确认**的吞吐：每条消息都要等 Broker 落盘（页缓存）+ 回执，因此吞吐受 RTT 限制；
  多连接/多生产者可近似线性叠加，但单 Broker 仍受磁盘与单线程段写入约束。
- 默认 `flush-mode=ASYNC`：消息写入页缓存即返回，**进程崩溃可恢复，机器断电可能丢最近数据**；
  切换 `INTERVAL`（默认 1s force）或 `SYNC`（每条 force）可缩短/消除该窗口，代价是吞吐下降。
- 消费者处理耗时包含在端到端吞吐里；业务越慢，吞吐越低（背压会介入）。
- 分区/多消费者可提升并行度；单分区仍是单 owner 串行。
- 结论：**单机每秒数千条**是舒适区；上万条需要更大批量、异步发送或换存储引擎。

---

## 3. 适用场景

**适合**

- 单体/小集群内的**异步解耦**：下单后发通知、发积分、写审计。
- **任务队列**：后台任务分发、重试、死信人工处理。
- **事件分发**：一个事件多个消费组各自处理（每组全量）。
- **内部工具/教学/原型**：想理解 MQ 的投递、位点、重投、死信、分区、背压。
- **边缘/离线环境**：单 jar 启动，无外部依赖（不需要 ZooKeeper/NameServer）。

**不适合**

- 需要**强一致/事务/恰好一次**的金融核心链路。
- 需要**跨机房高可用**、Broker 故障自动切换的生产系统。
- 公网直接暴露（无 TLS，token 是共享密钥）。
- 千万级积压、百万级 TPS 的大数据管道。

---

## 4. 快速开始

### 4.1 环境

- JDK 17+（本地验证 JDK 22）
- Maven 3.9+
- 默认端口：Broker `9999`，控制台 `8080`

### 4.2 启动控制台（推荐体验）

```bash
mvn -o -DskipTests package
java -jar WhisperMQ-console/target/WhisperMQ-console-1.0-SNAPSHOT.jar
# 打开 http://127.0.0.1:8080
```

或脚本：

```powershell
./scripts/start-console.ps1
```

```bash
./scripts/start-console.sh
```

数据目录：`./WhisperMQ-console-data`（消息在 `messages/`，位点在 `offsets/`），删除即可重置。

想体验分区：

```bash
java -jar WhisperMQ-console/target/WhisperMQ-console-1.0-SNAPSHOT.jar \
  --whisper-mq.default-partitions=2
```

同一消费组创建两个消费者，会各接管一个分区；组详情里可看到分配表与 epoch。

### 4.3 独立 Broker

```bash
mvn -o -DskipTests package
java -jar WhisperMQ-broker/target/WhisperMQ-broker-1.0-SNAPSHOT.jar
```

### 4.4 嵌入到自己的 Spring Boot

```java
@SpringBootApplication
@ComponentScan({"org.guoguo.common", "org.guoguo.broker.core",
                "org.guoguo.broker.handler", "org.guoguo.broker.util",
                "org.guoguo.broker.store", "org.guoguo.broker.ConsumerGroup"})
@EnableConfigurationProperties(MqConfigProperties.class)
public class MyApp {
    public static void main(String[] args) {
        SpringApplication.run(MyApp.class, args);
    }
}
```

依赖 `WhisperMQ-broker`（以及需要客户端时的 `producer`/`consumer`）。

---

## 5. 客户端使用

### 5.1 生产者

```java
MqConfigProperties config = new MqConfigProperties();
config.setBrokerHost("127.0.0.1");
config.setBrokerPort(9999);

MqProducer producer = new MqProducer(config);
producer.start();

MqMessageEnduring message = new MqMessageEnduring();
message.setTopic("ORDER_TOPIC");
message.setTag("CREATED");
message.setBizKey("order-1001");   // 可用于分区 hash / 幂等
message.setPayload("{\"orderId\":\"1001\"}");
message.setEnduring(true);          // false = 不落盘

Result<String> result = producer.send(message);
if (result.getCode() == 200) {
    System.out.println("messageId=" + result.getMessageId());
}
producer.close();
```

- `send()` 是**同步**的：Broker 落盘后返回 `P_CONFIRM_MSG`，超时会按 `producer-retry-count-limit` 重试。
- `bizKey` 非空时按它做分区 hash；否则按 messageId。

### 5.2 消费者

```java
MqConfigProperties config = new MqConfigProperties();
config.setBrokerHost("127.0.0.1");
config.setBrokerPort(9999);
config.setConsumerWorkerThreads(4);
config.setConsumerQueueCapacity(2000);

MqConsumerManager consumer = new MqConsumerManager(config);
consumer.start();
consumer.joinGroup("ORDER_GROUP");

SubscribeReqDTO subscribe = new SubscribeReqDTO();
subscribe.setTopic("ORDER_TOPIC");
subscribe.setTags(List.of("CREATED", "PAID"));   // 或 List.of("*")
consumer.groupSubscribe(subscribe, (message, messageId) -> {
    try {
        handle(message.getPayload());
        return true;                  // ACK_SUCCESS，位点前进
    } catch (Exception e) {
        return false;                 // ACK_FAIL，立即重投
    }
});

// 需要走死信重试计数时：
// consumer.reject(message, messageId, DeadType.BUSINESS_ERROR, false);

// consumer.close();
```

- 同一 topic 内按 worker 分片串行；不同 topic 可并行。
- 队列满会触发背压（Pause），回落后自动 Resume 并补投。
- 重复投递由消费端 LRU 去重（默认开启）。
- 断线后自动重连、重新入组、恢复订阅（指数退避）。

### 5.3 典型语义

| 场景 | 行为 |
|---|---|
| 监听器返回 `true` | ACK_SUCCESS，组位点推进 |
| 返回 `false` | ACK_FAIL，Broker 立即重投（计数 +1） |
| `reject(...)` | 走死信重试计数，达到上限入 DLQ |
| 进程崩溃/断线 | 未 ACK 消息在超时后重投给组内其他消费者 |
| 重复投递 | 消费端去重命中则直接 ACK，不执行业务 |
| 达到最大投递次数 | 进入死信队列，可控制台重放 |

---

## 6. 配置

### 6.1 核心项

| 配置 | 默认 | 说明 |
|---|---|---|
| `whisper-mq.broker-host` / `broker-port` | 0.0.0.0 / 9999 | 监听地址 |
| `whisper-mq.persist-path` | `./WhisperMQ-data` | 消息目录（`messages/` 子目录） |
| `whisper-mq.offset-persist-path` | `./WhisperMQ-offset` | 位点目录 |
| `whisper-mq.ack-timeout-millis` | 5000 | ACK 超时（扫描间隔为一半） |
| `whisper-mq.max-delivery-attempts` | 5 | 最大投递次数，超过入死信 |
| `whisper-mq.max-dead-letter-retry-count` | 3 | 死信重试次数 |
| `whisper-mq.new-group-start-from-earliest` | true | 新组从最早消息回溯 |
| `whisper-mq.subscribe-timeout-millis` | 5000 | 订阅/入组确认超时 |
| `whisper-mq.heartbeat-interval-millis` | 5000 | 心跳间隔 |
| `whisper-mq.reconnect-initial-delay-millis` | 500 | 重连初始退避 |
| `whisper-mq.reconnect-max-delay-millis` | 30000 | 重连最大退避 |

### 6.2 存储与分区

| 配置 | 默认 | 说明 |
|---|---|---|
| `whisper-mq.max-file-size` | 10MB | segment 滚动大小 |
| `whisper-mq.flush-mode` | ASYNC | 刷盘模式：ASYNC / INTERVAL / SYNC |
| `whisper-mq.store-flush-interval-millis` | 1000 | INTERVAL 模式的 force 间隔 |
| `whisper-mq.shutdown-drain-millis` | 3000 | 优雅停机等待在途消息的时间 |
| `whisper-mq.max-batch-size` | 1000 | 单次批量发送的最大消息数 |
| `whisper-mq.hot-cache-size` | 1000 | 内存热消息条数（0=不缓存消息体） |
| `whisper-mq.retention-millis` | 0 | 保留时长，0=不清理 |
| `whisper-mq.retention-max-bytes` | 0 | 容量上限，0=不限 |
| `whisper-mq.retention-check-interval-millis` | 60000 | 清理检查间隔 |
| `whisper-mq.default-partitions` | 1 | 默认分区数（>1 启用分区投递） |

### 6.3 消费端 / 可靠性

| 配置 | 默认 | 说明 |
|---|---|---|
| `whisper-mq.consumer-worker-threads` | 4 | 业务线程数 |
| `whisper-mq.consumer-queue-capacity` | 2000 | 每个 worker 队列容量 |
| `whisper-mq.backpressure-enabled` | true | 队列满暂停投递 |
| `whisper-mq.backpressure-pause-millis` | 3000 | 暂停时长 |
| `whisper-mq.backpressure-resume-threshold` | 0.5 | 队列回落到该比例恢复 |
| `whisper-mq.dedup-enabled` | true | 消费端去重 |
| `whisper-mq.dedup-cache-size` | 1000 | 去重 LRU 容量 |
| `whisper-mq.auth-enabled` | false | 开启 token 鉴权 |
| `whisper-mq.auth-token` | 空 | 共享 token |
| `whisper-mq.producer-retry-count-limit` | 3 | 生产端重试次数 |
| `whisper-mq.producer-retry-time-coefficient` | 500 | 重试等待系数（ms） |

Spring Boot 可用 `--whisper-mq.xxx=yyy` 覆盖；控制台配置文件在
`WhisperMQ-console/src/main/resources/application.properties`。

---

## 7. 控制台

- **发送/批量压测**：单条发送、示例填入、批量发送并统计吞吐。
- **消费者**：创建（组/Topic/Tag/行为）、切换行为、查看详情、停止。
  行为包括：正常、一直失败、首次失败、拒绝重试、抛异常、慢消费。
- **顶栏指标**：消息、在途、组、在线、死信、主题、暂停、存储段、可靠性；悬停看明细，点击跳转。
- **消费者组/位点**：在线、订阅、分区数、位点；点行看分配表与 epoch。
- **Broker 消息**：最近消息、过滤、分区列、详情弹窗。
- **死信队列**：过滤、详情、重放。
- **一键自检**：4 个场景（发送→消费→ACK→位点、ACK_FAIL 重投、Tag 隔离、死信重放）。
- **运行 Maven 测试**：仓库根执行 `mvn -o test`，日志实时回显、可取消。

控制台内嵌 Broker，页面上的生产者/消费者都是**真实 TCP 客户端**。

---

## 8. REST API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/health` | 健康检查 |
| GET | `/api/overview` | Broker/组/消费者/消息/死信快照 |
| GET | `/api/stats` | 指标：broker/store/topics/consumers |
| POST | `/api/messages` | 发送 `{topic,tag,payload,enduring,bizKey}` |
| POST | `/api/messages/batch` | 批量 `{topic,tag,count,payloadSize}` |
| GET | `/api/messages/{id}` | 消息详情 |
| POST | `/api/consumers` | 创建 `{group,topic,tags,mode}` |
| DELETE | `/api/consumers/{id}` | 停止消费者 |
| PATCH | `/api/consumers/{id}/mode` | 切换行为 `{mode}` |
| GET | `/api/deadletters` | 死信列表 |
| GET | `/api/deadletters/{id}` | 死信详情 |
| POST | `/api/deadletters/{id}/replay` | 重放 |
| POST/GET | `/api/test/self` | 启动/查询自检 |
| POST/GET | `/api/test/maven` | 启动/查询 Maven 测试 |
| POST | `/api/test/maven/cancel` | 取消 |

---

## 9. 存储、恢复与保留

```
<persistPath>/messages/
  WhisperMQ-persist-<ts>.log   消息行：id|json|status
  WhisperMQ-persist-<ts>.idx   索引：id|offset|length|base64(topic)
  dead-ids.log                 死信墓碑
<persistPath>/offsets/
  <group>-offset.log           位点：group:topic|id 或 group:topic#p|id
```

- 启动只加载**索引**；消息体按需读 segment；内存只保留热数据。
- **崩溃恢复**：启动时截断未写完的尾部消息、补齐落后/损坏的索引；写失败（磁盘满/IO 错误）
  会进入只读模式，`/api/health` 返回 `ok=false`，避免带着坏盘继续接收写入。
- **刷盘**：`ASYNC`（默认）/`INTERVAL`/`SYNC`；segment 滚动与进程退出前都会 force。
- 旧版本的单文件日志可直接作为 segment 读取，首次启动自动建索引。
- 死信状态：墓碑 + 原文件状态位改写，重启后仍然跳过。
- 保留策略按整段删除，当前段不删；默认关闭，需显式配置。
- 位点按 `(group, topic, partition)` 维护，分区 0 兼容旧格式。

---

## 10. 测试

```bash
# 单元测试（60 个）
mvn -o test

# 端到端集成（12 个场景，自建 Broker）
mvn -o -pl WhisperMQ-consumer -am test -Dtest=SmokeIT -DfailIfNoTests=false

# 性能基准（2000 条 256B）
mvn -o -pl WhisperMQ-consumer -am test -Dtest=BenchmarkIT -DfailIfNoTests=false
```

控制台里也可以一键运行 `mvn -o test` 并实时看日志。

---

## 11. 故障排查

| 现象 | 排查 |
|---|---|
| 发送返回 501/超时 | 检查 Broker 端口、`producer-retry-*`、Broker 日志；开启鉴权时确认 token |
| 消费者收不到消息 | 是否 `joinGroup` + `groupSubscribe`；Tag 是否匹配；组是否已被其他消费者占用位点 |
| 消息进入死信 | 看死信详情里的 `deadType`/`retryCount`；监听器是否抛异常/返回 false |
| 重启后消息没了 | 是否 `enduring=false`；保留策略是否删除了 segment |
| 位点不前进 | 是否返回 true / ACK 是否发出；是否为同一分区 |
| 队列满/消费慢 | 调大 `consumer-queue-capacity`/线程数；观察背压 `paused` 指标 |
| 重复消费 | at-least-once 语义；开启 `dedup-enabled` 或业务幂等 |

---

## 12. 版本与兼容

- 协议版本：`RpcMessageDTO.version=1`；服务端接受 `<= 当前`，高版本回 `ERROR`。
- 旧客户端不带 `version`/`partition`/`token` 时按默认值处理（1 / 0 / 关闭鉴权）。
- 旧位点文件、旧消息日志可直接读取，无需迁移。
- 升级步骤：停服 → 替换 jar → 启动（自动建索引/恢复）→ 观察 `/api/stats`。

## 13. 路线图

- **TLS**（当前明文）
- **Stage E**：多 Broker 副本、选主、故障转移
- 磁盘索引/分页、按分区二级索引、批量与压缩写
- 一致哈希/粘性分配减少再均衡搬迁；扩分区位点迁移

详见 [`docs/p1-roadmap.md`](p1-roadmap.md)。
