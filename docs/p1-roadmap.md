# WhisperMQ P1 升级路线图

> 目标：在**不破坏现有协议、存储格式与行为**的前提下，逐步具备分区、保留策略、背压、
> 幂等、可观测、安全与多 Broker 能力。每个阶段都可独立发布/回滚。

## 0. 设计原则（防后期冲突）

1. **只加不改**：协议字段、配置项、存储行格式只做"新增 + 默认值等价旧行为"；
   删除/重命名必须跨大版本，并提供兼容解析。
2. **能力开关**：新能力默认关闭或等价旧行为（分区默认 1、保留策略默认不限、
   背压默认超时重投），通过配置逐项打开。
3. **先数据面、后控制面**：先让消息/位点/存储能表达新维度，再改调度与再均衡，
   最后才做多 Broker。
4. **存储格式向前兼容**：解析器同时接受旧格式；写入端只用新格式中"旧端可忽略"的扩展位。
5. **可观测优先**：每个阶段先暴露指标，再做优化，避免"改了不知道效果"。
6. **单机可验证**：所有阶段都能在单进程（console + SmokeIT）跑通，再谈集群。

## 1. 阶段划分与依赖

```
Stage A  兼容底座（本次）
  ├─ A1 协议版本号
  ├─ A2 分区/位点数据模型（默认 1 分区，行为不变）
  ├─ A3 清理与历史测试迁移
  └─ A4 消费端指标暴露
        │
        ▼
Stage B  分区投递与再均衡（已完成）
  ├─ B1 (topic, partition) → consumer 分配表
  ├─ B2 加入/离开触发再均衡（epoch 防脑裂）
  ├─ B3 位点按分区推进 + 按分区回放
  └─ B4 Tag × 分区规则落地（见 §3.1）
        │
        ├──────────────► Stage D  可靠性与运维（已完成，TLS 待做）
        │                  ├─ D1 背压：Pause/Resume 投递
        │                  ├─ D2 消费端幂等/去重
        │                  ├─ D3 鉴权 + TLS
        │                  └─ D4 管理面指标端点
        ▼
Stage C  存储与保留（已完成）
  ├─ C1 Segment 文件 + 偏移索引
  ├─ C2 TTL / 容量保留策略
  ├─ C3 启动按需加载（不再全量进内存）
  └─ C4 回放走索引（不再全量扫描）
        │
        ▼
Stage E  多 Broker
  ├─ E1 元数据/路由表
  ├─ E2 副本复制与 ISR
  └─ E3 选主与故障转移
```

依赖关系：**A → B → C → E**，D 可在 A 之后与 B/C 并行。
没有 A 的协议版本与分区位点格式，B/C/E 都会产生不兼容迁移。

## 2. 兼容契约（必须遵守）

### 2.1 协议
- 信封 `RpcMessageDTO` 新增 `version`（默认 1）；服务端接受 `version <= 当前版本`，
  高于当前版本返回 `ERROR`（明确提示），未知字段忽略。
- 新增 `methodType` 只增不改；旧端收到未知类型忽略并记录日志（不崩溃）。
- Ack 新增 `partition` 字段（默认 0）；旧端不传时按分区 0 处理。

### 2.2 存储
- 消息行：`消息ID|JSON|状态` 不变（JSON 内新增 `partition` 字段，旧数据缺省为 0）。
- 位点行：`uniqueKey|offset` 不变；新格式 `group:topic#partition|offset`，
  旧格式 `group:topic|offset` 等价于分区 0。恢复时两者都解析。
- 死信行：`死信ID:...|消息原ID:...|时间|JSON` 不变。
- 文件滚动大小/命名不变。

### 2.3 配置
- 新配置项全部有默认值，默认值必须**等价于升级前行为**。
- `whisper-mq.default-partitions` 默认 1；>1 在 Stage B 完成前标记为实验能力。

## 3. 关键设计决策（避免后期返工）

### 3.1 分区与 Tag 的冲突
- 分区（partition）是**投递顺序与位点**的单位；Tag 是**消息过滤**属性。
- Stage B 采用"分区 owner + Tag 兼容"：owner 从该分区消息的 tag 匹配集合里选取；
  若同组消费者 tag 集合不一致，按 `(partition, tag)` 子集分配，牺牲跨 tag 的严格顺序。
- 长期方案：消费者以 `*` 订阅、客户端按 tag 过滤，broker 只保证分区顺序；
  该切换需要协议新增"客户端过滤"开关，放在 Stage D 之后评估。
- **禁止**把 tag 直接编码进分区号（会导致分区数与 tag 数耦合，无法扩展）。

### 3.2 位点
- 位点键：`(groupId, topic, partition)`；消息 ACK 只推进对应分区的水位。
- 保留 `group.topicOffsetMap` 作为"分区 0 视图"，兼容旧代码/旧数据。
- 组内共享位点 → 分区共享位点；不同分区之间不互相阻塞（解决乱序 ACK 跳过中间消息）。
- 回放按分区各自比较 `messageId > offset[partition]`，按消息 ID 升序。

### 3.3 背压
- 目标：队列满时 **Broker 暂停向该消费者投递**，而不是"不 ACK → 超时重投 → 死信"。
- 协议：`C_PAUSE` / `C_RESUME`（带 consumerId + 分区集合 + 过期时间），
  Broker 维护 `pausedUntil`，过期自动恢复；客户端由 dispatcher 的
  `queue.size()/capacity` 触发。
- 兼容：旧客户端不发 pause，仍走超时重投；Broker 不强制要求。

### 3.4 幂等/去重
- 语义仍是 at-least-once；提供两级去重：
  1. Broker：`bizKey` + 时间窗（默认关闭），用于生产者重试去重；
  2. Consumer：messageId LRU（默认开启，容量可配），用于重投去重。
- 不承诺 exactly-once；业务幂等键仍由使用方保证。

### 3.5 保留策略与索引
- 消息日志改为 segment（固定大小/时间切分），每 segment 配 `.idx` 偏移索引
  （消息ID → 文件偏移）。
- 内存仅保留"热区"（最近 N 条/时间窗），冷数据回放走索引；`messageMap` 语义变为缓存。
- 删除策略：按时间/容量删除整段，避免逐条删除。
- 旧数据（单文件 append）启动时可直接作为只读 segment 读取，不需要迁移。

### 3.6 多 Broker
- 前置条件：分区位点格式（A）、segment 索引（C）。
- 元数据：topic → partition 数、副本集合、leader；路由表由客户端/网关缓存。
- 一致性：分区 leader 负责写入与位点，follower 拉取复制；ISR + epoch 防脑裂。
- **不要**在 B 阶段就引入"brokerId 编码进 messageId/位点"，否则 E 阶段要迁移。

## 4. 验收标准（每阶段）

| 阶段 | 验收 |
|---|---|
| A | 旧数据/旧客户端行为不变；新增单测覆盖版本校验、分区位点、清理 |
| B | 2 分区下同分区消息顺序稳定；消费者上下线再均衡不丢不重（at-least-once）；位点按分区前进 |
| C | 1GB 日志启动 < 2s（按需加载）；TTL 到期后旧段可删；回放不扫描全量 |
| D | 队列满时消息不进入死信；重复投递被消费端去重；TLS 握手 + token 鉴权通过 |
| E | 单 broker 宕机后分区在副本上恢复，客户端自动重路由，位点不回退 |

## 5. 风险与回滚

| 风险 | 缓解 |
|---|---|
| 位点格式升级导致旧版本读不了 | 双格式写入/读取；新增 `#partition` 仅在分区 >0 时出现 |
| 分区分配抖动导致重复消费 | 分配表带 epoch；再均衡期间暂停投递，完成后恢复 |
| 背压协议不兼容旧客户端 | 新 methodType 可选；旧端继续走超时重投 |
| Segment 改造影响现有恢复 | 保留旧单文件读取路径，新写入按配置切换 |
| 多 Broker 引入一致性 bug | 先单机多进程验证，再做副本；位点/消息 ID 不编码 broker 信息 |

## 6. 落地清单

- [x] `RpcMessageDTO.version` + Broker 版本校验 + 单测
- [x] `MqMessage.partition`、`TopicMetadata`（分区数解析/计算，默认 1）
- [x] 位点按 `(topic, partition)` 存储与恢复（兼容旧格式），ACK 携带 partition
- [x] 清理：`deadLetterMap` 成功消费后移除、`leaveGroup` 清理监听器
- [x] 历史 `@Test` 从 `src/main/java` 迁到 `src/test/java` 并 `@Disabled`
- [x] 消费端 dispatcher 指标暴露到控制台（submitted/completed/failed/rejected）
- [x] 控制台展示分区与位点

### Stage B（已完成）

- [x] `PartitionAssigner`：消费者 ID 排序后轮询分配，输入稳定则结果稳定；
- [x] `ConsumerGroup` 增加订阅者集合（只有显式 GROUP_SUBSCRIBE 的消费者参与分配）、
      分配表与 `assignmentEpoch`；
- [x] 消费者加入 / 离开 / 订阅变化时触发再均衡；owner 下线时惰性重分配；
- [x] `BrokerManager` 分区 topic 按 owner 投递，重投/死信重试也走 owner；
      owner 与消息 Tag 不匹配时回退到 Tag 匹配消费者（并记录告警）；
- [x] 控制台展示每个 topic 的分区数、partition→consumer 分配与 epoch；
- [x] 2 分区端到端测试：再均衡前后不丢不重、同消费者同分区顺序递增。

**Stage B 已知限制**

- 分配用轮询取模，消费者上下线会让部分分区换 owner（Kafka 也如此），
  再均衡期间没有暂停投递；
- 消费者数 > 分区数时，多出的消费者暂时空闲；
- 同组 Tag 不一致时，owner 与 Tag 冲突会回退到其它消费者，该分区的严格顺序被放宽；
- 分区数变更（扩分区）暂不迁移已有位点，需要后续管理接口配合。

### Stage C（已完成）

- [x] `SegmentMessageStore`：segment（`.log`）+ 索引（`.idx`），沿用旧文件名；
      旧日志首次启动自动建索引，无需数据迁移；
- [x] 启动只加载索引，消息体按需读取；`messageMap` 变为热缓存（`hot-cache-size`）；
- [x] 订阅回溯按 topic 索引流式读取，不再全量扫描 + JSON 解析；
- [x] 保留策略：按 TTL / 总容量整段删除，当前段永不删除；默认关闭；
- [x] 死信：墓碑文件 + 原地状态位，重启后仍然跳过；
- [x] 控制台展示 segment 数、存活/死信条数、字节数。

**Stage C 已知限制**

- 索引常驻内存（每条百字节级），百万级消息需要后续做磁盘索引/分页；
- 回溯按 topic 线性扫描索引，尚未按 (topic, partition) 建二级索引；
- 保留以 segment 为粒度，可能多保留一个段；
- 消息仍是逐条同步追加，未做批量写/压缩。

### Stage D（已完成，TLS 待做）

- [x] D1 背压：`C_PAUSE` / `C_RESUME` 协议；消费者队列满时暂停 Broker 投递，
      暂停窗口内 ACK 超时扫描不计投递次数（避免"队列满 → 超时 → 死信"）；
      resume 后按组位点补投；
- [x] D2 消费端去重：messageId LRU（`dedup-cache-size`），只登记成功处理的消息，
      重复投递直接 ACK；
- [x] D3 鉴权（Token）：`auth-enabled` + `auth-token`，常量时间比较，
      失败回 ERROR 并断开；默认关闭，兼容旧客户端；
- [x] D4 指标：`GET /api/stats`（Broker/存储/背压/去重），
      控制台展示 paused / 去重命中 / 队列水位；
- [ ] TLS：需要证书与端口配置，留待下一阶段（当前仍是明文 TCP）。

**Stage D 已知限制**

- 暂停由消费者主动上报，Broker 不做按分区配额；
- Token 是共享密钥，没有 per-client 身份与轮换；
- 去重是消费端进程内 LRU，重启后失效，跨实例不共享；
- 极端场景（队列容量=1、慢消费）下靠暂停/恢复与超时重投收敛，吞吐会下降。

后续按 §1 推进：D 剩余 TLS → E（多 Broker）。

## 7. P0 单机稳态（2026-10 追加）

目标：不做集群，把单机版的“崩溃可恢复、磁盘可控、停机可优雅”补齐。

- [x] 可配置刷盘：`flush-mode=ASYNC/INTERVAL/SYNC`，`store-flush-interval-millis`；
- [x] 崩溃恢复：启动截断半条消息、补齐/重建索引尾部；
- [x] 写失败只读：磁盘满/IO 错误后拒绝新写入，`/api/health` 降级；
- [x] 优雅停机：等待在途消息（`shutdown-drain-millis`），关闭前 force；
- [x] 日志降噪：消息级 INFO → DEBUG；
- [x] fastjson 1.2.76 → fastjson2 2.0.36（消除旧版反序列化风险）；
- [x] 磁盘满/写失败的故障注入测试（可覆写 `writeBody` 模拟 IOException）；
- [x] 刷盘模式对比基准（`FlushModeBenchmarkIT`：ASYNC/INTERVAL/SYNC）。

## 8. P1 容量与吞吐（进行中）

- [x] 批量生产：`P_SEND_BATCH` + `BatchProducerMessageDTO` + `sendBatch()`；
      100 条/批约 10 345 msg/s（单条约 1 468 msg/s，约 7×）
- [x] 回溯流式化：按 500 条/批读取索引，避免大 topic 一次性快照
- [ ] 索引下盘/稀疏化（当前索引仍常驻内存）
- [x] 批量 ACK：`C_ACK_BATCH` + 客户端攒批（默认 50 条/200ms）
