# Raft 层生产化差距清单（2026-09-14 实测版）

> **范围**：只谈 **Raft 共识层** + **持久化层（对接 tiny-lsm）**。  
> 网络层（当前是 labrpc 模拟网络）你还没定，本文只留接口占位，不展开。
>
> **方法**：每一条结论都来自今天对你桌面源码的实际 grep / 读码，标注 `文件:行号` 作为铁证，不推测。  
> **用途**：这份是「待办清单」，后面你一条条实现，每项都带复选框。

---

# 二、正确性 / 安全性缺口（P0）

### 2.1 快照分块流式传输

- **铁证**：`raft.h:215` `std::string data;` —— 整个 blob 塞一条 RPC，没有 `offset` / `done` / `chunk_size`。
- **生产后果**：状态机几十 GB 时，一条 RPC 传几十 GB → 内存爆、连接占死、超时重传整块。**大状态机根本不可用**。
- **实现要点**：`InstallSnapshotArgs` 加 `offset` / `data` / `done` / `chunk_size`；follower 侧按 offset 追加写临时文件，收到 `done` 校验 CRC 后原子 rename。参考 etcd 的 `snap.Message`（带 `index/term/offset/data/done`）。
- **验收**：造 1GB 状态机，能稳定传完并 install。

### 2.2 RPC 消息完整性校验（CRC）

- **现状**：各 Args/Reply 的 `Serialize/Deserialize`（`raft.cpp:46-155`）**没有任何校验和**。网络层（`labrpc`）也不校验。
- **生产后果**：真实网络上 bit flip / 截断包会被当成合法数据解析 → 状态机静默损坏，且极难排查。
- **实现要点**：每个消息编码末尾加 CRC32/CityHash，反序列化时校验；失败直接丢弃当作丢包。
- **验收**：手工翻转一个字节，收端必须 reject 并计入丢包统计。

---

# 三、性能缺口（P1/P2）

### 3.1 锁粒度（P2，改动大，最后做）

- 现在是一把大锁 `mu_` 保护全部状态（`raft.h:412`），读路径（CheckQuorum/ReadIndex/GetState）也要抢。
- 生产做法：读路径无锁/RCU、日志与状态分离锁。但**改动面大、风险高，建议等前面都稳定后再动**。

---

# 四、持久化层：对接 tiny-lsm

## 4.0 ★ 先说一个关键设计决策：Raft log **不要**塞进 LSM 的 KV 接口

这是最容易走错的一步，我必须先讲清楚。

**Raft log 的访问模式和 LSM 是错配的**：

| 维度  | Raft log 的语义                             | LSM（tiny-lsm）的语义         |
| --- | ---------------------------------------- | ------------------------ |
| 写   | **严格顺序 append**                          | 随机 KV 覆盖写                |
| 读   | 按 index 顺序/随机读                           | KV 点查（有读放大）              |
| 删除  | **尾部截断**（冲突覆盖）+ **前缀截断**（快照后）            | 墓碑标记 + 后台 compaction 才真删 |
| 一致性 | 每条必须 fsync 后才算提交                         | 依赖 memtable flush        |
| 代价  | 用 LSM 存 → 同一 index 多版本、compaction 白忙、读放大 | —                        |

**业界真实做法（强烈建议照抄这个分工）**：

| 系统   | Raft log 用什么                                 | 状态机 KV 用什么       |
| ---- | -------------------------------------------- | ---------------- |
| etcd | **独立 WAL**（append-only segment file，`wal` 包） | BoltDB（B+tree）   |
| TiKV | **RaftEngine**（自研 append-only log）           | **RocksDB（LSM）** |
| 你的方案 | **独立 segment log**（自写或改造 tiny-lsm 的 WAL）     | **tiny-lsm** ✅   |

> 一句话：**tiny-lsm 用来装状态机 KV，不要用来装 Raft log。**  
> Raft log 单独写一个 append-only segment（顺序 append + 稀疏索引 + 前缀/后缀截断 + fsync），语义简单，300 行内可控，而且能精确控制 fsync 时机（这是正确性关键，交给 LSM 反而控制不了）。

## 4.1 tiny-lsm 现状（基于实际代码）

**项目形态**：xmake 构建（`xmake.lua`），命名空间 `tiny_lsm`，C++20。核心源码 `src/`，头文件 `include/`。

| 模块            | 位置                                                            | 说明                                                                                                                      |
| ------------- | ------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| MemTable      | `src/memtable/memtable.cpp`                                   | 底层跳表 `SkipList`；current + `std::list<frozen_tables>` 双表（`memtable.h:75-76`）                                             |
| SSTable       | `src/sst/sst.cpp`、`include/sst/sst.h`                         | Block 格式 + BlockMeta 索引；**BloomFilter 已实现**（`sst.h:72`）                                                                 |
| Block / Cache | `src/block/block.cpp`、`block_cache.cpp`                       | LRU-K 块缓存                                                                                                               |
| WAL           | `src/wal/wal.cpp`、`record.cpp`                                | `Record` 编码 `[len:u16][tranc_id:u64][op_type][key][value]`（`record.h:19`）；文件 `wal.<seq>`，超限 `reset_file`（`wal.cpp:206`） |
| 引擎门面          | `include/lsm/engine.h:88` `class LSM`；`:21` `class LSMEngine` | `get/put/remove/..._batch/begin/end/flush/flush_all/begin_tran`                                                         |
| 事务 / MVCC     | `src/lsm/transation.cpp`、`include/lsm/transaction.h`          | `TranManager`/`TranContext`；四种隔离级别，**SERIALIZABLE 未实现**（`transation.cpp:197` 注释）                                        |
| Compaction    | `src/lsm/engine.cpp`                                          | **Leveled**（L0 重叠，L1+ 不重叠）                                                                                              |
| VLog（WiscKey） | `src/vlog/vlog.cpp`                                           | 大 value 分离                                                                                                              |

**编译/测试**：`xmake` / `xmake run test_lsm`；静态库目标 `lsm_shared`（`xmake.lua:109`）；依赖 gtest / toml11 / spdlog。

## 4.2 tiny-lsm 的 4 个坑（接入前必须知道）

1. **🔴 WAL 只在"事务路径"写 —— 非事务 `put` 会丢数据**
   - 铁证：WAL 写入点仅在 `TranContext::commit`（`transation.cpp:257` → `write_to_wal` → `wal->log(records, true)` 强制 fsync）。
   - 非事务的 `LSM::put`（`engine.cpp:833`）**只进 memtable，不写 WAL**，要等超过 64MB（`LSM_TOL_MEM_SIZE_LIMIT`）触发 `flush()` 才落盘。
   - **后果**：Raft 已提交的写，如果走了非事务 API，断电就丢 → **直接违反 Raft 安全性**。
   - **对策**：Raft apply 后写状态机**必须走事务 API**（`begin_tran` + `put` + `commit`）。
2. **🔴 Compaction 是前台同步阻塞的**
   - 铁证：`flush()` 里若 L0 的 SST 数 ≥ 4 就调 `full_compact(0)`（`engine.cpp:472-476`），递归向高层合并；**没有后台 compaction 线程**（`transation.cpp:508` 的 flush_thread 已注释掉）。
   - **后果（对你来说是灾难级）**：Raft 的 applier 线程写状态机时，可能触发一次全量 compaction 被堵住几秒 → applier 不推进 → `last_cmd_index_` 不涨 → Get 全部卡死 → 心跳/选举超时。
   - **对策**：接入前**必须**先把 compaction 挪到后台线程。
3. **🟠 没有 Manifest**
   - SST 层级靠**目录扫描** `sst_<id>.<level>` 文件名重建（`engine.cpp:50-106`、`get_sst_path` `:512`）。
   - 后果：SST 多了以后启动慢，且存在"写了一半的 SST"的竞态风险。生产必须有 manifest。
4. **🟠 `WAL::flush()` 是空实现**（`wal.cpp:91`，只加锁不 sync）—— 名字极具误导性，别被它骗了，以为调了就落盘。

其他：无列族（ColumnFamily）、无 `scan(begin_key, end_key)` 半开区间 API、无快照序列化接口。

## 4.3 三块持久化的分工与 fsync 策略

| 数据                              | 特点                          | 用什么存                                                  | fsync 策略                                                                    |
| ------------------------------- | --------------------------- | ----------------------------------------------------- | --------------------------------------------------------------------------- |
| **Raft log**                    | append-only、按 index 读、前后缀截断 | **独立 segment log**（已实现 `SegmentLogStore`，file-backed） | **已实现**：提交屏障 `Sync(true)` 强制落盘 + `Sync(false)` 去抖合并；已提交 entry 必 fsync 后回客户端 |
| **Raft state**（term / votedFor） | 极小、频繁变更                     | 小文件原子写（tmp + rename），或跟 log 同 segment                 | 每次变更 fsync（便宜）                                                              |
| **状态机 KV**                      | 随机读写、量大                     | **tiny-lsm** `LSMEngine`                              | 走**事务 API**，由 LSM 的 WAL 保证                                                  |
| **快照 blob**                     | 大、偶发、整体替换                   | LSM `flush_all()` + 导出 / checkpoint                   | 锁外写（你现在的两阶段 `SaveSnapshotBlob` 思路可直接平移）                                     |

## 4.4 接入步骤（按这个顺序来，每步可独立验证）

- [ ] **Step 0**：在 macOS 上先跑通 tiny-lsm（`xmake` + `xmake run test_lsm`）。  
  ⚠️ 风险：README 标注 platform 为 Linux，macOS（Apple Silicon）**未明确保证**。这一步跑不通，后面方案要换。
- [ ] **Step 1**：定义 `LogStore` 接口（`Append/Get(index)/Term(index)/LastIndex/TruncateSuffix/TruncatePrefix`），先用**内存 vector** 实现，把 `raft.cpp` 里对 `logs_` 的直接操作收敛到接口后面。**此时测试应全绿（纯重构）**。
- [ ] **Step 2**：`StateStore`——term/votedFor 原子落盘（tmp + rename + fsync）。
- [ ] **Step 3**：**先改造 tiny-lsm**：把 `full_compact` 挪到后台线程（见 §4.2 坑2）。不然后面怎么接都会卡。
- [ ] **Step 4**：状态机换成 tiny-lsm。apply 用**事务 API**（`begin_tran`/`put`/`commit`）保证 WAL fsync。单线程 apply 正好契合 LSM 无并发写。
- [ ] **Step 5**：快照——先 `flush_all()` + `Level_Iterator` 全量导出（**注意 MVCC**：要以最大 `tranc_id` 取每个 key 的最新非删除值）；恢复时清空 + `put_batch` 重放。更优方案是实现 **checkpoint**（硬链接 SST 目录，RocksDB 做法，秒级）。

## 4.5 tiny-lsm 需要补的能力（你的下一个子项目）

- [ ] **后台 compaction 线程**（P0，阻塞问题）
- [ ] **Manifest**（替代目录扫描）
- [ ] **Checkpoint / 快照序列化**（Raft 快照要用）
- [ ] **非事务写也走 WAL**，或明确"只允许事务 API"并在文档/断言里锁死
- [ ] **`WAL::flush()` 真正实现或改名**（现在是个陷阱）
- [ ] （可选）列族、range scan API

---

# 五、网络层（留白，你还没定）

现在是 labrpc 模拟网络（`src/labrpc/`）。将来换真实传输层时，需要抽象出这几个接口（先不展开，等你定方案）：

- `CallAsync(method, args, callback, cancel_token)` — 已有雏形（`labrpc.h:120`）
- 连接管理 / 重连、消息大小上限、流控与背压、TLS、keepalive
- 超时与取消语义（现在的 `cancel_rpcs_` 可以直接平移）

> 生产级 RPC 的硬要求：**消息大小上限** + **流控/背压**（不做的话，一个慢 follower 能把 leader 的内存打爆）。

---

# 六、实施路线图（建议顺序）

**第一批（P0，持久化）**  
6\. [ ] macOS 跑通 tiny-lsm  
7\. [ ] tiny-lsm 后台 compaction（否则接入必卡）  
8\. [ ] 状态机接 tiny-lsm（走事务 API）  
9\. [ ] 快照：flush_all + 导出 → checkpoint

**第二批（P1，可用性与性能）**  
10\. [ ] 快照分块流式传输（#8，核心分块+CRC+频率节流已完成；abort 协议/断点续传/mTLS 待做）

**第三批（P2，工程成熟度）**  
11\. [ ] 可观测性：metrics（term / commit / apply lag / log size / snapshot 大小 / RPC 延迟）+ 结构化日志  
12\. [ ] 混沌测试 / 故障注入（在现有 tester 基础上加）  
13\. [ ] 锁粒度优化

---

**tiny-lsm**

- `transation.cpp:257` — WAL 仅在事务 commit 写；`engine.cpp:833` 非事务 put 不写 WAL
- `wal.cpp:91` — `WAL::flush()` 空实现
- `wal.cpp:98/118` — `WAL::log(records, force_flush)`，force 才 `sync()`
- `engine.cpp:472-476` — flush 触发 `full_compact`，前台阻塞
- `transation.cpp:508` — flush_thread 已注释（无后台线程）
- `engine.cpp:50-106 / 512` — 靠目录扫描重建，无 manifest
- `include/lsm/engine.h:88 / 21` — `LSM` / `LSMEngine` 门面
- `memtable.h:75-76` — current + frozen 双表；`shared_mutex`
- `sst.h:72` — BloomFilter 已接入
- `transaction.h:17` / `transation.cpp:197` — SERIALIZABLE 未实现
- `xmake.lua:109` — `lsm_shared` 静态库目标

### 一、raft 算法本身（语义 / 正确性 / 效率，面试含金量最高）

| #      | 项                                                   | 优先级 | 盘上 grep 核实（2026-10-01）                           |
| ------ | --------------------------------------------------- | --- | ------------------------------------------------ |
| **E1** | Per-follower Progress 状态机（Probe/Replicate/Snapshot） | -   | 仅朴素 `next_/match_index_` + 单 `ReplicateLoop`，无三态 |
| **E8** | Node-ID 稳定成员标识                                      | -   | 整数下标，`node_id` / `NodeId` **零命中**（重启/替换会身份错乱）    |

### 二、raft 工程化 / 运维（非算法、非持久化、非网络）

| #       | 项                      | 说明                                               |
| ------- | ---------------------- | ------------------------------------------------ |
| **#12** | 可观测性 metrics / tracing | 现仅 `fprintf(stderr)` 排错日志                        |
| **#13** | 锁粒度细化 / RCU            | 单大锁 `mu_`，改动面大风险高，最后做                            |
| **#15** | Raft log 最小保留窗口        | 落后 follower / learner 追平所需（#15 是"保留下限"细化，非"没截断"） |

### 三、快照传输协议（介于算法与网络之间，单列）

| #      | 项                | 说明                                                       |
| ------ | ---------------- | -------------------------------------------------------- |
| **#6** | abort/restart 机制 | 仅 CRC 部分归持久化；abort/restart 协议本身未做，跟 #8 配套最顺              |

## C. 推荐执行顺序

1. **E1 / E8**：算法内部机制级打磨（ETCD / dragonboat / TiKV 标配）。
2. **#8 / #6 快照传输 + #12 / #13 / #15**：工程打磨期。

> ⚠️ **雷区**：动 raft 核心算法路径（尤其 E1 / E8 等成员与进度状态机），交付前必须跑 `raft_test` + `kv_test` 全量 + TSan 硬化用例，确认无 term 暴涨 / 选举活锁回归（实测 `raft_test` 73 + `kv_test` 34 = **107** 条用例含 TSan 硬化，以二进制启动打印的「已注册 N 个用例」为准，详见 §九.D.5）。

*—— 本节由 WorkBuddy 于 2026-10-02 基于代码实核查补，非推测。*

---
