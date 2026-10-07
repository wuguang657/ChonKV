# Raft 层生产化差距清单（2026-09-14 实测版）

> **范围**：只谈 **Raft 共识层** + **持久化层（对接 tiny-lsm）**。  
> 网络层（当前是 labrpc 模拟网络）你还没定，本文只留接口占位，不展开。
>
> **方法**：每一条结论都来自今天对你桌面源码的实际 grep / 读码，标注 `文件:行号` 作为铁证，不推测。  
> **用途**：这份是「待办清单」，后面你一条条实现，每项都带复选框。

---


| 能力                  | 状态                   | 代码位置                                                                               | 生产评价                                |
| ------------------- | -------------------- | ---------------------------------------------------------------------------------- | ----------------------------------- |
| 持久化（2C）             | ⚠️ **内存模拟**          | `persister.h:31-72`                                                               | ❌ 无 fsync / 无 CRC / 无 WAL           |
| **快照分块**            | ❌ 单 blob             | `raft.h:215`                                                                        | ❌ 见 §2.1                            |

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

### 3.1 批处理 + Group Commit

- **现状**：`Start()` 一条命令一个 entry，一次 Persist 一次（将来一次 fsync）。
- **生产需要**：
  - **写批处理**：多个客户端写合并成一个 AE 批次（已有打包雏形，加上限即可）
  - **fsync 合并（group commit）**：多条 entry 攒一起 fsync 一次，而不是每条 fsync。这是持久化层吞吐的命门（见 §4）

### 3.2 锁粒度（P2，改动大，最后做）

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

| 数据                              | 特点                          | 用什么存                                  | fsync 策略                                                     |
| ------------------------------- | --------------------------- | ------------------------------------- | ------------------------------------------------------------ |
| **Raft log**                    | append-only、按 index 读、前后缀截断 | **独立 segment log**（自写，不用 LSM）         | **group commit**：攒多条一起 fsync；但**已提交的 entry 必须 fsync 后才回客户端** |
| **Raft state**（term / votedFor） | 极小、频繁变更                     | 小文件原子写（tmp + rename），或跟 log 同 segment | 每次变更 fsync（便宜）                                               |
| **状态机 KV**                      | 随机读写、量大                     | **tiny-lsm** `LSMEngine`              | 走**事务 API**，由 LSM 的 WAL 保证                                   |
| **快照 blob**                     | 大、偶发、整体替换                   | LSM `flush_all()` + 导出 / checkpoint   | 锁外写（你现在的两阶段 `SaveSnapshotBlob` 思路可直接平移）                      |

## 4.4 接入步骤（按这个顺序来，每步可独立验证）

- [ ] **Step 0**：在 macOS 上先跑通 tiny-lsm（`xmake` + `xmake run test_lsm`）。  
  ⚠️ 风险：README 标注 platform 为 Linux，macOS（Apple Silicon）**未明确保证**。这一步跑不通，后面方案要换。
- [ ] **Step 1**：定义 `LogStore` 接口（`Append/Get(index)/Term(index)/LastIndex/TruncateSuffix/TruncatePrefix`），先用**内存 vector** 实现，把 `raft.cpp` 里对 `logs_` 的直接操作收敛到接口后面。**此时测试应全绿（纯重构）**。
- [ ] **Step 2**：实现 `SegmentLogStore`（append-only 文件 + 稀疏索引 + fsync + 截断），替换 Step 1 的内存实现。加 CRC 防半截写。
- [ ] **Step 3**：`StateStore`——term/votedFor 原子落盘（tmp + rename + fsync）。
- [ ] **Step 4**：**先改造 tiny-lsm**：把 `full_compact` 挪到后台线程（见 §4.2 坑2）。不然后面怎么接都会卡。
- [ ] **Step 5**：状态机换成 tiny-lsm。apply 用**事务 API**（`begin_tran`/`put`/`commit`）保证 WAL fsync。单线程 apply 正好契合 LSM 无并发写。
- [ ] **Step 6**：快照——先 `flush_all()` + `Level_Iterator` 全量导出（**注意 MVCC**：要以最大 `tranc_id` 取每个 key 的最新非删除值）；恢复时清空 + `put_batch` 重放。更优方案是实现 **checkpoint**（硬链接 SST 目录，RocksDB 做法，秒级）。
- [ ] **Step 7**：group commit——`Start()` 后的 fsync 攒批（这是吞吐的关键，也是面试亮点）。

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
7\. [ ] `LogStore` 接口重构（纯重构，测试全绿）  
8\. [ ] `SegmentLogStore` 落盘 + CRC + group commit  
9\. [ ] tiny-lsm 后台 compaction（否则接入必卡）  
10\. [ ] 状态机接 tiny-lsm（走事务 API）  
11\. [ ] 快照：flush_all + 导出 → checkpoint

**第二批（P1，可用性与性能）**  
12\. [ ] 快照分块流式传输（#8，未做）  

**第三批（P2，工程成熟度）**  
13\. [ ] 可观测性：metrics（term / commit / apply lag / log size / snapshot 大小 / RPC 延迟）+ 结构化日志  
14\. [ ] 混沌测试 / 故障注入（在现有 tester 基础上加）  
15\. [ ] 锁粒度优化

---

## 附：本次实测铁证清单

> 注：以下为 2026-09-14 首次实测基线。其中 §2.7 多数派过滤（原 `peers_.size()` **未过滤** `is_member_`）、§4.1 流水线（原停等）、§4.2 条目数上限已被后续实现推翻——当前真实状态以 §九.C 待做清单与 §九 工程经验为准，本节不再逐条修订。

**Raft（cpp-6.824）**

- `raft.cpp:188` — `is_member_` 全 true 初始化，全项目唯一赋值处
- `raft.cpp:487 / 564` — 预投票 / 正式投票用 `peers_.size()`，**未过滤 is_member\_**
- `raft.cpp:1005-1013` — 提交中位数用 `peers_.size()`，**未过滤**
- `raft.cpp:336 / 345 / 994 / 1508 / 1579` — CheckQuorum / ReadIndex **用了 MemberCountLocked()**（与上面对比即漏洞）
- `raft.cpp:1492-1496` — `MemberCountLocked()` 定义
- `raft.h:215` — `InstallSnapshotArgs.data` 单 string，无 offset/done
- `raft.h:110` — `kPipelineMaxInFlight = 8 * kMaxEntriesPerRpc` 窗口化流水（取代旧 `inflight_log_` bool 停等）；`raft.cpp:1199/1251` 在途窗口判定
- `raft.h:105` — `kMaxEntriesPerRpc = 1024` 单 RPC 条目数上限（`raft.cpp:1324-1327` 截断）；**字节上限 `kMaxBytesPerRpc = 1MiB` 已实现**（双上限截断，2026-10-03 落地）
- `raft.cpp:895-898` — entries 打包受双上限约束（条目数 1024 / 字节 1MiB）
- `persister.h:31-72` — 纯内存，无 fsync/CRC
- `raft.cpp:1653` — `read_ctxs_.erase(ctx)`（**无泄漏，澄清**）
- grep `ConfChange|learner|chunk|offset` — **零命中**（仅 `config.cpp:268` 静态拓扑）

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

---

## A. 仍有效的部分（保留原文，未过期）

- **§4 tiny-lsm 分析**（4.0–4.5）：持久化层对接分析与 4 个坑仍有效（WAL 仅事务路径写、前台 compaction 阻塞、无 Manifest、`WAL::flush()` 空实现）。
- 以下"确仍缺失/未做"的小节结论正确，已在 §九.C 清单中保留：§2.1 快照分块、§2.2 CRC、§3.2 锁粒度。

## B. 剔除「持久化 + 网络」后的剩余待做清单（2026-10-01）

> **过滤口径**：持久化类（#10 真持久化 tiny-lsm、#11 Group Commit、#7 真·磁盘占比、#6 的 CRC、C4 log CRC、E4 Storage 接口、E5 Ready/Advance、E6 unstable 缓冲、D7 WAL 分离）与网络类（#14 网络流控、C2 字节 inflight、D2 快照限速、D3 Msg 优先级）已剔除。以下为剩下的真·待做。

### 一、raft 算法本身（语义 / 正确性 / 效率，面试含金量最高）

| #       | 项                                                   | 优先级     | 盘上 grep 核实（2026-10-01）                                                                                                                                                                                                                                         |
| ------- | --------------------------------------------------- | ------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **E1**  | Per-follower Progress 状态机（Probe/Replicate/Snapshot） | -       | 仅朴素 `next_/match_index_` + 单 `ReplicateLoop`，无三态                                                                                                                                                                                                               |
| **E8**  | Node-ID 稳定成员标识                                      | -       | 整数下标，`node_id` / `NodeId` **零命中**（重启/替换会身份错乱）                                                                                                                                                                                                                  |

### 二、raft 工程化 / 运维（非算法、非持久化、非网络）

| #       | 项                      | 说明                                                                                       |
| ------- | ---------------------- | ---------------------------------------------------------------------------------------- |
| **#12** | 可观测性 metrics / tracing | 现仅 `fprintf(stderr)` 排错日志                                                                |
| **#13** | 锁粒度细化 / RCU            | 单大锁 `mu_`，改动面大风险高，最后做                                                                    |
| **#15** | Raft log 最小保留窗口        | 落后 follower / learner 追平所需（#15 是"保留下限"细化，非"没截断"）                                         |

### 三、快照传输协议（介于算法与网络之间，单列）

| #      | 项                | 说明                                                       |
| ------ | ---------------- | -------------------------------------------------------- |
| **#8** | 快照分块 / 流式传输      | 当前单 blob 全量，`chunk` / `offset` / `SnapshotChunk` **零命中** |
| **#6** | abort/restart 机制 | 仅 CRC 部分归持久化；abort/restart 协议本身未做，跟 #8 配套最顺              |



## C. 推荐执行顺序

1. **E1 / E8**：算法内部机制级打磨（ETCD / dragonboat / TiKV 标配）。
2. **#8 / #6 快照传输 + #12 / #13 / #15**：工程打磨期。

> ⚠️ **雷区**：动 raft 核心算法路径（尤其 E1 / E8 等成员与进度状态机），交付前必须跑 `raft_test` + `kv_test` 全量 + TSan 硬化用例，确认无 term 暴涨 / 选举活锁回归（实测 `raft_test` 73 + `kv_test` 34 = **107** 条用例含 TSan 硬化，以二进制启动打印的「已注册 N 个用例」为准，详见 §九.D.5）。

*—— 本节由 WorkBuddy 于 2026-10-01 基于代码实核查补，非推测。*

---

## D. 工程实测经验（sanitizer / 构建系统 / 测试规模）

> 本节沉淀调试与工程实测经验，供后续开发与测试参考。

### D.3 Sanitizer 对重负载测试的已知影响（实测经验 ⚠️）

这是这两天调试沉淀的、文档之前没写的关键坑：

- **TSan 活性 flake（≈1/250，非 bug）**：`TestSnapshotUnreliableRecover3B` 在 TSan 构建下偶发 `no client made progress` 失败。实证：同种子副本 28 轮 PASS、桌面 FAIL，调度非确定；根因是 TSan 5–15× 减速偶尔让选主+提交首条超 5s 实时窗口。判定**非正确性 bug**（无 partition 配置、实时驱动测试）。
- **ASan 减速致 3B 单操作超时假失败**：`TestSnapshotUnreliable3B`（`GenericTest("3B",5,true,false,false,1000)`）在 ASan 构建下出现 `!!! get wrong value ... got []` + `⚠️ GIVE UP: Get(...) 超过 60s`。**无任何 ASan 报错**即说明不是内存 bug，而是 ASan 减速把 Clerk 的 60s 单操作墙钟挤爆（空读+超时，非数据损坏）。**证伪法**：`SEED=<n> ./build/kv_test TestSnapshotUnreliable3B -count 1`（非 sanitizer 构建）跑同种子，过了即实锤假失败。
- 关联工程事实：sanitizer 构建里 `SAN_OPT`（`-O1`，`--opt` 时 `-O2`）追加在命令行尾部，**覆盖** Release 的 `-O3`，所以 sanitizer 构建实际优化级是 `-O1/-O2` 而非 `-O3`（但 `-DNDEBUG` 仍生效）。重负载 3B 在 sanitizer 下测试，建议放宽或关掉 Clerk 的 60s 单操作超时，否则容易被误判失败。

### D.4 构建系统多档化（工程成熟度）

`build.sh` 现已支持多档构建（`--release` / `--opt` / `--asan` / `--ubsan` / `--tsan` / `--clean`），`test_part.sh` 提供并发压测 harness（`SAN` / `RELEASE` / `OPT` / `COUNT` / `PARALLEL` / `SEED` / `TEST_TIMEOUT_MS` / `BIN` 环境变量）。踩坑已固化进脚本：

- TSan 与 ASan **互斥**（CMake 层 `FATAL_ERROR` 守卫）；
- UBSan 必须 `halt_on_error=1`（否则 UB 测试假绿）；
- 优化会改变指令时序，**Debug 才是回归基线**（sanitizer 下偶发活性 flake 不计入正确性失败）。

### D.5 测试规模更正

- 实测：`raft_test` **73 条**（`kTests[]` @ `test_raft.cpp:4538-4647`）+ `kv_test` **34 条**（`kTests[]` @ `test_kvraft.cpp:2111-2195`）= **107 条**，含 TSan 硬化用例。
- ⚠️ 扩展用例（Membership / ReadIndex / CheckQuorum / MaxMessageSize）以 lambda 包 `GenericTest(...)` 注册，光 grep `void Test*` 会漏数——**以二进制启动打印的「已注册 N 个用例」为准**。

---

*—— 本节由 WorkBuddy 于 2026-10-02 基于代码实核查补，非推测。*

---

## E. 变更记录

- **2026-10-07**：「日志无界增长 / 磁盘水位保护」（原 §2.2、原第一批 P0）**已落地并验证通过**，从待办清单移除（文档已删条目并顺延编号）。落地清单（桌面实核）：
  - Raft 层水位检查：`Start()` / `ProposeConfChangeTo()` 在提交前判 `RaftStateSize ≥ max_raft_state_bytes_` 即返回 `disk_full`（`raft.cpp:1160` / `:2227`，均在 `push_back` 之前，重试不重复 append）；
  - KVServer 接线：`server.cpp:24` 设配额、`WaitOp` 收到 `disk_full` 调 `ForceSnapshotOnDiskFull()`（`:367`），压缩后仍满返 `kBusy`（`:377`）；
  - 快照频率节流：CAS 限流 `kForceSnapMinInterval = 50ms`（`server.cpp:209` / `:235`），任意 50ms 窗口至多一次真落盘，杜绝 fsync 风暴；
  - 失败重试退避：有界 `for(≤5 轮)` + 指数退避 2/4/8/16ms 封顶 50ms（`server.cpp:366-374`）；
  - 护栏用例：`TestKVDiskFullBurstThrottle`（`test_kvraft.cpp:470`，注册 `:2311`），负向对照证明是真护栏。
  - 编号顺延：原 §2.3 CRC → §2.2；路线图「第二批/第三批/第四批」→「第一批/第二批/第三批」。
