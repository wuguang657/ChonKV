# Raft 层生产化差距清单（2026-09-14 实测版）

> **范围**：只谈 **Raft 共识层** + **持久化层（对接 tiny-lsm）**。  
> 网络层（当前是 labrpc 模拟网络）你还没定，本文只留接口占位，不展开。
>
> **方法**：每一条结论都来自今天对你桌面源码的实际 grep / 读码，标注 `文件:行号` 作为铁证，不推测。  
> **用途**：这份是「待办清单」，后面你一条条实现，每项都带复选框。

---


## 〇、与 9-07 旧文档的关系（先看这个，避免重复劳动）

桌面上已有 `doc/1-生产级差距与路线图.md`（2026-09-07 写）。那份**已经过时了**——里面列的几条，这两个月你已经实现了。对照表：

| 旧文档条目                   | 旧结论                      | **今天实测的真实状态**                                   | 铁证                                                                           |
| ----------------------- | ------------------------ | ----------------------------------------------- | ---------------------------------------------------------------------------- |
| Pre-Vote 缺失             | "grep 零命中"               | ✅ **已实现**（完整，含不持久化状态的设计）                        | `raft.h:324` `RequestPreVote`；`raft.cpp:656` handler；`raft.cpp:437-466` 发起   |
| 读路径 Get 走日志、无 ReadIndex | "没有 ReadIndex/Lease"     | ✅ **已实现** ReadIndex（per-request ctx + no-op 保护） | `raft.h:280` `ReadIndex()`；`raft.cpp:1592`；`raft.h:549` `read_safe_commit_`  |
| 无 CheckQuorum           | 未提及                      | ✅ **已实现**（双路存活判定，今天刚修完误杀 bug）                   | `raft.cpp:315-367`；`raft.h:82` `kCQMaxConsecFail`                            |
| Leader Lease            | 未提及                      | ✅ 已实现但**默认关闭**（正确的工程选择）                         | `raft.h:95` `kEnableLeaseRead=false`                                         |
| 快照 blob 持锁落盘            | 未提及                      | ✅ **已实现两阶段锁外落盘**（这个做得比很多教学版都好）                  | `raft.cpp:1162-1181` `SaveSnapshotBlob`；`snap_io_mu_`；`saved_blob_index_` 单调 |
| 成员变更缺失                  | "Raft 状态机里没有任何运行时增删节点逻辑" | ⚠️ **仍然缺失，且比旧文档说的更危险**（见 §2.7）                  | `raft.cpp:188` 只初始化，全项目无第二处修改                                                |
| 真持久化                    | "Persister 纯内存"          | ⚠️ 仍然如此（本文 §7 专门解决，对接 tiny-lsm）                 | `persister.h:31-72` 全是 `std::string` + 锁                                     |
| 快照分块                    | "单 blob"                 | ⚠️ 仍然如此                                         | `raft.h:215` `std::string data`，无 offset/done                                |
| 批处理+流水线                 | "一次一个 entry"             | ⚠️ **部分改善**（会打包，但仍是 stop-and-wait）              | `raft.cpp:895-898` 打包无上限；`raft.h:501` `inflight_log_` 是 bool                 |

> **结论**：旧文档里"Pre-Vote / ReadIndex / CheckQuorum / 快照锁外落盘"四条可以划掉。  
> 剩下的核心是：**成员变更（含偶数节点）、真持久化、快照分块、流水线**。

---

## 一、现状能力盘点（今天实测）

| 能力                  | 状态              | 代码位置                                                         | 生产评价                      |
| ------------------- | --------------- | ------------------------------------------------------------ | ------------------------- |
| 选举 + 预投票            | ✅ 完整            | `raft.cpp:437` 预投票 / `:531` 正式投票                             | 合格，防 term 暴涨              |
| 日志复制 + 快速回退         | ✅ 完整            | `raft.cpp:802` ReplicateLoop；`AppendEntriesReply.next_index` | 合格                        |
| 持久化（2C）             | ⚠️ **内存模拟**     | `persister.h:31-72`                                          | ❌ 无 fsync / 无 CRC / 无 WAL |
| 快照（2D）              | ✅ 完整            | `raft.cpp:1186` Snapshot；`:1248` InstallSnapshot             | 合格，两阶段锁外落盘是亮点             |
| CheckQuorum         | ✅ 完整            | `raft.cpp:315-367`                                           | 合格，双路判定                   |
| ReadIndex           | ✅ 完整            | `raft.cpp:1592`；ctx 有 `erase`（`:1653`）无泄漏                    | 合格                        |
| Leader Lease        | ✅ 但默认关          | `raft.h:95`                                                  | 正确选择（时钟漂移风险，见 §5.4）       |
| **成员变更**            | ❌ **只有壳**       | `raft.h:555` `is_member_`；`raft.cpp:188`                     | ❌ 见 §2                    |
| **Learner**         | ❌ 无             | —                                                            | ❌ 见 §2.6                  |
| **Leader Transfer** | ❌ 无             | grep 零命中                                                     | ❌ 见 §4.1                  |
| **快照分块**            | ❌ 单 blob        | `raft.h:215`                                                 | ❌ 见 §3.2                  |
| **流水线**             | ❌ stop-and-wait | `raft.h:501` bool                                            | ❌ 见 §5.1                  |

---

# 二、【重点】偶数节点专项

你特别提到偶数节点，这块我展开讲透——因为**它和你当前的 `is_member_` 实现有直接的安全冲突**。

## 2.1 偶数节点为什么危险（4 条硬理由）

### ① 对半分区时，偶数节点**必然全挂**，奇数节点**必然存活**

这是最硬的一条。设 n 个节点，quorum = ⌊n/2⌋+1，考虑最常见的故障——**网络均匀对半分裂**：

| 节点数   | quorum | 对半分裂      | 最大分区  | 是否可用            |
| ----- | ------ | --------- | ----- | --------------- |
| 3     | 2      | 1 + 2     | 2     | ✅ **存活**（2 ≥ 2） |
| **4** | **3**  | **2 + 2** | **2** | ❌ **全挂**（2 < 3） |
| 5     | 3      | 2 + 3     | 3     | ✅ **存活**（3 ≥ 3） |
| **6** | **4**  | **3 + 3** | **3** | ❌ **全挂**（3 < 4） |

**规律**：n 为偶数时 quorum = n/2+1 **严格大于** n/2，所以对半分裂的两边都够不着 quorum → 整个集群既选不出 leader 也提交不了任何写 → **100% 不可用**。  
n 为奇数时，对半分裂必有一边是 ⌈n/2⌉ = quorum → **必然存活**。

> 这不是理论：两个机房间光纤断、两个机架的 TOR 交换机挂，都是"对半分裂"。偶数节点在这类故障下直接全站不可用。

### ② 容错性价比：偶数节点多花一台机器，容错能力一点没涨

| 节点数 | quorum | 能容忍挂几台             |
| --- | ------ | ------------------ |
| 3   | 2      | **1**              |
| 4   | 3      | **1** ← 多一台机器，容错一样 |
| 5   | 3      | **2**              |
| 6   | 4      | **2** ← 多一台机器，容错一样 |

**2f+1 和 2f+2 容错相同**。偶数节点 = 白养一台机器，还额外承担更大的 quorum（写要等更多副本 ack，尾延迟更差）。

### ③ 更容易平票（split vote），选举更慢

偶数节点下票数容易对半（2-2、3-3），本轮选举作废、等下一个随机超时重来 → 选主时间变长。你已有随机超时 + 预投票缓解，但**偶数节点平票概率显著高于奇数**是数学事实。

### ④ 更容易触发 CheckQuorum 退位抖动

quorum 越大，凑齐越难。4 节点 quorum=3，只要同时有 2 个 follower 抖动，leader 就自我退位 → 不必要的选主抖动。

---

## 2.2 单节点变更为什么是安全的（论文 §6 的核心结论）

Raft 成员变更的安全性建立在一条论证上：**任意两个"多数派"必须有交集**，这样就不可能同时选出两个 leader。

**一次只加/减 1 个节点时，C_old 的任意多数派和 C_new 的任意多数派必然相交。** 证明：

- **n 为偶数（n=2k），加入 1 个变 n+1=2k+1**：
  - quorum_old = k+1，quorum_new = k+1
  - 若两多数派不相交，需要 (k+1)+(k+1) = 2k+2 个不同节点，但 C_new 总共只有 2k+1 个 → **矛盾**，故必相交 ✅
- **n 为奇数（n=2k+1），加入 1 个变 n+1=2k+2**：
  - quorum_old = k+1，quorum_new = k+2
  - 若不相交需要 (k+1)+(k+2) = 2k+3 个，但 C_new 只有 2k+2 个 → **矛盾**，故必相交 ✅
- 移除 1 个（n → n-1）同理可证 ✅

> **前提（必须满足，否则上面证明不成立）**：
>
> 1. 变更通过**日志条目**传播，节点在 **apply 到该条目时**才切换配置（不能收到就切）
> 2. **同一时刻只允许一个变更在飞行中**——上一个变更 commit 后才能提下一个

---

## 2.3 一次变更多个节点 = 脑裂（反例：3 → 5，一次加 2 个）

注意：**危险来自"变更跨度"，不是目标态的奇偶**。就算目标是 5（奇数），一次加 2 个照样脑裂：

```
C_old = {A,B,C}          quorum_old = 2
C_new = {A,B,C,D,E}      quorum_new = 3

取 C_old 的一个多数派: {A,B}          (2 个, 满足 quorum_old=2)
取 C_new 的一个多数派: {C,D,E}        (3 个, 满足 quorum_new=3)

{A,B} ∩ {C,D,E} = ∅  ← 不相交！
```

后果：A、B 按旧配置选出 leader₁（term T）；C、D、E 按新配置选出 leader₂（term T+1）。  
**两个 leader 同时服务 → 数据分叉 → 已提交数据丢失。**

反向（缩容一次删 2 个）同样会脑裂。

---

## 2.4 单节点变更**必然经过偶数中间态**——运维纪律

3 节点扩容到 5，走单节点变更必须：

```
3 节点 (quorum=2, 容错1)
   ↓ 加 1
4 节点 (quorum=3, 容错1)   ← 偶数中间态！
   ↓ 加 1
5 节点 (quorum=3, 容错2)   ← 目标态
```

**4 节点这个中间态是真实存在的**，期间：

- 变更条目要在 4 个节点里拿到 3 票才 commit
- 容错仍是 1；此时再挂一台 → 剩 3 台，quorum 仍 3 → 必须 3 台全活着，任何抖动就停摆
- 再挂一台 → 全挂

**运维纪律（写进你的实现里当断言）**：

- [ ] 变更进行中（有未 commit 的 conf change entry）**拒绝**接受新的变更请求
- [ ] 变更进行中**禁止**节点下线/重启（或至少告警）
- [ ] 尽快走到奇数目标态，不要在偶数态停留

---

## 2.5 Joint Consensus vs 单节点变更（怎么选）

| 维度     | 单节点变更                         | Joint Consensus（论文 §6 原版）                |
| ------ | ----------------------------- | ---------------------------------------- |
| 一次能改几个 | **只能 1 个**                    | 任意多个                                     |
| 安全性论证  | 多数派必相交（§2.2）                  | C_old,new 需要 **同时**拿到 C_old 和 C_new 的多数派 |
| 实现复杂度  | 低（`is_member_` 数组 + 一条 entry） | 高（要同时维护两份配置，`is_member_` 这种 bool 表达不了）   |
| 中间态    | 会经过偶数                         | C_old,new 阶段天然安全                         |
| 谁在用    | etcd / TiKV 的默认路径             | etcd v3.4+ 也实现了，用于复杂变更                   |
| 对你的建议  | **先实现这个**                     | 等单节点变更稳定后再加                              |

**关键实现差异**：Joint 需要表达"某节点同时属于 C_old 和 C_new"，你现在的 `std::vector<bool> is_member_`（`raft.h:555`）**表达不了这个状态**。要支持 Joint 必须改成类似：

```cpp
// 方案A：两份配置
std::vector<bool> is_member_old_;   // C_old
std::vector<bool> is_member_new_;   // C_new
bool in_joint_ = false;             // 是否处于 C_old,new

// 方案B：三态（推荐，learner 也能复用）
enum class MemberRole { kRemoved, kLearner, kVoter };
std::vector<MemberRole> role_;
```

---

## 2.6 Learner：让扩容"零可用性损失"的关键

**问题**：新节点加入时日志是空的，要从快照追数据（可能几分钟～几小时）。如果它一上来就是 voter：

- 5 节点加 1 → 6 节点（偶数），quorum 从 3 涨到 4
- 新节点追数据期间，它的 match_index_ 一直是 0，**拉低提交的中位数**
- 更糟：quorum 按 6 算（需要 4 票），但能正常投票的只有 5 个老节点 → 只要同时有 2 个老节点抖动就凑不齐

**生产做法**：新节点先当 **learner**——

- ✅ 接收 AppendEntries / InstallSnapshot（追数据）
- ❌ **不参与投票**
- ❌ **不计入 quorum / MemberCount**
- ✅ 追上进度（match_index 接近 leader）后，再提一个 conf change 把它 promote 成 voter

这样扩容全程 quorum 不变，可用性零损失。

**你当前代码的问题**：`is_member_` 是 `bool`（`raft.h:555`），只有"是/否成员"两态，**表达不了 learner 这种"在集群里但不投票"的第三态**。必须扩展成三态（见 §2.5 方案B）。

---


## 2.7 ★ 当前代码在"偶数节点 + 成员变更"下的**已存在漏洞**（本篇最重要的发现）

我把所有算 majority 的地方都 grep 了一遍，结果是**不一致的**——这是一颗**已经埋好的定时炸弹**：

| 位置                       | 用途                     | 用的基数                  | 是否过滤 `is_member_` |
| ------------------------ | ---------------------- | --------------------- | ----------------- |
| `raft.cpp:336`           | CheckQuorum majority   | `MemberCountLocked()` | ✅ 过滤（`:345`）      |
| `raft.cpp:994`           | Leader Lease 计数        | `MemberCountLocked()` | ✅ 过滤              |
| `raft.cpp:1508`          | ReadIndex 心跳发给自己人      | `is_member_[i]`       | ✅ 过滤              |
| `raft.cpp:1579`          | ReadIndex ack majority | `MemberCountLocked()` | ✅ 过滤              |
| **`raft.cpp:487`**       | **预投票过半判定**            | **`peers_.size()`**   | ❌ **不过滤**         |
| **`raft.cpp:564`**       | **正式投票过半判定**           | **`peers_.size()`**   | ❌ **不过滤**         |
| **`raft.cpp:1005-1013`** | **日志提交中位数**            | **`peers_.size()`**   | ❌ **不过滤**         |

**为什么这是炸弹**：现在 `is_member_` 全是 `true`，`MemberCountLocked() == peers_.size()`，所以看不出问题，**测试全绿**。  
但一旦你实现成员变更、把某个 `is_member_[x]` 置成 `false`：

- CheckQuorum 说"我需要 3 票（4 成员）"
- 选举说"我按 5 个节点算，3 票就当选"
- **被移除的节点仍然能投票、仍然参与提交计票** → 它一票就能帮人凑出多数派 → **双 leader、丢已提交数据**

而且这恰好在**偶数节点场景最容易触发**（偶数节点扩缩容最频繁，最容易走到"成员数 ≠ peers\_.size()"的状态）。

**修复（做成员变更时的第一步，必须先做）**：

- [ ] 把 `raft.cpp:487`（预投票）、`:564`（正式投票）、`:1005`（提交中位数）全部改成用 `MemberCountLocked()`，并跳过 `!is_member_[i]` 的节点
- [ ] 投票时**不要向非成员发 RPC**（`raft.cpp:462`、`:544` 的循环里加过滤）
- [ ] 更彻底的方案：抽出 `MajorityOf(int n)` 工具函数 + `ForEachMember(lambda)`，全项目统一，杜绝再次漏改

---

## 2.8 如果必须用偶数部署：Witness / 2+1（选型备注）

现实约束：你可能只有 2 个机房（偶数）。业界做法是 **2 副本 + 1 witness**：

```
机房A: 副本 (全量数据)
机房B: 副本 (全量数据)
机房C: witness (只投票，不存/少存数据)   ← 便宜的小机器
```

- 投票成员 3 个 → quorum=2，容错 1
- 但只有 2 份全量数据 → 存储成本 = 2 副本

⚠️ **诚实标注**：witness 的正确性比 learner 复杂得多。Raft 投票要求"候选人日志至少和我一样新"，witness 不存日志就无从判断；常见做法是 witness 仍接收日志元数据（entry 的 index/term，不存 payload）。**这块需要单独设计，不建议作为第一批实现**。优先把 §2.7 的一致性漏洞和单节点变更做对。

---

## 2.9 偶数节点落地检查清单

- [ ] **（P0，最先做）** 统一 majority 计算：三处 `peers_.size()` → `MemberCountLocked()` + 成员过滤（§2.7）
- [ ] **（P0）** `is_member_` 从 `bool` 改三态 `enum MemberRole { kRemoved, kLearner, kVoter }`
- [ ] **（P0）** conf change 作为**日志条目**传播，节点在 **apply 时**才切换本地配置（不是收到就切）
- [ ] **（P0）** 同时只允许一个变更在飞行（有未 commit 的 conf entry 就拒绝新变更）
- [ ] **（P1）** conf change entry **持久化**（进 logs_ 一起落盘，重启能恢复配置）
- [ ] **（P1）** 被移除的节点：停止给它发 AE、停止计票；它自己超时后应自行退出/降级
- [ ] **（P1）** 如果 leader 自己被移除：必须先 TransferLeadership 再退位（否则集群空窗）
- [ ] **（P1）** Learner：接收复制但不投票、不计入 quorum；追上后 promote
- [ ] **（P2）** 集群规模建议：内部加一个断言/告警——检测到成员数为偶数且非中间态，打 WARN
- [ ] **（P2）** （可选）Joint consensus，支持一次变更多个节点

---

# 三、正确性 / 安全性缺口（P0）

### 3.1 成员变更（Conf Change）—— 最大结构性缺失

- **铁证**：`raft.cpp:188` `is_member_.assign(peers_.size(), true);` —— 全项目**唯一**一处赋值，无任何修改路径。grep `AddServer|RemoveServer|ConfChange|joint` 只命中 `config.cpp:268` 的 `net_->AddServer`（那是 labrpc 静态搭拓扑，不是 Raft 成员变更）。
- **生产后果**：集群规模**构造时固定**，无法在线扩缩容。换机器、扩容、机房迁移全都要重建集群。这不是"差点意思"，是"不叫分布式系统"。
- **实现要点**：
  1. 先修 §2.7 的 majority 不一致（否则做完变更反而不安全）
  2. `is_member_` 改三态 + 持久化
  3. 新增 entry 类型（在 `LogEntry` 里加 `is_conf_change` 或单独 type 字段，`raft.h:123`）
  4. apply conf entry 时切换本地配置
  5. 一次一个变更
- **验收**：3 节点 → 加 1 变 4 → 加 1 变 5，全程不停写、porcupine 线性化通过；5 → 4 → 3 缩容同理；变更期间 kill leader 不丢数据。

### 3.2 快照分块流式传输

- **铁证**：`raft.h:215` `std::string data;` —— 整个 blob 塞一条 RPC，没有 `offset` / `done` / `chunk_size`。
- **生产后果**：状态机几十 GB 时，一条 RPC 传几十 GB → 内存爆、连接占死、超时重传整块。**大状态机根本不可用**。
- **实现要点**：`InstallSnapshotArgs` 加 `offset` / `data` / `done` / `chunk_size`；follower 侧按 offset 追加写临时文件，收到 `done` 校验 CRC 后原子 rename。参考 etcd 的 `snap.Message`（带 `index/term/offset/data/done`）。
- **验收**：造 1GB 状态机，能稳定传完并 install。

### 3.3 日志无界增长 / 磁盘水位保护

- **现状**：快照触发完全靠上层 KVServer 调 `Snapshot()`（按 `RaftStateSize()` 阈值），**Raft 层自身没有任何保护**。上层没调/调不动，日志就无限涨。
- **生产后果**：磁盘写满 → 整个节点崩溃，且无法自愈（重启要 replay 巨量日志）。
- **实现要点**：Raft 层加磁盘水位检查（日志大小 > 阈值时**拒绝新写入 / 返回 ErrDiskFull** 而不是继续吞）、快照频率节流（两次快照最小间隔）、快照失败重试退避。
- **验收**：写满磁盘时集群降级为"只读"而不是崩溃。

### 3.4 RPC 消息完整性校验（CRC）

- **现状**：各 Args/Reply 的 `Serialize/Deserialize`（`raft.cpp:46-155`）**没有任何校验和**。网络层（`labrpc`）也不校验。
- **生产后果**：真实网络上 bit flip / 截断包会被当成合法数据解析 → 状态机静默损坏，且极难排查。
- **实现要点**：每个消息编码末尾加 CRC32/CityHash，反序列化时校验；失败直接丢弃当作丢包。
- **验收**：手工翻转一个字节，收端必须 reject 并计入丢包统计。

---

# 四、可用性 / 运维缺口（P1）

### 4.1 Leader Transfer（主动让贤）

- **现状**：grep `TransferLeader` **零命中**。
- **为什么必须**：滚动重启、机器下线、leader 在不合适的机房（跨地域延迟高）时，都要主动把 leader 交给指定节点。没有它，只能 kill 掉等选举 → 有几百 ms～几秒的写不可用空窗。
- **实现要点**：leader 收到 `TransferLeader(target)` → 若 target 的 match_index 落后，先加速复制 → 追上后给 target 发一条特殊 RPC（或提前触发它选举）让它立即竞选 → 自己退位。**超时则撤销transfer**（否则永久卡住）。
- **关联**：§3.1 里"leader 自己被移除"必须先 transfer 再退位。

### 4.2 优雅关闭 / 重启

- **现状**：`Kill()`（`raft.h:283`）是硬停。
- **生产需要**：graceful shutdown——停止接受新写、等已提交日志 apply 完、落盘、再退出。重启时快速恢复（不用 replay 全量日志）。

### 4.3 变更的并发保护

见 §2.4 检查清单——同一时刻只允许一个 conf change 在飞行。

### 4.4 选主优先级（可选）

多机房部署时希望 leader 落在主机房。已有了预投票，可在此基础上加优先级（低优先级节点延迟发起选举）。属于 P2，先不做。

---

# 五、性能缺口（P1/P2）

### 5.1 流水线（Pipelining）—— 当前是 stop-and-wait

- **铁证**：`raft.h:501` `std::vector<bool> inflight_log_;` —— 是 **bool**，意味着**每个 follower 同一时刻只有 1 个"带日志"的 AE 在途**。`raft.cpp:849` 的等待条件 `if (!inflight_log_[s]) return true;` 直接卡住第二发。
- **生产后果**：吞吐 = 1 个 batch / RTT。跨机房 RTT 10ms 时，每 10ms 才能推一批 → 吞吐被 RTT 锁死。
- **实现要点**：`inflight_log_` 改成滑动窗口（如 `inflight_count_` + 已发出但未确认的最大 index），允许窗口内多发；用 `next_index_`/`match_index_` 跟踪。要注意乱序回包的 `match_index_` 单调保护。

### 5.2 单条 AE 的条目数/字节数上限 —— 当前**没有上限**

- **铁证**：`raft.cpp:895-898`
  ```cpp
  int last = LastLogIndexLocked();
  for (int i = next; i <= last; i++) {
    args.entries.push_back(logs_[i - snapshot_index_]);
  }
  ```
  **从 next 一路塞到 last，一个上限都没有。**
- **生产后果**：follower 落后 100 万条日志时，一条 AE 塞 100 万条 → 单 RPC 几百 MB → 序列化爆内存、网络超时、整批重传。
- **实现要点**：加 `kMaxEntriesPerRpc`（如 1000 条）和 `kMaxBytesPerRpc`（如 1MB）双上限，超过就分批发。

### 5.3 批处理 + Group Commit

- **现状**：`Start()` 一条命令一个 entry，一次 Persist 一次（将来一次 fsync）。
- **生产需要**：
  - **写批处理**：多个客户端写合并成一个 AE 批次（已有打包雏形，加上限即可）
  - **fsync 合并（group commit）**：多条 entry 攒一起 fsync 一次，而不是每条 fsync。这是持久化层吞吐的命门（见 §7）

### 5.4 Leader Lease 的时钟风险（说明，不是必改）

- `raft.h:75` `kClockDriftBoundMs = 25` 是**硬编码假设**，`kLeaderLeaseMs = 125`。
- 你用的是 `steady_clock`（`util.h:183`）→ **单调、不受 NTP 跳变影响，这个选择是对的**。
- 但 Lease 的安全性依赖"跨机器的时钟速率一致"这个假设（晶振漂移通常 ppm 级，125ms 窗口下影响可忽略），且**没有任何监控去验证这个假设**。
- 现状 `kEnableLeaseRead = false`（`raft.h:95`）默认走 ReadIndex 心跳确认 → **已经规避了风险，保持默认关闭就好**。真要开，必须同时监控时钟漂移。

### 5.5 锁粒度（P2，改动大，最后做）

- 现在是一把大锁 `mu_` 保护全部状态（`raft.h:412`），读路径（CheckQuorum/ReadIndex/GetState）也要抢。
- 生产做法：读路径无锁/RCU、日志与状态分离锁。但**改动面大、风险高，建议等前面都稳定后再动**。

---

# 六、客户端会话与资源回收

- [ ] **（P1）去重表无 TTL/LRU**：KV 层 `(client_id, seq_id)` 去重表（`server.cpp` 相关）只增不减。客户端数量一多（生产上几十万），内存无界增长。需要：session TTL + LRU 淘汰 + 淘汰时通知客户端"session 过期需重建"。
- [ ] **（P2）Raft 层提供 session 抽象**：现在 session 在 KV 层。生产 Raft 通常把 session 管理下沉（etcd 的 `Lessor`/lease），便于快照时统一序列化。

> 说明：`ReadIndexCtx` 我已经查过，`raft.cpp:1653` 有 `read_ctxs_.erase(ctx)`，**没有泄漏**，这条不用改。（避免你白找一遍）

---

# 七、持久化层：对接 tiny-lsm

## 7.0 ★ 先说一个关键设计决策：Raft log **不要**塞进 LSM 的 KV 接口

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


## 7.1 tiny-lsm 现状（基于实际代码）

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


## 7.2 tiny-lsm 的 4 个坑（接入前必须知道）

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

## 7.3 三块持久化的分工与 fsync 策略

| 数据                              | 特点                          | 用什么存                                  | fsync 策略                                                     |
| ------------------------------- | --------------------------- | ------------------------------------- | ------------------------------------------------------------ |
| **Raft log**                    | append-only、按 index 读、前后缀截断 | **独立 segment log**（自写，不用 LSM）         | **group commit**：攒多条一起 fsync；但**已提交的 entry 必须 fsync 后才回客户端** |
| **Raft state**（term / votedFor） | 极小、频繁变更                     | 小文件原子写（tmp + rename），或跟 log 同 segment | 每次变更 fsync（便宜）                                               |
| **状态机 KV**                      | 随机读写、量大                     | **tiny-lsm** `LSMEngine`              | 走**事务 API**，由 LSM 的 WAL 保证                                   |
| **快照 blob**                     | 大、偶发、整体替换                   | LSM `flush_all()` + 导出 / checkpoint   | 锁外写（你现在的两阶段 `SaveSnapshotBlob` 思路可直接平移）                      |

## 7.4 接入步骤（按这个顺序来，每步可独立验证）

- [ ] **Step 0**：在 macOS 上先跑通 tiny-lsm（`xmake` + `xmake run test_lsm`）。  
  ⚠️ 风险：README 标注 platform 为 Linux，macOS（Apple Silicon）**未明确保证**。这一步跑不通，后面方案要换。
- [ ] **Step 1**：定义 `LogStore` 接口（`Append/Get(index)/Term(index)/LastIndex/TruncateSuffix/TruncatePrefix`），先用**内存 vector** 实现，把 `raft.cpp` 里对 `logs_` 的直接操作收敛到接口后面。**此时测试应全绿（纯重构）**。
- [ ] **Step 2**：实现 `SegmentLogStore`（append-only 文件 + 稀疏索引 + fsync + 截断），替换 Step 1 的内存实现。加 CRC 防半截写。
- [ ] **Step 3**：`StateStore`——term/votedFor 原子落盘（tmp + rename + fsync）。
- [ ] **Step 4**：**先改造 tiny-lsm**：把 `full_compact` 挪到后台线程（见 §7.2 坑2）。不然后面怎么接都会卡。
- [ ] **Step 5**：状态机换成 tiny-lsm。apply 用**事务 API**（`begin_tran`/`put`/`commit`）保证 WAL fsync。单线程 apply 正好契合 LSM 无并发写。
- [ ] **Step 6**：快照——先 `flush_all()` + `Level_Iterator` 全量导出（**注意 MVCC**：要以最大 `tranc_id` 取每个 key 的最新非删除值）；恢复时清空 + `put_batch` 重放。更优方案是实现 **checkpoint**（硬链接 SST 目录，RocksDB 做法，秒级）。
- [ ] **Step 7**：group commit——`Start()` 后的 fsync 攒批（这是吞吐的关键，也是面试亮点）。

## 7.5 tiny-lsm 需要补的能力（你的下一个子项目）

- [ ] **后台 compaction 线程**（P0，阻塞问题）
- [ ] **Manifest**（替代目录扫描）
- [ ] **Checkpoint / 快照序列化**（Raft 快照要用）
- [ ] **非事务写也走 WAL**，或明确"只允许事务 API"并在文档/断言里锁死
- [ ] **`WAL::flush()` 真正实现或改名**（现在是个陷阱）
- [ ] （可选）列族、range scan API

---

# 八、网络层（留白，你还没定）

现在是 labrpc 模拟网络（`src/labrpc/`）。将来换真实传输层时，需要抽象出这几个接口（先不展开，等你定方案）：

- `CallAsync(method, args, callback, cancel_token)` — 已有雏形（`labrpc.h:120`）
- 连接管理 / 重连、消息大小上限、流控与背压、TLS、keepalive
- 超时与取消语义（现在的 `cancel_rpcs_` 可以直接平移）

> 生产级 RPC 的硬要求：**消息大小上限** + **流控/背压**（不做的话，一个慢 follower 能把 leader 的内存打爆）。

---

# 九、实施路线图（建议顺序）

**第一批（P0，安全性，不做就不能叫生产）**

- [ ] 统一 majority 计算，修 §2.7 的三处不一致 ← **最先做，改动小、收益大**
- [ ] `is_member_` 三态化 + 持久化
- [ ] 单节点成员变更（一次一个、apply 时切换）
- [ ] Learner
- [ ] 日志/磁盘水位保护

**第二批（P0，持久化）**  
6\. [ ] macOS 跑通 tiny-lsm  
7\. [ ] `LogStore` 接口重构（纯重构，测试全绿）  
8\. [ ] `SegmentLogStore` 落盘 + CRC + group commit  
9\. [ ] tiny-lsm 后台 compaction（否则接入必卡）  
10\. [ ] 状态机接 tiny-lsm（走事务 API）  
11\. [ ] 快照：flush_all + 导出 → checkpoint

**第三批（P1，可用性与性能）**  
12\. [ ] 快照分块流式传输  
13\. [ ] Leader Transfer  
14\. [ ] 单条 AE 的条目数/字节数上限  
15\. [ ] 流水线（滑动窗口）  
16\. [ ] 客户端 session TTL / LRU  
17\. [ ] 优雅关闭

**第四批（P2，工程成熟度）**  
18\. [ ] 可观测性：metrics（term / commit / apply lag / log size / snapshot 大小 / RPC 延迟）+ 结构化日志  
19\. [ ] 混沌测试 / 故障注入（在现有 tester 基础上加）  
20\. [ ] 锁粒度优化  
21\. [ ] （可选）Joint consensus、优先级选举、Witness

---


## 附：本次实测铁证清单

**Raft（cpp-6.824）**

- `raft.cpp:188` — `is_member_` 全 true 初始化，全项目唯一赋值处
- `raft.cpp:487 / 564` — 预投票 / 正式投票用 `peers_.size()`，**未过滤 is_member\_**
- `raft.cpp:1005-1013` — 提交中位数用 `peers_.size()`，**未过滤**
- `raft.cpp:336 / 345 / 994 / 1508 / 1579` — CheckQuorum / Lease / ReadIndex **用了 MemberCountLocked()**（与上面对比即漏洞）
- `raft.cpp:1492-1496` — `MemberCountLocked()` 定义
- `raft.h:215` — `InstallSnapshotArgs.data` 单 string，无 offset/done
- `raft.h:501` — `inflight_log_` 是 bool（无流水线）
- `raft.cpp:895-898` — entries 打包无上限
- `raft.h:75 / 95` — `kClockDriftBoundMs=25` 硬编码；`kEnableLeaseRead=false`
- `util.h:183` — `steady_clock`（好选择）
- `persister.h:31-72` — 纯内存，无 fsync/CRC
- `raft.cpp:1653` — `read_ctxs_.erase(ctx)`（**无泄漏，澄清**）
- grep `TransferLeader|ConfChange|joint|learner|chunk|offset` — **零命中**（仅 `config.cpp:268` 静态拓扑）

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
