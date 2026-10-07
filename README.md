# ChonKV — C++ Raft 共识核 + KV-Raft 服务

> 基于 MIT 6.824（2020）课程框架从零手写的 C++ Raft 实现，并已显著超出原 lab 范围。
> A from-scratch C++ Raft implementation built on the MIT 6.824 framework, extended with production-oriented features.

---

## English Abstract

**ChonKV** is a from-scratch C++ implementation of the Raft consensus algorithm and its
KV-Raft application layer. It started from the MIT 6.824 (2020) course scaffolding and has
been extended well beyond the original labs. It passes the full Lab 2 suite
(election / log replication / persistence / snapshots) and Lab 3 suite (linearizable KV
service with snapshots), and adds **Pre-Vote**, **CheckQuorum leader step-down**,
**ReadIndex + Lease read**, **dynamic cluster membership change** (learner role, Q2
removal freeze), **chunked snapshot streaming**, and **log/disk watermark protection**.
The test suite ships **81 Raft tests + 37 KV tests (118 total)**, including a
`porcupine`-style linearizability checker, a ThreadSanitizer/ASan/UBSan build matrix, and a
parallel stress harness (`test_part.sh`).

> **Persistence**: the Raft layer supports **two modes** — **file-backed (default**,
> crash-safe on-disk via `tmp → fsync → rename → fsync(dir)`) and **in-memory**. See
> [持久化模式开关](#持久化模式开关) for the exact switch location.
> **Transport** is still simulated by `labrpc` (no real network).

### Build & test in 30 seconds

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug && cmake --build build -j
./build/raft_test 2A          # run a Raft test group
./build/kv_test   3B          # run a KV test group
```

---

## 中文概述

ChonKV 是一份**从零手写**的 C++ Raft 实现 + 跑在其上的 KV-Raft 服务。起点是
MIT 6.824（2020）的课程框架，但实现已经显著超出原 lab 范围：

- 共识核完整覆盖 **Lab 2A~2D**（领导者选举、日志复制、崩溃恢复持久化、快照压缩）；
- KV 层覆盖 **Lab 3A/3B**（线性一致 KV 服务 + 快照）；
- 额外自研了多项**生产向**特性（见下）；
- 配套 **118 条测试**（Raft 81 + KV 37），含 porcupine 线性化校验、TSan/ASan/UBSan
  构建矩阵，以及进程级并发压测脚本（`test_part.sh`）。

> ✅ **持久化层**：Raft 层已支持**双模式** —— **文件模式（默认**，崩溃安全落盘
> `tmp → fsync → rename → fsync(dir)`）与**内存模式**，开关位置见
> [持久化模式开关](#持久化模式开关)。
>
> ⚠️ **传输层（labrpc）仍是软件模拟**，真实网络尚未接入；KV 测试脚手架的 Persister
> 目前固定为内存模式（详见开关一节）——详见下方路线图。

---

## 特性 / Features

**共识核心（Lab 2，全过）**

- 领导者选举（随机化选举超时、任期机制）
- 日志复制与提交（majority 提交、按 voter 实时计票，已正确处理偶数节点）
- 崩溃恢复持久化（term / votedFor / log 持久化接口）
- 日志压缩 / 安装快照（2D）

**KV 服务（Lab 3，全过）**

- 线性一致读写（Get / Put / Append）
- 客户端 exactly-once 去重（`(client_id, seq_id)`）
- 快照与状态机重放
- porcupine 线性化模型检查器

**生产向扩展（在原 lab 之上自研）**

| 特性 | 说明 |
|---|---|
| **Pre-Vote** | 分区恢复时避免无意义选举打断现有 leader |
| **CheckQuorum** | leader 被隔离后主动退位，避免双主脏写 |
| **ReadIndex + Lease Read** | 线性一致读路径；Lease 快路径默认关闭（见 `raft.h` 的 `kEnableLeaseRead`） |
| **动态成员变更** | 单飞闸（single-flight）+ 最后 voter 阀门，保证变更期间不丢 Majority |
| **Learner 三态成员** | `kVoter / kLearner / kRemoved`，Learner 不计票、可追平后提拔 |
| **Q2 移除冻结** | 被移除节点日志硬冻结在移除配置条目下标，不泄漏后续条目（机密隔离） |
| **文件持久化（双模式）** | `Persister` 支持内存 / 文件两态；文件模式走崩溃安全写 `tmp → fsync → rename → fsync(dir)`，落盘 `raft_state.bin` + `snapshot.bin`，**默认开启** |
| **日志 / 磁盘水位保护** | `Start()` 提交前判 `RaftStateSize ≥ maxraftstate` → 返回 `disk_full`，上层强制压缩后重试；与老快照机制共用同一阈值 |
| **快照节流 + 指数退避** | 强制快照走 CAS 限流（任意 50ms 窗口至多一次真落盘，防并发突发下的 fsync 风暴）+ `WaitOp` 指数退避（2/4/8/16ms，封顶 50ms） |
| **分块快照流式传输** | `InstallSnapshot` 按 `kSnapshotChunkSize` 分块（offset / done / crc / chunk_size），避免大 blob 一次性顶穿收发两端内存 |

**工程质量**

- 异步 RPC + cancel 刹车（Kill 后不空等）
- 动态扩容线程池（避免阻塞型 handler 饿死心跳）
- T2/T3 崩溃窗口双层防护（快照 install 与 KV map 更新非原子问题）
- `std::chrono::steady_clock` 单调时钟（租约不受墙钟 NTP 回跳影响）
- **多档构建矩阵**：Debug / Release / LTO，加 TSan / ASan / UBSan 任意组合
- **并发压测 harness**：每条用例独立进程、换种子多轮、日志分文件、可并发

---

## 持久化模式开关（内存 / 文件）

Raft 的持久化层 `Persister`（`src/labgob/persister.h`）有两种模式，**开关只有一个参数**：

| 模式 | 含义 | 是否落盘 | 怎么开 |
|---|---|---|---|
| **文件模式**（**默认**） | 每个节点一个独立目录，持久化真写盘 | ✅ `raft_state.bin` + `snapshot.bin` | `file_backed = true` |
| **内存模式** | 原 lab 行为，纯内存，零磁盘 I/O | ❌ 不落盘 | `file_backed = false` |

### ① 全局开关：改一行默认值（raft 测试脚手架）

```cpp
// src/raft/config.h:58
Config(int n, bool unreliable, bool file_backed = true);
//                                              ^^^^ 默认值：true = 文件模式
```

把 `true` 改成 `false`，整个 `raft_test` 全部用例就切回内存模式；改回 `true` 即恢复文件模式。
**这是最省事的切换方式：一行改完全局。**

### ② 单条用例覆盖：构造 Config 时显式传参

```cpp
auto cfg = std::make_shared<Config>(servers, /*unreliable=*/false, /*file_backed=*/false);
```

第三个参数显式给值即可覆盖默认值（个别用例想单独走内存模式时用）。

### ③ 更底层：直接选 Persister 工厂

```cpp
// src/labgob/persister.h:293 / :298
auto p_mem  = labgob::MakePersister();                    // 内存模式（无参数版本）
auto p_file = labgob::MakePersister("/tmp/xxx/node_0");   // 文件模式（传目录）
```

### 文件模式把数据写到哪？

根目录由测试脚手架按 `pid + 序号` 生成，**每节点一个子目录**：

```
/tmp/cpp6raft_<pid>_<seq>/          ← 根目录，src/raft/config.cpp:70-71
    ├── node_0/
    │   ├── raft_state.bin          ← term / votedFor / log
    │   └── snapshot.bin            ← 快照 blob
    ├── node_1/
    └── copy_<seq>/                 ← 崩溃重启时 Copy() 出的目录副本
```

- `Cleanup()` 时整棵树由 `RemoveTree()` 递归删掉，不会在 `/tmp` 留垃圾。
- 崩溃重启用例（`Crash1` + `Start1`）会 `Copy()` 出一份**目录副本**，保证旧实例的
  残留线程改不到新实例的数据。副本名是扁平的 `copy_<seq>`（刻意不沿用源目录名，
  否则反复重启会逐层嵌套、撑爆 `NAME_MAX=255`）。

### 落盘失败会怎样？

**直接 `std::abort()`（fail-stop），绝不"假装写成了"**。这是 Raft 铁律：持久化成功前
不可提交，写不下去还继续跑会基于幽灵写入推进 `commitIndex` —— 那种情况下崩溃才是
正确行为（宁崩不撒谎）。见 `persister.h` 的 `SaveRaftState` / `SaveStateAndSnapshot` /
`SaveSnapshotOnly`。

> 说明：测试 harness 模拟的是**进程崩溃重启**（`Crash1` + `Start1`），不是真实断电；
> 但写路径本身（`tmp → fsync → rename → fsync(目录)`）是掉电安全的。

### ⚠️ KV 测试目前固定为内存模式（开关尚未接线）

`kvraft::Config` **没有** `file_backed` 参数（`src/kvraft/config.h:27`），`StartServer()`
里固定调用无参工厂：

```cpp
// src/kvraft/config.cpp:128
saved_[i] = labgob::MakePersister();   // ← 内存模式
```

所以 **KV 侧（3A / 3B）跑的是内存持久化**。若想让 KV 也走文件模式，需自行给
`kvraft::Config` 加一个 `file_backed` 参数并透传到 `StartServer()`（约 3 处改动：
`config.h` 构造函数签名、`config.cpp` 构造函数、`StartServer` 里的工厂调用）。

---

## 目录结构 / Structure

```
cpp-6.824/
├── src/
│   ├── common/           # 工具：随机数、时间、channel、线程跟踪器
│   ├── labrpc/           # 网络仿真层（丢包/延迟/乱序/断连/字节统计）
│   ├── raft/             # ★ 共识核：raft.h / raft.cpp / config / persister / test_raft.cpp
│   ├── kvraft/           # KV-Raft 服务（复用同一份 raft.cpp）
│   │   ├── server.cpp / client.cpp / config.cpp
│   │   ├── porcupine.cpp # 线性一致性模型检查器（对应 Go 版 porcupine）
│   │   └── test_kvraft.cpp
│   └── tinylsm/          # ⚠️ WIP：LSM 存储引擎，尚未接入构建（见路线图）
├── doc/                  # 设计 / 差距分析文档（深度，建议阅读）
│   ├── 1-Raft生产化差距清单-含偶数节点与LSM持久化.md
│   └── 2-快照分块生产化改造清单.md
├── patches/              # 已落地的加固补丁（raft/kv 各 fix，含 ReadIndex 相关）
├── CMakeLists.txt        # 构建定义（raft_test / kv_test / labrpc_selftest）
├── build.sh              # 一键编译 + 跑测试（支持全套构建变体）
├── test_part.sh          # 唯一压测入口（2A~3B/CheckQuorum/ReadIndex/Membership...）
├── tsan.sh               # ThreadSanitizer 一键体检
├── build*/  testdir/     # ⚠️ 本地编译产物（已 gitignore，clone 后不会出现）
├── LICENSE               # MIT
└── README.md
```

> `raft.cpp` 是整个项目的核心实现文件；其余 `.h` / `config` / `persister` / `test_*`
> 是框架与脚手架，正常情况下无需修改。

---

## 快速开始 / Quick Start

### 环境要求 / Requirements

| 依赖 | 版本要求 | 检查 |
|---|---|---|
| C++ 编译器 | 支持 C++17（clang++ / g++） | `clang++ --version` |
| CMake | ≥ 3.14 | `cmake --version` |
| 线程库 | POSIX threads | macOS / Linux 自带 |

Apple Silicon Mac 自带的 `clang++`（Xcode Command Line Tools）完全够用。

### 构建与运行 / Build & Run

`build.sh` 是 `cmake` 的一层薄封装，支持按过滤词跑指定用例、以及全套构建变体：

```bash
cd cpp-6.824
./build.sh                          # 编译 + 跑全部测试（Raft Lab2 + KV Lab3）
./build.sh 2A                       # 只跑 2A（按前缀自动识别：2* → raft_test，3* → kv_test）
./build.sh 2B -count 10             # 2B 跑 10 遍（抓偶发 bug 的必备姿势）
./build.sh --release                # Release 版（-O3 + NDEBUG）
./build.sh --opt                    # 开 LTO 优化（Debug 下附赠 -O2；可与下面任意组合）
./build.sh --tsan                   # ThreadSanitizer 查数据竞争（慢 5~15 倍）
./build.sh --asan                   # AddressSanitizer 查内存越界 / Use-After-Free
./build.sh --ubsan                  # UBSan 查未定义行为（可与 --asan 组合）
./build.sh --release --asan --ubsan # 生产级配置下做内存 + UB 体检
./build.sh --clean                  # 清空全部编译产物（build*/）
```

等价于直接用 CMake（构建变体的 `-D` 开关与脚本一致）：

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
./build/raft_test 2A      # 跑指定用例（过滤词即测试名子串）
./build/kv_test   3B
```

> **sanitizer 提醒**：默认二进制（build/）**抓不到 data race**（C++ 需另编一个目录）。
> 请定期跑 `./tsan.sh` 或 `./build.sh --tsan`（等价于 Go 的 `go test -race`）。
> `build.sh` / `test_part.sh` 每次启动也会打印当前 sanitizer 状态横幅，避免忘了这件事。

---

## 测试 / Testing

### 测试套件构成（实测 118 条）

**Raft（`raft_test`，81 条）**

| 分组 | 数量 | 内容 |
|---|---:|---|
| Ext · 文件持久化 / 磁盘水位 | 5 | FileBackedCrashRecover / CrashMinority / CrashLeader / CrashAfterSnapshot / RaftStateDiskQuota |
| 2A 选举 | 2 | InitialElection / ReElection |
| 2B 日志复制 | 8 | BasicAgree / RPCBytes / FailAgree / FailNoAgree / ConcurrentStarts / Rejoin / Backup / Count |
| 2C 持久化 | 8 | Persist1/2/3 / Figure8 / UnreliableAgree / Figure8Unreliable / ReliableChurn / UnreliableChurn |
| 2D 快照 | 4 | SnapshotTruncatesLog / InstallSnapshotCatchUp / SnapshotRestart / SnapshotStateMachine |
| Ext · 分块快照 | 2 | ChunkedSnapshotTransmit / ChunkedSnapshotLargeStateMachine |
| Ext · CheckQuorum | 7 | 生产向：leader 隔离主动退位、不误杀、分区稳定 |
| Ext · ReadIndex | 14 | 线性一致读 / 防脏读 / 并发合并 / 快照追赶 / 不可靠网 |
| Ext · Membership | 27 | 单飞闸 / Learner 提拔 / Q2 冻结 / 并发变更 fuzz / 移除节点隔离 |
| Ext · C1 / C3 / C5 | 4 | 未提交背压 / 最大消息字节 / 单条 AE 字节上限 / 移除节点静默 |
| **合计** | **81** | |

**KV（`kv_test`，37 条，含 1 个平时跳过的调试入口）**

| 分组 | 数量 | 内容 |
|---|---:|---|
| 3A 基础/重定向/背压 | 10 | Basic / Concurrent / Unreliable / OneKey / OnePartition / ManyPartitions×2 / KVRedirectLeaderId / KVBackpressureBusy / KVDiskFullBurstThrottle |
| 3A Sessions/FollowerRead | 10 | KVSessions(Eviction/SnapshotRoundTrip/Determinism/Fence/ClientProtocol/Tombstone×2/FollowerRead) / ConcurrentFollowerReadUnreliable / FollowerReadAfterSnapshot |
| 3A 持久化专项 | 6 | PersistOneClient / PersistConcurrent / PersistConcurrentUnreliable / PersistPartition / PersistPartitionUnreliable / PersistPartitionUnreliableLinearizable |
| 3B 快照 | 10 | SnapshotRPC / SnapshotSize / MultiChunk×2 / Recover / RecoverManyClients / Unreliable / UnreliableRecover / UnreliableRecoverConcurrentPartition / Linearizable |
| 调试入口 | 1 | kv_mini_lin_dup（设 `KV_MINI_LIN=1` 才跑，平时 skip） |
| **合计** | **37** | |

> 注：大量 KV 用例以 lambda 包 `GenericTest(...)` 注册进 `kTests[]`（如
> `TestSnapshotUnreliable3B` = `GenericTest("3B", 5, true, false, false, 1000)`），
> 并非独立 `void Test*()` 函数——所以光 grep 函数定义会漏数，以二进制启动打印的
> 「已注册 N 个用例」为准。

### 过滤规则

测试二进制用**子串匹配**挑选用例（与某个用例名**完全相等**则精确只跑那一条）：

- 过滤词以 `2` 开头 → 跑 `raft_test`（如 `2A` / `2B` / `ReadIndex` / `Membership`）
- 过滤词以 `3` 开头 → 跑 `kv_test`（如 `3A` / `3B`）
- 不写 → 两个都跑

### 一键测试：`build.sh`

最常用的入口。过滤词决定跑哪个二进制，后面可追加测试二进制自己的参数（如 `-count N`
重复 N 遍抓偶发失败）。

```bash
./build.sh 2B -count 20     # 2B 重复 20 遍
./build.sh 3A               # 跑 KV 3A 全部分组
./build.sh CheckQuorum      # 只跑 Ext: CheckQuorum 7 条
./build.sh Membership       # 只跑 Ext: Membership 27 条
./build.sh FileBacked       # 只跑文件持久化崩溃恢复 4 条（需文件模式，默认即是）
./build.sh DiskQuota        # 只跑磁盘水位保护用例 TestRaftStateDiskQuota
```

### 并发压测：`test_part.sh`

把某一个 part 的**每一条**用例各起一个独立进程并发跑，每条重复 `COUNT` 轮、每轮换随机
种子，日志按 `part-用例名` 分文件落进 `testdir/`。labrpc 是纯内存模拟网络，进程间零共享，
因此可以安全并发。这是抓稀有竞态 / 偶发 flake 的主力武器。

```bash
./test_part.sh 2B                       # 每条 25 轮，并发度 4（TSan 版）
COUNT=50   ./test_part.sh 2B            # 每条压 50 轮
PARALLEL=2 ./test_part.sh 3B            # TSan 吃资源，把并发调小
SAN=none  ./test_part.sh 2B            # 日常版（build/，无 TSan，快 5~15 倍）
SAN=asan  ./test_part.sh 2B            # ASan 版（build-asan/）
SAN=asan,ubsan ./test_part.sh 2B       # ASan + UBSan 组合（build-asan-ubsan/）
RELEASE=1 ./test_part.sh 2B            # Release 版（-O3 + NDEBUG）
RELEASE=1 OPT=1 COUNT=50 SAN=asan,ubsan ./test_part.sh 3B   # 四合一：生产级 + 内存/UB 体检
SEED=12345 ./build-tsan/raft_test TestReadIndex   # 固定种子复现某条
```

支持的环境变量：`PART`（位置参数）、`COUNT`（默认 25）、`PARALLEL`（默认 4）、
`SAN`（`tsan`/`asan`/`ubsan`/`asan,ubsan`/`none`，默认 `none` 即 Debug 版，快 5~15 倍；
`TSAN=1` 或 `SAN=tsan` 开 TSan）、`RELEASE`（0/1）、`OPT`（0/1）、`SEED`（固定随机种子）、
`BIN`（显式指定二进制，sanitizer 配置按目录名反推）。

`test_part.sh` 是唯一压测入口：`./test_part.sh 3B` 即压测 3B（原 test2A.sh ~
test3B.sh 薄封装已并入，直接用 part 参数）。

### ThreadSanitizer 体检：`tsan.sh`

等价于 Go 的 `go test -race`，是唯一能自动抓数据竞争的手段。默认只跑一个子集
（TSan 下慢 5~15 倍，全量可能要几十分钟）。

```bash
./tsan.sh          # 默认 2A
./tsan.sh 2B
./tsan.sh 3A -count 2
```

### 构建变体速查

| 构建 | 命令 | 目录 | 优化 / 工具 | 用途 |
|---|---|---|---|---|
| Debug（默认） | `./build.sh` | `build/` | `-O0 -g`，assert 开 | 日常开发 / 测试基线 |
| Release | `./build.sh --release` | `build-release/` | `-O3 -DNDEBUG` | 性能评估（关 assert） |
| LTO | `./build.sh --opt` | `build/`（附 `-O2+LTO`） | `-O2 + LTO` | 接近 Release 性能，仍带 assert |
| TSan | `./build.sh --tsan` | `build-tsan/` | `-O1 + TSan` | 查数据竞争（慢 5~15×） |
| ASan | `./build.sh --asan` | `build-asan/` | `-O1 + ASan` | 查内存越界 / UAF |
| UBSan | `./build.sh --ubsan` | `build-ubsan/` | `-O1 + UBSan` | 查未定义行为（自动 `halt_on_error`） |
| ASan+UBSan | `./build.sh --asan --ubsan` | `build-asan-ubsan/` | `-O1 + 双` | 内存 + UB 同查 |
| Release+TSan | `./build.sh --release --tsan` | `build-release-tsan/` | `-O1 + TSan`¹ | 生产级配置下竞态体检 |
| Release+ASan+UBSan | `./build.sh --release --asan --ubsan` | `build-release-asan-ubsan/` | `-O1 + 双`¹ | 生产级配置下内存/UB 体检 |

> 说明：`--opt` 在 Debug 下附赠 `-O2`，在 Release 下只额外加 LTO（`-O3` 照旧）。
> 各 sanitizer 独立占目录，避免配置互踩整目录重编。
> ¹ sanitizer 构建的统一优化级由 `SAN_OPT` 决定（默认 `-O1`，加 `--opt` 升 `-O2`），
> 它追加在命令行尾部、会覆盖 Release 的 `-O3`；但 Release 的 `-DNDEBUG`（关 assert）仍然生效。
> 即：开 sanitizer 时有效优化级是 `SAN_OPT`，而非 `-O3`。

### 重要提醒 / Caveats

- **默认二进制抓不到 data race**：必须用 `./tsan.sh` 或带 `--tsan` / `SAN=tsan` 的构建。
- **TSan 与 ASan 互斥**：clang 编译期直接拒绝同开（`build.sh` 与 CMake 都会提前报错）。
- **UBSan 必须 `halt_on_error=1`**：否则 UBSan 默认只打印不中断，测试照样"假绿"。
  脚本已自动设置 `UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1`，手动跑需自行导出。
- **优化会改变指令时序**：`-O2/-O3` 下结果面可能与 Debug 基线不同，回归时请以 Debug
  为"已知绿 / 已知挂"基线，优化构建用于性能评估。
- **sanitizer 慢 5~15 倍**：TSan 下某些实时窗口测试（如 `TestSnapshotUnreliableRecover3B`）
  偶发活性 flake（概率约 1/250），属调度抖动而非正确性 bug；本地排查可对同种子多跑验证。

### 排错三板斧

```bash
./build.sh 2B -count 20        # 1. 偶发失败？重复跑，把概率性问题变成必现
./build.sh --tsan 2B           # 2. 怀疑数据竞争？开 TSAN（慢但一抓一个准）
RAFT_LOG=1 ./build/raft_test 2B # 3. 看不清状态机？开 trace
```

测试失败时 `Config::DumpState()` 会把每个节点的 term / state / commitIndex / logLen
打到 stderr，先看这个。

**统计数字怎么读**（测试输出形如 `Passed -- 3.2 3 121 1088 6`）：

| 列 | 含义 |
|---|---|
| 第 1 列 | 耗时（秒） |
| 第 2 列 | 节点数 |
| 第 3 列 | 总 RPC 次数 |
| 第 4 列 | 总字节数 |
| 第 5 列 | 本次新增已提交命令数 |

`TestCount2B` / `TestRPCBytes2B` 就是查这两列：空闲 1 秒 RPC 增量 > 60 说明心跳过频或空转；
复制 N 条命令字节数远超 `N × 节点数 × 命令大小` 说明同一批日志被重复发送。

---

## 设计要点 / Design Notes

更深入的设计与差距分析见 [`doc/`](./doc)：

- [`doc/1-Raft生产化差距清单-含偶数节点与LSM持久化.md`](./doc/1-Raft生产化差距清单-含偶数节点与LSM持久化.md) — 当前实现 vs etcd/TiKV 的结构性差距与落地路线（偶数节点 majority、LSM 持久化、磁盘水位等专项清单）
- [`doc/2-快照分块生产化改造清单.md`](./doc/2-快照分块生产化改造清单.md) — 分块快照（`kSnapshotChunkSize`）从 lab 形态到生产态的改造清单

几处比原版 lab 更生产化的实现（已在代码中落实）：

- **T2/T3 崩溃窗口双层防护**：快照 install 与 KV map 更新非原子问题，用 `CondInstallSnapshot` 裁决 + 构造期 `ReadSnapshot()` 兜底
- **异步 RPC + cancel 刹车**：`CallAsync` + `cancel_rpcs_`，避免 `Kill` 后卡 7s
- **动态扩容线程池**：全忙即扩，避免阻塞型 handler 饿死心跳
- **exactly-once 去重**：`(client_id, seq_id)` 去重表
- **porcupine 线性化校验**：测试级别已很强

## 许可证 / License

[MIT](./LICENSE) © 2026 吴崇廣 (wuguang657)

## 致谢 / Acknowledgements

- [MIT 6.824: Distributed Systems](https://pdos.csail.mit.edu/6.824/) — 课程框架与测试用例设计源泉
- etcd / TiKV / Consul — 生产级 Raft 工程实践的对照参考

## 推送代码到远程仓库

```bash
git push github master:main # 推送github 
git push origin master # 推送gitee
```
