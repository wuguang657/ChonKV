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
**ReadIndex + Lease read**, and **dynamic cluster membership change** (learner role, Q2
removal freeze). The test suite ships **68 Raft tests + 9 KV tests**, including a
`porcupine`-style linearizability checker and ThreadSanitizer support.

> Persistence and transport layers are still in-memory / simulated — see
> [Roadmap](#路线图--roadmap已知差距) for the productionization gaps.

---

## 中文概述

ChonKV 是一份**从零手写**的 C++ Raft 实现 + 跑在其上的 KV-Raft 服务。起点是
MIT 6.824（2020）的课程框架，但实现已经显著超出原 lab 范围：

- 共识核完整覆盖 **Lab 2A~2D**（领导者选举、日志复制、崩溃恢复持久化、快照压缩）；
- KV 层覆盖 **Lab 3A/3B**（线性一致 KV 服务 + 快照）；
- 额外自研了多项**生产向**特性（见下）；
- 配套 **68 个 Raft 测试 + 9 个 KV 测试**，含 porcupine 线性化校验与 ThreadSanitizer 支持。

> ⚠️ 当前**持久化层（Persister）仍是纯内存**、**传输层（labrpc）仍是软件模拟**，
> 真实落盘 / 真实网络尚未接入——详见下方路线图。

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

**工程质量**

- 异步 RPC + cancel 刹车（Kill 后不空等）
- 动态扩容线程池（避免阻塞型 handler 饿死心跳）
- T2/T3 崩溃窗口双层防护（快照 install 与 KV map 更新非原子问题）
- `std::chrono::steady_clock` 单调时钟（租约不受墙钟 NTP 回跳影响）

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
│   ├── 1-生产级差距与路线图.md
│   └── 2-Raft生产化差距清单-含偶数节点与LSM持久化.md
├── CMakeLists.txt        # 构建定义（raft_test / kv_test / labrpc_selftest）
├── build.sh              # 一键编译 + 跑测试
├── tsan.sh / test2A.sh / test3B.sh / ...   # 便捷脚本
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

```bash
cd cpp-6.824
./build.sh                # 编译 + 跑全部测试（Raft Lab2 + KV Lab3）
./build.sh 2A             # 只跑 2A（按前缀自动识别：2* → raft_test，3* → kv_test）
./build.sh 2B -count 10   # 2B 跑 10 遍（抓偶发 bug 的必备姿势）
./build.sh --tsan         # 开 ThreadSanitizer 查数据竞争（慢 5~15 倍）
./build.sh --clean        # 清空编译产物
```

等价于直接用 CMake：

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
./build/raft_test 2A      # 跑指定用例
./build/kv_test 3B
```

> **sanitizer 提醒**：默认二进制**抓不到 data race**（C++ 需另编一个目录）。
> 请定期跑 `./tsan.sh`（等价于 Go 的 `go test -race`）。`build.sh` 每次启动也会打印
> 当前 sanitizer 状态横幅，避免忘了这件事。

---

## 测试 / Testing

测试二进制用**子串匹配**挑选用例：

- 过滤词以 `2` 开头 → 跑 `raft_test`（如 `2A` / `2B` / `ReadIndex`）
- 过滤词以 `3` 开头 → 跑 `kv_test`（如 `3A` / `3B`）
- 不写 → 两个都跑

**规模（实测）**

- Raft：`68` 个测试用例，覆盖 2A~2D 及成员变更 / Learner / ReadIndex / CheckQuorum / Pre-Vote 加固
- KV：`9` 个测试用例（7×3A + 2×3B），含 porcupine 线性化校验

**排错三板斧**

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

- [`doc/1-生产级差距与路线图.md`](./doc/1-生产级差距与路线图.md) — 当前实现 vs etcd/TiKV 的结构性差距与落地路线
- [`doc/2-Raft生产化差距清单-含偶数节点与LSM持久化.md`](./doc/2-Raft生产化差距清单-含偶数节点与LSM持久化.md) — 偶数节点 majority、LSM 持久化、快照分块、流水线、磁盘水位等专项清单

几处比原版 lab 更生产化的实现（已在代码中落实）：

- **T2/T3 崩溃窗口双层防护**：快照 install 与 KV map 更新非原子问题，用 `CondInstallSnapshot` 裁决 + 构造期 `ReadSnapshot()` 兜底
- **异步 RPC + cancel 刹车**：`CallAsync` + `cancel_rpcs_`，避免 `Kill` 后卡 7s
- **动态扩容线程池**：全忙即扩，避免阻塞型 handler 饿死心跳
- **exactly-once 去重**：`(client_id, seq_id)` 去重表
- **porcupine 线性化校验**：测试级别已很强

---

## 路线图 / Roadmap（已知差距）

> 以下均为**尚未实现**的生产化项，按优先级大致排序。所有"缺失"结论来自实际读码，非推测。

**🔴 高优先级**

1. **真实持久化** — `Persister` 当前纯内存，无 WAL / `fsync` / CRC；需抽 `LogStore` 接口 → `SegmentLogStore` 落盘
2. **真实传输层** — `labrpc` 是软件模拟，无消息大小上限 / 流控背压 / TLS

**🟡 中优先级**

3. **复制流水线** — 当前为停等（`inflight_log_` 是 bool），改为滑动窗口可显著提升跨机房吞吐
4. **快照分块** — 当前整块塞进单条 RPC（`InstallSnapshotArgs::data`），大状态机需按 `offset` 流式分块
5. **磁盘水位保护** — 日志只增不减，需设警戒线触发快照排水或拒绝写入，否则磁盘写满崩溃

**🟢 进行中 / 待接入**

6. **tinylsm 接入** — `src/tinylsm/` 目录已存在，但**尚未编入 CMake、未被 kvraft 引用**，是独立 WIP 模块
7. **其余生产项** — Leader Transfer、优雅关闭、group commit、客户端 session TTL/LRU、metrics 可观测性、锁粒度细化

---

## 许可证 / License

[MIT](./LICENSE) © 2026 吴崇廣 (wuguang657)

## 致谢 / Acknowledgements

- [MIT 6.824: Distributed Systems](https://pdos.csail.mit.edu/6.824/) — 课程框架与测试用例设计源泉
- etcd / TiKV / Consul — 生产级 Raft 工程实践的对照参考

## 如何提交
git push github master:main     # 推送github
git push origin master         # 推送gitee
