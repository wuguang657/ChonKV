# ChonKV — C++ Raft 共识核 + KV-Raft 服务

> 基于 MIT 6.824（2020）从零手写的 C++ Raft 实现，已显著超出原 lab 范围。
> A from-scratch C++ Raft implementation on MIT 6.824, extended with production features.

通过 Lab 2（选举/复制/持久化/快照）与 Lab 3（线性一致 KV + 快照）全套用例，并自研 Pre-Vote、CheckQuorum、ReadIndex+Lease、动态成员变更、分块快照流式传输、日志/磁盘水位保护等生产向特性。测试套件共 **121 条**（Raft 84 + KV 37），含 porcupine 线性化校验、TSan/ASan/UBSan 构建矩阵、进程级并发压测脚本。

> **持久化**：Raft 层支持**内存 / 文件双模式**（文件模式默认，崩溃安全写 `tmp→fsync→rename→fsync(dir)`）。详见[持久化模式开关](#持久化模式开关)。
> **传输层**仍为 labrpc 软件模拟，未接真实网络；KV 测试脚手架当前固定内存模式。

## 快速开始

| 依赖 | 要求 |
|---|---|
| C++ 编译器 | 支持 C++17（clang++ / g++） |
| CMake | ≥ 3.14 |
| 线程库 | POSIX threads（macOS / Linux 自带） |

```bash
./build.sh                 # 编译 + 跑全部测试
./build.sh 2A              # 只跑 2A（2*→raft_test，3*→kv_test）
./build.sh 2B -count 10    # 重复 10 遍抓偶发
./build.sh --tsan          # ThreadSanitizer 查数据竞争
./build.sh --release --asan --ubsan   # 生产级 + 内存/UB 体检
```
等价于 `cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug && cmake --build build -j`，再 `./build/raft_test 2A` / `./build/kv_test 3B`。

## 特性

**共识核心（Lab 2 全过）**：选举 / 日志复制与提交 / 崩溃恢复持久化 / 日志压缩与安装快照。
**KV 服务（Lab 3 全过）**：线性一致读写 / exactly-once 去重 / 快照重放 / porcupine 校验。

**生产向扩展（自研）**

| 特性 | 说明 |
|---|---|
| Pre-Vote | 分区恢复时避免无意义选举打断现 leader |
| CheckQuorum | leader 被隔离后主动退位，避免双主脏写 |
| ReadIndex + Lease Read | 线性一致读；Lease 快路径默认关（`kEnableLeaseRead`） |
| 动态成员变更 | 单飞闸 + 最后 voter 阀门，变更期不丢 Majority |
| Learner 三态 | `kVoter / kLearner / kRemoved`，Learner 不计票、可提拔 |
| Q2 移除冻结 | 被移除节点日志冻结在移除配置下标，不泄漏后续条目 |
| 文件持久化（双模式） | 内存/文件两态，文件模式崩溃安全落盘 `raft_state.bin`+`snapshot.bin`，默认开 |
| 日志 / 磁盘水位保护 | `Start()` 前判 `RaftStateSize ≥ maxraftstate` 返回 `disk_full`，压缩后重试 |
| 快照节流 + 指数退避 | 强制快照 CAS 限流（50ms 窗口至多一次真落盘）+ `WaitOp` 退避（2→50ms） |
| 分块快照流式传输 | `InstallSnapshot` 按 `kSnapshotChunkSize` 分块，避免大 blob 顶穿内存 |

**工程质量**：异步 RPC + cancel 刹车、动态扩容线程池、T2/T3 崩溃窗口双层防护、`steady_clock` 单调租约、多档构建矩阵（Debug/Release/LTO + TSan/ASan/UBSan）、进程级并发压测 harness。

## 目录结构

```
cpp-6.824/
├── src/
│   ├── common/      # 工具：随机/时间/channel/线程跟踪
│   ├── labrpc/      # RPC 框架 + 可插拔传输（inproc 模拟 / socket 真网络）
│   ├── raft/        # ★ 共识核 raft.h/cpp/config/test_raft.cpp
│   ├── persist/     # 持久化域：log_store.h + log/（段文件·内存）+ state/（RaftState）
│   ├── kvraft/      # KV-Raft 服务（复用 raft.cpp）+ porcupine.cpp
│   └── tinylsm/     # ⚠️ WIP：LSM 引擎，未接入构建
├── doc/             # 设计/差距分析（1-Raft生产化差距清单, 2-快照分块改造清单）
├── patches/         # 已落地加固补丁
├── CMakeLists.txt / build.sh / test_part.sh / tsan.sh
└── README.md
```

## 测试

**共 117 条**：Raft 80（2A 2 / 2B 8 / 2C 8 / 文件持久化 4 / 磁盘水位·背压 5 / 分块快照 2 / 快照 CRC 1 / 段日志存储 2 / CheckQuorum 7 / ReadIndex 14 / Membership 27）+ KV 37（3A 26 / 3B 10 / 调试入口 1）。

两种跑法：

- `./build.sh <过滤词>`：**单进程跑一遍**，按子串匹配用例名（如 `./build.sh 2B`、`./build.sh CheckQuorum`）。Ext 组用全名做过滤词，否则如 `TestReadIndex` 会误伤其余 9 个同名前缀用例
- `./test_part.sh <组名>`：**并发压测入口**——组内每条用例各起独立测试进程（某轮 abort 不连坐其余轮次），默认各重复 25 轮、每轮换随机种子（失败可扣种子固定复现），按组最多 4 条同时跑；日志落在 `testdir/<组>-<用例>.log`，结束后自动汇总通过率

| 组名 | 二进制 | 用例数 | 备注 |
|---|---|---|---|
| 2A | raft_test | 2 | |
| 2B | raft_test | 8 | |
| 2C | raft_test | 12 | 含 4 条文件持久化崩溃恢复（`TestFileBacked*`） |
| 3A | kv_test | 26 | |
| 3B | kv_test | 4† | †当前处于调试态：10 条中只启用 4 条（其余 6 条在脚本中注释） |
| ChunkedSnapshotTransmit | raft_test | 2 | 分块快照 |
| SegmentLog | raft_test | 2 | 段文件日志存储 + fail-stop 单测 |
| CheckQuorum | raft_test | 7 | |
| ReadIndex | raft_test | 14 | |
| Membership | raft_test | 27 | 成员变更 / Learner 三态 / 27 条全名单 |
| MaxMessageSize | raft_test | 5 | 消息字节上限、AE 单 RPC 字节封顶、未提交日志背压、磁盘水位 |

```bash
./test_part.sh 2B                        # 默认 SAN=none（build/，最快），每条 25 轮，并发 4
COUNT=100 PARALLEL=2 ./test_part.sh 2C   # 自定义重复轮数与并发度
SAN=tsan ./test_part.sh 2B               # TSan 版（build-tsan/，慢 5~15 倍，唯一自动抓 data race 的手段）
SAN=asan,ubsan ./test_part.sh 3A         # ASan+UBSan 组合（build-asan-ubsan/；UBSan 自动 halt_on_error）
RELEASE=1 OPT=1 ./test_part.sh 2A        # Release(-O3+NDEBUG) + LTO（build-release/）
BIN=./build-tsan/raft_test ./test_part.sh Membership  # 跳过增量编译，直接用指定已编好的二进制
SEED=12345 ./build/raft_test TestReadIndex            # 拿日志里的种子固定复现（Ext 组务必用全名）
```

环境变量：

| 变量 | 取值 | 说明 |
|---|---|---|
| `SAN` | `tsan`（旧 `TSAN=1` 兼容）/ `asan` / `ubsan` / `asan,ubsan` / `none`（默认） | 生成 `build[-release]-<san>/` 区分不同体检组合；TSan 与 ASan 不可同开 |
| `COUNT` / `PARALLEL` | 整数 | 每条用例重复轮数（默认 25）/ 组内进程级并发度（默认 4；TSan 建议 2~4） |
| `RELEASE` / `OPT` | `0`/`1` | Release -O3+NDEBUG / 追加 -O2+LTO（Release 下只加 LTO） |
| `BIN` | 路径 | 显指定二进制时按其目录名反推 sanitizer 配置，跳过编译 |
| `LABRPC_TRANSPORT` / `LABRPC_PERSIST` | `inproc`/`memory`（默认） | 可插拔后端开关，脚本透传给被测进程；非默认后端会争抢端口/目录，自动 `PARALLEL=1` |

## 设计要点

- T2/T3 崩溃窗口双层防护：snapshot ApplyMsg 的 `index >= last_cmd_index_` 不变式（违例 fail-stop）+ 构造期 `ReadSnapshot()` 兜底
- 异步 RPC + cancel 刹车（`CallAsync`+`cancel_rpcs_`），避免 `Kill` 后空等
- 动态扩容线程池，避免阻塞型 handler 饿死心跳
- exactly-once 去重 `(client_id, seq_id)`；porcupine 线性化校验已强

## 许可证 / License

[MIT](./LICENSE) © 2026 吴崇廣 (wuguang657)

## 致谢 / Acknowledgements

- [MIT 6.824: Distributed Systems](https://pdos.csail.mit.edu/6.824/) — 课程框架与测试用例设计源泉
- etcd / TiKV / Consul — 生产级 Raft 工程实践对照参考

## 推送代码至远程仓库
```bash
git push github master:main # 推送github 
git push origin master # 推送gitee
```
