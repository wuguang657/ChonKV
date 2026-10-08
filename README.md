# ChonKV — C++ Raft 共识核 + KV-Raft 服务

> 基于 MIT 6.824（2020）从零手写的 C++ Raft 实现，已显著超出原 lab 范围。
> A from-scratch C++ Raft implementation on MIT 6.824, extended with production features.

通过 Lab 2（选举/复制/持久化/快照）与 Lab 3（线性一致 KV + 快照）全套用例，并自研 Pre-Vote、CheckQuorum、ReadIndex+Lease、动态成员变更、分块快照流式传输、日志/磁盘水位保护等生产向特性。测试套件共 **118 条**（Raft 81 + KV 37），含 porcupine 线性化校验、TSan/ASan/UBSan 构建矩阵、进程级并发压测脚本。

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

## 持久化模式开关（内存 / 文件）

开关只有一个参数 `file_backed`：

| 模式 | 落盘 | 怎么开 |
|---|---|---|
| 文件模式（**默认**） | ✅ `raft_state.bin` + `snapshot.bin` | `file_backed = true` |
| 内存模式 | ❌ | `file_backed = false` |

```cpp
// src/raft/config.h:58
Config(int n, bool unreliable, bool file_backed = true);  // 改这行默认值即全局切换
// 单用例覆盖：
auto cfg = std::make_shared<Config>(servers, false, /*file_backed=*/false);
```

数据写在 `/tmp/cpp6raft_<pid>_<seq>/node_N/{raft_state.bin, snapshot.bin}`，`Cleanup()` 整树删除；崩溃重启用例 `Copy()` 出目录副本防残留线程改数据。

**落盘失败直接 `std::abort()`（fail-stop）**——持久化成功前不可提交。⚠️ KV 测试脚手架（`kvraft::Config`）暂无 `file_backed` 参数，`StartServer()` 固定内存模式；想让 KV 也走文件需给 `kvraft::Config` 加参数并透传（约 3 处）。

## 目录结构

```
cpp-6.824/
├── src/
│   ├── common/      # 工具：随机/时间/channel/线程跟踪
│   ├── labrpc/      # 网络仿真（丢包/延迟/乱序/断连）
│   ├── raft/        # ★ 共识核 raft.h/cpp/config/persister/test_raft.cpp
│   ├── kvraft/      # KV-Raft 服务（复用 raft.cpp）+ porcupine.cpp
│   └── tinylsm/     # ⚠️ WIP：LSM 引擎，未接入构建
├── doc/             # 设计/差距分析（1-Raft生产化差距清单, 2-快照分块改造清单）
├── patches/         # 已落地加固补丁
├── CMakeLists.txt / build.sh / test_part.sh / tsan.sh
└── README.md
```

## 测试

**共 118 条**：Raft 81（2A 2 / 2B 8 / 2C 8 / 2D 4 / 分块快照 2 / CheckQuorum 7 / ReadIndex 14 / Membership 27 / 文件持久化+磁盘水位 5）+ KV 37（3A 26 / 3B 10 / 调试入口 1）。过滤词 `2*`→raft_test，`3*`→kv_test；不写则都跑。

```bash
./build.sh CheckQuorum    # 只跑某组（按子串匹配用例名）
./test_part.sh 2B         # 每条并发 25 轮抓稀有竞态（SAN=tsan/asan/ubsan 等环境变量）
./tsan.sh 2B              # TSan 体检（慢 5~15 倍，唯一能自动抓 data race 的手段）
```

> ⚠️ 默认 `build/` 二进制**抓不到 data race**，务必定期 `--tsan` / `tsan.sh`。TSan 下实时窗口测试偶发活性 flake（≈1/250），属调度抖动非正确性 bug。

## 设计要点

- T2/T3 崩溃窗口双层防护：`CondInstallSnapshot` 裁决 + 构造期 `ReadSnapshot()` 兜底
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
