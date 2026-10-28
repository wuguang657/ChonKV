# bench —— 性能压测工具集

两个独立目标，**不参与** `build.sh` / `test_part.sh` 的任何测试编排：

| 目标 | 测什么 | 场景 |
|---|---|---|
| `raft_bench` | 共识层：饱和吞吐 + raft 内部延迟/锁/批量/复制滞后 | **开环**（提案不等 apply，压极限） |
| `kv_bench` | KV 栈：客户端可感延迟 + 吞吐 | **闭环**（每条命令等 raft 提交才返回，天然背压） |

为什么分两个：开环吞吐回答"raft 能提交多快"，闭环延迟回答"用户感受到多快"。
两者之差就是排队/节流/客户端侧开销 —— 只测一个都会得出片面结论。

## 构建

```bash
# 默认构建（bench 可跑，但无 raft 内部埋点 —— 测试路径零开销铁律）
cmake -S . -B build && cmake --build build -j

# 埋点构建（raft 内部指标；另编一个目录，勿污染测试基线）
cmake -S . -B build-bench -DRAFT_BENCH_METRICS=ON && cmake --build build-bench -j
```

`RAFT_BENCH_METRICS` 未定义时 `src/common/metrics.h` 是空头，被埋点的代码路径
同样展开为空，默认二进制一条指令都不多（nm 验证：0 个埋点符号）。

## 用法

```bash
SEED=7 ./build-bench/raft_bench --clients 5 --warmup 10 --duration 30 \
    [--nodes 3] [--payload 64] [--unreliable] [--memory] \
    [--label xxx] [--out bench/results/baseline.csv]

SEED=7 ./build-bench/kv_bench --clients 5 --warmup 10 --duration 30 \
    [--servers 3] [--keys 100] [--payload 64] [--writepct 100] [--unreliable] \
    [--label xxx] [--out bench/results/baseline.csv]
```

- `SEED` 固定 labrpc 全局随机序列，**同 SEED 完全可复现**；对比任何修改前后
  必须同 SEED（SEED=0 无效，util.h 静默回落 1）。
- `--warmup + --duration ≤ 90s`：Config::End() 内置 120s 看门狗，超了会误杀。
- 持久化：`raft_bench` 默认 file 模式（段日志 O(1) append，与测试默认一致），
  `--memory` 切内存 persister；`kv_bench` 由 `LABRPC_PERSIST` 环境变量决定
  （默认 memory，与 kv_test 一致；`LABRPC_PERSIST=file` 切文件模式）。
- 传输：默认 in-proc（客户端与节点共享进程 CPU，>20 并发建议
  `LABRPC_TRANSPORT=socket` 跨进程跑）。
- 每轮结束自动调 `Config::End()`：强制执行 apply 顺序 + 跨副本一致性检查
  （raft）/ 副本状态机全量比对（kv）—— **bench 同时是一轮安全长跑**，
  断言失败以 exit 1 + stderr 报告，绝不静默。

## CSV 格式（长表，追加写）

```
ts,label,kind,name,n,p50,p99,p999,max
```

`name` 后缀即单位：`_ns`（纳秒）/ `_entries`（条目数）/ `_count`（个数）。
主要指标：

| name | 含义 |
|---|---|
| `propose_to_apply_ns` | Start() 追加 → ApplyLoop 收集到该 index（raft 侧端到端延迟） |
| `mu_wait_start_ns` | Start() 等 mu_ 时长（纯锁竞争） |
| `start_critical_ns` | Start() 临界区墙钟（file 模式含等落盘的解锁等待） |
| `apply_collect_critical_ns` | ApplyLoop 批量收集段墙钟 |
| `persist_duration_ns` | PersistLocked 调用时长 |
| `replica_lag_entries` | commit_index_ 与最慢 voter match_index 之差 |
| `uncommitted_backlog_entries` | 未提交积压（客户端排队深度） |
| `apply_batch_entries` | ApplyLoop 每批条数（验 128 封顶） |
| `worker_busy/total_count` | in-proc 工作池水位（贴 512 = 池子将满） |

客户端可感延迟（kv_bench）不走埋点，是全量样本精确百分位，打在 stdout。

## 第一条基线（2026-10-26，本机 Apple Silicon Air，Debug + 埋点，in-proc，3 节点，可靠网，payload 64B）

| 场景 | 吞吐 | 关键内部指标 |
|---|---|---|
| raft file c1 | 48,789 ops/s | apply_batch p50=128（贴顶），backlog p50=448 |
| raft file c5 | 49,682 ops/s | 与 c1 持平（±2%） |
| raft file c20 | 49,770 ops/s | backlog p50=8192，replica_lag p99=22.5k |
| raft file c5 SEED=29 | 52,028 ops/s | 重复性 ±2.3% |
| **raft memory c5** | **341 ops/s** | 换主 54 次/40s，start_critical p50=459µs |
| kv c1 | 86 ops/s | 写延迟 p50=11.70ms p999=32.81ms |
| kv c5 | 555 ops/s | 写延迟 p50=10.20ms p99=13.33ms |

### 基线暴露的三个结论

1. **共识层饱和吞吐 ~50k ops/s 且与客户端并发无关**（c1→c20 平台化）：
   瓶颈在 leader 单点提交路径（AdvanceCommit + 单线程 ReplicateLoop 群发），
   不在客户端。要再上行得动 leader 复制路径本身，加客户端无效。
2. **KV 闭环写延迟地板 ~10-11ms，且分布极紧**：这是
   `kMinSendIntervalMs=10ms`（raft.h:98）的 per-follower 发送节流 ——
   每条提案平均要等半个发送周期才被复制出去。开环 bench 完全看不到这层
   （不等 apply），只有闭环能暴露。这是当前最大的单点优化空间。
3. **内存 persister 的 PersistLocked 是全量编码 O(日志条数)**（日志编在
   state blob 里）：开环饱和下 mu_ 被提案线程占满 → 心跳饿死 →
   CheckQuorum 连续换主（54 次/40s）→ 吞吐坍到 file 模式的 1/145。
   测试负载低察觉不到，高压下现形 —— 生产部署必须 file 模式
   （段日志 O(1) append + group commit）。

### 读数的坑

- in-proc 后端客户端与 3 个 raft 节点**共享进程 CPU**：>20 并发的数字含
  "客户端自压"伪影，要高并发结论切 `LABRPC_TRANSPORT=socket`。
- Debug 与 `-O2+LTO`（`ENABLE_OPT=ON`）指令时序不同，两套数字永远分开比。
- 开环模式不重试：未提交提案遇换主会被覆盖丢失（Raft 语义，客户端应重试），
  吞吐以 apply 数为准；`leader_changes` 与 `ops_rejected_*` 要一起看。
- 带 sanitizer 的二进制不测性能（慢 5~15 倍）。
- 换主/快照会清掉在途提案的时间戳 → `propose_to_apply` 的 n 小于
  `ops_proposed`；差额另有 `apply_entries_without_propose_ts` 计数
  （多节点集群里 ≈ (n-1)×提案数，属正常，follower 从不提案）。

## 复现

```bash
cmake -S . -B build-bench -DRAFT_BENCH_METRICS=ON && cmake --build build-bench -j
mkdir -p bench/results
SEED=7 ./build-bench/raft_bench --clients 5 --warmup 10 --duration 30 --out bench/results/baseline.csv
SEED=7 ./build-bench/kv_bench  --clients 5 --warmup 10 --duration 30 --out bench/results/baseline.csv
```
