// raft_bench.cpp —— Raft 共识层性能压测（独立目标，不掺和测试编排）
//
// 【测什么】
//   * 饱和吞吐：clients 个提案线程【开环】狂发 Start()（不等 apply），
//     observer 线程每 200ms 采样全集群 apply 进度 → ops/s
//   * raft 侧端到端延迟：propose→apply（内部埋点，RAFT_BENCH_METRICS=ON 时）
//   * 锁竞争（mu_ 等待/持锁）/ apply 批量 / 复制滞后 / worker 水位（同上）
//   * 安全性白捡：cfg.End() 会强制执行 apply 顺序 + 跨副本一致性检查
//     （raft/config.cpp:555-577）—— 每一轮 bench 同时是一轮长跑安全测试
//
// 【不测什么】
//   * 客户端可感延迟（含 RPC 往返）→ kv_bench 的闭环场景更真实
//
// 【用法】
//   ./build/raft_bench [--nodes 3] [--clients 5] [--payload 64]
//                      [--warmup 10] [--duration 20] [--unreliable]
//                      [--label xxx] [--out bench/results.csv]
//   SEED=7 ./build/raft_bench ...   # SEED 固定随机序列，同 SEED 完全可复现
//   （SEED=0 无效，util.h 会静默回落 1）
//
// 【读数的三个坑】
//   1. in-proc 后端下客户端与 3 个 raft 节点【共享进程 CPU】—— clients 档位
//      超过 ~20 后 bench 自己先饱和，数字含"客户端自压"伪影；要测高并发切
//      LABRPC_TRANSPORT=socket（跨进程，CPU 隔离）。
//   2. 默认 file 模式（段日志 O(1) append，与测试默认配置一致）；
//      --memory 切内存 persister，其 PersistLocked 是全量编码
//      （O(日志条数)），饱和下会拖垮心跳路径 —— 两种模式的数字永远分开比。
//   3. Debug 与 -O2+LTO 的指令时序不同，数字不可混比。

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "../raft/config.h"
#include "../raft/raft.h"
#include "../common/metrics.h"

namespace {

#ifdef RAFT_BENCH_METRICS
// CSV 行时间戳（仅埋点构建的 CSV 输出需要）
std::string IsoNow() {
  const std::time_t t = std::time(nullptr);
  std::tm tm_buf{};
  localtime_r(&t, &tm_buf);
  char buf[32];
  std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", &tm_buf);
  return buf;
}
#endif

struct Args {
  int nodes = 3;
  int clients = 5;
  int payload = 64;
  int warmup_sec = 10;
  int duration_sec = 20;
  bool unreliable = false;
  bool memory = false;  // true = 内存 persister（默认 file：段日志 O(1) append，
                        // 与测试默认配置一致，也是代表性基线）
  std::string label;
  std::string out;
};

Args ParseArgs(int argc, char** argv) {
  Args a;
  for (int i = 1; i < argc; ++i) {
    const std::string k = argv[i];
    auto next = [&]() -> const char* {
      if (i + 1 >= argc) {
        std::fprintf(stderr, "参数 %s 缺值\n", k.c_str());
        std::exit(2);
      }
      return argv[++i];
    };
    if (k == "--nodes") {
      a.nodes = std::atoi(next());
    } else if (k == "--clients") {
      a.clients = std::atoi(next());
    } else if (k == "--payload") {
      a.payload = std::atoi(next());
    } else if (k == "--warmup") {
      a.warmup_sec = std::atoi(next());
    } else if (k == "--duration") {
      a.duration_sec = std::atoi(next());
    } else if (k == "--unreliable") {
      a.unreliable = true;
    } else if (k == "--memory") {
      a.memory = true;
    } else if (k == "--label") {
      a.label = next();
    } else if (k == "--out") {
      a.out = next();
    } else {
      std::fprintf(stderr, "未知参数: %s\n", k.c_str());
      std::exit(2);
    }
  }
  return a;
}

}  // namespace

int main(int argc, char** argv) {
  const Args a = ParseArgs(argc, argv);
  // Config::End() 内置 120s 看门狗（raft/config.cpp:78，自【构造】时刻起算，
  // 不是 Begin 时刻）。bench 总时长 = 等 leader + warmup + duration + 收尾，
  // 压到 90s 内给看门狗留余量，否则 End() 会把 bench 误杀成"超时失败"。
  if (a.warmup_sec + a.duration_sec > 90) {
    std::fprintf(stderr,
                 "拒绝执行: warmup(%d)+duration(%d) > 90s —— Config::End 的 "
                 "120s 看门狗会误杀 bench（raft/config.cpp:78）\n",
                 a.warmup_sec, a.duration_sec);
    return 2;
  }
  const char* seed_env = std::getenv("SEED");
  const std::string seed = (seed_env != nullptr && *seed_env != '\0')
                               ? std::string(seed_env)
                           : std::string("1");
  char label[256];
  if (!a.label.empty()) {
    std::snprintf(label, sizeof(label), "%s", a.label.c_str());
  } else {
    std::snprintf(label, sizeof(label), "raft_n%d_c%d_p%d_%s_%s_seed%s", a.nodes,
                  a.clients, a.payload, a.unreliable ? "unrel" : "rel",
                  a.memory ? "mem" : "file", seed.c_str());
  }
  std::printf("=== raft_bench [%s] nodes=%d clients=%d payload=%dB warmup=%ds "
              "measure=%ds net=%s persist=%s ===\n",
              label, a.nodes, a.clients, a.payload, a.warmup_sec,
              a.duration_sec, a.unreliable ? "unreliable" : "reliable",
              a.memory ? "memory" : "file");
  std::fflush(stdout);

  // file 模式（默认）：段日志 O(1) append + state blob 只写元数据，与测试
  // 默认配置一致。--memory 切内存 persister：日志编在 state blob 里，
  // 每次 PersistLocked 是全量编码（O(日志条数)）—— 开环饱和下会看到
  // mu_ 被提案线程占满、心跳饿死、CheckQuorum 连续换主，那是该模式的
  // 真实特性而非 bug，两种模式的数字永远分开比。
  raft::Config cfg(a.nodes, a.unreliable, /*file_backed=*/!a.memory);
  cfg.Begin("raft_bench");
#ifdef RAFT_BENCH_METRICS
  auto& M = raftcpp::Metrics::Instance();
#endif

  // ---- 等首个 leader（最多 10s；同时是活性 smoke）----
  std::shared_ptr<raft::Raft> leader;
  for (int i = 0; i < 200 && !leader; ++i) {
    for (int s = 0; s < a.nodes && !leader; ++s) {
      auto rf = cfg.GetRaft(s);
      if (rf && rf->GetState().second) leader = rf;
    }
    if (!leader) std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  if (!leader) {
    std::fprintf(stderr, "10s 内没选出 leader —— 集群活性有问题，bench 终止\n");
    cfg.Cleanup();
    return 1;
  }

  std::atomic<bool> stop{false};
  std::atomic<int64_t> proposed{0}, not_leader{0}, backpressure{0};

  // observer：每 200ms 采样全集群 apply 进度（取 max ≈ leader 的 commit 点）
  // 与 RPC 总数。吞吐 = apply 进度差 / 时间差，与提案线程计数相互印证。
  struct Sample {
    double t;
    int64_t applied;
    int rpcs;
  };
  std::mutex samples_mu;
  std::vector<Sample> samples;
  std::thread observer([&] {
    const auto t0 = std::chrono::steady_clock::now();
    while (!stop.load(std::memory_order_relaxed)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
      int64_t applied = 0;
      for (int s = 0; s < a.nodes; ++s) {
        auto rf = cfg.GetRaft(s);
        if (rf) {
          const int64_t la = rf->LastApplied();
          if (la > applied) applied = la;
        }
      }
      Sample sm;
      sm.t =
          std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
              .count();
      sm.applied = applied;
      sm.rpcs = cfg.RpcTotal();
      std::lock_guard<std::mutex> lk(samples_mu);
      samples.push_back(sm);
    }
  });

  // ---- 提案线程：开环饱和 ----
  // 不等 apply：测的是"raft 能提交多快"。in-flight 上限由 C1 背压
  // （kMaxUncommittedEntriesDefault=10000）兜底，被拒会计进 backpressure。
  const raft::Command payload(static_cast<size_t>(a.payload), 'x');
  std::vector<std::thread> proposers;
  for (int c = 0; c < a.clients; ++c) {
    proposers.emplace_back([&] {
      std::shared_ptr<raft::Raft> rf = leader;  // 线程本地缓存，无共享写
      while (!stop.load(std::memory_order_relaxed)) {
        const raft::StartResult r = rf->Start(payload);
        if (r.is_leader) {
          proposed.fetch_add(1, std::memory_order_relaxed);
          continue;
        }
        if (r.backpressure) {
          backpressure.fetch_add(1, std::memory_order_relaxed);
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
          continue;
        }
        not_leader.fetch_add(1, std::memory_order_relaxed);
        // 掉线：退避后重新找 leader（稳定集群里这是罕见路径）
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        for (int s = 0; s < a.nodes; ++s) {
          auto cand = cfg.GetRaft(s);
          if (cand && cand->GetState().second) {
            rf = cand;
            break;
          }
        }
      }
    });
  }

  // ---- warmup → 测量段 ----
  // 不清空 samples（observer 并发写，清了会丢基线）：记下"此刻"作为测量
  // 起点，统计时只取 t >= t_mark 的样本，无竞态且基线精确。
  std::this_thread::sleep_for(std::chrono::seconds(a.warmup_sec));
  double t_mark = 0.0;
  {
    std::lock_guard<std::mutex> lk(samples_mu);
    if (!samples.empty()) t_mark = samples.back().t;
  }
  proposed.store(0, std::memory_order_relaxed);
  not_leader.store(0, std::memory_order_relaxed);
  backpressure.store(0, std::memory_order_relaxed);

  std::this_thread::sleep_for(std::chrono::seconds(a.duration_sec));
  stop.store(true);
  observer.join();
  for (auto& t : proposers) t.join();

  // ---- 统计 ----
  double ops_s = 0, rpc_s = 0, rpc_per_op = 0, el = 0;
  int64_t ops = 0;
  {
    std::lock_guard<std::mutex> lk(samples_mu);
    const Sample* first = nullptr;
    const Sample* last = nullptr;
    for (const auto& s : samples) {
      if (s.t < t_mark) continue;
      if (first == nullptr) first = &s;
      last = &s;
    }
    if (first != nullptr && last != nullptr && first != last) {
      ops = last->applied - first->applied;
      el = last->t - first->t;
      const int64_t rpcs = last->rpcs - first->rpcs;
      if (el > 0) {
        ops_s = static_cast<double>(ops) / el;
        rpc_s = static_cast<double>(rpcs) / el;
        rpc_per_op =
            ops > 0 ? static_cast<double>(rpcs) / static_cast<double>(ops) : 0;
      }
    }
  }
  std::printf("---- raft_bench 结果 [%s] ----\n", label);
  std::printf("  窗口 %.1fs：提交 %lld 条 → 吞吐 %.0f ops/s"
              "（节点间 RPC %.0f/s，%.2f RPC/op）\n",
              el, static_cast<long long>(ops), ops_s, rpc_s, rpc_per_op);
  std::printf("  提案线程：成功 %lld / 非leader拒绝 %lld / 背压拒绝 %lld\n",
              static_cast<long long>(proposed.load()),
              static_cast<long long>(not_leader.load()),
              static_cast<long long>(backpressure.load()));
  std::printf("  注：开环模式不重试 —— 未提交提案遇换主会被覆盖丢失"
              "（Raft 语义：客户端应重试），吞吐以 apply 数为准\n");

  // ---- 收尾：End() 顺带强制执行 apply 顺序 + 跨副本一致性（安全断言）----
  int rc = 0;
  try {
    cfg.End();
  } catch (const raft::TestFailure& e) {
    std::fprintf(stderr, "!! 安全断言失败（apply 顺序/跨副本不一致）: %s\n",
                 e.what());
    rc = 1;
  }
  cfg.Cleanup();

  if (ops <= 0) {
    std::fprintf(stderr, "!! 活性失败：测量窗口内 0 条提交\n");
    rc = 1;
  }

#ifdef RAFT_BENCH_METRICS
  M.Dump(stdout);
  if (!a.out.empty()) {
    FILE* f = std::fopen(a.out.c_str(), "a");
    if (f != nullptr) {
      if (std::ftell(f) == 0) {
        std::fprintf(f, "ts,label,kind,name,n,p50,p99,p999,max\n");
      }
      M.WriteCsv(f, IsoNow(), label);
      std::fclose(f);
    } else {
      std::fprintf(stderr, "打不开 CSV: %s\n", a.out.c_str());
    }
  }
#endif
  std::fflush(stdout);
  return rc;
}
