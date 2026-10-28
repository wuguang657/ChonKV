// kv_bench.cpp —— KV 栈（raft + kvraft）端到端性能压测
//
// 【测什么】
//   * 闭环场景：clients 个 Clerk 线程同步调 Put/Get —— 每条命令等 raft
//     提交才返回，天然背压。测的是【客户端可感延迟 + 吞吐】，这是性能
//     优化的最终判据（raft_bench 的开环吞吐是中间量）。
//   * raft 内部埋点（同进程链接 raft.cpp，RAFT_BENCH_METRICS=ON 时生效）
//     给出内部分布，与客户端侧延迟对照即可定位"延迟花在谁身上"。
//   * 安全性白捡：cfg.End() 在全网连通时强制 CheckConsistency
//     （kvraft/config.cpp:446）—— 所有副本状态机必须收敛到同一状态。
//
// 【与 kv_test 的区别】
//   * 不跑 porcupine 线性化检查（bench 的 key 空间不是为线性化设计的，
//     且一次检查几十秒）；一致性靠 End() 的全量比对兜底。
//   * maxraftstate=-1：不触发快照，测纯 raft 复制路径（快照与复制的
//     交互由 TestSnapshot*3B 系列覆盖）。
//
// 【用法】
//   ./build/kv_bench [--servers 3] [--clients 5] [--duration 20]
//                    [--warmup 10] [--keys 100] [--payload 64]
//                    [--writepct 100] [--unreliable]
//                    [--label xxx] [--out bench/results.csv]
//   SEED=7 ./build/kv_bench ...
//
// 【坑】
//   * Clerk::history_ 无界增长 —— bench 定期 DrainHistory()，否则长跑 OOM。
//   * in-proc 后端客户端与服务端共享进程 CPU，高并发档位建议切
//     LABRPC_TRANSPORT=socket。

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "../kvraft/client.h"
#include "../kvraft/common.h"
#include "../kvraft/config.h"
#include "../kvraft/test_status.h"
#include "../common/metrics.h"

namespace {

// 与 raftcpp::NowNs 同源，但不依赖 RAFT_BENCH_METRICS —— 客户端可感延迟
// 在任何构建下都要测（埋点开关只决定 raft 内部分布是否记录）。
uint64_t NowNsAny() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

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

// 全量样本精确百分位（v 会被排序）
double PercentileOf(std::vector<uint64_t>& v, double q) {
  if (v.empty()) return 0.0;
  std::sort(v.begin(), v.end());
  size_t i = static_cast<size_t>(static_cast<double>(v.size()) * q);
  if (i >= v.size()) i = v.size() - 1;
  return static_cast<double>(v[i]);
}

struct Args {
  int servers = 3;
  int clients = 5;
  int duration_sec = 20;
  int warmup_sec = 10;
  int keys = 100;
  int payload = 64;
  int writepct = 100;  // 写请求百分比（其余为读）
  bool unreliable = false;
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
    if (k == "--servers") {
      a.servers = std::atoi(next());
    } else if (k == "--clients") {
      a.clients = std::atoi(next());
    } else if (k == "--duration") {
      a.duration_sec = std::atoi(next());
    } else if (k == "--warmup") {
      a.warmup_sec = std::atoi(next());
    } else if (k == "--keys") {
      a.keys = std::atoi(next());
    } else if (k == "--payload") {
      a.payload = std::atoi(next());
    } else if (k == "--writepct") {
      a.writepct = std::atoi(next());
    } else if (k == "--unreliable") {
      a.unreliable = true;
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
  // 与 raft_bench 同理：kv End() 也有 120s 看门狗（kvraft/config.cpp:433，
  // 自 Begin 时刻起算），总时长压到 90s 内。
  if (a.warmup_sec + a.duration_sec > 90) {
    std::fprintf(stderr,
                 "拒绝执行: warmup(%d)+duration(%d) > 90s —— kv End() 的 120s "
                 "看门狗会误杀 bench（kvraft/config.cpp:433）\n",
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
    std::snprintf(label, sizeof(label), "kv_n%d_c%d_k%d_p%d_w%d_%s_seed%s",
                  a.servers, a.clients, a.keys, a.payload, a.writepct,
                  a.unreliable ? "unrel" : "rel", seed.c_str());
  }
  std::printf("=== kv_bench [%s] servers=%d clients=%d keys=%d payload=%dB "
              "writepct=%d%% warmup=%ds measure=%ds net=%s ===\n",
              label, a.servers, a.clients, a.keys, a.payload, a.writepct,
              a.warmup_sec, a.duration_sec,
              a.unreliable ? "unreliable" : "reliable");
  std::fflush(stdout);

  // maxraftstate=-1：不触发快照（纯 raft 复制路径）。
  // linearizability=false：bench 不跑 porcupine（一致性由 End() 兜底）。
  kvraft::Config cfg(a.servers, a.unreliable, /*maxraftstate=*/-1,
                     /*linearizability=*/false);
  cfg.Begin("kv_bench");
#ifdef RAFT_BENCH_METRICS
  auto& M = raftcpp::Metrics::Instance();
#endif

  // ---- 活性 smoke：10s 内至少一次成功写 ----
  {
    auto probe = cfg.MakeClient(cfg.All());
    bool ok = false;
    for (int i = 0; i < 200 && !ok; ++i) {
      if (probe->Put("__bench_probe__", "1") == kvraft::Err::kOK) {
        ok = true;
      } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
      }
    }
    if (!ok) {
      std::fprintf(stderr, "10s 内没有一次成功写 —— 集群活性有问题，bench 终止\n");
      cfg.DeleteClient(probe.get());
      cfg.Cleanup();
      return 1;
    }
    cfg.DeleteClient(probe.get());
  }

  std::atomic<bool> stop{false};
  std::atomic<bool> measuring{false};  // warmup 之后的样本才计入统计
  std::atomic<int64_t> ops{0}, write_err{0};

  struct Sample {
    double t;
    int64_t ops;
    int rpcs;
  };
  std::mutex samples_mu;
  std::vector<Sample> samples;
  std::thread observer([&] {
    const auto t0 = std::chrono::steady_clock::now();
    while (!stop.load(std::memory_order_relaxed)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
      Sample sm;
      sm.t =
          std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
              .count();
      sm.ops = ops.load(std::memory_order_relaxed);
      sm.rpcs = cfg.RpcTotal();
      std::lock_guard<std::mutex> lk(samples_mu);
      samples.push_back(sm);
    }
  });

  // ---- Clerk 线程：闭环 ----
  std::vector<std::shared_ptr<kvraft::Clerk>> clerks;
  for (int c = 0; c < a.clients; ++c) clerks.push_back(cfg.MakeClient(cfg.All()));

  // 每线程本地延迟样本（全量保存，结束时精确百分位；不清空——warmup 段
  // 的样本由 measuring 标志过滤，避免与工作线程争抢 vector）。
  std::vector<std::vector<uint64_t>> write_lat(static_cast<size_t>(a.clients));
  std::vector<std::vector<uint64_t>> read_lat(static_cast<size_t>(a.clients));
  std::vector<std::thread> workers;
  for (int c = 0; c < a.clients; ++c) {
    workers.emplace_back([&, c] {
      auto ck = clerks[static_cast<size_t>(c)];
      auto& wl = write_lat[static_cast<size_t>(c)];
      auto& rl = read_lat[static_cast<size_t>(c)];
      std::mt19937 rng(static_cast<unsigned>(c * 7919 + 13));  // 每线程独立流
      int64_t local_ops = 0;
      const std::string val(static_cast<size_t>(a.payload), 'v');
      while (!stop.load(std::memory_order_relaxed)) {
        const std::string key =
            "k" + std::to_string(rng() % static_cast<unsigned>(a.keys));
        const bool is_write =
            static_cast<int>(rng() % 100) < a.writepct;
        const uint64_t t0 = NowNsAny();
        if (is_write) {
          const kvraft::Err e = ck->Put(key, val);
          const uint64_t t1 = NowNsAny();
          if (e == kvraft::Err::kOK) {
            if (measuring.load(std::memory_order_relaxed)) {
              wl.push_back(t1 - t0);
              ops.fetch_add(1, std::memory_order_relaxed);
            }
          } else if (measuring.load(std::memory_order_relaxed)) {
            write_err.fetch_add(1, std::memory_order_relaxed);
          }
        } else {
          // Get 内部重试到成功/放弃；返回 "" = key 不存在，也是完成的读
          ck->Get(key);
          const uint64_t t1 = NowNsAny();
          if (measuring.load(std::memory_order_relaxed)) {
            rl.push_back(t1 - t0);
            ops.fetch_add(1, std::memory_order_relaxed);
          }
        }
        // history_ 无界增长：定期排水防 OOM（DrainHistory 自带锁）
        if ((++local_ops & 8191) == 0) ck->DrainHistory();
      }
      ck->DrainHistory();  // 收尾排空
    });
  }

  // ---- warmup → 测量段 ----
  std::this_thread::sleep_for(std::chrono::seconds(a.warmup_sec));
  double t_mark = 0.0;
  {
    std::lock_guard<std::mutex> lk(samples_mu);
    if (!samples.empty()) t_mark = samples.back().t;
  }
  ops.store(0, std::memory_order_relaxed);
  write_err.store(0, std::memory_order_relaxed);
  measuring.store(true, std::memory_order_relaxed);

  std::this_thread::sleep_for(std::chrono::seconds(a.duration_sec));
  stop.store(true);
  measuring.store(false, std::memory_order_relaxed);
  observer.join();
  for (auto& t : workers) t.join();

  // ---- 统计 ----
  double ops_s = 0, el = 0;
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
      el = last->t - first->t;
      const int64_t done = last->ops - first->ops;
      if (el > 0) ops_s = static_cast<double>(done) / el;
    }
  }
  std::vector<uint64_t> wl_all, rl_all;
  for (auto& v : write_lat) wl_all.insert(wl_all.end(), v.begin(), v.end());
  for (auto& v : read_lat) rl_all.insert(rl_all.end(), v.begin(), v.end());
  std::printf("---- kv_bench 结果 [%s] ----\n", label);
  std::printf("  窗口 %.1fs：完成 %zu ops → 吞吐 %.0f ops/s（写 %zu / 读 %zu）\n",
              el, wl_all.size() + rl_all.size(), ops_s, wl_all.size(),
              rl_all.size());
  std::printf("  写延迟 ms: p50=%.2f p99=%.2f p999=%.2f max=%.2f\n",
              PercentileOf(wl_all, 0.50) / 1e6, PercentileOf(wl_all, 0.99) / 1e6,
              PercentileOf(wl_all, 0.999) / 1e6,
              PercentileOf(wl_all, 1.0) / 1e6);
  std::printf("  读延迟 ms: p50=%.2f p99=%.2f p999=%.2f max=%.2f\n",
              PercentileOf(rl_all, 0.50) / 1e6, PercentileOf(rl_all, 0.99) / 1e6,
              PercentileOf(rl_all, 0.999) / 1e6,
              PercentileOf(rl_all, 1.0) / 1e6);
  std::printf("  写错误（非 kOK）: %lld\n",
              static_cast<long long>(write_err.load()));

  // ---- 收尾：End() 强制副本一致性（安全断言）----
  int rc = 0;
  try {
    cfg.End();
  } catch (const kvraft::TestFailure& e) {
    std::fprintf(stderr, "!! 安全断言失败（副本状态机不一致）: %s\n", e.what());
    rc = 1;
  }
  cfg.Cleanup();

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
