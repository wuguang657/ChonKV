// metrics.h —— 性能埋点核心（bench 专用）
//
// 【为什么整个文件被 #ifdef 包住】
//   埋点会改变指令时序（多几次 clock::now()、原子加）。测试基线
//   （80/80 已知绿/已知挂清单）必须跑在【零埋点】的二进制上 —— 这是
//   项目铁律：任何影响时序的东西都不进默认构建。所以未定义
//   RAFT_BENCH_METRICS 时，本头文件展开为空，被埋点的代码路径同样
//   展开为空，一条指令都不多。只有 bench 构建显式开
//   -DRAFT_BENCH_METRICS=ON（CMake option）才生效。
//
// 【启用后的设计约束】
//   1. 埋点自身【禁止加锁】：直方图/计数器全是 atomic，Record 就是几次
//      fetch_add —— 埋点不许变成新的锁竞争热点，否则测出来的是埋点
//      自己的开销，不是被测对象的。
//   2. 定长数组 + 枚举下标：Record 不做任何名字查找（无字符串比较）。
//   3. Dump/WriteCsv 只在 bench 主线程、负载停止后调用，无需考虑并发。
//   4. 直方图是 log2 主桶 + 每 octave 8 子桶（≈12.5% 相对分辨率），
//      Record O(1)；百分位取桶下界近似 —— 看分布形状与数量级够用，
//      客户端可感延迟（kv_bench）不走这里，那边是全量样本精确百分位。

#pragma once

#ifdef RAFT_BENCH_METRICS

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <string>

namespace raftcpp {

// 单调时钟纳秒时间戳（steady_clock：不受系统时间调整影响）
inline uint64_t NowNs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

inline int FloorLog2(uint64_t v) {
#if defined(__GNUC__)
  return 63 - __builtin_clzll(v);
#else
  int e = 0;
  while ((v >>= 1) != 0) ++e;
  return e;
#endif
}

// ---------------------------------------------------------------------------
// 直方图
// ---------------------------------------------------------------------------
// 分桶规则（值单位一律为 ns；条目数/线程数等计数型 metric 同样适用）：
//   v == 0        → 桶 0
//   1 <= v < 16   → 桶 1+v（小值精确分桶，延迟埋点的高频区）
//   v >= 16       → 桶 17 + (floor(log2 v) - 4) * 8 + ((v >> (e-3)) & 7)
// 百分位取"桶下界"近似；MaxNs 取最高非空桶的下界。
class Histogram {
 public:
  void Record(uint64_t v) {
    const uint32_t b = BucketOf(v);
    if (b < kBuckets) {
      buckets_[b].fetch_add(1, std::memory_order_relaxed);
      total_.fetch_add(1, std::memory_order_relaxed);
    } else {
      overflow_.fetch_add(1, std::memory_order_relaxed);
    }
  }

  uint64_t count() const { return total_.load(std::memory_order_relaxed); }
  uint64_t overflow() const { return overflow_.load(std::memory_order_relaxed); }

  // q ∈ [0,1]；空直方图返回 0
  uint64_t Percentile(double q) const {
    const uint64_t total = count();
    if (total == 0) return 0;
    uint64_t want = static_cast<uint64_t>(static_cast<double>(total) * q);
    if (want == 0) want = 1;
    if (want > total) want = total;
    uint64_t seen = 0;
    for (uint32_t b = 0; b < kBuckets; ++b) {
      seen += buckets_[b].load(std::memory_order_relaxed);
      if (seen >= want) return BucketLowerBoundNs(b);
    }
    return BucketLowerBoundNs(kBuckets - 1);
  }

  uint64_t MaxNs() const {
    for (int b = static_cast<int>(kBuckets) - 1; b >= 0; --b) {
      if (buckets_[static_cast<uint32_t>(b)].load(std::memory_order_relaxed) > 0) {
        return BucketLowerBoundNs(static_cast<uint32_t>(b));
      }
    }
    return 0;
  }

 private:
  // 桶下标 → 值下界（ns）
  static uint64_t BucketLowerBoundNs(uint32_t b) {
    if (b == 0) return 0;
    if (b <= 16) return b - 1;
    const uint32_t e = (b - 17) / 8 + 4;  // floor(log2)
    const uint32_t sub = (b - 17) % 8;
    const uint64_t base = uint64_t{1} << e;
    return base + (base >> 3) * sub;
  }

  static uint32_t BucketOf(uint64_t v) {
    if (v < 16) return static_cast<uint32_t>(v) + 1;
    const int e = FloorLog2(v);  // v >= 16 → e >= 4
    const uint32_t sub = static_cast<uint32_t>((v >> (e - 3)) & 7);
    return 17 + (static_cast<uint32_t>(e) - 4) * 8 + sub;
  }

  // e ∈ [4,63] → 60 个 octave × 8 子桶 + 17 个小值桶 = 497
  static constexpr uint32_t kBuckets = 497;

  std::atomic<uint64_t> buckets_[kBuckets] = {};
  std::atomic<uint64_t> total_{0};
  std::atomic<uint64_t> overflow_{0};
};

// ---------------------------------------------------------------------------
// 埋点 ID（稳定序号 = 数组下标；只能追加，禁止重排/复用已删除项）
// ---------------------------------------------------------------------------
enum MetricId {
  kProposeToApply = 0,     // Start() 追加日志 → ApplyLoop 收集到该 index（raft 侧端到端延迟）
  kMuWaitStart,            // Start() 等 mu_ 的时长（纯锁竞争；客户端直接可感）
  kMuHoldStart,            // Start() 临界区墙钟时长（file 模式含 PersistLocked 等落盘时的解锁等待）
  kMuHoldApplyCollect,     // ApplyLoop 批量收集段墙钟时长（wait_for 睡眠不计；含 conf 持久化窗口）
  kMuHoldBecomeLeader,     // 当选初始化临界区墙钟时长
  kMuHoldAdvanceCommit,    // 提交推进（AdvanceCommitIndexLocked）墙钟时长
  kPersistDuration,        // PersistLocked 调用时长（file 模式含等 flusher 落盘）
  kReplicaLag,             // commit_index_ - 最慢 voter 的 match_index_（条目）
  kUncommittedBacklog,     // LastLogIndex - commit_index_（未提交积压，条目）
  kApplyBatchSize,         // ApplyLoop 每批条目数（仅非空批；验证 128 封顶与批边界）
  kWorkerBusy,             // in-proc 工作池忙线程数（每任务采样）
  kWorkerTotal,            // in-proc 工作池总线程数（每任务采样；看 512 上限触达）
  kLogAppend,              // SegmentLogStorage::Append：每条 entry 的 encode + 2×pwrite 到 page cache
  kLogSync,                // SegmentLogStorage::Sync 内 ::fsync 时长（去抖窗口触发时才付）
  kMetricCount
};

enum CounterId {
  kOpsProposed = 0,        // Start() 成功追加的提案数
  kOpsNotLeader,           // Start() 因"非 leader / 退位宽限"被拒
  kOpsBackpressure,        // Start() 因 C1 背压被拒
  kApplySkipped,           // apply 时查不到提案时间戳的条目数。两类来源：
                           //   ① follower 节点（从不提案，它 apply 的每条都计入）
                           //      —— 多节点集群里这个数 ≈ (n-1) × 提案数，属正常；
                           //   ② 换主/快照前被清表的提案（真实丢样）。
                           // 判据：propose_to_apply 的 n 应 ≈ kOpsProposed，
                           // 若明显小于则说明丢样严重（选举风暴场景）。
  kLeaderChanges,          // 当选次数（任期切换）
  kLogFsync,               // 段文件实际 fsync 次数（去抖窗口触发才 +1）
  kCounterCount
};

inline const char* MetricName(MetricId id) {
  switch (id) {
    case kProposeToApply: return "propose_to_apply_ns";
    case kMuWaitStart: return "mu_wait_start_ns";
    case kMuHoldStart: return "start_critical_ns";
    case kMuHoldApplyCollect: return "apply_collect_critical_ns";
    case kMuHoldBecomeLeader: return "become_leader_critical_ns";
    case kMuHoldAdvanceCommit: return "advance_commit_critical_ns";
    case kPersistDuration: return "persist_duration_ns";
    case kReplicaLag: return "replica_lag_entries";
    case kUncommittedBacklog: return "uncommitted_backlog_entries";
    case kApplyBatchSize: return "apply_batch_entries";
    case kWorkerBusy: return "worker_busy_count";
    case kWorkerTotal: return "worker_total_count";
    case kLogAppend: return "log_append_ns";
    case kLogSync: return "log_sync_ns";
    default: return "?";
  }
}

inline const char* CounterName(CounterId id) {
  switch (id) {
    case kOpsProposed: return "ops_proposed";
    case kOpsNotLeader: return "ops_rejected_not_leader";
    case kOpsBackpressure: return "ops_rejected_backpressure";
    case kApplySkipped: return "apply_entries_without_propose_ts";
    case kLeaderChanges: return "leader_changes";
    case kLogFsync: return "log_fsync_count";
    default: return "?";
  }
}

// ---------------------------------------------------------------------------
// 全局埋点单例（magic static 保证线程安全初始化）
// ---------------------------------------------------------------------------
class Metrics {
 public:
  static Metrics& Instance() {
    static Metrics m;
    return m;
  }

  void Record(MetricId id, uint64_t v) { hists_[id].Record(v); }
  void Incr(CounterId id, int64_t d = 1) {
    counters_[id].fetch_add(d, std::memory_order_relaxed);
  }
  int64_t Counter(CounterId id) const {
    return counters_[id].load(std::memory_order_relaxed);
  }
  const Histogram& Hist(MetricId id) const { return hists_[id]; }

  // 人读表格（bench 结束时打 stdout）
  void Dump(FILE* out) {
    std::fprintf(out, "---- raft 内部埋点（RAFT_BENCH_METRICS）----\n");
    for (int i = 0; i < kMetricCount; ++i) {
      const Histogram& h = hists_[i];
      if (h.count() == 0) continue;
      std::fprintf(out,
                   "  %-26s n=%-9llu p50=%-10llu p99=%-10llu p999=%-10llu max=%-10llu\n",
                   MetricName(static_cast<MetricId>(i)),
                   static_cast<unsigned long long>(h.count()),
                   static_cast<unsigned long long>(h.Percentile(0.50)),
                   static_cast<unsigned long long>(h.Percentile(0.99)),
                   static_cast<unsigned long long>(h.Percentile(0.999)),
                   static_cast<unsigned long long>(h.MaxNs()));
    }
    for (int i = 0; i < kCounterCount; ++i) {
      const int64_t v = Counter(static_cast<CounterId>(i));
      if (v == 0) continue;
      std::fprintf(out, "  %-26s %lld\n", CounterName(static_cast<CounterId>(i)),
                   static_cast<long long>(v));
    }
    std::fflush(out);
  }

  // CSV 长表：ts,label,kind,name,n,p50,p99,p999,max
  // （计数型 metric 的"单位"见 name 后缀：_ns / _entries / _count）
  void WriteCsv(FILE* out, const std::string& iso_ts, const std::string& label) {
    for (int i = 0; i < kMetricCount; ++i) {
      const Histogram& h = hists_[i];
      if (h.count() == 0) continue;
      std::fprintf(out, "%s,%s,hist,%s,%llu,%llu,%llu,%llu,%llu\n",
                   iso_ts.c_str(), label.c_str(),
                   MetricName(static_cast<MetricId>(i)),
                   static_cast<unsigned long long>(h.count()),
                   static_cast<unsigned long long>(h.Percentile(0.50)),
                   static_cast<unsigned long long>(h.Percentile(0.99)),
                   static_cast<unsigned long long>(h.Percentile(0.999)),
                   static_cast<unsigned long long>(h.MaxNs()));
    }
    for (int i = 0; i < kCounterCount; ++i) {
      const int64_t v = Counter(static_cast<CounterId>(i));
      std::fprintf(out, "%s,%s,counter,%s,%lld,0,0,0,0\n", iso_ts.c_str(),
                   label.c_str(), CounterName(static_cast<CounterId>(i)),
                   static_cast<long long>(v));
    }
    std::fflush(out);
  }

 private:
  Metrics() = default;
  Histogram hists_[kMetricCount];
  std::atomic<int64_t> counters_[kCounterCount] = {};
};

// ---------------------------------------------------------------------------
// RAII：记录"声明点到作用域退出"的时长（典型用法：持锁区间）。
// 声明点必须放在【锁获取之后】——逆序析构保证它先于锁对象析构，
// 记录到的就是"解锁前最后一刻"的持锁时长。
// ---------------------------------------------------------------------------
class BenchHoldGuard {
 public:
  explicit BenchHoldGuard(MetricId id) : id_(id), t0_(NowNs()) {}
  ~BenchHoldGuard() { Metrics::Instance().Record(id_, NowNs() - t0_); }
  BenchHoldGuard(const BenchHoldGuard&) = delete;
  BenchHoldGuard& operator=(const BenchHoldGuard&) = delete;

 private:
  MetricId id_;
  uint64_t t0_;
};

}  // namespace raftcpp

#endif  // RAFT_BENCH_METRICS
