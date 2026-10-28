// config.cpp —— KV 测试脚手架实现（对应 Go 版 src/kvraft/config.go）

#include "config.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <utility>

#include "../common/util.h"
#include "../labrpc/transport_factory.h"  // labrpc::MakeTransport：按 LABRPC_TRANSPORT 选后端
#include "../persist/state/persister_factory.h"  // persist::MakePersister：按 LABRPC_PERSIST 选后端
#include "../common/thread_tracker.h"
#include "kv_model.h"
#include "test_status.h"  // Fatal() / g_failed（让 CheckLinearizability 失败能传到 runner）

namespace kvraft {

// 端点名必须全局唯一
namespace {
std::atomic<int> g_end_seq{0};
std::string NewEndName(const char* tag) {
  return std::string(tag) + std::to_string(g_end_seq.fetch_add(1)) + "_" +
         raftcpp::RandString(20);
}
}  // namespace

// ===========================================================================
// 构造 / 析构
// ===========================================================================

Config::Config(int n, bool unreliable, int maxraftstate, bool linearizability)
    : n_(n),
      maxraftstate_(maxraftstate),
      check_linearizability_(linearizability) {
  net_ = labrpc::MakeTransport();
  kvservers_.assign(n, nullptr);
  saved_.assign(n, nullptr);
  endnames_.assign(n, {});

  // 与 persist::MakePersister() 的目录决策保持同一条判据（persister_factory.h）：
  // LABRPC_PERSIST_DIR 未设/为空 → 工厂自建 /tmp/cpp6raft_<pid>_<seq> 自动目录，
  // 那种目录 pid 私有，本 Config 负责在 Cleanup 清理掉；设了 → 用户的地盘，
  // 可能多进程/多轮共享，绝不碰（见 persist_dirs_ 注释）。
  {
    const char* d = std::getenv("LABRPC_PERSIST_DIR");
    persist_auto_dir_ = (d == nullptr || d[0] == '\0');
  }

  // 注意：不开 LongDelays —— 对齐 Go 版 kvraft/config.go（make_config 只有
  // Reliable）。Go 注释：“many kv tests require the client to try each server
  // in fairly rapid succession”——kv 客户端要快速换服重试，0~7000ms 的失败
  // 延迟会把用例顶爆看门狗（实测 TestSnapshotMultiChunkUnreliable3B 148s>120s）。
  // longDelays 是 raft config 专属（raft/config.cpp）。

  for (int i = 0; i < n_; i++) StartServer(i);
  ConnectAll();
  net_->Reliable(!unreliable);
}

Config::~Config() { Cleanup(); }

void Config::Cleanup() {
  if (cleaned_) return;
  cleaned_ = true;

  // 先停掉所有 KVServer（内部会 Kill raft 并 join applier 线程）
  for (int i = 0; i < n_; i++) ShutdownServer(i);
  // 再停网络线程池
  if (net_) net_->Cleanup();
  // 等所有 detached 线程（Raft 的 ApplyLoop / ReplicateLoop / ElectionTimerLoop）
  // 退出后，才能安全析构 Raft 对象。漏了这行会导致：detached 线程在 mu_ 已销毁
  // 后还去 lock → "mutex lock failed: Invalid argument"（崩溃栈落在
  // raft::Raft::ApplyLoop）。raft::Config::Cleanup 里有完全相同的一行
  // （raft/config.cpp:552），这里是从 Lab 2 移植时漏掉的。
  raftcpp::ThreadTracker::Instance().WaitAll(10000);

  // 最后清理本 Config 自己造的持久化目录。时机安全：上面 WaitAll 已保证所有
  // raft 线程退出、ShutdownServer 已 join kv applier → 没有存活写者。
  // 只清自动目录（pid 私有）；用户指定 LABRPC_PERSIST_DIR 时 persist_dirs_
  // 根本是空的。清掉的包括每次 Copy 克隆出的 copy_<pid>_N —— 不清理的话
  // 实测一个 3B 用例就往 /tmp 根漏 35 个克隆目录 + 5 个持久化根。
  for (const std::string& d : persist_dirs_) {
    persist::RemoveTree(d);
  }
  persist_dirs_.clear();
}

void Config::TrackPersisterDir(const std::shared_ptr<raft::Persister>& p) {
  // 内存模式 Dir() 为空；用户目录模式不跟踪（可能被别的进程共享，不能删）
  if (!persist_auto_dir_ || !p || !p->FileBacked() || p->Dir().empty()) return;
  persist_dirs_.push_back(p->Dir());
}

// ===========================================================================
// 服务器生命周期
// ===========================================================================

void Config::ShutdownServer(int i) {
  std::unique_lock<std::mutex> lk(mu_);

  DisconnectUnlocked(i, All());

  // 必须先摘掉 server，再换 Persister。
  // Go 版注释写得很清楚：避免旧实例对一个 Append 回了成功、
  // 却把结果写进了即将被替换掉的 Persister。
  net_->DeleteServer(i);

  // 换一块"新盘"：内容是旧盘的拷贝。
  // 这样旧实例的残留线程就算还在跑，也改不到新实例要用的数据。
  if (saved_[i]) {
    auto old = kvservers_[i];
    if (old && old->raft_for_test()) {
      saved_[i] = old->raft_for_test()->CopyPersisterUnderLock();
    } else {
      saved_[i] = saved_[i]->Copy();  // 已无存活实例，无并发写盘，直接克隆
    }
    TrackPersisterDir(saved_[i]);  // 记账：Cleanup 时统一清理（仅自动目录模式）
  }

  auto kv = std::move(kvservers_[i]);  // kvservers_[i] = nullptr
  graves_.push_back(kv);

  lk.unlock();
  if (kv) kv->Kill();
}

void Config::StartServer(int i) {
  std::vector<std::string> names;
  std::vector<std::shared_ptr<labrpc::ClientEnd>> ends(n_);
  std::shared_ptr<raft::Persister> persister;

  {
    std::lock_guard<std::mutex> lk(mu_);

    // 一整套全新的端点名：旧实例残留的 ClientEnd 就再也发不出消息了
    // （对应 Go 版 start1 里换 endnames 的防幽灵 RPC 设计）
    names.clear();
    for (int j = 0; j < n_; j++) names.push_back(NewEndName("s"));
    endnames_[i] = names;

    for (int j = 0; j < n_; j++) {
      ends[j] = net_->MakeEnd(names[j]);
      net_->Connect(names[j], j);
    }

    if (saved_[i]) {
      saved_[i] = saved_[i]->Copy();
    } else {
      saved_[i] = persist::MakePersister();
    }
    TrackPersisterDir(saved_[i]);  // 记账：Cleanup 时统一清理（仅自动目录模式）
    persister = saved_[i];
  }  // ---- 释放锁再建 KVServer（StartKVServer 会起线程）----

  auto kv = StartKVServer(ends, i, persister, maxraftstate_);

  {
    std::lock_guard<std::mutex> lk(mu_);
    kvservers_[i] = kv;
  }

  // 一个网络节点上挂两个服务：KVServer（给客户端）+ Raft（给其他节点）
  auto srv = std::make_shared<labrpc::Server>();
  srv->AddService(MakeKVServerService(kv));
  srv->AddService(raft::MakeRaftService(kv->raft_for_test()));
  net_->AddServer(i, srv);

  ConnectUnlocked(i, All());
}

// ===========================================================================
// 网络编排
// ===========================================================================

std::vector<int> Config::All() const {
  std::vector<int> v;
  for (int i = 0; i < n_; i++) v.push_back(i);
  return v;
}

void Config::ConnectUnlocked(int i, const std::vector<int>& to) {
  for (int j : to) {
    if (j < 0 || j >= n_) continue;
    if (i < static_cast<int>(endnames_.size()) &&
        j < static_cast<int>(endnames_[i].size())) {
      net_->Enable(endnames_[i][j], true);
    }
  }
}

void Config::DisconnectUnlocked(int i, const std::vector<int>& from) {
  for (int j : from) {
    if (j < 0 || j >= n_) continue;
    if (i < static_cast<int>(endnames_.size()) &&
        j < static_cast<int>(endnames_[i].size())) {
      net_->Enable(endnames_[i][j], false);
    }
  }
}

void Config::ConnectAll() {
  std::lock_guard<std::mutex> lk(mu_);
  for (int i = 0; i < n_; i++) ConnectUnlocked(i, All());
  partitioned_.store(false);
}

// 切两半：p1 内部互通、p2 内部互通，两边之间不通
void Config::Partition(const std::vector<int>& p1, const std::vector<int>& p2) {
  std::lock_guard<std::mutex> lk(mu_);
  // 任一组为空 = 没有真的切开（Go 版测试常用 partition(All(), {}) 表示"全网连通"）
  partitioned_.store(!p1.empty() && !p2.empty());
  for (int i : p1) {
    DisconnectUnlocked(i, p2);
    ConnectUnlocked(i, p1);
  }
  for (int i : p2) {
    DisconnectUnlocked(i, p1);
    ConnectUnlocked(i, p2);
  }
}

// 自动分区，保证当前 leader 落在少数派（这样多数派能选出新 leader）
void Config::MakePartition(std::vector<int>* p1, std::vector<int>* p2) {
  int leader = 0;
  Leader(&leader);

  p1->clear();
  p2->clear();
  for (int i = 0; i < n_; i++) {
    if (i == leader) continue;
    if (static_cast<int>(p1->size()) < n_ / 2 + 1) {
      p1->push_back(i);
    } else {
      p2->push_back(i);
    }
  }
  p2->push_back(leader);  // 把 leader 塞进少数派
  Partition(*p1, *p2);
}

// 对应 Go 版 test_test.go:126-141 的 partitioner()：每台 server 独立 50%。
//
//   a[i] = rand.Int() % 2        // 每台独立抛硬币
//   pa[0] = { i : a[i]==0 },  pa[1] = { i : a[i]==1 }
//   cfg.partition(pa[0], pa[1])
//
// 与 MakePartition 的区别：
//   * 形态 2^n 种、每轮全新随机（MakePartition 是固定 n/2+1 vs n/2）；
//   * 允许空组（全 0 或全 1）→ 那一轮等价于"全网连通"，是合法扰动；
//   * leader 落在哪边看运气（约 50% 在多数派）。
void Config::MakePartitionRandom(std::vector<int>* p1, std::vector<int>* p2) {
  p1->clear();
  p2->clear();
  for (int i = 0; i < n_; i++) {
    if (raftcpp::RandInt(2) == 0) {
      p1->push_back(i);
    } else {
      p2->push_back(i);
    }
  }
  Partition(*p1, *p2);
}

// ===========================================================================
// 客户端
// ===========================================================================

std::shared_ptr<Clerk> Config::MakeClient(const std::vector<int>& to) {
  std::vector<std::string> names;
  std::vector<std::shared_ptr<labrpc::ClientEnd>> ends(n_);

  {
    std::lock_guard<std::mutex> lk(mu_);
    for (int j = 0; j < n_; j++) {
      names.push_back(NewEndName("c"));
      ends[j] = net_->MakeEnd(names[j]);
      net_->Connect(names[j], j);
    }
  }

  auto ck = std::make_shared<Clerk>(ends);
  {
    std::lock_guard<std::mutex> lk(mu_);
    clerks_[ck.get()] = ck;       // Config 持有 shared_ptr，Clerk 寿命延长到 Config 析构
    clerk_ends_[ck.get()] = names; 
  }

  if (!to.empty()) {
    ConnectClient(ck.get(), to);
  }
  return ck;
}

void Config::ConnectClient(Clerk* ck, const std::vector<int>& to) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = clerk_ends_.find(ck);
  if (it == clerk_ends_.end()) return;

  for (int j = 0; j < n_; j++) {
    bool connected = std::find(to.begin(), to.end(), j) != to.end();
    net_->Enable(it->second[j], connected);
  }
}

// caller 应持有 mu_。只对 `from` 内的 server 端点 Disable，其余保持原状，
// 与 Go 版 DisconnectClientUnlocked 语义一致（它只动 from 里的，不碰其它）。
void Config::DisconnectClientUnlocked(Clerk* ck, const std::vector<int>& from) {
  auto it = clerk_ends_.find(ck);
  if (it == clerk_ends_.end()) return;

  for (int j : from) {
    if (j >= 0 && j < static_cast<int>(it->second.size())) {
      net_->Enable(it->second[j], false);
    }
  }
}

// 公开的 client 级断开（加锁包装，对齐 Go 的 DisconnectClientUnlocked 调用点）
void Config::DisconnectClient(Clerk* ck, const std::vector<int>& from) {
  std::lock_guard<std::mutex> lk(mu_);
  DisconnectClientUnlocked(ck, from);
}

// 拆掉一个 client：禁用并注销它那组 ClientEnd，再从两个 map 移除，
// 释放 Config 持有的 shared_ptr（Clerk 及其 ClientEnd 随之析构）。
// 对齐 Go 版 deleteClient。
void Config::DeleteClient(Clerk* ck) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = clerk_ends_.find(ck);
  if (it != clerk_ends_.end()) {
    for (const auto& name : it->second) {
      net_->Enable(name, false);     // 先全部断开
      net_->RemoveClientEnd(name);   // 再从 network 注销（对齐 Go cfg.net.Remove）
    }
    clerk_ends_.erase(it);
  }
  clerks_.erase(ck);  // 释放 shared_ptr → Clerk 析构
}

// ===========================================================================
// 度量
// ===========================================================================

int Config::LogSize() const {
  std::lock_guard<std::mutex> lk(mu_);
  int m = 0;
  for (int i = 0; i < n_; i++) {
    // 优先问【活着的】KVServer：它的 RaftStateSize() 是 raft 全量口径
    // （state blob + raft.log.seg）。file 模式下 saved_[i] 的 persister
    // 口径只剩标量（恒定 ~8B），直接拿它会让 3B 的三条
    // "logs were not trimmed"断言恒真 —— 日志真没修剪也测不出来（假绿）。
    // 停机中的节点（ShutdownServer 已摘）没有活 raft 可问，退回 saved_[i]
    // 快照口径：少测一台好过没有，且少测只会漏报、不会误报。
    // 锁序：Config::mu_ → raft::mu_ 单向（同 ShutdownServer 的
    // CopyPersisterUnderLock），raft 永不回调 KV/Config，无死锁。
    int sz = 0;
    if (i < static_cast<int>(kvservers_.size()) && kvservers_[i]) {
      sz = kvservers_[i]->RaftStateSize();
    } else if (saved_[i]) {
      sz = saved_[i]->RaftStateSize();
    }
    m = std::max(m, sz);
  }
  return m;
}

int Config::SnapshotSize() const {
  std::lock_guard<std::mutex> lk(mu_);
  int m = 0;
  for (int i = 0; i < n_; i++) {
    if (saved_[i]) m = std::max(m, saved_[i]->SnapshotSize());
  }
  return m;
}

int Config::RpcTotal() { return net_->GetTotalCount(); }

bool Config::Leader(int* leader_id) {
  std::lock_guard<std::mutex> lk(mu_);
  for (int i = 0; i < n_; i++) {
    if (!kvservers_[i]) continue;
    if (kvservers_[i]->is_leader_for_test()) {
      *leader_id = i;
      return true;
    }
  }
  return false;
}

// ===========================================================================
// 计时 / 断言
// ===========================================================================

void Config::Begin(const std::string& desc) {
  desc_ = desc;
  desc_printed_ = true;
  std::printf("%s ...\n", desc.c_str());
  std::fflush(stdout);
  t0_ = std::chrono::steady_clock::now();
  rpcs0_ = RpcTotal();
  ops_.store(0);
}

void Config::Op() { ops_.fetch_add(1); }

void Config::Fail(const std::string& msg) {
  failed_.store(true);
  std::lock_guard<std::mutex> lk(mu_);
  if (fail_msg_.empty()) fail_msg_ = msg;
}

void Config::End() {
  double secs = std::chrono::duration<double>(
                    std::chrono::steady_clock::now() - t0_)
                    .count();
  int nrpc = RpcTotal() - rpcs0_;

  if (failed_.load()) {
    Fatal(fail_msg_.empty() ? "test failed (cfg->Fail without message)"
                             : fail_msg_);
  }

  const double kEndTimeoutSec =
#if defined(__SANITIZE_THREAD__) || (defined(__has_feature) && __has_feature(thread_sanitizer))
      360.0;
#else
      120.0;
#endif
  if (secs > kEndTimeoutSec) {
    char buf[128];
    std::snprintf(buf, sizeof(buf), "test took longer than %.0f seconds",
                  kEndTimeoutSec);
    Fatal(buf);
  }

  // ---- 副本一致性：所有 server 的状态机必须收敛到同一个状态 ----
  //
  // Go 版 kvraft 的 end() 不做这项检查（它靠客户端 Get 做线性读 + porcupine
  // 做线性一致性验证）。但 C++ 这一侧没有 porcupine，客户端 Get 又只能读到
  // leader 的视图 —— 于是"某个 follower 状态机缺数据"这类 bug 是漏网的。
  // 这里补上，相当于给 3A/3B 加一道兜底。
  if (!partitioned_.load()) {
    std::string err;
    bool consistent = false;
    // 收敛窗口：10 秒（100 × 100ms）。
    for (int attempt = 0; attempt < 100 && !consistent; attempt++) {
      if (CheckConsistency(&err)) {
        consistent = true;
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    if (!consistent) {
      char buf[512];
      std::snprintf(buf, sizeof(buf), "replicas did not converge: %s",
                    err.c_str());
      Fatal(buf);
    }
  }

  // ---- 线性一致性检查（porcupine）----
  //
  // 只对 Linearizable 用例开启。代价：单 key 分片跑 DFS，可能 10~60 秒。
  // 这是 Lab 3 最强的不变式：不仅"所有副本一致"，还要"所有客户端看到的
  // 操作历史能排成一条线性化序列"。违反这条的 bug 极隐蔽，常规一致性
  // 检查抓不到（比如 lost update：两条并发 Put，服务端各应用了一半）。
  if (check_linearizability_) {
    if (!CheckLinearizability()) {
      // CheckLinearizability 内部已经 Fail() 并打印
      return;
    }
  }

  std::printf("  ... Passed --  %5.1f  %d %5d %4d\n", secs, n_, nrpc,
              ops_.load());
  std::fflush(stdout);
}

// 收集所有 Clerk 的 op 历史，跑 porcupine 做线性一致性检查。
//
// 拿历史的方法：从 clerk_ends_（已经按 Clerk* 索引了所有 Clerk）逐个调
// DrainHistory —— 内部 std::vector::swap 走，所以是 O(1) 转移。
bool Config::CheckLinearizability() {
  // std::printf("[lin] 开始 CheckLinearizability\n");
  std::fflush(stdout);
  std::vector<KvOperation> history;
  bool history_incomplete = false;
  {
    std::lock_guard<std::mutex> lk(mu_);
    // std::printf("[lin] 拿到 Config::mu_\n");
    std::fflush(stdout);
    for (const auto& kv : clerk_ends_) {
      // Clerk 在 Put/Append 上超时放弃过 → 那条命令可能已经在服务端生效，
      // 却没进 history。porcupine 看到的就是"少一条 Put，但状态机有它的效果"，
      // 只能判 Illegal —— 这是脚手架自己造的假失败，Go 版不存在
      // （Go 的 Clerk 永不放弃，client.go 的 for 循环没有退出条件）。
      if (kv.first->gave_up_on_write()) history_incomplete = true;
      auto sub = kv.first->DrainHistory();
      // std::printf("[lin] DrainHistory 拿到 %zu 条\n", sub.size());
      std::fflush(stdout);
      // 追加到总历史（sub 已经是 std::move 出来的 vector，swap 进 history）
      history.insert(history.end(),
                     std::make_move_iterator(sub.begin()),
                     std::make_move_iterator(sub.end()));
    }
    // std::printf("[lin] drain 完成，total=%zu\n", history.size());
    std::fflush(stdout);
  }

  // history 不完整时不能判线性一致性 —— 缺失的写会被当成"凭空出现的值"。
  // 直接跳过并大声提示，让真因（GIVE UP 日志）可见，而不是伪装成 Illegal。
  if (history_incomplete) {
    std::printf("  ... [warn] 有 Clerk 在 Put/Append 上 GIVE UP，history 可能缺条目\n"
                "            → 跳过线性一致性判定（否则 porcupine 会把缺失的写"
                "判成 Illegal，是假失败）。真因看上面的 [kv-clerk] GIVE UP 日志。\n");
    std::fflush(stdout);
    return true;
  }

  if (history.empty()) {
    Fatal("linearizability: no operations recorded");
  }

  // std::printf("  ... checking linearizability on %zu ops ...\n",
              // history.size());
  std::fflush(stdout);

  // 调试：把 history 全部 dump 出来，方便排查 Illegal 时定位是哪条 op 触发了违规。
  // 按 key 桶 + (call_time, return_time) 排版，便于肉眼比对客户端记录。
  std::map<std::string, std::vector<size_t>> by_key;
  for (size_t i = 0; i < history.size(); ++i) {
    by_key[history[i].input.key].push_back(i);
  }
  for (auto& kv : by_key) {
    std::sort(kv.second.begin(), kv.second.end(),
              [&](size_t a, size_t b) {
                if (history[a].call_time_ns != history[b].call_time_ns)
                  return history[a].call_time_ns < history[b].call_time_ns;
                return history[a].return_time_ns < history[b].return_time_ns;
              });
  }
  // std::printf("    [lin-history] %zu keys, history 按 key 分桶如下：\n",
              // by_key.size());
  for (auto& kv : by_key) {
    // std::printf("    [lin-history] key=%s ops=%zu\n", kv.first.c_str(),
                // kv.second.size());
    for (size_t i = 0; i < kv.second.size(); ++i) {
      const auto& op = history[kv.second[i]];
      const char* m = op.input.op == kvraft::KV_OP_GET
                          ? "GET"
                          : (op.input.op == kvraft::KV_OP_PUT ? "PUT" : "APP");
      (void)m;
                          std::printf("      [%zu] client=%d seq=%lld %s key=%s val_in=\"%s\" val_out=\"%s\" "
                  "call=%lld ret=%lld\n",
                  i, op.client_id, (long long)op.seq_id, m,
                  op.input.key.c_str(),
                  op.input.value.c_str(), op.output.value.c_str(),
                  (long long)op.call_time_ns, (long long)op.return_time_ns);
    }
  }
  std::fflush(stdout);

  auto model = MakeKvModel();
  std::printf("[lin] model 创建完成，调 CheckOperations\n");
  std::fflush(stdout);
  auto result =
      porcupine::CheckOperations(model, history, std::chrono::seconds(120));
  std::printf("[lin] CheckOperations 返回 result=%d\n", (int)result);
  std::fflush(stdout);

  switch (result) {
    case porcupine::CheckResult::Ok:
      std::printf("  ... linearizability OK\n");
      return true;
    case porcupine::CheckResult::Illegal:
      // 必须同时调 Fatal() —— 它会 flip test runner 的 g_failed，
      // 否则只 Config::Fail() 的话 main() 那边的 g_failed 永远是 false，
      // 把失败的用例算成"通过 1 个，失败 0 个"。
      Fatal("history is NOT linearizable "
            "(服务端可能违反线性一致性，如 lost update / 读到旧值)");
      failed_.store(true);
      return false;
    case porcupine::CheckResult::Unknown: {
      static const bool unknown_pass = []() {
        const char* p = std::getenv("KV_UNKNOWN_PASS");
        return p != nullptr && p[0] != '\0' && p[0] != '0';
      }();
      if (unknown_pass) {
        std::printf("  ... [warn] 120s 内没搜完（Unknown），按 Go 版语义"
                    "假设历史 OK 并放行（KV_UNKNOWN_PASS=1）\n");
        std::fflush(stdout);
        return true;
      }
      Fatal("linearizability check timed out (120s 内没搜完；可能是 history 太大)");
      failed_.store(true);
      return false;
    }
  }
  return false;  // 不可达
}

// 校验所有副本的状态机是否一致。
// 注意：分区期间各副本本来就允许短暂不一致，所以只在"全网连通且 quiesce"之后调。
bool Config::CheckConsistency(std::string* err) {
  std::vector<std::map<std::string, std::string>> stores;
  {
    std::lock_guard<std::mutex> lk(mu_);
    for (int i = 0; i < n_; i++) {
      if (kvservers_[i]) stores.push_back(kvservers_[i]->SnapshotStore());
    }
  }
  if (stores.size() < 2) return true;

  for (size_t i = 1; i < stores.size(); i++) {
    if (stores[i] != stores[0]) {
      // 找出第一个不一致的 key，方便排错
      for (const auto& p : stores[0]) {
        auto it = stores[i].find(p.first);
        if (it == stores[i].end() || it->second != p.second) {
          {
            std::lock_guard<std::mutex> lk(mu_);
            // std::fprintf(stderr, "[consistency-diverge] key=%s\n", p.first.c_str());
            for (int j = 0; j < n_; j++) {
              if (!kvservers_[j]) continue;
              auto st = kvservers_[j]->SnapshotStore();
              auto vit = st.find(p.first);
              (void)vit;
              // std::fprintf(stderr,
                            // "  server[%d]: snapIdx=%d lastApplied=%d "
                            // "%s=\"%s\"\n",
                            // j, kvservers_[j]->SnapshotIndex(),
                            // kvservers_[j]->raft_for_test()->LastApplied(),
                            // p.first.c_str(),
                            // vit == st.end() ? "<missing>"
                                            // : vit->second.c_str());
            }
            std::fflush(stderr);
          }
          char buf[256];
          std::snprintf(buf, sizeof(buf),
                        "state diverged: server[0] vs server[%zu] key=%s", i,
                        p.first.c_str());
          *err = buf;
          return false;
        }
      }
      *err = "state diverged (different key sets)";
      return false;
    }
  }
  return true;
}

}  // namespace kvraft
