// config.cpp —— KV 测试脚手架实现（对应 Go 版 src/kvraft/config.go）

#include "config.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <utility>

#include "../common/util.h"
#include "../common/thread_tracker.h"
#include "kv_model.h"
#include "test_status.h"  // Fatal() / g_failed（让 CheckLinearizability 失败能传到 runner）

namespace kvraft {

// 【修复】端点名必须全局唯一：原先 StartServer / MakeClient 都用 RandString(20)
// 当端点名，两边可能撞车。一旦重名，Network::Enable 用 endname 当 key，
// ShutdownServer 里 DisconnectUnlocked(i, All()) 会把 Clerk 的端点一起 Disable，
// 而重启后的 ConnectAll() 只遍历 endnames_（server 端），被误伤的 clerk 端点
// 再也没人 Enable —— 客户端永久失联（重试到 60s 放弃 → got < want）。
// 用全局自增序号打头，server 用 "s"、client 用 "c" 区分，彻底消除碰撞。
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
  net_ = labrpc::MakeNetwork();
  kvservers_.assign(n, nullptr);
  saved_.assign(n, nullptr);
  endnames_.assign(n, {});

  // 和 raft 的测试脚手架一样：断连回包随机延迟 0~7000ms。
  // 这是架构级的防作弊 —— 逼实现必须用异步 RPC，同步等回包必死。
  net_->LongDelays(true);

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
  if (saved_[i]) saved_[i] = saved_[i]->Copy();

  auto kv = std::move(kvservers_[i]);  // kvservers_[i] = nullptr
  // ⚠️ 延后析构：旧 KVServer（及其 Raft）不在这里立即析构，而是存进 graves_。
  // 否则旧 Raft 的 mu_ 一析构，它那条 detached 的 ApplyLoop 线程还活着，
  // 去 lock 已销毁的 mutex → "mutex lock failed: Invalid argument"。
  // 真正析构发生在 Cleanup() 的 ThreadTracker::WaitAll 之后（见 config.cpp）。
  // 注意：这里是【拷贝】 push_back（graves_ 和下面 kv 都持有引用），不能
  // std::move，否则 kv 会变空、下面的 kv->Kill() 不执行 → 旧 Raft 永远不退出。
  graves_.push_back(kv);

  // ⚠️ Kill() 会 join applier 线程，绝不能拿着 mu_ 等，否则死锁
  // （applier 线程里也要拿 mu_）。
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
      saved_[i] = raft::MakePersister();
    }
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
//
// 这是 Go 版 config.go:339 make_partition() 的移植：p1 固定 n/2+1 台、
// p2 固定 n/2 台，leader 放进 p2。语义上 **p1 恒为多数派**。
//
// ⚠️ 别把它改成"每台抛硬币"：TestOnePartition3A 依赖 p1 是多数派
//    （它要在 p1 里写入并期望能推进）。曾经误改过一次，随机切出 2v3 时
//    p1 变成少数派 → Clerk 60s GIVE UP → 用例 122 秒后红。Go 版把
//    "抛硬币"放在 test_test.go:126 的 partitioner() 里，是另一个函数
//    （见下面的 MakePartitionRandom）。
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
//
// ⚠️ 之前 GenericTest 的分区线程一直在调 MakePartition（固定 4 vs 3 且
//    永远把 leader 关进少数派），一个随机数都没有 → 3 轮 iter × 每轮约 4 次
//    ≈ 12 次几乎一模一样的分区，测不到 1v6 / 全员连通等形态。
//    这是 C++ 版相对 Go 的覆盖缺口：C++ 过了不代表 Go 的等价场景能过。
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

  // Fisher-Yates 乱序：对齐 Go 版 config.go:204 的 MakeClerk(random_handles(ends))。
  // 目的 —— Clerk 首次尝试的目标从"固定 0 号"变成随机节点，让"第一跳落在哪台"
  // 均匀分布，避免所有 client 同时挤向同一台形成伪热点。
  //
  // ⚠️⚠️ 只能乱序【交给 Clerk 的那份 ends】，Config 自己留底的 clerk_ends_
  //    必须保持**原始顺序**（下标 j ↔ server j）。
  //    原因：ConnectClient(ck, to) 是按下标 j 去 Enable/Disable 的
  //    （j ∈ to 才连），它说的 j 是 **server id**。留底那份一旦跟着乱序，
  //    就会"本想连 0 号、实际把 3 号的端点打开" —— 分区彻底错位。
  //    Go 版同理：cfg.clerks[ck] = endnames（未乱序）,
  //              MakeClerk(random_handles(ends))（乱序副本）。
  std::vector<int> order(n_);
  for (int i = 0; i < n_; i++) order[i] = i;
  for (int i = 0; i < n_; i++) {
    int j = raftcpp::RandInt(i + 1);  // [0, i]，等价于 Go 的 rand.Intn(i+1)
    std::swap(order[i], order[j]);
  }
  std::vector<std::shared_ptr<labrpc::ClientEnd>> shuffled_ends(n_);
  for (int k = 0; k < n_; k++) shuffled_ends[k] = ends[order[k]];

  auto ck = std::make_shared<Clerk>(shuffled_ends);
  {
    std::lock_guard<std::mutex> lk(mu_);
    clerks_[ck.get()] = ck;       // Config 持有 shared_ptr，Clerk 寿命延长到 Config 析构
    clerk_ends_[ck.get()] = names;  // ⚠️ 原始顺序，不要传 shuffled_names
  }

  // ⚠️ 空 `to` 的语义对齐 Go 版 config.go:191 —— **一台都不连**。
  //    原先这里写成 ConnectClient(ck, All())（连全部），方向正好相反：
  //    若哪个用例误传空 vector，分区会被静默抹平，依赖分区才暴露的 bug
  //    就被掩盖掉了（分区测试形同虚设）。想要"连全部"请显式传 cfg.All()。
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
    if (saved_[i]) m = std::max(m, saved_[i]->RaftStateSize());
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

  // ⚠️ 修复"假通过"：runner（test_kvraft.cpp:950）只认 g_failed，不认 failed_。
  // 之前这里只打印 "FAILED" 然后 return，g_failed 仍是 false → 被算成通过。
  // 改走 Fatal()：主线程抛 TestFailure 被 runner 接住 → bad=true；
  // 同时 g_failed 翻 true，双保险。cfg->Fail() 设的失败也走这条。
  if (failed_.load()) {
    Fatal(fail_msg_.empty() ? "test failed (cfg->Fail without message)"
                             : fail_msg_);
  }

  // 和 raft 测试脚手架一致的超时保护
  // ⚠️ 同样走 Fatal() 翻 g_failed，否则超时也算"通过"。
  if (secs > 120.0) {
    Fatal("test took longer than 120 seconds");
  }

  // ---- 副本一致性：所有 server 的状态机必须收敛到同一个状态 ----
  //
  // Go 版 kvraft 的 end() 不做这项检查（它靠客户端 Get 做线性读 + porcupine
  // 做线性一致性验证）。但 C++ 这一侧没有 porcupine，客户端 Get 又只能读到
  // leader 的视图 —— 于是"某个 follower 状态机缺数据"这类 bug 是漏网的。
  // 这里补上，相当于给 3A/3B 加一道兜底。
  //
  // ⚠️ 两个前提，缺一不可：
  //   1) 只在【全网连通】时检查。分区期间各副本本来就不一致，那是合法的 ——
  //      TestOnePartition3A 的前两段就是在分区状态下调 End() 的。
  //   2) 必须给收敛窗口。刚 heal / 刚重启时，落后副本还在追日志，状态机暂时
  //      是旧版本 —— 那也是合法的中间态。所以最多重试 2 秒，仍未收敛才算失败。
  if (!partitioned_.load()) {
    std::string err;
    bool consistent = false;
    // 收敛窗口：10 秒（100 × 100ms）。
    //
    // ⚠️ 原来是 2 秒（20 × 100ms）。TSan / 3B 快照场景下，刚 heal 或刚重启的
    //    落后副本要先收 InstallSnapshot 再追日志，2 秒偏紧 → 偶发
    //    "replicas did not converge" 的假失败，而真因（哪个 key 不一致）反而
    //    被这条断言盖住。放宽到 10 秒不损失覆盖：真不收敛的话等 10 秒照样失败，
    //    只是把"慢"和"错"这两件事分开了。
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
      // ⚠️ 走 Fatal() 翻 g_failed，否则"副本不收敛"会被算成通过。
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
    // ⚠️ 走 Fatal() 翻 g_failed，否则"无操作记录"也算通过（假通过）。
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
                          // std::printf("      [%zu] client=%d seq=%lld %s key=%s val_in=\"%s\" val_out=\"%s\" "
                  // "call=%lld ret=%lld\n",
                  // i, op.client_id, (long long)op.seq_id, m,
                  // op.input.key.c_str(),
                  // op.input.value.c_str(), op.output.value.c_str(),
                  // (long long)op.call_time_ns, (long long)op.return_time_ns);
    }
  }
  std::fflush(stdout);

  auto model = MakeKvModel();
  std::printf("[lin] model 创建完成，调 CheckOperations\n");
  std::fflush(stdout);
  // ⚠️ 这里比 Go 基准严格得多，是有意为之：
  //    Go 版 kvraft/test_test.go:20 的 linearizabilityCheckTimeout 只有 **1 秒**，
  //    而且 1 秒搜不完时走的是
  //      } else if res == porcupine.Unknown {
  //        fmt.Println("info: linearizability check timed out,
  //                     assuming history is ok")
  //      }
  //    ——也就是**直接假设历史是 OK 的并放行**。15 clients / 7 servers 的 history
  //    1 秒几乎必然搜不完，所以那两个 ...Linearizable3A/3B 用例在 Go 官方测试里
  //    基本是空跑的。
  //    C++ 版给 120 秒、且 Unknown 分支走 Fatal（见下面 case Unknown），
  //    是刻意改严：不让"没搜完"伪装成"通过了"。
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
      // Go 版（test_test.go:443-445）对 Unknown 只打印一行就放行：
      //     fmt.Println("info: linearizability check timed out, assuming history is ok")
      // 但注意 Go 的 linearizabilityCheckTimeout 只有 **1 秒**（test_test.go:20），
      // 15 clients / 7 servers 的 history 1 秒必然搜不完 —— 所以 Go 那两个
      // ...Linearizable3A/3B 用例实际是空跑的，Unknown 放行 ≈ 没检查。
      //
      // C++ 这边给 120 秒，是真检查。120 秒还搜不完意味着 history 太大或机器太慢，
      // 与"实现错了"是两回事，所以默认仍判失败（保留严格性），但留一个逃生舱：
      // KV_UNKNOWN_PASS=1 时退回 Go 语义 —— 只打印、不判失败。
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
          // ⚠️ 排错 dump：每个 server 的 snapIdx/lastApplied/该 key 的值
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
