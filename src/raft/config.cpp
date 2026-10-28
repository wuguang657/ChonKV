// config.cpp —— 测试脚手架的实现

#include "config.h"

#include <algorithm>
#include <cstdio>
#include <dirent.h>

#include "../labrpc/transport_factory.h"  // labrpc::MakeTransport：按 LABRPC_TRANSPORT 选后端
#include "../persist/state/persister_factory.h"  // persist::PersistFileBacked / MakePersister：按 LABRPC_PERSIST 选后端

namespace raft {
using namespace persist;

static std::atomic<int> g_cfg_seq{0};
// TSan 插桩令 raft 事件处理慢 5~15×。One() 内层等待（原始 2s）在成员变更 / 不可靠网
// 场景下会被拖超，把“TSan 慢调度”误判成“日志达不成一致”——属于测试侧假阳性，
// 而非 raft 逻辑 bug（TSan 本身也没报 data race，只报了 agreement 超时）。
// 仅在 TSan 下放宽内层等待，非 TSan 路径零变化（与 KV 那套“改测试本身”思路一致）。
#if defined(__SANITIZE_THREAD__) || \
    (defined(__has_feature) && __has_feature(thread_sanitizer))
constexpr double kOneInnerWaitSec = 20.0;  // 非 TSan 的 2s ×10，覆盖典型 TSan 减速
#else
constexpr double kOneInnerWaitSec = 2.0;
#endif

// 外层总窗口：必须 >= 内层等待的整数倍，否则内层 20s 会耗尽整个窗口、剥夺 one() 的重试节奏。
// TSan 下事件处理慢 5~15x，分区愈合类用例(TestRejoin2B)初期 leader 动荡，必须靠多次重试
// 逮住稳定后的 leader —— 故外层随 TSan 放宽到 40s(内层 20s -> 至少 2 次重试)，非 TSan 保持 10s。
#if defined(__SANITIZE_THREAD__) || \
    (defined(__has_feature) && __has_feature(thread_sanitizer))
constexpr double kOneOuterWaitSec = 40.0;
#else
constexpr double kOneOuterWaitSec = 10.0;
#endif
// ===========================================================================
// 构造 / 析构
// ===========================================================================

Config::Config(int n, bool unreliable, bool file_backed)
    : n_(n), file_backed_(PersistFileBacked(file_backed)) {
  net_ = labrpc::MakeTransport(); // 造一个全新的网络（默认 in-proc，可切真实 socket）

  // file-backed persister：仅文件模式需要；本场景建独立临时目录根（pid+seq 保证不重名）
  if (file_backed_) {
    // 撞名防御（与 persist::MakePersister()/CopyDir 的 access 换号逻辑对称）
    do {
      raft_dir_base_ =
          "/tmp/cpp6raft_" + std::to_string(::getpid()) + "_" + std::to_string(++g_cfg_seq);
    } while (persist::FileExists(raft_dir_base_));
    persist::MkdirAll(raft_dir_base_);
  }

  apply_err_.assign(n, "");   // 按节点数n初始化为空字符串
  connected_.assign(n, false);
  saved_.assign(n, nullptr); // n块模拟硬盘
  endnames_.assign(n, {});
  logs_.assign(n, {});
  apply_chs_.assign(n, nullptr);
  rafts_.assign(n, nullptr);
  start_ = raftcpp::Now();

  net_->Reliable(!unreliable);  // 模拟不可靠网络环境
  // "端点仍通、但目标 server 已删"的失败回包随机延迟 0~7000ms（断连是立即失败）。
  // 目的是惩罚"在持有锁或主线程里同步发 RPC"的实现 —— 那种写法在真实网络里会卡死。
  net_->LongDelays(true);

  for (int i = 0; i < n_; i++) Start1(i);
  for (int i = 0; i < n_; i++) Connect(i);
}

Config::~Config() { Cleanup(); }

[[noreturn]] void Config::Fatal(const std::string& msg) {
  throw TestFailure{msg};
}

void Config::CheckTimeout() {
  if (raftcpp::SecondsSince(start_) > 120.0) {
    Fatal("test took longer than 120 seconds");
  }
}

std::shared_ptr<Raft> Config::GetRaft(int i) {
  std::lock_guard<std::mutex> lk(mu_);
  return (i >= 0 && i < n_) ? rafts_[i] : nullptr;
}

// 检查某台机器是否插线了
bool Config::Connected(int i) {
  std::lock_guard<std::mutex> lk(mu_);
  return (i >= 0 && i < n_) ? connected_[i] : false;
}
// 返回某个节点的所有logs
std::map<int64_t, Command> Config::GetState(int i) {
  std::lock_guard<std::mutex> lk(mu_);
  return (i >= 0 && i < n_) ? logs_[i] : std::map<int64_t, Command>{};
}

void Config::ApplyErrLocked(int i, const std::string& err) {
  std::fprintf(stderr, "apply error: %s\n", err.c_str());
  apply_err_[i] = err;
}

// ===========================================================================
// 节点操作
// ===========================================================================

void Config::Crash1(int i) {
  Disconnect(i);     // 把第i个机器网线拔掉
  // 在网络层把第i个机器的端点名删除，置nullptr，为什么不删key？是为了区分
  // "这台机从来没注册过"和"这台机注册过但被移除了"两种状态，便于排错。
  // 删掉服务端rpc，客户端rpc发来消息找不到了就
  net_->DeleteServer(i);  // 禁止再有客户端连到这台

  // 磁盘内容（raft_state）原样保留下来给重启用
  std::shared_ptr<Raft> rf;
  {
    std::lock_guard<std::mutex> lk(mu_);
    rf = rafts_[i];
    rafts_[i] = nullptr;
    // 克隆必须在旧实例【仍在自己的锁保护下】时做。
    if (saved_[i]) {
      if (rf) {
        saved_[i] = rf->CopyPersisterUnderLock();
      } else {
        saved_[i] = saved_[i]->Copy();  // 已无存活实例，无并发写盘，直接克隆
      }
    }
    if (rf) graves_.push_back(rf);  // 延后析构，等它自己的线程收工
  }
  if (rf) rf->Kill();
}

// ===========================================================================
// 快照感知状态机：把"已 apply 的状态"（index → command）序列化成快照 blob，
// 以及从 blob 反序列化回来（InstallSnapshot 装快照时调用）。
// ===========================================================================
namespace {
std::string EncodeKV(const std::map<int64_t, std::string>& kv) {
  labrpc::Encoder e;
  e.Int(static_cast<int>(kv.size()));
  for (const auto& p : kv) e.Zigzag(p.first).Bytes(p.second);
  return e.Take();
}
std::map<int64_t, std::string> DecodeKV(const std::string& s) {
  std::map<int64_t, std::string> kv;
  if (s.empty()) return kv;
  labrpc::Decoder d(s);
  int n = 0;
  if (!d.Int(n)) return kv;
  for (int i = 0; i < n; i++) {
    int64_t k = 0;
    std::string v;
    if (!(d.Zigzag(k) && d.Bytes(v))) return kv;
    kv[k] = std::move(v);
  }
  return kv;
}
}  // namespace

void Config::Start1(int i) {
  // 模拟第i个机器网线拔了、服务器挂了、但磁盘内容（raft_state）原样保留下来给重启用，且保证旧实例残留的线程不会偷偷改磁盘。
  Crash1(i);

  // 换一套全新的端点名：这样旧实例残留的 ClientEnd 就发不出消息了,本身线程也被kill了，双重防御
  {
    std::lock_guard<std::mutex> lk(mu_); // 互斥锁
    endnames_[i].clear();
    for (int j = 0; j < n_; j++) {
      // 记录第i个节点与其他节点的端点名，包括他自己，比如节点0到节点1的端点名
      // std::vector<std::vector<std::string>> endnames_;
      endnames_[i].push_back(raftcpp::RandString(20));
    }
  }

  // 为服务器i的每个endname新建一个服务器i的发信客户端rpc，类似与新建n_个网卡能连到服务端rpc
  std::vector<std::shared_ptr<labrpc::ClientEnd>> ends(n_);
  for (int j = 0; j < n_; j++) {
    std::string name;
    {
      std::lock_guard<std::mutex> lk(mu_);
      name = endnames_[i][j];
    }
    ends[j] = net_->MakeEnd(name);
    net_->Connect(name, j);
  }

  // 这段是给新 Raft 配一块独立的"模拟磁盘"（首次启动就造块空盘，重启就 Copy 一份带历史状态的副本）
  // 把 Crash1 存好的状态再 Copy 一份给新 Raft (见该函数第一行)
  std::shared_ptr<Persister> persister;
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (saved_[i]) {
      saved_[i] = saved_[i]->Copy();
    } else {
      // 首次启动：按 harness 选定的模式造 persister
      //   file_backed_  → 文件模式（每节点独立目录，落盘）
      //   !file_backed_ → 内存模式（原行为，无磁盘 IO）
      saved_[i] = file_backed_
          ? MakePersister(raft_dir_base_ + "/node_" + std::to_string(i))
          : MakePersister();
    }
    persister = saved_[i];
  }

  // 新建一个 apply channel，并起线程消费它。
  // 这个线程做的事非常重要：检查"同一个下标在不同机器上值是否一致"，
  // 以及"是否按顺序提交"。这是 Raft 安全性最核心的检查。
  auto apply_ch = std::make_shared<raftcpp::Chan<ApplyMsg>>();
  {
    std::lock_guard<std::mutex> lk(mu_);
    apply_chs_[i] = apply_ch;
  }
  appliers_.emplace_back([this, i, apply_ch]() {
    ApplyMsg m;
    while (apply_ch->Pop(m)) {
      // Crash1 会在持锁状态下把 rafts_[i] 置空（本文件 Crash1 内），Kill 在其后；
      // 而 chan 的 Pop 是【排空语义】（common/chan.h：Close 之后先把残留消息吐完
      // 才返回 false）→ Kill 之后 applier 仍会继续处理残留 ApplyMsg，此时
      // rafts_[i] 已被置空 → 锁外解引用空指针 = 段错误。
      // 修法：锁内取 shared_ptr 本地副本（引用计数 +1 保命），后续全程用副本；
      // 副本为空说明本节点已崩溃，直接丢弃这条残留消息。
      std::shared_ptr<Raft> rf;
      {
        std::lock_guard<std::mutex> lk(mu_);
        rf = rafts_[i];
      }
      if (!rf) continue;
      if (m.command_valid) {
        // 可能要触发自动压缩；先收集好数据，再释放 config 锁再调 Raft
        // （避免"持 config 锁跨进 raft 锁"，也避免快照编码时持锁太久）。
        bool need_snap = false;
        int64_t snap_idx = 0;
        std::string snap;
        {
          std::lock_guard<std::mutex> lk(mu_);
          std::string err;
          for (int j = 0; j < n_; j++) {
            auto it = logs_[j].find(m.command_index);
            if (it != logs_[j].end() && it->second != m.command) {
              char buf[256];
              std::snprintf(buf, sizeof(buf),
                            "commit index=%lld server=%d %s != server=%d %s",
                            static_cast<long long>(m.command_index), i,
                            m.command.c_str(), j, it->second.c_str());
              err = buf;
            }
          }
          bool prevok = logs_[i].count(m.command_index - 1) > 0;
          logs_[i][m.command_index] = m.command;
          if (m.command_index > max_index_) max_index_ = m.command_index;
          if (m.command_index > 1 && !prevok) {
            char buf[128];
            std::snprintf(buf, sizeof(buf), "server %d apply out of order %lld",
                          i, static_cast<long long>(m.command_index));
            err = buf;
          }
          if (!err.empty()) ApplyErrLocked(i, err);

          // 快照感知状态机：本节点日志过长就主动压缩（Lab 3 的 maybeSnapshot）。
          // 只压缩已 apply 的部分（LastApplied），保证不截断未 apply 的日志。
          if (snapshot_compaction_ && rf->LogSize() > kSnapshotThreshold) {
            snap_idx = rf->LastApplied();
            if (snap_idx > 0) {
              snap = EncodeKV(logs_[i]);  // 序列化当前已 apply 状态机状态
              need_snap = true;
            }
          }
        }  // ---- 释放 config 锁 ----
        if (need_snap) rf->Snapshot(snap_idx, snap);
      } else if (m.snapshot_valid) {
        // 收到 leader 的 InstallSnapshot：把快照重水化进状态机。
        // 单相设计下 raft 已在 handler 内完成截断/推进/落盘，并用 last_applied_
        // 挡掉陈旧快照；apply channel FIFO，且 blob 是该 index 的全量状态，
        // 直接安装即收敛（与 kvraft server.cpp 的 snapshot_valid 分支同一模式）。
        std::map<int64_t, std::string> st = DecodeKV(m.snapshot);
        std::lock_guard<std::mutex> lk(mu_);
        for (auto& p : st) logs_[i][p.first] = p.second;
      }
    }
  });

  // 传入该服务器id==i可以连接到的服务器的所有客户端rpc
  auto rf = std::make_shared<Raft>(ends, i, persister, apply_ch);
  rf->Start();  // 构造完才能起后台线程（要用 shared_from_this）
  {
    std::lock_guard<std::mutex> lk(mu_);
    rafts_[i] = rf;   // 引用计数+1
  }

  // 反射 = 程序在运行时能"看自己的结构"——知道某个类型有哪些方法、字段叫什么名字、参数类型是什么。
  // c++ 没有go那样的反射方法
  auto svc = MakeRaftService(rf);
  auto srv = std::make_shared<labrpc::Server>();
  srv->AddService(svc);
  net_->AddServer(i, srv);

  // 新实例已上线，重新接通它与其它节点之间的网络链路。
  // 否则 Crash1(i) 里 Disconnect(i) 关掉的"其它节点→i"的链路
  // （endnames_[j][i]）会一直保持 disabled，leader 发给 i 的 RPC
  // （AppendEntries / InstallSnapshot）永远 !ok，i 永远追不上来。
  Connect(i);
}

void Config::Connect(int i) {
  // 把节点 i "插上网"，并和所有"同样在线"的节点 j 把 RPC 双向链路都打开。
  std::lock_guard<std::mutex> lk(mu_);
  connected_[i] = true;
  for (int j = 0; j < n_; j++) {
    if (!connected_[j]) continue;
    // endnames_[i][j] = "节点 i 用来给节点 j 发消息的端点名"（随机 20 字符串）
    // rpc双向两条线
    if (endnames_[i].size() > static_cast<size_t>(j)) {
      net_->Enable(endnames_[i][j], true);
    }
    if (endnames_[j].size() > static_cast<size_t>(i)) {
      net_->Enable(endnames_[j][i], true);
    }
  }
}

// 相当于把第i个机器网线拔掉，断连是双向的，i->j和j->i都断开
void Config::Disconnect(int i) {
  std::lock_guard<std::mutex> lk(mu_);
  connected_[i] = false;
  for (int j = 0; j < n_; j++) {
    // static_cast 编译器显示转换
    if (endnames_[i].size() > static_cast<size_t>(j)) {
      net_->Enable(endnames_[i][j], false);
    }
    if (endnames_[j].size() > static_cast<size_t>(i)) {
      net_->Enable(endnames_[j][i], false);
    }
  }
}

// ===========================================================================
// 断言
// ===========================================================================
// 轮询找唯一的 leader
int Config::CheckOneLeader() {
  for (int iters = 0; iters < 10; iters++) {
    raftcpp::SleepMs(450 + raftcpp::RandInt(100));

    std::map<int64_t, std::vector<int>> leaders;
    for (int i = 0; i < n_; i++) {
      std::shared_ptr<Raft> rf;
      {
        std::lock_guard<std::mutex> lk(mu_);
        if (!connected_[i]) continue;
        rf = rafts_[i];
      }
      if (!rf) continue;
      auto st = rf->GetState();
      if (st.second) leaders[st.first].push_back(i);
    }

    int64_t last_term_with_leader = -1;
    for (const auto& kv : leaders) {
      if (kv.second.size() > 1) {
        char buf[128];
        std::snprintf(buf, sizeof(buf), "term %lld has %zu (>1) leaders",
                      static_cast<long long>(kv.first), kv.second.size());
        Fatal(buf);
      }
      if (kv.first > last_term_with_leader) last_term_with_leader = kv.first;
    }
    if (!leaders.empty()) return leaders[last_term_with_leader][0];
  }
  Fatal("expected one leader, got none");
}

//  全员任期一致
int64_t Config::CheckTerms() {
  int64_t term = -1;
  for (int i = 0; i < n_; i++) {
    std::shared_ptr<Raft> rf;
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (!connected_[i]) continue;
      rf = rafts_[i];
    }
    if (!rf) continue;
    int64_t xterm = rf->GetState().first;
    if (term == -1) {
      term = xterm;
    } else if (term != xterm) {
      Fatal("servers disagree on term");
    }
  }
  return term;
}

void Config::CheckNoLeader() {
  for (int i = 0; i < n_; i++) {
    std::shared_ptr<Raft> rf;
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (!connected_[i]) continue;
      rf = rafts_[i];
    }
    if (!rf) continue;
    if (rf->GetState().second) {
      // 把所有节点的状态打出来，方便定位"为什么这里还有 leader"
      std::fprintf(stderr, "--- CheckNoLeader 现场 ---\n");
      for (int j = 0; j < n_; j++) {
        auto r = GetRaft(j);
        int64_t term = -1;
        bool is_leader = false;
        if (r) {
          auto st = r->GetState();
          term = st.first;
          is_leader = st.second;
        }
        std::fprintf(stderr, "  server=%d connected=%d term=%lld leader=%d %s\n",
                     j, Connected(j) ? 1 : 0, static_cast<long long>(term), is_leader ? 1 : 0,
                     r ? r->LogStatus().c_str() : "(null)");
      }
      std::fprintf(stderr, "--------------------------\n");
      char buf[128];
      std::snprintf(buf, sizeof(buf),
                    "expected no leader, but %d claims to be leader", i);
      Fatal(buf);
    }
  }
}

std::pair<int, std::optional<Command>> Config::NCommitted(int64_t index) {
  std::lock_guard<std::mutex> lk(mu_);
  for (int i = 0; i < n_; i++) {
    if (!apply_err_[i].empty()) Fatal(apply_err_[i]);
  }
  int count = 0;
  std::optional<Command> cmd;
  for (int i = 0; i < n_; i++) {
    auto it = logs_[i].find(index);
    if (it == logs_[i].end()) continue;
    if (count > 0 && cmd && *cmd != it->second) {
      char buf[256];
      std::snprintf(buf, sizeof(buf),
                    "committed values do not match: index %lld, %s, %s",
                    static_cast<long long>(index),
                    cmd->c_str(), it->second.c_str());
      Fatal(buf);
    }
    count++;
    cmd = it->second;
  }
  return {count, cmd};
}

std::optional<Command> Config::Wait(int64_t index, int n, int64_t start_term) {
  int64_t to = 10;
  for (int iters = 0; iters < 30; iters++) {
    int nd = NCommitted(index).first;
    if (nd >= n) break;
    raftcpp::SleepMs(to);
    if (to < 1000) to *= 2;
    if (start_term > -1) {
      for (int i = 0; i < n_; i++) {
        auto rf = GetRaft(i);
        if (rf && rf->GetState().first > start_term) {
          return std::nullopt;  // 有人进入更高任期，这次提交没戏了
        }
      }
    }
  }
  auto [nd, cmd] = NCommitted(index);
  if (nd < n) {
    char buf[128];
    std::snprintf(buf, sizeof(buf), "only %d decided for index %lld; wanted %d",
                  nd, static_cast<long long>(index), n);
    Fatal(buf);
  }
  return cmd;
}

// 把所有节点的内部状态打到 stderr，排错用
void Config::DumpState(const char* where) {
  std::fprintf(stderr, "--- 状态转储 @ %s ---\n", where);
  for (int j = 0; j < n_; j++) {
    auto r = GetRaft(j);
    std::fprintf(stderr, "  connected=%d %s\n", Connected(j) ? 1 : 0,
                 r ? r->LogStatus().c_str() : "(null)");
  }
  {
    std::lock_guard<std::mutex> lk(mu_);
    for (int j = 0; j < n_; j++) {
      std::string s;
      for (const auto& kv : logs_[j]) {
        s += std::to_string(kv.first) + "=" + kv.second + " ";
      }
      std::fprintf(stderr, "  已提交 server=%d: %s\n", j, s.c_str());
    }
  }
  std::fprintf(stderr, "------------------------------\n");
}

int64_t Config::One(const Command& cmd, int expected_servers, bool retry) {
  auto t0 = raftcpp::Now();
  int starts = 0;
  while (raftcpp::SecondsSince(t0) < kOneOuterWaitSec) {
    int64_t index = -1;
    for (int si = 0; si < n_; si++) {
      starts = (starts + 1) % n_;
      std::shared_ptr<Raft> rf;
      {
        std::lock_guard<std::mutex> lk(mu_);
        if (connected_[starts]) rf = rafts_[starts];
      }
      if (!rf) continue;
      StartResult r = rf->Start(cmd);
      if (r.is_leader) {
        index = r.index;
        break;
      }
    }

    if (index != -1) {
      auto t1 = raftcpp::Now();
      while (raftcpp::SecondsSince(t1) < kOneInnerWaitSec) {
        auto [nd, cmd1] = NCommitted(index);
        if (nd > 0 && nd >= expected_servers && cmd1 && *cmd1 == cmd) {
          return index;
        }
        raftcpp::SleepMs(20);
      }
      if (!retry) {
        DumpState("one() failed");
        Fatal("one(" + cmd + ") failed to reach agreement");
      }
    } else {
      raftcpp::SleepMs(50);
    }
  }
  DumpState("one() failed (10s timeout)");
  Fatal("one(" + cmd + ") failed to reach agreement");
}

// ===========================================================================
// 起止
// ===========================================================================

void Config::Begin(const std::string& description) {
  std::printf("%s ...\n", description.c_str());
  std::fflush(stdout);
  t0_ = raftcpp::Now();
  rpcs0_ = net_->GetTotalCount(); // 初始 RPC 次数
  bytes0_ = net_->GetTotalBytes(); // 初始字节数
  std::lock_guard<std::mutex> lk(mu_);
  max_index0_ = max_index_; // 初始已提交索引
}

void Config::End() {
  CheckTimeout();
  {
    std::lock_guard<std::mutex> lk(mu_);
    for (int i = 0; i < n_; i++) {
      if (!apply_err_[i].empty()) Fatal(apply_err_[i]);
    }
  }
  double t = raftcpp::SecondsSince(t0_);
  int nrpc = net_->GetTotalCount() - rpcs0_;
  int64_t nbytes = net_->GetTotalBytes() - bytes0_;
  int64_t ncmds;
  {
    std::lock_guard<std::mutex> lk(mu_);
    ncmds = max_index_ - max_index0_;
  }
  std::printf("  ... Passed --  %4.1f  %d %4d %7lld %4lld\n", t, n_, nrpc,
              static_cast<long long>(nbytes), static_cast<long long>(ncmds));
  std::fflush(stdout);
  // std::printf 默认是行缓冲：输出先存在内存缓冲区里，遇到 \n 或缓冲区满了才真正写出去。
  // 但当 stdout 不是终端（比如被重定向到文件、或跑在 CI 里）时，会变成全缓冲，即使有 \n 也不立刻输出。
  // 测试框架经常在 Fatal 时直接 throw 终止进程，或者测试中途崩溃 —— 如果不 fflush，最后那行 "...Passed" 可能还躺在缓冲区里没写出去，你就看不到测试到底跑到哪一步、通过没有。
}

void Config::Cleanup() {
  if (cleaned_) return;
  cleaned_ = true;

  for (int i = 0; i < n_; i++) {
    auto rf = GetRaft(i);
    if (rf) rf->Kill();
  }
  {
    std::lock_guard<std::mutex> lk(mu_);
    for (auto& r : graves_) {
      if (r) r->Kill();
    }
  }

  // 先停网络：所有还在等 RPC 的线程会立刻拿到 false 返回
  net_->Cleanup();

  // 收掉消费 applyCh 的线程（Raft::Kill 已经把 channel 关了）
  for (auto& t : appliers_) {
    if (t.joinable()) t.join();
  }

  // 等所有 detached 线程退出，之后才能安全析构 Raft 对象
  raftcpp::ThreadTracker::Instance().WaitAll(10000);

  {
    std::lock_guard<std::mutex> lk(mu_);
    rafts_.clear();
    graves_.clear();
  }
  if (raftcpp::SecondsSince(start_) > 120.0) {
    std::fprintf(stderr,
                 "warning: test took longer than 120 seconds "
                 "(reported at cleanup, not fatal here)\n");
  }

  if (!raft_dir_base_.empty()) RemoveTree(raft_dir_base_);
}

}  // namespace raft
