// config.cpp —— 测试脚手架的实现

#include "config.h"

#include <algorithm>
#include <cstdio>

namespace raft {

// ===========================================================================
// 构造 / 析构
// ===========================================================================

Config::Config(int n, bool unreliable) : n_(n) {
  net_ = labrpc::MakeNetwork(); // 造一个全新的网络
  apply_err_.assign(n, "");   // 按节点数n初始化为空字符串
  connected_.assign(n, false);
  saved_.assign(n, nullptr); // n块模拟硬盘
  endnames_.assign(n, {});
  logs_.assign(n, {});
  apply_chs_.assign(n, nullptr);
  rafts_.assign(n, nullptr);
  start_ = raftcpp::Now();

  net_->Reliable(!unreliable);  // 模拟不可靠网络环境
  // 断连时随机延迟 0~7000ms。目的是惩罚"在持有锁或主线程里同步发 RPC"
  // 的实现 —— 那种写法在真实网络里会卡死。
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
std::map<int, Command> Config::GetState(int i) {
  std::lock_guard<std::mutex> lk(mu_);
  return (i >= 0 && i < n_) ? logs_[i] : std::map<int, Command>{};
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
    // 先复制一份持久化状态，免得旧实例一边还在写、一边我们读
    if (saved_[i]) saved_[i] = saved_[i]->Copy();
    rf = rafts_[i];
    rafts_[i] = nullptr;
    if (rf) graves_.push_back(rf);  // 延后析构，等它自己的线程收工
  }
  if (rf) rf->Kill();
  // 多此一举
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (saved_[i]) {
      std::string raftlog = saved_[i]->ReadRaftState();
      saved_[i] = std::make_shared<Persister>();
      saved_[i]->SaveRaftState(raftlog);
    }
  }
}

// ===========================================================================
// 快照感知状态机：把"已 apply 的状态"（index → command）序列化成快照 blob，
// 以及从 blob 反序列化回来（InstallSnapshot 装快照时调用）。
// ===========================================================================
namespace {
std::string EncodeKV(const std::map<int, std::string>& kv) {
  labrpc::Encoder e;
  e.Int(static_cast<int>(kv.size()));
  for (const auto& p : kv) e.Int(p.first).Bytes(p.second);
  return e.Take();
}
std::map<int, std::string> DecodeKV(const std::string& s) {
  std::map<int, std::string> kv;
  if (s.empty()) return kv;
  labrpc::Decoder d(s);
  int n = 0;
  if (!d.Int(n)) return kv;
  for (int i = 0; i < n; i++) {
    int k = 0;
    std::string v;
    if (!(d.Int(k) && d.Bytes(v))) return kv;
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
      saved_[i] = MakePersister();
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
      if (m.command_valid) {
        // 可能要触发自动压缩；先收集好数据，再释放 config 锁再调 Raft
        // （避免"持 config 锁跨进 raft 锁"，也避免快照编码时持锁太久）。
        bool need_snap = false;
        int snap_idx = 0;
        std::string snap;
        {
          std::lock_guard<std::mutex> lk(mu_);
          std::string err;
          for (int j = 0; j < n_; j++) {
            auto it = logs_[j].find(m.command_index);
            if (it != logs_[j].end() && it->second != m.command) {
              char buf[256];
              std::snprintf(buf, sizeof(buf),
                            "commit index=%d server=%d %s != server=%d %s",
                            m.command_index, i, m.command.c_str(), j,
                            it->second.c_str());
              err = buf;
            }
          }
          bool prevok = logs_[i].count(m.command_index - 1) > 0;
          logs_[i][m.command_index] = m.command;
          if (m.command_index > max_index_) max_index_ = m.command_index;
          if (m.command_index > 1 && !prevok) {
            char buf[128];
            std::snprintf(buf, sizeof(buf), "server %d apply out of order %d", i,
                          m.command_index);
            err = buf;
          }
          if (!err.empty()) ApplyErrLocked(i, err);

          // 快照感知状态机：本节点日志过长就主动压缩（Lab 3 的 maybeSnapshot）。
          // 只压缩已 apply 的部分（LastApplied），保证不截断未 apply 的日志。
          //
          // ⚠️⚠️ 【这段是 C++ 独有的，Go 基准里根本不存在】⚠️⚠️
          //
          //   本项目的 Go 基准是 2020 版（只有 2A/2B/2C，没有 2D）。实测：
          //     grep -n "snapshot" src/raft/config.go   → 零处命中
          //     src/raft/raft.go:950                    → // rf.applyCh <- snapMsg（被注释）
          //     src/raft/config.go:172-174              → 只有 CommandValid 分支，
          //                                                连 SnapshotValid 都没有
          //   也就是说 Go 2020 的 harness 既不主动触发 Snapshot，也不主动
          //   CondInstallSnapshot —— 整条 2D 管道在 Go 侧是空的。
          //
          //   本文件的 4 个 TestSnapshot*2D 来自 2021 官方，但它们验证的这条
          //   管道（harness 触发快照 + harness 安装快照）是 C++ 自己搭的，
          //   【没有 Go 侧参照物】。所以：
          //     ✅ 能抓回归（改动后行为变了会被发现）
          //     ❌ 不能证明与官方 2D 语义一致
          //   → 2D 的绿灯含金量低于 2A/2B/2C，心里要有数。
          //
          //   另一个已知分叉：下面是 **merge**（logs_[i] 不清空直接合并），
          //   canonical 6.824 是 **replace**（用快照整体替换状态机）。
          if (snapshot_compaction_ && rafts_[i]->LogSize() > kSnapshotThreshold) {
            snap_idx = rafts_[i]->LastApplied();
            if (snap_idx > 0) {
              snap = EncodeKV(logs_[i]);  // 序列化当前已 apply 状态机状态
              need_snap = true;
            }
          }
        }  // ---- 释放 config 锁 ----
        if (need_snap) rafts_[i]->Snapshot(snap_idx, snap);
      } else if (m.snapshot_valid) {
        // 收到 leader 的 InstallSnapshot：确认能否安全安装，并把快照装进状态机。
        // 注意参数顺序：CondInstallSnapshot(index, term, snapshot)
        bool installed = rafts_[i]->CondInstallSnapshot(
            m.snapshot_index, m.snapshot_term, m.snapshot);
        if (installed) {
          // 快照字节就是被压缩掉的那段状态机状态（index→command），重水化进来。
          std::map<int, std::string> st = DecodeKV(m.snapshot);
          std::lock_guard<std::mutex> lk(mu_);
          for (auto& p : st) logs_[i][p.first] = p.second;
        }
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
  //
  // ⚠️⚠️ 这一行是 C++ 版【独有的】，Go 基准没有 —— 别被老注释误导：
  //    Go 版 raft/config.go:138-234 的 start1() 从头到尾**没有** cfg.connect(i)，
  //    末尾只做 net.AddServer(i, svc) 就收工了。而 MakeEnd 新建的端点默认
  //    enabled=false（labrpc.go:324），所以 Go 那边 start1 之后节点 i 仍处在
  //    【断网】状态，必须由用例显式 cfg.connect(i) 才上线。
  //
  //    差别的后果：C++ 里 Start1(i) 会顺手把 i 连上网；照抄 Go 用例时如果漏了
  //    Start1 之后的 Disconnect 步骤，Go 那边节点是离线的、C++ 这边却在线，
  //    依赖"i 必须离线"的分区时序会被静默放行。
  //    当前测试集（test_raft.cpp）已逐条显式 Connect/Disconnect，所以暂时 benign；
  //    但**移植新用例时要格外小心** —— 想保 Go 的语义就必须自己补 Disconnect。
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

    std::map<int, std::vector<int>> leaders;
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

    int last_term_with_leader = -1;
    for (const auto& kv : leaders) {
      if (kv.second.size() > 1) {
        char buf[128];
        std::snprintf(buf, sizeof(buf), "term %d has %zu (>1) leaders",
                      kv.first, kv.second.size());
        Fatal(buf);
      }
      if (kv.first > last_term_with_leader) last_term_with_leader = kv.first;
    }
    if (!leaders.empty()) return leaders[last_term_with_leader][0];
  }
  Fatal("expected one leader, got none");
}

//  全员任期一致
int Config::CheckTerms() {
  int term = -1;
  for (int i = 0; i < n_; i++) {
    std::shared_ptr<Raft> rf;
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (!connected_[i]) continue;
      rf = rafts_[i];
    }
    if (!rf) continue;
    int xterm = rf->GetState().first;
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
        int term = -1;
        bool is_leader = false;
        if (r) {
          auto st = r->GetState();
          term = st.first;
          is_leader = st.second;
        }
        std::fprintf(stderr, "  server=%d connected=%d term=%d leader=%d %s\n",
                     j, Connected(j) ? 1 : 0, term, is_leader ? 1 : 0,
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

std::pair<int, std::optional<Command>> Config::NCommitted(int index) {
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
                    "committed values do not match: index %d, %s, %s", index,
                    cmd->c_str(), it->second.c_str());
      Fatal(buf);
    }
    count++;
    cmd = it->second;
  }
  return {count, cmd};
}

std::optional<Command> Config::Wait(int index, int n, int start_term) {
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
    std::snprintf(buf, sizeof(buf), "only %d decided for index %d; wanted %d",
                  nd, index, n);
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

int Config::One(const Command& cmd, int expected_servers, bool retry) {
  auto t0 = raftcpp::Now();
  int starts = 0;
  while (raftcpp::SecondsSince(t0) < 10) {
    int index = -1;
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
      while (raftcpp::SecondsSince(t1) < 2) {
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
  int ncmds;
  {
    std::lock_guard<std::mutex> lk(mu_);
    ncmds = max_index_ - max_index0_;
  }
  std::printf("  ... Passed --  %4.1f  %d %4d %7lld %4d\n", t, n_, nrpc,
              static_cast<long long>(nbytes), ncmds);
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
  CheckTimeout();
}

}  // namespace raft
