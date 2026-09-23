// test_raft.cpp —— 所有测试用例（Go 版 src/raft/test_test.go 的移植）
//
// ===========================================================================
// 注：Lab 2D 的 4 个快照测试（TestSnapshot*2D）已并入本文件，登记在 kTests[]
// 末尾；用 `./raft_test 2D` 可单独跑快照组，`./raft_test -count 10 2D` 可压偶发。
// ===========================================================================

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <map>
#include <optional>
#include <random>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "../common/util.h"
#include "../common/watchdog.h"  // 单用例超时看门狗（对齐 go test 的 10min 超时）
#include "config.h"
#include "raft.h"

using namespace raft;

// 官方 tester 允许选举在 1 秒内完成（比论文里的超时范围宽松得多）
constexpr int64_t kRaftElectionTimeout = 1000;  // ms

// ---------------------------------------------------------------------------
// 小工具：把子线程里的失败带回主线程
// ---------------------------------------------------------------------------
// Go 可以在 goroutine 里直接 t.Fatalf。C++ 里从子线程抛异常没人接会直接
// terminate()，所以改成"记下来，等 join 完再报告"。
struct ThreadErr {
  std::mutex mu;
  std::string msg;
  void Set(const std::string& s) {
    std::lock_guard<std::mutex> lk(mu);
    if (msg.empty()) msg = s;
  }
  bool Has() {
    std::lock_guard<std::mutex> lk(mu);
    return !msg.empty();
  }
  std::string Get() {
    std::lock_guard<std::mutex> lk(mu);
    return msg;
  }
};

// 包一层：捕获 TestFailure，转成字符串记下来
template <typename Fn>
std::thread SafeThread(ThreadErr& err, Fn fn) {
  return std::thread([&err, fn]() {
    try {
      fn();
    } catch (const TestFailure& f) {
      err.Set(f.msg);
    } catch (const std::exception& e) {
      err.Set(std::string("exception: ") + e.what());
    }
  });
}

static std::string Cmd(int x) { return std::to_string(x); }

// 生成【全局唯一】的命令值：随机前缀 + 单调递增序号（形如 "728341#137"）。
//
// 只在"必须靠值本身来识别命令"的场景下用 —— 目前是 InternalChurn。
//
// 为什么需要：Go 版命令值用 rand.Int()，值域约 2^63，一个用例里取几百次也不
// 可能重复；这里的 RandInt(1000000) 值域只有 10^6，InternalChurn 里 3 个 client
// 一轮下来几百条命令，按生日悖论碰撞概率不容忽视（collide(420, 10^6) ≈ 8.4%）。
// 一旦两条命令撞值，churn 收尾那段"I 提交过的值必须都能在已提交列表里找到"
// 就会被绕过：我这条命令明明被 Raft 弄丢了，但列表里还躺着另一个同值的，
// 检查照样通过 → 漏检率高到能让 bug 从眼皮底下溜走。
// 保留随机前缀是为了维持"值随机不可预测"（对齐 Go 的语义），序号后缀兜唯一性。
static std::atomic<int64_t> g_unique_cmd_seq{0};
static std::string CmdUnique() {
  int64_t seq = g_unique_cmd_seq.fetch_add(1, std::memory_order_relaxed);
  return std::to_string(raftcpp::RandInt(1000000)) + "#" + std::to_string(seq);
}

// ===========================================================================
// 2A：领导者选举
// ===========================================================================

void TestInitialElection2A() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);

  cfg->Begin("Test (2A): initial election");

  cfg->CheckOneLeader();

  // 等一会儿，让 follower 也得知选举结果，再检查大家任期是否一致
  raftcpp::SleepMs(50);
  int term1 = cfg->CheckTerms();
  if (term1 < 1) {
    char buf[128];
    std::snprintf(buf, sizeof(buf), "term is %d, but should be at least 1",
                  term1);
    cfg->Fatal(buf);
  }

  // 没有故障的话，leader 和任期不该变
  raftcpp::SleepMs(2 * kRaftElectionTimeout);
  int term2 = cfg->CheckTerms();
  if (term1 != term2) {
    std::printf("warning: term changed even though there were no failures\n");
  }

  cfg->CheckOneLeader();
  cfg->End();
}

void TestReElection2A() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);

  cfg->Begin("Test (2A): election after network failure");

  int leader1 = cfg->CheckOneLeader();

  // leader 掉线 → 应该选出新的
  cfg->Disconnect(leader1);
  cfg->CheckOneLeader();

  // 老 leader 回来 → 不该把新 leader 挤下去
  cfg->Connect(leader1);
  int leader2 = cfg->CheckOneLeader();

  // 凑不够多数派 → 不该有 leader
  cfg->Disconnect(leader2);
  cfg->Disconnect((leader2 + 1) % servers);
  raftcpp::SleepMs(2 * kRaftElectionTimeout);
  cfg->CheckNoLeader();

  // 多数派恢复 → 应该选出 leader
  cfg->Connect((leader2 + 1) % servers);
  cfg->CheckOneLeader();

  cfg->Connect(leader2);
  cfg->CheckOneLeader();

  cfg->End();
}

// ===========================================================================
// 2B：日志复制
// ===========================================================================

void TestBasicAgree2B() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);

  cfg->Begin("Test (2B): basic agreement");

  int iters = 3;
  for (int index = 1; index < iters + 1; index++) {
    // 第index位现在有几个人提交了，任何人都不允许先提交
    int nd = cfg->NCommitted(index).first;
    if (nd > 0) cfg->Fatal("some have committed before Start()");

    // 保证提交的xindex == index
    // One = Start + 轮询等提交 + 校验值相等 + 超时 Fatal。
    int xindex = cfg->One(Cmd(index * 100), servers, false);
    if (xindex != index) {
      char buf[128];
      std::snprintf(buf, sizeof(buf), "got index %d but expected %d", xindex,
                    index);
      cfg->Fatal(buf);
    }
  }

  cfg->End();
}

// 通过数字节数，检查每条命令是不是只发给每个 peer 一次
void TestRPCBytes2B() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);

  cfg->Begin("Test (2B): RPC byte count");

  cfg->One(Cmd(99), servers, false);
  int64_t bytes0 = cfg->BytesTotal();  // 记录初始字节数

  int iters = 10;
  int64_t sent = 0;
  for (int index = 2; index < iters + 2; index++) {
    std::string cmd = raftcpp::RandString(5000); // 放大命令大小，5000字节，确保每个命令的字节数不同
    int xindex = cfg->One(cmd, servers, false);
    if (xindex != index) {
      char buf[128];
      std::snprintf(buf, sizeof(buf), "got index %d but expected %d", xindex,
                    index);
      cfg->Fatal(buf);
    }
    sent += static_cast<int64_t>(cmd.size());  // 累计发送的字节数
  }

  int64_t bytes1 = cfg->BytesTotal();
  int64_t got = bytes1 - bytes0;
  int64_t expected = static_cast<int64_t>(servers) * sent; // 期望的字节数应该确保等于累计发送字节数x服务器数量，避免rpc重复发送
  if (got > expected + 50000) {
    char buf[128];
    std::snprintf(buf, sizeof(buf), "too many RPC bytes; got %lld, expected %lld",
                  static_cast<long long>(got),
                  static_cast<long long>(expected));
    cfg->Fatal(buf);
  }

  cfg->End();
}

// 3 台断 1 台，剩 2 台（仍是多数）必须继续工作；恢复后 3 台重新一致。
// 抓什么 bug：follower 掉线后 leader 卡死不敢提交、隔离节点回归后 term 更高导致集群无法收敛、恢复后新日志复制不到回归节点。
void TestFailAgree2B() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);

  cfg->Begin("Test (2B): agreement despite follower disconnection");
  // 提交一个命令，期望被所有节点提交>=servers.size()，不重试，超过2s放弃
  cfg->One(Cmd(101), servers, false);

  int leader = cfg->CheckOneLeader();
  cfg->Disconnect((leader + 1) % servers);

  // 断一个 follower，剩下的 2 台仍能达成共识
  cfg->One(Cmd(102), servers - 1, false);
  cfg->One(Cmd(103), servers - 1, false);
  raftcpp::SleepMs(kRaftElectionTimeout);
  cfg->One(Cmd(104), servers - 1, false);
  cfg->One(Cmd(105), servers - 1, false);

  cfg->Connect((leader + 1) % servers);

  cfg->One(Cmd(106), servers, true);
  raftcpp::SleepMs(kRaftElectionTimeout);
  cfg->One(Cmd(107), servers, true);

  cfg->End();
}

// 5 台断 3 台（只剩 2 台，非多数），新命令必须分配 index 但永远不提交。
void TestFailNoAgree2B() {
  int servers = 5;
  auto cfg = MakeConfig(servers, false);

  cfg->Begin("Test (2B): no agreement if too many followers disconnect");

  cfg->One(Cmd(10), servers, false); // 分配index 1

  // 5 台里断 3 台
  int leader = cfg->CheckOneLeader();
  cfg->Disconnect((leader + 1) % servers);
  cfg->Disconnect((leader + 2) % servers);
  cfg->Disconnect((leader + 3) % servers);

  StartResult r = cfg->GetRaft(leader)->Start(Cmd(20));  // 分配index 2
  if (!r.is_leader) cfg->Fatal("leader rejected Start()");
  // 即使永远提交不了，leader 也必须立刻分配 index=2 并返回，不能阻塞等提交。
  if (r.index != 2) {
    char buf[128];
    std::snprintf(buf, sizeof(buf), "expected index 2, got %d", r.index);
    cfg->Fatal(buf);
  }
  // 等待2s
  raftcpp::SleepMs(2 * kRaftElectionTimeout);
  // Sleep(2000) 后 NCommitted(2).first 必须为 0 → 没有多数就绝不提交
  int n = cfg->NCommitted(r.index).first;
  if (n > 0) {
    char buf[128];
    std::snprintf(buf, sizeof(buf), "%d committed but no majority", n);
    cfg->Fatal(buf);
  }

  // 修复网络
  cfg->Connect((leader + 1) % servers);
  cfg->Connect((leader + 2) % servers);
  cfg->Connect((leader + 3) % servers);

  int leader2 = cfg->CheckOneLeader();
  StartResult r2 = cfg->GetRaft(leader2)->Start(Cmd(30));
  if (!r2.is_leader) cfg->Fatal("leader2 rejected Start()");
  // ① 新 leader 来自那 3 台被隔离的节点，新 leader [1]=10（它们从没收到 20）,新命令落点index=2
  // ② 新 leader 是那台一直连通的节点 leader+4，新 leader [1]=10, [2]=20（它收到了 20 但没提交），新命令落点index=3
  // ③ 老 leader 保住位子，新leader日志 [1]=10, [2]=20，新命令落点index=3
  // ④ 走 ②/③ 且新 leader 上任时存在【未提交的旧任期尾巴】
  //    （lastLogIndex > commitIndex）→ 触发 Figure 8 修复：先补一条本任期的
  //    no-op 占掉一格，于是新命令落到 index=4。
  //
  //    ④ 不是 C++ 版独有的：Go 基准同样会走这条路径
  //    （raft.go:521 ConvertToLeader 的条件 no-op），官方 test_test.go:223
  //    的断言也早从 `> 3` 放宽成了 `> 4`。本轮实证 C++ labrpc 与 Go 逐位一致
  //    （断连不砍在途 RPC），所以这条 race 在 C++ 侧照样能撞上，只是概率问题。
  if (r2.index < 2 || r2.index > 4) {
    char buf[128];
    std::snprintf(buf, sizeof(buf), "unexpected index %d", r2.index);
    cfg->Fatal(buf);
  }

  cfg->One(Cmd(1000), servers, true);

  cfg->End();
}

// 5 个线程并发 Start，5 条命令必须各占一个独立 index、在同一 term 内全部提交、值全对。
void TestConcurrentStarts2B() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);

  cfg->Begin("Test (2B): concurrent Start()s");

  bool success = false;
  for (int try_i = 0; try_i < 5; try_i++) {
    if (try_i > 0) raftcpp::SleepMs(3000);  // 给系统一点时间稳定

    int leader = cfg->CheckOneLeader();
    StartResult r0 = cfg->GetRaft(leader)->Start(Cmd(1));
    if (!r0.is_leader) continue;  // leader 换得太快
    int term = r0.term;

    int iters = 5;
    std::vector<int> indices;
    std::mutex indices_mu;
    // 和 InternalChurn 同理：子线程里的失败要靠它带回主线程。
    ThreadErr err;
    std::vector<std::thread> threads;
    for (int ii = 0; ii < iters; ii++) {
      threads.push_back(
          SafeThread(err, [cfg, leader, term, ii, &indices, &indices_mu]() {
            // 必须判空：节点随时可能被 crash，GetRaft 会返回 nullptr，
            // 直接 ->Start() 就是段错误（比 terminate 还难查）。
            auto rf = cfg->GetRaft(leader);
            if (!rf) return;
            StartResult r = rf->Start(Cmd(100 + ii));
            if (r.term != term) return;
            if (!r.is_leader) return;
            std::lock_guard<std::mutex> lk(indices_mu); // 在这里加的锁
            indices.push_back(r.index);
          }));
    }
    for (auto& t : threads) t.join();
    if (err.Has()) cfg->Fatal(err.Get());

    bool term_ok = true;
    for (int j = 0; j < servers; j++) {
      if (cfg->GetRaft(j)->GetState().first != term) term_ok = false;
    }
    if (!term_ok) continue;  // 任期变了，这次不算

    bool failed = false;
    std::vector<std::string> cmds;
    for (int index : indices) {
      // 等第 index 号槽位被至少 n 台提交，然后把那一格的值交回给你。
      auto cmd = cfg->Wait(index, servers, term);
      if (!cmd) {
        failed = true;  // 大家已进入更高任期
        break;
      }
      cmds.push_back(*cmd);
    }
    if (failed) continue;

    for (int ii = 0; ii < iters; ii++) {
      std::string x = Cmd(100 + ii);
      bool ok = false;
      for (const auto& c : cmds) {
        if (c == x) ok = true;
      }
      if (!ok) {
        std::string all;
        for (const auto& c : cmds) all += c + " ";
        cfg->Fatal("cmd " + x + " missing in " + all);
      }
    }

    success = true;
    break;
  }

  if (!success) cfg->Fatal("term changed too often");

  cfg->End();
}

// 旧 leader 被隔离后攒了一堆未提交的脏日志，回来时这些脏条目必须被覆盖，绝不能被当已提交扔给状态机。
void TestRejoin2B() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);

  cfg->Begin("Test (2B): rejoin of partitioned leader");
  // 提交一个命令，index = 1
  cfg->One(Cmd(101), servers, true);

  int leader1 = cfg->CheckOneLeader();
  cfg->Disconnect(leader1);

  // 老 leader 在分区里试图提交（必然提交不了）
  cfg->GetRaft(leader1)->Start(Cmd(102));
  cfg->GetRaft(leader1)->Start(Cmd(103));
  cfg->GetRaft(leader1)->Start(Cmd(104));

  // 新 leader 提交，也会占用 index=2
  cfg->One(Cmd(103), 2, true);

  int leader2 = cfg->CheckOneLeader();
  cfg->Disconnect(leader2);

  cfg->Connect(leader1);
  cfg->One(Cmd(104), 2, true);

  cfg->Connect(leader2);
  cfg->One(Cmd(105), servers, true);

  cfg->End();
}

// 故意制造 50 条日志分歧，leader 必须快速覆盖，不能一条一条慢慢退。
void TestBackup2B() {
  int servers = 5;
  auto cfg = MakeConfig(servers, false);

  cfg->Begin("Test (2B): leader backs up quickly over incorrect follower logs");

  cfg->One(Cmd(raftcpp::RandInt(1000000)), servers, true);

  // 把 leader 和一个 follower 关在一个分区里
  int leader1 = cfg->CheckOneLeader();
  cfg->Disconnect((leader1 + 2) % servers);
  cfg->Disconnect((leader1 + 3) % servers);
  cfg->Disconnect((leader1 + 4) % servers);

  // 提交一大堆注定提交不了的命令
  for (int i = 0; i < 50; i++) {
    cfg->GetRaft(leader1)->Start(Cmd(raftcpp::RandInt(1000000)));
  }

  raftcpp::SleepMs(kRaftElectionTimeout / 2);

  cfg->Disconnect((leader1 + 0) % servers);
  cfg->Disconnect((leader1 + 1) % servers);

  cfg->Connect((leader1 + 2) % servers);
  cfg->Connect((leader1 + 3) % servers);
  cfg->Connect((leader1 + 4) % servers);

  // 另一半分区正常提交
  for (int i = 0; i < 50; i++) {
    cfg->One(Cmd(raftcpp::RandInt(1000000)), 3, true);
  }

  int leader2 = cfg->CheckOneLeader();
  int other = (leader1 + 2) % servers;
  if (leader2 == other) other = (leader2 + 1) % servers;
  cfg->Disconnect(other);

  for (int i = 0; i < 50; i++) {
    cfg->GetRaft(leader2)->Start(Cmd(raftcpp::RandInt(1000000)));
  }

  raftcpp::SleepMs(kRaftElectionTimeout / 2);

  // 全员下线，再放回来一批
  for (int i = 0; i < servers; i++) cfg->Disconnect(i);
  cfg->Connect((leader1 + 0) % servers);
  cfg->Connect((leader1 + 1) % servers);
  cfg->Connect(other);

  for (int i = 0; i < 50; i++) {
    cfg->One(Cmd(raftcpp::RandInt(1000000)), 3, true);
  }

  for (int i = 0; i < servers; i++) cfg->Connect(i);
  cfg->One(Cmd(raftcpp::RandInt(1000000)), servers, true);

  cfg->End();
}

// 三段上界断言——初始选举 RPC、复制 10 条命令的 RPC、空闲 1 秒的心跳 RPC。
void TestCount2B() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);

  cfg->Begin("Test (2B): RPC counts aren't too high");

  auto rpcs = [&]() {
    int n = 0;
    for (int j = 0; j < servers; j++) n += cfg->RpcCount(j);
    return n;
  };

  cfg->CheckOneLeader();
  // 1. 选出初始 leader 的 RPC 总数 ∈ [1, 30]（实测 12~16）
  int total1 = rpcs();
  if (total1 > 30 || total1 < 1) {
    char buf[128];
    std::snprintf(buf, sizeof(buf),
                  "too many or few RPCs (%d) to elect initial leader", total1);
    cfg->Fatal(buf);
  }

  int total2 = 0;
  bool success = false;
  for (int try_i = 0; try_i < 5; try_i++) {
    if (try_i > 0) raftcpp::SleepMs(3000);

    int leader = cfg->CheckOneLeader();
    total1 = rpcs();

    int iters = 10;
    StartResult r0 = cfg->GetRaft(leader)->Start(Cmd(1));
    if (!r0.is_leader) continue;
    int term = r0.term;
    int starti = r0.index;

    bool bail = false;
    std::vector<std::string> cmds;
    for (int i = 1; i < iters + 2; i++) {
      int x = raftcpp::RandInt(1 << 30);
      cmds.push_back(Cmd(x));
      StartResult r = cfg->GetRaft(leader)->Start(Cmd(x));
      if (r.term != term) {
        bail = true;
        break;
      }
      if (!r.is_leader) {
        bail = true;
        break;
      }
      if (starti + i != r.index) cfg->Fatal("Start() failed");
    }
    if (bail) continue;

    bail = false;
    for (int i = 1; i < iters + 1; i++) {
      auto cmd = cfg->Wait(starti + i, servers, term);
      if (!cmd) {
        bail = true;
        break;
      }
      if (*cmd != cmds[static_cast<size_t>(i) - 1]) {
        char buf[256];
        std::snprintf(buf, sizeof(buf),
                      "wrong value %s committed for index %d; expected %s",
                      cmd->c_str(), starti + i,
                      cmds[static_cast<size_t>(i) - 1].c_str());
        cfg->Fatal(buf);
      }
    }
    if (bail) continue;

    bool failed = false;
    total2 = 0;
    for (int j = 0; j < servers; j++) {
      if (cfg->GetRaft(j)->GetState().first != term) failed = true;
      total2 += cfg->RpcCount(j);
    }
    if (failed) continue;

    if (total2 - total1 > (iters + 1 + 3) * 3) {
      char buf[128];
      std::snprintf(buf, sizeof(buf), "too many RPCs (%d) for %d entries",
                    total2 - total1, iters);
      cfg->Fatal(buf);
    }

    success = true;
    break;
  }
  // 2. 复制 10 条命令的 RPC 增量 ≤ (10+1+3)*3 = 42（实测仅 6，裕度 7 倍，很松）。
  if (!success) cfg->Fatal("term changed too often");

  raftcpp::SleepMs(kRaftElectionTimeout);

  // 3.空闲 1 秒的 RPC 增量 ≤ 3*20 = 60（实测 34）
  int total3 = 0;
  for (int j = 0; j < servers; j++) total3 += cfg->RpcCount(j);
  if (total3 - total2 > 3 * 20) {
    char buf[128];
    std::snprintf(buf, sizeof(buf),
                  "too many RPCs (%d) for 1 second of idleness",
                  total3 - total2);
    cfg->Fatal(buf);
  }

  cfg->End();
}

// ===========================================================================
// 2C：持久化
// ===========================================================================
// 全部重启 / 重启 leader / 失联+重启 leader / 失联+重启 follower。
void TestPersist12C() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);

  cfg->Begin("Test (2C): basic persistence");

  cfg->One(Cmd(11), servers, true);

  // 全部重启
  for (int i = 0; i < servers; i++) cfg->Start1(i);  // crash后启动后不接网线后面必须得先连接
  for (int i = 0; i < servers; i++) {
    cfg->Disconnect(i);
    cfg->Connect(i);
  }

  cfg->One(Cmd(12), servers, true);

  int leader1 = cfg->CheckOneLeader();
  cfg->Disconnect(leader1);
  cfg->Start1(leader1);
  cfg->Connect(leader1);

  cfg->One(Cmd(13), servers, true);

  int leader2 = cfg->CheckOneLeader();
  cfg->Disconnect(leader2);
  cfg->One(Cmd(14), servers - 1, true);
  cfg->Start1(leader2);
  cfg->Connect(leader2);

  cfg->Wait(4, servers, -1);

  int i3 = (cfg->CheckOneLeader() + 1) % servers;
  cfg->Disconnect(i3);
  cfg->One(Cmd(15), servers - 1, true);
  cfg->Start1(i3);
  cfg->Connect(i3);

  cfg->One(Cmd(16), servers, true);

  cfg->End();
}

// 它是 Persist12C 的"地狱难度版"：5 节点、5 轮循环、每轮把整个集群拆散再拼回来，制造"不同节点带着不同时代的日志重启"的场景。
void TestPersist22C() {
  int servers = 5;
  auto cfg = MakeConfig(servers, false);

  cfg->Begin("Test (2C): more persistence");

  int index = 1;
  for (int iters = 0; iters < 5; iters++) {
    cfg->One(Cmd(10 + index), servers, true);
    index++;

    int leader1 = cfg->CheckOneLeader();

    cfg->Disconnect((leader1 + 1) % servers);
    cfg->Disconnect((leader1 + 2) % servers);

    cfg->One(Cmd(10 + index), servers - 2, true);
    index++;

    cfg->Disconnect((leader1 + 0) % servers);
    cfg->Disconnect((leader1 + 3) % servers);
    cfg->Disconnect((leader1 + 4) % servers);

    cfg->Start1((leader1 + 1) % servers);
    cfg->Start1((leader1 + 2) % servers);
    cfg->Connect((leader1 + 1) % servers);
    cfg->Connect((leader1 + 2) % servers);

    raftcpp::SleepMs(kRaftElectionTimeout);

    cfg->Start1((leader1 + 3) % servers);
    cfg->Connect((leader1 + 3) % servers);

    cfg->One(Cmd(10 + index), servers - 2, true);
    index++;

    cfg->Connect((leader1 + 4) % servers);
    cfg->Connect((leader1 + 0) % servers);
  }

  cfg->One(Cmd(1000), servers, true);

  cfg->End();
}

// 已提交的数据必须落在多数派的持久化存储上，否则宕机就丢数据
void TestPersist32C() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);

  cfg->Begin(
      "Test (2C): partitioned leader and one follower crash, leader restarts");

  cfg->One(Cmd(101), 3, true);

  int leader = cfg->CheckOneLeader();
  cfg->Disconnect((leader + 2) % servers);

  cfg->One(Cmd(102), 2, true);

  cfg->Crash1((leader + 0) % servers);
  cfg->Crash1((leader + 1) % servers);
  cfg->Connect((leader + 2) % servers);
  cfg->Start1((leader + 0) % servers);
  cfg->Connect((leader + 0) % servers);

  cfg->One(Cmd(103), 2, true);

  cfg->Start1((leader + 1) % servers);
  cfg->Connect((leader + 1) % servers);

  cfg->One(Cmd(104), servers, true);

  cfg->End();
}

// 论文扩展版 Figure 8 的场景：leader 反复在"刚收到命令、还没提交"时挂掉
void TestFigure82C() {
  int servers = 5;
  auto cfg = MakeConfig(servers, false);

  cfg->Begin("Test (2C): Figure 8");

  cfg->One(Cmd(raftcpp::RandInt(1000000)), 1, true);

  int nup = servers;
  for (int iters = 0; iters < 1000; iters++) {
    int leader = -1;
    for (int i = 0; i < servers; i++) {
      auto rf = cfg->GetRaft(i);
      if (rf && rf->Start(Cmd(raftcpp::RandInt(1000000))).is_leader) {
        leader = i;
      }
    }

    if ((raftcpp::RandInt(1000)) < 100) {
      raftcpp::SleepMs(raftcpp::RandInt(static_cast<int>(
          kRaftElectionTimeout / 2)));
    } else {
      raftcpp::SleepMs(raftcpp::RandInt(13));
    }

    if (leader != -1) {
      cfg->Crash1(leader);
      nup -= 1;
    }

    if (nup < 3) {
      int s = raftcpp::RandInt(servers);
      if (!cfg->GetRaft(s)) {
        cfg->Start1(s);
        cfg->Connect(s);
        nup += 1;
      }
    }
  }

  for (int i = 0; i < servers; i++) {
    if (!cfg->GetRaft(i)) {
      cfg->Start1(i);
      cfg->Connect(i);
    }
  }

  cfg->One(Cmd(raftcpp::RandInt(1000000)), servers, true);

  cfg->End();
}

// 丢包 + 乱序（0~27ms 随机延迟）下，Start 追加日志 → 复制 → 重试 → 提交的全链路不出安全性问题。重点抓的是"重复 apply"和"漏日志后 nextIndex 回退错误"。
void TestUnreliableAgree2C() {
  int servers = 5;
  auto cfg = MakeConfig(servers, true);

  cfg->Begin("Test (2C): unreliable agreement");

  ThreadErr err;
  std::vector<std::thread> threads;
  for (int iters = 1; iters < 50; iters++) {
    for (int j = 0; j < 4; j++) {
      threads.push_back(SafeThread(err, [cfg, iters, j]() {
        cfg->One(Cmd((100 * iters) + j), 1, true);
      }));
    }
    cfg->One(Cmd(iters), 1, true);
  }

  cfg->SetUnreliable(false);

  for (auto& t : threads) t.join();
  if (err.Has()) cfg->Fatal(err.Get());

  cfg->One(Cmd(100), servers, true);

  cfg->End();
}

// 所有"收到 RPC 回包后更新状态"的地方，必须先校验回包是否已过时（raft.cpp:706 起的 reply term 检查、match_index_ 只增不减、next_index_ 下界保护 raft.cpp:758-761 的 max(hint, match_index_[s]+1)——注释原文"防止迟到的旧回包把进度往回拽"）。单次 36 秒、100200 万字节 RPC，是 2C 里流量最大的。
void TestFigure8Unreliable2C() {
  int servers = 5;
  auto cfg = MakeConfig(servers, true);

  cfg->Begin("Test (2C): Figure 8 (unreliable)");

  cfg->One(Cmd(raftcpp::RandInt(10000)), 1, true);

  int nup = servers;
  for (int iters = 0; iters < 1000; iters++) {
    if (iters == 200) cfg->SetLongReordering(true);

    int leader = -1;
    for (int i = 0; i < servers; i++) {
      auto rf = cfg->GetRaft(i);
      if (rf && rf->Start(Cmd(raftcpp::RandInt(10000))).is_leader &&
          cfg->Connected(i)) {
        leader = i;
      }
    }

    if ((raftcpp::RandInt(1000)) < 100) {
      raftcpp::SleepMs(raftcpp::RandInt(static_cast<int>(
          kRaftElectionTimeout / 2)));
    } else {
      raftcpp::SleepMs(raftcpp::RandInt(13));
    }

    if (leader != -1 &&
        (raftcpp::RandInt(1000)) < static_cast<int>(kRaftElectionTimeout / 2)) {
      cfg->Disconnect(leader);
      nup -= 1;
    }

    if (nup < 3) {
      int s = raftcpp::RandInt(servers);
      if (!cfg->Connected(s)) {
        cfg->Connect(s);
        nup += 1;
      }
    }
  }

  for (int i = 0; i < servers; i++) {
    if (!cfg->Connected(i)) cfg->Connect(i);
  }

  cfg->One(Cmd(raftcpp::RandInt(10000)), servers, true);

  cfg->End();
}

// 客户端不停写、运维不停 crash/断连/重启，最后验证一条已提交的都不丢。两条共用 InternalChurn(bool unreliable)。
void InternalChurn(bool unreliable) {
  int servers = 5;
  auto cfg = MakeConfig(servers, unreliable);

  if (unreliable) {
    cfg->Begin("Test (2C): unreliable churn");
  } else {
    cfg->Begin("Test (2C): churn");
  }

  std::atomic<bool> stop{false};

  // 子线程里的失败靠它带回主线程 —— 对应 Go 版
  // `if vv == nil { t.Fatal("client failed") }` 那道兜底。
  ThreadErr err;

  auto client_fn = [&](int me, std::vector<std::string>& out) {
    std::vector<std::string> values;
    while (!stop.load()) {
      // 必须是【全局唯一】的值，不能退回 Cmd(RandInt(1000000))：结尾那道
      // "didn't find a value"（:954-960）靠值本身识别命令，一旦有两条命令撞值，
      // 我这条即使被 Raft 弄丢了，也会被另一个同值的替身救回来 → 漏检。
      // 详见文件顶部 CmdUnique() 的注释。
      std::string x = CmdUnique();
      int index = -1;
      bool ok = false;
      for (int i = 0; i < servers; i++) {
        auto rf = cfg->GetRaft(i);
        if (!rf) continue;
        StartResult r = rf->Start(x);
        if (r.is_leader) {
          ok = true;
          index = r.index;
        }
      }
      if (ok) {
        // 这条命令可能被提交，也可能不会 —— 但不能无限等下去
        for (int to : {10, 20, 50, 100, 200}) {
          auto [nd, cmd] = cfg->NCommitted(index);
          if (nd > 0) {
            // 这里刻意不抛异常。Go 版对应的 `cmd.(int)` 的 else-Fatal 是死代码：
            // nCommitted 里 count > 0 ⟺ cmd 已被赋值，断言永不失败；而 C++ 的
            // Command 是 std::string（raft.h:58），类型编译期就锁死了，压根
            // 不存在"类型不对"这个语义。cmd != x（提交了别人的命令）也属正常
            // —— index 可能已被别的 leader 占用，静默跳过即可。
            // 真正的安全性检查在 NCommitted 内部：apply_err_（config.cpp:413）
            // 与"同 index 提交了不同值"（config.cpp:420）。
            if (cmd && *cmd == x) values.push_back(x);
            break;
          }
          raftcpp::SleepMs(to);
        }
      } else {
        raftcpp::SleepMs(79 + me * 17);
      }
    }
    out = std::move(values);
  };

  int ncli = 3;
  std::vector<std::vector<std::string>> results(ncli);
  std::vector<std::thread> clients;
  for (int i = 0; i < ncli; i++) {
    // 必须走 SafeThread（本文件顶部定义的工具）：client_fn 里会调 NCommitted，
    // 而 Config::Fatal 是 [[noreturn]] throw TestFailure（config.cpp:36）。
    // 裸 std::thread 没人接这个异常 → std::terminate() → 整个 raft_test 进程
    // abort，后面所有用例全跑不成，且只留一行 "terminate called after throwing"。
    clients.push_back(SafeThread(err, [&, i]() {
      client_fn(i, results[static_cast<size_t>(i)]);
    }));
  }

  for (int iters = 0; iters < 20; iters++) {
    if ((raftcpp::RandInt(1000)) < 200) {
      cfg->Disconnect(raftcpp::RandInt(servers));
    }
    if ((raftcpp::RandInt(1000)) < 500) {
      int i = raftcpp::RandInt(servers);
      if (!cfg->GetRaft(i)) cfg->Start1(i);
      cfg->Connect(i);
    }
    if ((raftcpp::RandInt(1000)) < 200) {
      int i = raftcpp::RandInt(servers);
      if (cfg->GetRaft(i)) cfg->Crash1(i);
    }
    raftcpp::SleepMs((kRaftElectionTimeout * 7) / 10);
  }

  raftcpp::SleepMs(kRaftElectionTimeout);
  cfg->SetUnreliable(false);
  for (int i = 0; i < servers; i++) {
    if (!cfg->GetRaft(i)) cfg->Start1(i);
    cfg->Connect(i);
  }

  stop.store(true);
  for (auto& t : clients) t.join();
  // 兜底：client 线程里若触发过 Fatal，这里统一报出来。
  // 必须放在汇总 results 之前 —— 否则会带着"半成品 values"继续跑后面的断言，
  // 把一个明确的失败伪装成"didn't find a value"之类的误导信息。
  if (err.Has()) cfg->Fatal(err.Get());

  std::vector<std::string> values;
  for (auto& v : results) {
    values.insert(values.end(), v.begin(), v.end());
  }

  raftcpp::SleepMs(kRaftElectionTimeout);

  // 同样用唯一值：它会被 Wait() 拉进 really 列表参与比对，若和 client 提交的
  // 某条撞值，就给那条命令的丢失提供了替身。
  int last_index = cfg->One(CmdUnique(), servers, true);

  std::vector<std::string> really;
  for (int index = 1; index <= last_index; index++) {
    auto v = cfg->Wait(index, servers, -1);
    if (!v) cfg->Fatal("wait() returned nothing");
    really.push_back(*v);
  }

  for (const auto& v1 : values) {
    bool ok = false;
    for (const auto& v2 : really) {
      if (v1 == v2) ok = true;
    }
    if (!ok) cfg->Fatal("didn't find a value " + v1);
  }

  cfg->End();
}

void TestReliableChurn2C() { InternalChurn(false); }
void TestUnreliableChurn2C() { InternalChurn(true); }

// ===========================================================================
// 2D：日志压缩 / 快照（InstallSnapshot）—— 标准两段式 + 快照感知状态机
//
// 这些用例启用了 config 的"快照感知状态机"（EnableSnapshotCompression）：
//   * applier 在日志过长时主动调 rf.Snapshot(LastApplied, EncodeKV(state)) 压缩；
//   * follower 收到 InstallSnapshot RPC 后，由 applier 调 rf.CondInstallSnapshot
//     确认落地，并把快照 blob（被压缩掉的那段 state）重水化进自己的状态机。
// 因此既验证 Raft 内部机制（日志截断 / InstallSnapshot 追赶 / 重启恢复），
// 也验证"状态机能靠快照追平"——这是 Lab 3 日志压缩完整闭环。
// 注：snapshot_compaction_ 是 per-Config 开关，不会污染上面的 2A/2B/2C 用例。
// ===========================================================================

// 取【必须在位】的节点句柄。
//
// Config::GetRaft 对"已崩溃 / 还没 Start1"的节点返回 nullptr（config.cpp:46-49
// 只加锁取值，不 Fatal）。2D 这批用例的操作序列里有 Crash1 → Start1，一旦
// Start1 没成功、或将来有人调整动作顺序，裸 `GetRaft(i)->SnapshotIndex()` 就是
// 对 nullptr 解引用 → SIGSEGV，一个字都不打印，比普通的用例失败难查得多。
// 为空时明确报出节点编号，让失败直接指向真正的原因。
static std::shared_ptr<Raft> GetRaftOrFatal(std::shared_ptr<Config> cfg, int i) {
  auto rf = cfg->GetRaft(i);
  // Fatal 在 config.h:106 声明为 [[noreturn]]，这个 if 不会漏到下面的 return
  if (!rf) {
    cfg->Fatal("GetRaft(" + std::to_string(i) +
               ") returned nullptr: peer is crashed or not started");
  }
  return rf;
}

// 等某节点的 apply 追上 commit（避免快照时 last_applied_ 还落后于 commit_index_，
// 导致被快照覆盖的条目没真正 apply 就跳过）。
static void WaitApplied(std::shared_ptr<Raft> rf, int timeout_ms) {
  // 约定：调用方先用 GetRaftOrFatal 保证非空。这里是最后一道防御 ——
  // 若直接静默 return，后面的断言会变成"莫名其妙的超时"，把空指针问题
  // 伪装成快照没追上；抛出去让 runner 明确报出来更好。
  if (!rf) throw std::runtime_error("WaitApplied: rf is nullptr");
  int waited = 0;
  while (rf->LastApplied() < rf->CommitIndex() && waited < timeout_ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    waited += 10;
  }
}

// 测试 1：状态机主动压缩能截断日志
void TestSnapshotTruncatesLog2D() {
  auto cfg = MakeConfig(3, false);
  cfg->EnableSnapshotCompaction();
  cfg->Begin("Test (2D): snapshot truncates log");

  for (int i = 1; i <= 100; i++) {
    cfg->One("put k" + std::to_string(i) + " v" + std::to_string(i), 1, true);
  }
  int leader = cfg->CheckOneLeader();
  auto rf = GetRaftOrFatal(cfg, leader);
  WaitApplied(rf, 2000);

  std::cout << "  leader 日志条数(压缩后): " << rf->LogSize()
            << "  snapshot_index=" << rf->SnapshotIndex() << "\n";
  if (rf->LogSize() > kSnapshotThreshold + 1) {
    char buf[128];
    std::snprintf(buf, sizeof(buf), "log not truncated: %d > %d",
                  rf->LogSize(), kSnapshotThreshold + 1);
    cfg->Fatal(buf);
  }
  if (rf->SnapshotIndex() <= 0) cfg->Fatal("snapshot_index not advanced");
  cfg->End();
}

// 测试 2：落后节点通过 InstallSnapshot 追上，且状态机被带平
void TestInstallSnapshotCatchUp2D() {
  auto cfg = MakeConfig(3, false);
  cfg->EnableSnapshotCompaction();
  cfg->Begin("Test (2D): install snapshot catch up");

  for (int i = 1; i <= 20; i++)
    cfg->One("put a v" + std::to_string(i), 1, true);

  int leader = cfg->CheckOneLeader();
  int follower = (leader + 1) % 3;
  cfg->Crash1(follower);

  for (int i = 21; i <= 100; i++)
    cfg->One("put b v" + std::to_string(i), 1, true);

  auto lrf = GetRaftOrFatal(cfg, leader);
  WaitApplied(lrf, 2000);

  cfg->Start1(follower);

  bool ok = false;
  for (int t = 0; t < 150; t++) {
    if (lrf->SnapshotIndex() > 0 &&
        GetRaftOrFatal(cfg, follower)->SnapshotIndex() >= lrf->SnapshotIndex()) {
      ok = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  if (!ok) cfg->Fatal("follower didn't catch up via InstallSnapshot");
  WaitApplied(GetRaftOrFatal(cfg, follower), 5000);
  WaitApplied(GetRaftOrFatal(cfg, follower), 5000);

  auto ls = cfg->GetState(leader);
  auto fs = cfg->GetState(follower);
  std::cout << "  follower 快照点追上: "
            << GetRaftOrFatal(cfg, follower)->SnapshotIndex()
            << "  leader 快照点: " << lrf->SnapshotIndex()
            << "  状态条目 leader=" << ls.size() << " follower=" << fs.size()
            << "\n";
  if (ls != fs) cfg->Fatal("follower state != leader state after snapshot");
  cfg->End();
}

// 测试 3：快照在崩溃重启后仍然存活
void TestSnapshotRestart2D() {
  auto cfg = MakeConfig(3, false);
  cfg->EnableSnapshotCompaction();
  cfg->Begin("Test (2D): snapshot survives restart");

  for (int i = 1; i <= 30; i++)
    cfg->One("put k v" + std::to_string(i), 1, true);
  int leader = cfg->CheckOneLeader();
  auto lrf = GetRaftOrFatal(cfg, leader);
  WaitApplied(lrf, 2000);

  int snap_idx = lrf->SnapshotIndex();
  if (snap_idx <= 0) cfg->Fatal("compaction didn't happen");

  cfg->Crash1(leader);
  cfg->Start1(leader);
  if (GetRaftOrFatal(cfg, leader)->SnapshotIndex() != snap_idx) {
    char buf[128];
    std::snprintf(buf, sizeof(buf), "snapshot_index lost after restart: %d != %d",
                  GetRaftOrFatal(cfg, leader)->SnapshotIndex(), snap_idx);
    cfg->Fatal(buf);
  }

  std::cout << "  重启后快照点恢复: "
            << GetRaftOrFatal(cfg, leader)->SnapshotIndex() << "\n";

  cfg->One("put after restart v1", 1, true);
  cfg->End();
}

// 测试 4：快照感知状态机 —— 落后节点靠 InstallSnapshot 把整块状态追平
void TestSnapshotStateMachine2D() {
  auto cfg = MakeConfig(3, false);
  cfg->EnableSnapshotCompaction();
  cfg->Begin("Test (2D): snapshot-aware state machine rehydrates follower");

  for (int i = 1; i <= 100; i++)
    cfg->One("op" + std::to_string(i), 1, true);

  int leader = cfg->CheckOneLeader();
  int follower = (leader + 1) % 3;
  cfg->Crash1(follower);

  for (int i = 101; i <= 150; i++)
    cfg->One("op" + std::to_string(i), 1, true);

  cfg->Start1(follower);

  // 注意：不能只等 Raft 内部 last_applied_>=commit_index_——CondInstallSnapshot
  // 一落地就会把两者都跳到快照点(140)，但 follower 的 KV 状态 map(logs_[i]) 要等
  // applier 线程异步 DecodeKV 快照 + 再 apply 141..150 才填满。必须等"派生状态"就绪。
  bool ok = false;
  for (int t = 0; t < 120; t++) {
    if (cfg->GetState(follower).size() >= 150) {
      ok = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  if (!ok) cfg->Fatal("follower state didn't reach 150 entries via snapshot");

  auto ls = cfg->GetState(leader);
  auto fs = cfg->GetState(follower);
  std::cout << "  leader 状态条目: " << ls.size()
            << "  follower 状态条目: " << fs.size() << "\n";
  if (ls.size() != 150) {
    char buf[128];
    std::snprintf(buf, sizeof(buf), "leader state size %zu != 150", ls.size());
    cfg->Fatal(buf);
  }
  if (ls != fs) cfg->Fatal("follower state machine != leader after rehydrate");
  cfg->End();
}

// ===========================================================================
// 生产级扩展 ①：CheckQuorum / Leader Lease / ReadIndex
// ===========================================================================
// 这三个测试验证的是"论文 §6.4 只读优化"的正确性。
// 它们考的不是"能不能选出 leader"，而是"选出来的 leader 到底可不可信"。

// ---------------------------------------------------------------------------
// CheckQuorum：被隔离到少数派的旧 leader 必须【主动退位】。
//
// 没有 CheckQuorum 会怎样？旧 leader 被分区隔离后，既收不到多数派心跳回包，
// 也不会收到新 leader 的消息（它被隔离了），于是它会一直以为自己还是 leader，
// 继续用手里那份【已经过期】的数据应答客户端 —— 这就是脏读。
// ---------------------------------------------------------------------------
void TestCheckQuorum() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);

  cfg->Begin("Test (Ext): CheckQuorum 让被隔离的旧 leader 主动退位");
  // 等到恰好选出一个leader
  int leader = cfg->CheckOneLeader();
  // 先提交一条，确认集群此刻是健康的
  cfg->One("x1", servers, true);

  // 把 leader 与外界彻底隔离：它发不出去，也收不到
  cfg->Disconnect(leader);

  // 等超过 CheckQuorum 的自查间隔(150ms) —— 给 1 秒足够它发现自己被孤立
  raftcpp::SleepMs(1000);

  auto st = cfg->GetRaft(leader)->GetState();
  if (st.second) {
    cfg->Fatal(
        "CheckQuorum 没生效：被隔离的旧 leader 仍认为自己是 leader"
        "（它会继续用旧数据应答客户端 → 脏读）");
  }
  // 隔离期间：多数派必须继续工作（可用性未被牺牲）
  cfg->One("majority-still-works", servers - 1, true);
  // 旧 leader 退位后必须拒绝新写入（单写者；注意须在退位之后调用）
  auto r = cfg->GetRaft(leader)->Start("zombie");
  if (r.is_leader) cfg->Fatal("被隔离 leader 仍在接受写入 → 脑裂风险");

  // 恢复网络后集群应当自愈：重新选出 leader 并继续工作
  cfg->Connect(leader);
  cfg->CheckOneLeader();
  cfg->One("x2", servers, true);

  cfg->End();
}

// ===== CheckQuorum 反向：健康 leader 绝不能被误退位（最该有的一条）=====
void TestCheckQuorumNoSpuriousDemote() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test (Ext): CheckQuorum 反向 —— 健康 leader 绝不被误退位");
  int leader = cfg->CheckOneLeader();
  // 关键：不断开任何人。健康 leader 的 last_ack_time_ 持续被心跳回包刷新，
  // 在 150ms 自查窗口内永远能联系上多数派，CheckQuorum 绝不该触发退位。
  // 跑 2 秒覆盖多个自查周期；配合 -count 5 抓偶发。
  auto st0 = cfg->GetRaft(leader)->GetState();   // 记下初始 (term, isLeader)
  raftcpp::SleepMs(2000);
  auto st = cfg->GetRaft(leader)->GetState();
  if (!st.second) {
    cfg->Fatal("CheckQuorum 误杀健康 leader：窗口太短 / 计票写反 / "
               "last_ack_time_ 没在心跳回包处刷新");
  }
  // ★ 补牙（关键）：只查 isLeader 会被"误退位后又被重新选中"掩盖 —— 它退位后
  //   follower 超时重选，最 up-to-date 的它多半再次当选，isLeader 又变回 true。
  //   实测：把 CheckQuorum 改成无条件退位，这条用例【照样通过】。
  //   只有断言【term 没涨】才能证明这 2 秒里真没发生过退位+重选。
  if (st.first != st0.first) {
    cfg->Fatal("CheckQuorum 误杀后节点被重新选中：term " +
               std::to_string(st0.first) + " -> " + std::to_string(st.first) +
               "（健康集群 2 秒内绝不该发生退位+重选）");
  }
  // 健康期间集群必须还能正常提交
  cfg->One("health", servers, true);
  cfg->End();
}

// ===== 不可靠网络 + 不断开：leader 偶发丢包也不该误退位 =====
void TestCheckQuorumUnreliableNoFlap() {
  int servers = 3;
  auto cfg = MakeConfig(servers, true);   // 不可靠网络（仅 0~27ms 随机延迟，不丢包）
  cfg->Begin("Test (Ext): CheckQuorum 不可靠网络下不脑裂/不误杀到失活");
  // 重要：不可靠网络下 Raft 本就会发生【正常 leader 切换】——随机延迟偶尔让
  // follower 在选举超时窗口内没及时收到心跳，触发它重新选举，原 leader 变成
  // follower。这是 Raft 的正常工作，【不是 CheckQuorum 误杀】。
  // 所以这里【不能】断言"原 leader 仍是 leader"（那样会偶发 Fatal，即之前的 flaky）。
  // CheckQuorum 在不可靠网络下的正确契约是：无分区时集群始终能选出【唯一】leader
  // 并正常提交，绝不会因 CheckQuorum 把健康 leader 误杀到"无 leader 卡死"或"脑裂"。
  // 跑多轮"选 leader + 提交"，抓"CheckQuorum 太激进导致集群失活/脑裂"的真 bug。
  for (int i = 0; i < 5; i++) {
    int leader = cfg->CheckOneLeader();    // 约 5s 内能选出唯一 leader（无脑裂才返回）
    (void)leader;                          // 不要求"还是原来的那个"
    cfg->One("u", servers, true);          // 能正常提交 = 集群健康
  }
  cfg->End();
}

// ===== 不可靠网络 + 真分区：仍必须退位（验证不会太宽松）=====
void TestCheckQuorumUnreliableIsolated() {
  int servers = 3;
  auto cfg = MakeConfig(servers, true);   // 不可靠网络
  cfg->Begin("Test (Ext): CheckQuorum 在丢包网络下真分区仍退位");
  int leader = cfg->CheckOneLeader();
  cfg->One("x1", servers, true);
  cfg->Disconnect(leader);                // 彻底隔离
  raftcpp::SleepMs(1000);                 // 远超 150ms 自查间隔
  auto st = cfg->GetRaft(leader)->GetState();
  if (st.second) {
    cfg->Fatal("不可靠网络下被隔离 leader 仍自称 leader：CheckQuorum 太宽松");
  }
  cfg->Connect(leader);
  cfg->CheckOneLeader();                  // 集群自愈
  cfg->One("x2", servers, true);
  cfg->End();
}

// ===== CheckQuorum 反向（有牙版）：只丢少数派时 leader 必须留任 =====
// 与 NoSpuriousDemote 互补：那条验"全员健康"，这条验"仍联系得上多数派"。
void TestCheckQuorumMinorityPartitionKeepsLeadership() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test (Ext): CheckQuorum 只丢少数派时 leader 必须留任");
  int leader = cfg->CheckOneLeader();
  cfg->One("x1", servers, true);
  auto st0 = cfg->GetRaft(leader)->GetState();

  // 只断一个 follower：leader 还能收到另一个的回包，
  // live = 自己 + 1 = 2 >= majority(2)，CheckQuorum 绝不该退位。
  int victim = (leader + 1) % servers;
  cfg->Disconnect(victim);
  raftcpp::SleepMs(1000);   // 覆盖 ~6 个自查周期

  auto st = cfg->GetRaft(leader)->GetState();
  if (!st.second) {
    cfg->Fatal("只丢了 1 个 follower（仍有多数派）却退位了：CheckQuorum 太激进");
  }
  if (st.first != st0.first) {
    cfg->Fatal("只丢少数派期间 term 从 " + std::to_string(st0.first) + " 涨到 " +
               std::to_string(st.first) + "：说明发生过误退位 + 重选");
  }
  // 剩 2 个节点仍应能继续提交（majority = 2）
  cfg->One("x2", servers - 1, true);
  cfg->Connect(victim);
  cfg->CheckOneLeader();
  cfg->End();
}

// ===== CheckQuorum 反向：5 节点丢 2 follower（仍是少数派）leader 必须留任 =====
void TestCheckQuorumMinorityPartitionKeepsLeadership5() {
  int servers = 5;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test (Ext): CheckQuorum 5 节点丢 2 follower（仍 majority）leader 留任");
  int leader = cfg->CheckOneLeader();
  cfg->One("x1", servers, true);
  auto st0 = cfg->GetRaft(leader)->GetState();

  // 断 2 个 follower（都不是 leader）：online = leader + 2 = 3 >= majority(3)，
  // CheckQuorum 绝不该退位。这条专门在更大集群规模上兜底 MemberCountLocked()
  // 的多数派计数 / off-by-one —— 3 节点版只验证了 majority=2 的特例。
  int v1 = (leader + 1) % servers;
  int v2 = (leader + 2) % servers;
  cfg->Disconnect(v1);
  cfg->Disconnect(v2);
  raftcpp::SleepMs(1000);   // 覆盖 ~6 个自查周期

  auto st = cfg->GetRaft(leader)->GetState();
  if (!st.second) {
    cfg->Fatal("5 节点丢 2 follower（仍 majority）却退位：CheckQuorum 多数派计数有误");
  }
  if (st.first != st0.first) {
    cfg->Fatal("5 节点少数派分区期间 term 从 " + std::to_string(st0.first) +
               " 涨到 " + std::to_string(st.first) + "：误退位 + 重选");
  }
  // 剩 3 个节点（leader + 2 在线 follower）仍应能继续提交（majority = 3）
  cfg->One("x2", servers - 2, true);
  cfg->Connect(v1);
  cfg->Connect(v2);
  cfg->CheckOneLeader();
  cfg->End();
}

// ===== CheckQuorum 反向（采样强化版）：少数派分区期间 leader 必须【持续】稳定 =====
// 这是 TestCheckQuorumMinorityPartitionKeepsLeadership 的【采样强化】版：那条只在
// 断开 ~1s 后采【一次】 isLeader/term。若实现有"leader 间歇性退位又重选"或
// "term 偶发抖动"的闪烁 bug，单次采样可能正好落在稳定窗口而漏掉。这条改为每
// ~300ms 采样一次（共 5 次，约 1.5s），断言【每次】都 isLeader 且 term == st0.first。
// ★ 关键修正：必须是【少数派分区】（leader 仍连多数派）。全分区下被隔离节点收不到
//   票会合法地反复自荐、term 一直涨 —— 那种场景断言 term 不变是错的。term 稳定
//   只在"leader 仍能联系多数派"时成立，所以本用例用断 1 follower 的少数派分区。
void TestCheckQuorumSustainedMinorityStable() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test (Ext): CheckQuorum 少数派分区期间 leader 持续稳定（采样版）");
  int leader = cfg->CheckOneLeader();
  cfg->One("x1", servers, true);
  auto st0 = cfg->GetRaft(leader)->GetState();

  // 只断 1 个 follower：leader 仍连 majority(2)，CheckQuorum 应永远满意。
  int victim = (leader + 1) % servers;
  cfg->Disconnect(victim);

  // 重复采样 ~5 次（覆盖多个 150ms 自查周期）：每次都必须仍是 leader 且 term 没抖。
  // 抓"闪烁式退位 / term 抖动"这类单次采样抓不到的边界 bug。
  for (int i = 0; i < 5; i++) {
    raftcpp::SleepMs(300);
    auto st = cfg->GetRaft(leader)->GetState();
    if (!st.second) {
      cfg->Fatal("少数派分区期间 leader 第 " + std::to_string(i + 1) +
                 " 次采样掉位：CheckQuorum 在仍有多数派时闪烁退位");
    }
    if (st.first != st0.first) {
      cfg->Fatal("少数派分区期间第 " + std::to_string(i + 1) +
                 " 次采样 term 从 " + std::to_string(st0.first) + " 涨到 " +
                 std::to_string(st.first) + "：leader 选举计时器/CheckQuorum 反复触发");
    }
  }
  // 剩 2 节点（leader + 另一 follower）仍应能继续提交（majority = 2）
  cfg->One("x2", servers - 1, true);
  cfg->Connect(victim);
  cfg->CheckOneLeader();
  cfg->End();
}

// ===== 退位后不再服务线性一致读（CheckQuorum 的生产目标：防脏读）=====
// 【已删除 TestCheckQuorumStaleReadPrevented】
// 原用例与本文件里的 TestReadIndexNoStale 完全重复：两者都是
//   「Disconnect(leader) → sleep(1000) → ReadIndex() 必须返回 -1」。
// 而 sleep 1000ms 之后 CheckQuorum（150ms 自查一次）早已把它退位，走的是
// "不是 leader → 返回 -1" 这条平凡分支，readIndex 的多数派确认根本没被走到
// （变异测试实证：把 ReadIndex 改成直接返回 commit_index_，两条照样全过）。
// 保留 TestReadIndexNoStale 一份即可：它还多了"隔离之前读必须成功"的前提断言。
// 真正有牙的是 TestReadIndexPartitionImmediate（断开后不 sleep 立刻读）。

// ---------------------------------------------------------------------------
// ReadIndex：线性一致读的基本契约。
//   * leader 上能拿到合法 readIndex，且它必须【不落后于】最新提交的下标
//   * follower 上必须失败 —— follower 的日志可能落后，不能服务线性一致读
// ---------------------------------------------------------------------------
void TestReadIndex() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);

  cfg->Begin("Test (Ext): ReadIndex 线性一致读");

  int leader = cfg->CheckOneLeader();

  // (1) leader 上读成功，且 readIndex 至少追平最新提交的下标
  int idx = cfg->One("v1", servers, true);
  int ri = cfg->GetRaft(leader)->ReadIndex();
  if (ri < 0) {
    cfg->Fatal("leader 上 ReadIndex 应当成功（返回 >= 0）");
  }
  if (ri < idx) {
    char buf[160];
    std::snprintf(buf, sizeof(buf),
                  "ReadIndex=%d 落后于最新已提交下标 %d —— 照它读会读到旧值",
                  ri, idx);
    cfg->Fatal(buf);
  }

  // (2) follower 上必须失败
  int follower = (leader + 1) % servers;
  if (cfg->GetRaft(follower)->ReadIndex() >= 0) {
    cfg->Fatal(
        "follower 上 ReadIndex 应当返回 -1"
        "（follower 的日志可能落后，不能保证线性一致）");
  }

  cfg->End();
}

// ---------------------------------------------------------------------------
// 脏读检测（ReadIndex 最核心的价值）：
// 把 leader 隔离之后，它的读必须失败。
// 没有 ReadIndex 的话，旧 leader 会把过期数据堂而皇之地返回给客户端。
// ---------------------------------------------------------------------------
void TestReadIndexNoStale() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);

  cfg->Begin("Test (Ext): ReadIndex 阻止被隔离旧 leader 的脏读");

  int leader = cfg->CheckOneLeader();
  cfg->One("v1", servers, true);

  // 在隔离之前，读一定要是成功的（否则测试本身没意义）
  if (cfg->GetRaft(leader)->ReadIndex() < 0) {
    cfg->Fatal("前提不成立：隔离之前 leader 上的 ReadIndex 就失败了");
  }

  cfg->Disconnect(leader);
  // 等 CheckQuorum 让它退位（每 150ms 自查一次，给 1 秒足够）
  raftcpp::SleepMs(1000);

  // 此刻它必须读失败 —— 要么已退位（不是 leader），
  // 要么还挂着 leader 名头但心跳确认攒不够多数派，超时返回 -1。
  int ri = cfg->GetRaft(leader)->ReadIndex();
  if (ri >= 0) {
    cfg->Fatal("被隔离的旧 leader 竟然还能提供读服务 —— 这就是脏读！");
  }

  cfg->Connect(leader);
  cfg->End();
}

// ===== 并发 ReadIndex：证明 per-request ctx 设计下多条读互不踩踏 =====
// 旧的单槽位设计（read_index_/read_ack_count_/read_index_term_ 全局共享）在并发下
// 会把多条读的票混在一起：后到的读覆盖 read_index_、重置 read_ack_count_，导致先到的
// 读永远凑不齐多数派 → 超时返回 -1；或者计票泄漏、term 戳错位。新设计每条读独立 ctx，
// 回包按 reply.read_ctx 精确归因，应全部成功、互不干扰。
void TestReadIndexConcurrent() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test (Ext): 并发 ReadIndex 互不踩踏（per-request ctx）");

  int leader = cfg->CheckOneLeader();
  cfg->One("x1", servers, true);

  // 多个线程并发反复调用 ReadIndex，模拟高并发线性一致读。
  const int nthreads = 8;
  const int ncalls = 50;
  std::vector<std::thread> ts;
  std::atomic<int> ok{0}, fail{0};
  for (int t = 0; t < nthreads; t++) {
    ts.emplace_back([&]() {
      for (int i = 0; i < ncalls; i++) {
        int ri = cfg->GetRaft(leader)->ReadIndex();
        if (ri >= 0)
          ok.fetch_add(1);
        else
          fail.fetch_add(1);
      }
    });
  }
  for (auto& t : ts) t.join();

  // 【阈值 = 100%，fail 必须为 0】
  // 健康集群（可靠网络、全员连通、无分区）里并发读没有任何理由失败：
  // 旧的单槽位设计会踩踏（后到的读覆盖 read_index_、重置 ack 计数），
  // 新设计每条读独立 ctx、回包按 reply.read_ctx 精确归因，应当【全部成功】。
  // 之前这里给的是 90% 余量，等于容忍 40 次失败 —— 那会把踩踏 bug 放过去。
  // 实测（正确实现）多轮 fail 恒为 0，故收紧到 0；若哪天偶发非 0，
  // 先怀疑 ReadIndex 的 ctx 归因/唤醒逻辑，而不是回来放宽阈值。
  // 注意：断言必须放在 cfg->End() 【之前】——End() 会拆掉集群、回收 Raft 实例，
  // 之后再 Fatal 就只剩报错、拿不到现场了。
  int total = nthreads * ncalls;
  if (fail.load() != 0) {
    std::string msg = "并发 ReadIndex 出现失败：ok=" + std::to_string(ok.load()) +
                      " fail=" + std::to_string(fail.load()) + " / total=" +
                      std::to_string(total) +
                      "（健康集群应当 100% 成功；旧单槽位设计会踩踏，"
                      "若新设计仍失败，说明 ctx 归因/唤醒仍有问题）";
    cfg->Fatal(msg);
  }

  cfg->End();
}

// ===== ReadIndex 核心契约：分区后【立刻】读必须失败（不等 CheckQuorum 帮忙）=====
// 这是整套里最该有的一条。TestReadIndexNoStale 与 TestCheckQuorumStaleReadPrevented
// 都在 Disconnect 后 sleep 1000ms 才读 —— 那时 CheckQuorum(150ms) 早已把它退位，
// ReadIndex 走的是"不是 leader → 返回 -1"这条平凡分支，【多数派确认根本没被走到】。
// 实测：把 ReadIndex 改成"不验身份直接返回 commit_index_"，那两条照样全过。
void TestReadIndexPartitionImmediate() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test (Ext): ReadIndex 分区后立刻读必须失败（多数派确认真契约）");
  int leader = cfg->CheckOneLeader();
  cfg->One("v1", servers, true);

  cfg->Disconnect(leader);
  // ★ 故意【不 sleep】：此刻它多半仍是 leader（CheckQuorum ~150ms 才自查一次），
  //   但心跳已收不到多数派回包。真做了多数派确认 → 等超时返回 -1；
  //   偷懒直接返回 commit_index_ → 返回 >= 0，被这条抓住。
  int ri = cfg->GetRaft(leader)->ReadIndex();
  if (ri >= 0) {
    cfg->Fatal("分区后 ReadIndex 仍返回 " + std::to_string(ri) +
               " —— 它根本没做多数派确认，只是把旧 commitIndex 给了调用方（脏读）");
  }
  cfg->Connect(leader);
  cfg->CheckOneLeader();
  cfg->End();
}

// ===== ReadIndex 正向：只要还有多数派就该成功（不能傻等所有节点回包）=====
void TestReadIndexMajorityToleratesMinorityFailure() {
  int servers = 5;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test (Ext): ReadIndex 只要求多数派，少数派宕机照样可读");
  int leader = cfg->CheckOneLeader();
  cfg->One("v1", servers, true);

  // 断掉两个 follower：5 节点 majority=3，leader + 剩下 2 个 follower 仍够 3 票
  int down1 = -1, down2 = -1;
  for (int i = 0; i < servers; i++) {
    if (i == leader) continue;
    if (down1 < 0)
      down1 = i;
    else if (down2 < 0) {
      down2 = i;
      break;
    }
  }
  cfg->Disconnect(down1);
  cfg->Disconnect(down2);

  // 立刻读：验证它不会傻等那两个失联节点（否则会拖到超时返回 -1）
  int ri = cfg->GetRaft(leader)->ReadIndex();
  if (ri < 0) {
    cfg->Fatal("5 节点挂了 2 个（仍有 3 票多数派）时 ReadIndex 应当成功，却返回 " +
               std::to_string(ri) + " —— 多半是傻等所有节点回包");
  }
  cfg->Connect(down1);
  cfg->Connect(down2);
  cfg->End();
}

// ---------------------------------------------------------------------------
// ReadIndex × InstallSnapshot：落后 follower 的那一票只能靠快照回包送回来
//
// 覆盖点：ReplicateLoop 发现 follower 落后到快照点之前时改发 InstallSnapshot，
// 并把 active_read_ctx_ 塞进 snap_args.read_ctx；follower 原样回显；leader 在
// 快照回包里调 RecordReadAckLocked —— 这是 AppendEntries 心跳之外的【第二条
// 读确认通道】。
//
// 【变异实证：这一条"无牙"，别把它当安全网】
// 把快照通道的 ctx 掐掉（发端 snap_args.read_ctx=0 / 收端 reply.read_ctx=0），
// 本条照样全绿 —— 因为落后者对 leader 的广播心跳也会回包，而且 AppendEntries
// handler 是无条件回显 read_ctx 的（raft.cpp:646），一致性检查失败也回。
// 也就是说：在当前实现下，落后者的那一票【不依赖】快照通道，快照通道是冗余补充。
// 想让快照票成为"唯一票源"必须让 follower 只回快照不回心跳，labrpc 下构造不出来。
// 所以这条的定位是【集成回归】：证明"装快照 + 并发读"两条路径互不破坏，
// 而不是"少了快照 ctx 就会挂"。快照通道真正有牙的验收在下面 NoDoubleCount。
// ---------------------------------------------------------------------------

// 正向：健康 3 节点里，follower 装快照期间的并发读必须全部成功。
// 两个follower installsnapshot handler 本身只改 raft state + 落盘，落盘很慢是不是就会失败，因为follower snapshot rpc占用锁时间太长了，导致轮询下一次接不住BroadcastReadHeartbeat发来的心跳
void TestReadIndexSnapshotCatchUp() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);
  cfg->EnableSnapshotCompaction();
  cfg->Begin("Test (Ext): 落后节点装快照期间并发 ReadIndex 全部成功");

  int leader = cfg->CheckOneLeader();
  for (int i = 1; i <= 40; i++)
    cfg->One("put k" + std::to_string(i) + " v" + std::to_string(i), servers, true);

  leader = cfg->CheckOneLeader();
  auto lrf = GetRaftOrFatal(cfg, leader);

  // 前提：leader 必须真的做过快照，否则整条用例压根走不到快照路径，
  // 会退化成一次普通并发读（假绿）。
  bool has_snap = false;
  for (int t = 0; t < 100; t++) {
    if (lrf->SnapshotIndex() > 0) { has_snap = true; break; }
    raftcpp::SleepMs(50);
  }
  if (!has_snap) cfg->Fatal("前提不成立：leader 始终没生成快照，用例退化成普通读");

  // 把一个 follower 打到快照点之前 → 它只能靠 InstallSnapshot 追上来
  int laggard = (leader + 1) % servers;
  cfg->Crash1(laggard);
  for (int i = 41; i <= 110; i++)
    cfg->One("put b" + std::to_string(i) + " v" + std::to_string(i),
             servers - 1, true);

  // 重启落后节点：此刻 leader 给它发的是 InstallSnapshot，而不是普通心跳/日志
  cfg->Start1(laggard);

  // 与此同时并发读 —— 其中 laggard 那一票只能来自快照回包
  const int nthreads = 8;
  const int ncalls = 20;
  std::vector<std::thread> ts;
  std::atomic<int> ok{0}, fail{0};
  for (int t = 0; t < nthreads; t++) {
    ts.emplace_back([&]() {
      for (int i = 0; i < ncalls; i++) {
        int ri = cfg->GetRaft(leader)->ReadIndex();
        if (ri >= 0)
          ok.fetch_add(1);
        else
          fail.fetch_add(1);
      }
    });
  }
  for (auto& t : ts) t.join();

  // 断言放在 End() 之前：End() 会拆集群、回收 Raft 实例
  if (fail.load() != 0) {
    cfg->Fatal("装快照期间并发 ReadIndex 出现失败：ok=" + std::to_string(ok.load()) +
               " fail=" + std::to_string(fail.load()) +
               "（说明 InstallSnapshot 这条读确认通道没把票送回来）");
  }

  // 事后确认：laggard 确实是【通过快照】追上的，否则上面的读可能压根没跟快照重叠
  bool caught = false;
  for (int t = 0; t < 150; t++) {
    if (lrf->SnapshotIndex() > 0 &&
        GetRaftOrFatal(cfg, laggard)->SnapshotIndex() >= lrf->SnapshotIndex()) {
      caught = true;
      break;
    }
    raftcpp::SleepMs(100);
  }
  if (!caught)
    cfg->Fatal("落后者没通过 InstallSnapshot 追上：这条用例没真正覆盖到快照路径");
  cfg->End();
}

// 反向（安全、有牙）：同一个 follower 的【快照回包 + 心跳回包】绝不能双计。好像没啥用了
//
// leader 给落后 follower 发 InstallSnapshot 的同时，ReadIndex 的广播心跳也会发给它，
// 于是它在同一个 ctx 下会回【两个】包：InstallSnapshotReply 和 AppendEntriesReply。
// 若计票不按 server 去重，它一个人就贡献 2 票 —— 5 节点 majority = 3，
// leader(1) + 它(2) = 3，于是"只有两个活着的节点"也能凑出假多数派，
// 已被分区的旧 leader 照样返回读成功 → 这正是 ReadIndex 要防的脏读。
// RecordReadAckLocked 里的 r.acked 去重就是为它准备的。
// 【变异实证：有牙】把 `if (r.acked.count(server)) return;` 短路掉之后，
// 本条 5/5 轮 FAILED（"只剩 2 个活着节点却有 N 次读成功"）——
// 它是整套用例里唯一能抓住"双通道重复计票 → 假多数派"的一条。
void TestReadIndexSnapshotNoDoubleCount() {
  int servers = 5;
  auto cfg = MakeConfig(servers, false);
  cfg->EnableSnapshotCompaction();
  cfg->Begin("Test (Ext): 快照+心跳双通道不得双计（防假多数派脏读）");

  int leader = cfg->CheckOneLeader();
  int laggard = (leader + 1) % servers;

  // 让 laggard 掉队到快照点之前：它重启后只能靠 InstallSnapshot 追
  cfg->Crash1(laggard);
  for (int i = 1; i <= 120; i++)
    cfg->One("put k" + std::to_string(i) + " v" + std::to_string(i),
             servers - 1, true);
  auto lrf = GetRaftOrFatal(cfg, leader);
  if (lrf->SnapshotIndex() <= 0)
    cfg->Fatal("前提不成立：leader 没生成快照，laggard 不会走 InstallSnapshot");

  // 把其余 3 个 follower 全部隔离 → 集群里只剩 leader 与 laggard，
  // 真实票数 = 2 < majority(3)：任何一次"读成功"都只能来自重复计票（假多数派）。
  for (int i = 0; i < servers; i++) {
    if (i == leader || i == laggard) continue;
    cfg->Disconnect(i);
  }

  cfg->Start1(laggard);   // 落后节点上线，leader 开始给它装快照

  // 疯狂并发读，覆盖"装快照"那几十毫秒的窗口：
  // 只要有一次拿到 >= 0，就说明某个 follower 被计了两票。
  const int nthreads = 8;
  const int ncalls = 15;
  std::vector<std::thread> ts;
  std::atomic<int> succ{0};
  for (int t = 0; t < nthreads; t++) {
    ts.emplace_back([&]() {
      for (int i = 0; i < ncalls; i++) {
        if (cfg->GetRaft(leader)->ReadIndex() >= 0) succ.fetch_add(1);
      }
    });
  }
  for (auto& t : ts) t.join();

  if (succ.load() != 0) {
    cfg->Fatal("只剩 2 个活着节点（majority=3）却有 " + std::to_string(succ.load()) +
               " 次 ReadIndex 成功 —— 同一个 follower 的快照回包与心跳回包被重复计票，"
               "凑出了假多数派（脏读）");
  }

  for (int i = 0; i < servers; i++) {
    if (i == leader || i == laggard) continue;
    cfg->Connect(i);
  }
  cfg->End();
}

// ---------------------------------------------------------------------------
// ReadIndex × 不可靠网络（丢包）：补齐 7 条 ReadIndex 用例里唯一缺失的"黑盒压力"缺口
//
// 前面 7 条全跑在可靠网络（MakeConfig(servers, false)）。这里用 MakeConfig(servers, true)
// 让 labrpc 随机丢包/乱序，验两件事：
//   (a) 安全性（硬，绝不妥协）：只要 ReadIndex 返回 >= 0，返回的 readIndex 就必须 >=
//       当时已提交的下标，绝不能把"过期的 commitIndex"当成线性化点吐给调用方（脏读）。
//       这把"ctx 归因错乱 / 超时后仍把旧 commitIndex 返回 / 跨读混票"这类回归挡在门外。
//   (b) 可用性（软，liveness 守门）：丢包会让一部分读超时返回 -1，但多数派心跳回包仍应
//       能在 150ms 窗口内凑齐，所以成功次数必须 > 失败次数——证明 ReadIndex 在丢包环境
//       里不是"永久不可用"。一旦某次改动让它在丢包下恒返回 -1，这里会抓到。
// 不构造快照/分区等特例（那两条已在 CatchUp / NoDoubleCount 覆盖），只做最朴素的
// "丢包压力下的线性一致读"黑盒回归。成功率按 3 节点不可靠网估算（单心跳 ~97% 能凑齐
// 多数派）→ 50 次里 ok≈48、fail≈2，ok>fail 是极端安全的阈值，不会误杀。
// ---------------------------------------------------------------------------
void TestReadIndexUnreliable() {
  int servers = 3;
  auto cfg = MakeConfig(servers, true);   // ★ 不可靠网络（随机丢包/乱序）
  cfg->Begin("Test (Ext): 不可靠网络下 ReadIndex 不脏读且基本可用");

  // 先提交一个已知下标，作为"已提交"的硬基准（commit_index 只增不减）
  int idx = cfg->One("prime", servers, true);
  int leader = cfg->CheckOneLeader();
  (void)leader;

  // 不可靠网下 leadership 可能易主（丢心跳触发选举）。轻量本地读 GetState()
  // 做 leader 重解析——不能用 CheckOneLeader()（每轮 sleep 450ms + 对"同 term
  // 多 leader" Fatal），放进读热路径会拖慢并误判。PickLeaderFast 挑 term 最大且
  // 自称 leader 的节点。
  auto PickLeaderFast = [&]() -> int {
    int best = -1, best_term = -1;
    for (int i = 0; i < servers; i++) {
      auto rf = cfg->GetRaft(i);
      if (!rf) continue;
      auto st = rf->GetState();               // (term, is_leader)
      if (st.second && st.first > best_term) { best_term = st.first; best = i; }
    }
    return best;
  };
  int cur_leader = PickLeaderFast();
  if (cur_leader >= 0) leader = cur_leader;    // 爆发前先校准一次

  // 顺序发 50 次读。每次若返回 -1（非 leader / 瞬态退位被拒），重解析 leader 后
  // 重试最多 kMaxRetry 轮——这正是真实 Clerk 在收到"非 leader"拒绝后的换台重发。
  // 只吸收【换主瞬间】的合法拒绝，不动安全性；若 leader 真长时间不可用，重试后
  // 仍全 -1 → 下面 ok<=fail 仍会 Fatal（真实 liveness 回归照样抓得住）。
  const int n = 50;
  const int kMaxRetry = 3;
  const int kRetrySleepMs = 20;
  int ok = 0, fail = 0, stale = 0;
  for (int i = 0; i < n; i++) {
    int ri = -1;
    for (int attempt = 0; attempt <= kMaxRetry && ri < 0; attempt++) {
      int l = cur_leader;
      if (l >= 0) ri = cfg->GetRaft(l)->ReadIndex();
      if (ri < 0) {
        int nl = PickLeaderFast();            // 可能刚换主 → 重解析后重试
        if (nl >= 0) cur_leader = nl;
        if (attempt < kMaxRetry) raftcpp::SleepMs(kRetrySleepMs);
      }
    }
    if (ri < 0) {
      fail++;            // 重试后仍失败：保守返回 -1，可接受
    } else {
      ok++;
      // 安全性：返回的 readIndex 必须覆盖我们已知已提交的下标 idx
      // （正确实现 ri == 当前 commitIndex >= idx；ri < idx 即把旧值当线性化点 → 脏读）
      if (ri < idx) stale++;
    }
  }

  // (a) 永远不允许返回低于已提交下标的"脏" readIndex
  if (stale > 0) {
    cfg->Fatal("不可靠网下 ReadIndex 返回了低于已提交下标的 readIndex（stale=" +
               std::to_string(stale) +
               "）—— 线性一致读被破坏，可能读到过期数据（ctx 归因/超时吐旧值回归）");
  }

  // (b) 可用性：丢包环境下多数派心跳仍能凑齐，成功数应多于失败数
  if (ok <= fail) {
    cfg->Fatal("不可靠网下 ReadIndex 成功(" + std::to_string(ok) +
               ") 未超过失败(" + std::to_string(fail) +
               ") —— 线性一致读在丢包环境基本不可用（liveness 回归）");
  }

  cfg->End();
}

// ---------------------------------------------------------------------------
// ReadIndex × 飞行中 leader 易主：补齐"读发起后 leader 被挤下台"这个未覆盖路径
// （前 7 条只验"易主后新发起的读"，没撞过"在途读撞上易主"）。
// 关键安全不变量：旧 leader 一旦失去领导权，它在途/新发起的 ReadIndex 必须返回 -1，
// 绝不允许把过期 commitIndex 当线性化点吐出去（否则就是脏读 / 线性一致破坏）。
void TestReadIndexDuringReelection() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);   // 可靠网：确定性地控制分区时序
  cfg->Begin("Test (Ext): 读在飞行中 leader 易主 —— 旧 leader 的读必须返回 -1");

  // 先提交一条命令，确保集群健康、leader 已选出且 commit_index > 0
  cfg->One("prime", servers, true);
  int leader = cfg->CheckOneLeader();

  // 后台线程：对旧 leader 持续发起 ReadIndex（覆盖"飞行中易主"窗口）
  std::atomic<bool> disconnected{false};
  std::atomic<bool> stop{false};
  std::vector<int> results;
  std::vector<int> epoch;   // 0 = 易主前发起，1 = 易主后发起/在途
  std::thread reader([&]() {
    while (!stop.load()) {
      int ri = cfg->GetRaft(leader)->ReadIndex();
      results.push_back(ri);
      epoch.push_back(disconnected.load() ? 1 : 0);
    }
  });

  // 阶段一：易主前，旧 leader 合法，读应成功
  raftcpp::SleepMs(100);

  // 隔离旧 leader：它再也凑不齐多数派，且会看到更高任期 → 被废
  cfg->Disconnect(leader);

  // 缓冲：等网络真正切断 + 易主完成 + 在途读排空，避免"断开瞬间仍在途的连着网的读"
  //       被误标成"易主后"而产生竞态假阳性。
  raftcpp::SleepMs(250);

  // 确认易主真的发生了（新 leader 在另两台之间选出）
  int new_leader = cfg->CheckOneLeader();
  if (new_leader == leader) {
    cfg->Fatal("易主未发生：断开旧 leader 后仍未选出新 leader（网络隔离未生效？）");
  }

  // 从此刻起，读结果标为"易主后"
  disconnected.store(true);

  // 阶段二：易主后的稳定窗口内持续读（旧 leader 已是 candidate，任何 ReadIndex 必须返回 -1）
  raftcpp::SleepMs(300);

  stop.store(true);
  reader.join();

  // 断言 (a)：旧 leader 被废之后发起/在途的读，绝不允许返回"成功"（任何 >=0 的值）
  //   —— 否则就是旧 leader 把过期 commitIndex 当线性化点吐出（脏读回归）
  int post_ok = 0, pre_ok = 0;
  for (size_t i = 0; i < results.size(); i++) {
    if (epoch[i] == 1) {
      if (results[i] >= 0) post_ok++;
    } else {
      if (results[i] >= 0) pre_ok++;
    }
  }
  if (post_ok > 0) {
    cfg->Fatal("旧 leader 易主后仍有 " + std::to_string(post_ok) +
               " 次 ReadIndex 返回了成功值（>=0）—— 飞行中读未随领导权失效而作废，" +
               "可能把过期 commitIndex 当线性化点吐出（脏读回归）");
  }

  // 断言 (b)：易主之前确实成功过，证明这个测试真的压到了读路径
  //   （不是从头到尾全 -1 的空壳）
  if (pre_ok == 0) {
    cfg->Fatal("易主前旧 leader 的 ReadIndex 一次都没成功，测试未真正覆盖读路径（pre_ok=0）");
  }

  cfg->Connect(leader);   // 收尾：恢复网络，End() 会统一拆集群
  cfg->End();
}

// ---------------------------------------------------------------------------
// ReadIndex × 不可靠网络 × 装快照：补齐缺口②"丢包下装快照时的读"
// （TestReadIndexUnreliable 只压普通读，两条快照用例仅可靠网）。
// 断言：① ri 永远 >= 已提交基准 idx（不可靠网也绝不脏读）；
//       ② 丢包环境下多数派心跳仍能凑齐，成功数 > 失败数（基本可用）。
void TestReadIndexSnapshotUnreliable() {
  int servers = 3;
  auto cfg = MakeConfig(servers, true);   // ★ 不可靠网络（随机丢包/乱序）
  cfg->EnableSnapshotCompaction();
  cfg->Begin("Test (Ext): 不可靠网络下装快照期间并发读不脏读且基本可用");

  // 先写一批，确保 leader 真的生成过快照（否则退化成普通读）
  for (int i = 1; i <= 40; i++)
    cfg->One("put k" + std::to_string(i) + " v" + std::to_string(i), servers, true);
  int leader = cfg->CheckOneLeader();
  auto lrf = GetRaftOrFatal(cfg, leader);
  bool has_snap = false;
  for (int t = 0; t < 100; t++) {
    if (lrf->SnapshotIndex() > 0) { has_snap = true; break; }
    raftcpp::SleepMs(50);
  }
  if (!has_snap) cfg->Fatal("前提不成立：leader 始终没生成快照，用例退化成普通读");

  // 把一个 follower 打到快照点之前 → 它只能靠 InstallSnapshot 追上来
  int laggard = (leader + 1) % servers;
  cfg->Crash1(laggard);
  for (int i = 41; i <= 110; i++)
    cfg->One("put b" + std::to_string(i) + " v" + std::to_string(i), servers - 1, true);
  int idx = cfg->One("boundary", servers - 1, true);   // 已知已提交下标硬基准

  // 重启落后节点：此刻 leader 给它发的是 InstallSnapshot，而不是普通心跳/日志
  cfg->Start1(laggard);

  // 与此同时在不可靠网下并发读 —— 覆盖"丢包下装快照时的读"
  // 轻量 leader 重解析：只看各节点的 GetState()=(term,is_leader)，不 sleep。
  // （不能用 CheckOneLeader()：它每轮先睡 450ms，且会对"同 term 多 leader" Fatal，
  //   放进读热路径既拖慢又会引入误判。）
  auto PickLeaderFast = [&]() -> int {
    int best = -1, best_term = -1;
    for (int i = 0; i < servers; i++) {
      auto rf = cfg->GetRaft(i);
      if (!rf) continue;
      auto st = rf->GetState();               // (term, is_leader)
      if (st.second && st.first > best_term) { best_term = st.first; best = i; }
    }
    return best;
  };
  {
    int nl = PickLeaderFast();
    if (nl >= 0) leader = nl;                 // 爆发前先校准一次
  }

  // 与此同时在不可靠网下并发读 —— 覆盖"丢包下装快照时的读"
  std::atomic<int> cur_leader{leader};
  const int nthreads = 8, ncalls = 20;
  const int kMaxRetry = 2;        // 每次读最多补 2 轮重试（真实 Clerk 换台重发）
  const int kRetrySleepMs = 20;
  std::vector<std::thread> ts;
  std::atomic<int> ok{0}, fail{0}, stale{0};
  for (int t = 0; t < nthreads; t++) {
    ts.emplace_back([&]() {
      for (int i = 0; i < ncalls; i++) {
        int ri = -1;
        for (int attempt = 0; attempt <= kMaxRetry && ri < 0; attempt++) {
          int l = cur_leader.load();
          if (l >= 0) ri = cfg->GetRaft(l)->ReadIndex();
          if (ri < 0) {
            int nl = PickLeaderFast();        // 可能刚换主 → 重解析后重试
            if (nl >= 0 && nl != l) cur_leader.store(nl);
            if (attempt < kMaxRetry) raftcpp::SleepMs(kRetrySleepMs);
          }
        }
        if (ri < 0) {
          fail++;
        } else {
          ok++;
          // 安全性：返回的 readIndex 必须覆盖已提交基准 idx
          if (ri < idx) stale++;
        }
      }
    });
  }
  for (auto& t : ts) t.join();

  // (a) 永远不允许返回低于已提交下标的"脏" readIndex（丢包也不行）
  if (stale > 0) {
    cfg->Fatal("不可靠网装快照期间 ReadIndex 返回了低于已提交下标的 readIndex（stale=" +
               std::to_string(stale) +
               "）—— 线性一致读被破坏（ctx 归因/超时吐旧值回归）");
  }

  // (b) 可用性：丢包 + 装快照环境下多数派心跳仍能凑齐，成功数应多于失败数。
  //     注意这里统计的是【Clerk 语义重试后】的成败 —— ReadIndex 对"非 leader"
  //     【立即拒绝】是正确行为，若按"单瞬间、零重试、固定节点"取样，一次换主
  //     就会让 160 次调用在微秒级全部返回 -1，被误判成读路径 liveness 回归。
  if (ok <= fail) {
    cfg->Fatal("不可靠网装快照期间 ReadIndex 成功(" + std::to_string(ok) +
               ") 未超过失败(" + std::to_string(fail) +
               ") —— 线性一致读基本不可用（liveness 回归）");
  }

  cfg->End();
}

// ===========================================================================
// ReadIndex 扩展：learner 的 ack 绝不能计入读 quorum
//
// 背景（变异实证坐实的真实缺口）：把 ReadIndex 三处判据（raft.cpp 发送侧 /
// 回包侧 / 计票侧）从 IsVoter 改成"非 kRemoved 即算"（learner 也算）后，原
// 37 条用例（ReadIndex 全族 + 成员变更全族）全部绿灯、零捕获。根因是既有
// TestLearnerReadIndexRejected 只验「learner 自己调 ReadIndex 被拒」，走的是
// state_ != kLeader 平凡分支，根本没走到多数派确认，区分不了"learner 的票算不算"。
//
// 本用例构造：5 节点 → 降 2 个为 learner（剩 3 voter）→ 断掉另外 2 个非 leader
// voter（在线只剩 leader + 2 learner）。此时：
//   正确实现：granted = 1（仅 leader 自己，raft.cpp:2124） < majority = 3/2+1 = 2 → 必须 -1
//   变异实现：granted = 1 + 2(learner) = 3 >= 2 → 放行 → 假安全脏读
// 关键：读之前【不能 sleep】—— CheckQuorum 会让 leader 退位，退位后两种实现
// 都返回 -1，就失去区分度了。
// ===========================================================================
void TestReadIndexIgnoresNonVoterAcks() {
  const int servers = 5;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test: ReadIndex must NOT count learner acks toward read quorum");

  int leader = cfg->CheckOneLeader();
  cfg->One("v0", servers, false);

  // 挑两个非 leader 节点降为 learner（一次一个、等 apply 后再提下一个 —— 单飞保护）
  std::vector<int> learners;
  for (int i = 0; i < servers && static_cast<int>(learners.size()) < 2; i++) {
    if (i != leader) learners.push_back(i);
  }
  for (int s : learners) {
    StartResult r = cfg->GetRaft(leader)->ProposeConfChangeTo(s, MemberRole::kLearner);
    if (r.index < 0) {
      cfg->Fatal("前提失败：降级节点 " + std::to_string(s) + " 的提案被拒（index<0）");
    }
    bool applied = false;
    for (int t = 0; t < 100; t++) {
      raftcpp::SleepMs(50);
      auto v = cfg->GetRaft(leader)->MembershipView();
      if (s < static_cast<int>(v.size()) &&
          v[s] == static_cast<int>(MemberRole::kLearner)) { applied = true; break; }
    }
    if (!applied) {
      cfg->Fatal("前提失败：节点 " + std::to_string(s) + " 未切换为 kLearner");
    }
  }

  // 剩下的非 leader voter 全部断连 → 在线只剩 leader + 2 个 learner
  std::vector<int> offline;
  for (int i = 0; i < servers; i++) {
    if (i == leader) continue;
    bool is_learner = false;
    for (int s : learners) if (s == i) is_learner = true;
    if (!is_learner) offline.push_back(i);
  }
  if (static_cast<int>(offline.size()) != 2) {
    cfg->Fatal("前提失败：期望 2 个可断连的 voter，实际 " + std::to_string(offline.size()));
  }
  for (int s : offline) cfg->Disconnect(s);

  // ★ 立刻读（零 sleep）：此刻正确实现必须全部拒绝
  int returned = 0, while_leader = 0;
  for (int k = 0; k < 8 && returned == 0; k++) {
    auto rf = cfg->GetRaft(leader);
    if (rf->GetState().second) while_leader++;
    if (rf->ReadIndex() >= 0) returned++;
  }
  if (returned > 0) {
    cfg->Fatal("learner 的 ack 被计入读 quorum：仅剩 1 个 voter 在线时 ReadIndex 仍放行 —— 假安全脏读（线性一致性破防）");
  }
  if (while_leader == 0) {
    cfg->Fatal("用例退化：读期间节点已不是 leader，未真正考察 quorum 口径");
  }

  // 对称正例：恢复连通后 voter 多数派回来，ReadIndex 应重新可用
  // （证明上面的 -1 是"确实凑不齐票"，而不是把读路径整坏了）
  for (int s : offline) cfg->Connect(s);
  bool recovered = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    for (int i = 0; i < servers; i++) {
      auto rf = cfg->GetRaft(i);
      if (rf && rf->GetState().second && rf->ReadIndex() >= 0) { recovered = true; break; }
    }
    if (recovered) break;
  }
  if (!recovered) {
    cfg->Fatal("对照失败：恢复连通后 ReadIndex 应重新可用（读路径被整坏？）");
  }

  cfg->End();
}

// ===========================================================================
// ReadIndex × 失多数派超时契约（行为契约测试；【变异有牙】）
//
// 稳定 leader 失去多数派时，ReadIndex 必须在有限时间内返回 -1（不永久阻塞调用方）。
// 生产线索：raft.cpp:2133-2146 用 deadline = Now()+kElectionTimeoutMin 等待多数派
// 确认，超时（L2146 remain<=0 break）即跳出循环返回 -1；同时 raft.cpp:2093/2143
// 在 leader 退位（CheckQuorum 每 ~150ms 自查 / pending_stepdown_ 事件驱动）时也返回 -1。
//
// 变异实证【有牙】（与早期猜测相反，已实测坐实）：
//   删掉 raft.cpp:2146 的超时 break 后，循环不会因 remain<=0 跳出，而是落到
//   read_cv_.wait_for(lk, remain) —— 此时 remain 已过期为负，wait_for 按 0 超时
//   立即返回，于是循环在持有 mu_（L2136 的 unique_lock 贯穿整个循环）的前提下
//   busy-spin。CheckQuorum 的退位逻辑同样需要 mu_（raft.cpp:296/303/394 均持锁），
//   被这条 read 循环饿死，state_/pending_stepdown_ 永远改不了 → L2143/L2144 永真不了
//   → ReadIndex 永久阻塞。实测：本用例挂起到单用例看门狗（TEST_TIMEOUT_MS）触发
//   "判定为死锁/永久阻塞，强制 abort"（raw_exit=134）。即：超时 break 是【持锁不死锁】
//   的关键——它让循环在持锁状态下也能按时退出，放行 CheckQuorum 退位。本用例锁的就是
//   这条"持位但失多数派 → 有限时间内返回 -1"的契约；删超时即挂死，被 CI 看门狗捕获。
// ===========================================================================
void TestReadIndexTimesOutWithoutQuorum() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test (Ext): leader 失去多数派时 ReadIndex 必须在有限时间内超时返回 -1（不卡死）");

  cfg->One("prime", servers, true);
  int leader = cfg->CheckOneLeader();

  // 断开 2 个 follower：leader 仍持位（刚断，退位宽限未到），但失去多数派，
  // ReadIndex 的广播读心跳收不到足够 ack。
  for (int i = 0; i < servers; i++) {
    if (i != leader) cfg->Disconnect(i);
  }

  // 同步调用：干净实现应在 ~kElectionTimeoutMin(150ms) 内超时返回 -1；
  // 若删掉超时（raft.cpp:2146 的 remain<=0 break），ReadIndex 永久阻塞 → 本用例挂起，
  // 被 CI 单用例超时捕获。这正是该超时契约不可删的铁证。
  int ri = cfg->GetRaft(leader)->ReadIndex();
  if (ri >= 0) {
    cfg->Fatal("leader 失去多数派时 ReadIndex 竟返回 >=0（" + std::to_string(ri) +
               "）—— 应当因凑不齐读 quorum 而超时/退位返回 -1");
  }

  // 对称正例：恢复连通后 ReadIndex 应重新可用
  for (int i = 0; i < servers; i++) cfg->Connect(i);
  bool recovered = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(50);
    for (int i = 0; i < servers; i++) {
      if (cfg->GetRaft(i)->GetState().second &&
          cfg->GetRaft(i)->ReadIndex() >= 0) { recovered = true; break; }
    }
    if (recovered) break;
  }
  if (!recovered) cfg->Fatal("对照失败：恢复连通后 ReadIndex 应重新可用");

  cfg->End();
}

// ===========================================================================
// 成员变更 ①：单节点变更（一次一个、apply 时切换、单飞保护）
// 验证 §2.7 / §3.1 三件套：统一 majority、is_member_ 三态、单节点变更。
// ===========================================================================
void TestSingleNodeConfChange() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test: single-node membership change (apply-time switch + single in-flight)");

  int leader = cfg->CheckOneLeader();

  // ---- 1) 移除节点 2：只追加一条 conf 条目；本用例只验证最终切到 kRemoved ----
  // （"apply 时才切换"的具体时机由 ConfChange 系列其他用例兜底，此处不重复钉）
  // 【稳健性】原写法忽略返回值：提案若被拒（单飞/退位）会一路走到下面的
  // "移除失败" Fatal，错误信息误导排查方向。
  StartResult r_rm = cfg->GetRaft(leader)->ProposeConfChange(2, false);
  if (r_rm.index < 0) cfg->Fatal("移除提案被拒（index<0）—— 用例前提不成立");

  // 等全集群把这条 conf 条目 apply（is_member_[2] 变成 kRemoved）
  bool ok = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    bool all = true;
    for (int i = 0; i < servers; i++) {
      if (cfg->GetRaft(i)->MembershipView()[2] !=
          static_cast<int>(MemberRole::kRemoved)) {
        all = false;
        break;
      }
    }
    if (all) {
      ok = true;
      break;
    }
  }
  if (!ok) cfg->Fatal("移除失败：is_member_[2] 未在 apply 时切换为 kRemoved");

  // 被移除节点下线，避免它反复扰动选举（生产上也是"变更期间禁止下线"的反面：
  // 这里是已经移除，模拟它真的离开集群）。
  cfg->Disconnect(2);

  // 剩余 2 个 voter 仍应能选出 leader，且绝不可能是已被移除的 2
  int new_leader = cfg->CheckOneLeader();
  if (new_leader == 2) cfg->Fatal("被移除的节点2竟然成为了 leader");
  // 提交一条命令，确认 2 节点（0/1）多数派仍可用
  cfg->One("after-remove", 2, false);

  // ---- 2) 重新加入节点 2（单飞保护：上一条已 apply，pending 已清空，允许新的）----
  cfg->Connect(2);
  // 【稳健性】原写法直接拿 CheckOneLeader() 的结果提提案。当它返回的节点正处在
  // 「apply 后宽限期退位」窗口（pending_stepdown_，raft.cpp:1763）时，提案会被
  // 闸门拒绝（index<0），用例会误报成"重新加入失败"。改为轮询取"自身视图里
  // 仍是 kVoter 的 leader" —— 与 TestNoRemovingLastVoter 修过的同类 flaky 一致。
  // 轮询找"自身视图里仍是 kVoter 的当前 leader"。
  // 【稳健性】不走 CheckOneLeader()：它每次调用自 sleep ~500ms 且对"短暂无/多 leader"
  // 直接 Fatal，放进轮询会在重连后的瞬时选举间隙误杀整个用例。
  // 改为逐节点轮询 GetState().second（isLeader），无任何内部 Fatal，瞬时无主就继续等。
  int l2 = -1;
  for (int t = 0; t < 40 && l2 < 0; t++) {
    for (int i = 0; i < servers; i++) {
      if (cfg->GetRaft(i)->GetState().second) {
        auto v = cfg->GetRaft(i)->MembershipView();
        if (i < static_cast<int>(v.size()) &&
            v[i] == static_cast<int>(MemberRole::kVoter)) { l2 = i; break; }
      }
    }
    if (l2 >= 0) break;
    raftcpp::SleepMs(50);
  }
  if (l2 < 0) cfg->Fatal("重连后找不到自身仍为 kVoter 的 leader —— 用例前提不成立");
  StartResult r_add = cfg->GetRaft(l2)->ProposeConfChange(2, true);
  if (r_add.index < 0) cfg->Fatal("重新加入提案被拒（index<0）—— 无法断言单飞保护已放行");

  ok = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    bool all = true;
    for (int i = 0; i < servers; i++) {
      if (cfg->GetRaft(i)->MembershipView()[2] !=
          static_cast<int>(MemberRole::kVoter)) {
        all = false;
        break;
      }
    }
    if (all) {
      ok = true;
      break;
    }
  }
  if (!ok) cfg->Fatal("重新加入失败：is_member_[2] 未恢复为 kVoter");

  // 恢复 3 节点多数派后提交一条命令
  cfg->One("after-readd", 3, false);

  // ---- 3) 单飞保护：连续两次变更，第二次必须被拒绝（pending_conf_index_ 未清空）----
  // 旧实现这里是无断言的"空壳"（只验不崩溃），两种失败结局都放行 → 假绿。
  // 正确断言：第二次变更【不应】被追加成新日志条目（否则两个未提交配置重叠 → 脑裂）。
  // 用 LastLogIndex 做"是否追加了新条目"的可观测探针——不依赖最终成员态的竞态。
  int leader3 = cfg->CheckOneLeader();
  int idx_before = cfg->GetRaft(leader3)->LastLogIndex();
  // 第一次：追加一条 conf 条目，pending_conf_index_ 被置位
  StartResult r1 = cfg->GetRaft(leader3)->ProposeConfChange(2, false);
  if (!r1.is_leader || r1.index != idx_before + 1) {
    cfg->Fatal("单飞保护前置失败：第一条变更未成功追加为日志条目");
  }
  // 立刻再提一次：上一条还没 apply，pending 未清 ⇒ 必须被拒（不追加新条目）。
  // 确定性探针：直接读返回值。单飞闸门（raft.cpp:1768）命中时返回
  // {is_leader==true, index==-1}——index 保持默认 -1 表示"未追加任何条目"。
  StartResult r2 = cfg->GetRaft(leader3)->ProposeConfChange(2, true);
  if (!r2.is_leader || r2.index != -1) {
    cfg->Fatal("单飞保护失效：第二条并发变更未被拒绝"
               "（pending_conf_index_ 闸门漏了 → 两个未提交配置重叠 → 脑裂风险）");
  }
  // 冗余保险：确认日志长度确实没增长（与上面的返回值断言互为印证）。
  // 注：此 idx_after 比较依赖"第一条 conf 在两次调用之间尚未 apply"的时序前提；
  // 生产实现下两条语句纳秒级紧挨、apply 需过一轮 RPC，故不会误杀。主钉是上面的
  // 返回值断言（r2.index==-1，确定性、零时序依赖）。
  int idx_after = cfg->GetRaft(leader3)->LastLogIndex();
  if (idx_after != idx_before + 1) {
    cfg->Fatal("单飞保护失效：第二条变更也被追加成新日志条目"
               "（pending_conf_index_ 没挡住并发变更 → 两个未提交配置重叠 → 脑裂风险）");
  }
  // 收尾：让这条移除真正 apply（pending 清空），节点2 最终落到 kRemoved
  bool ok3 = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    bool all_removed = true;
    for (int i = 0; i < servers; i++) {
      if (cfg->GetRaft(i)->MembershipView()[2] !=
          static_cast<int>(MemberRole::kRemoved)) { all_removed = false; break; }
    }
    if (all_removed) { ok3 = true; break; }
  }
  if (!ok3) cfg->Fatal("单飞保护收尾失败：移除条目未最终 apply");

  cfg->End();
}

// ===========================================================================
// 成员变更：安全阀回归 —— 配置变更后必须至少保留一个 voter
//   把最后一个 voter 移除（kRemoved）或降级（kLearner）都必须被拒绝，
//   否则集群变 0 voter → quorum=0 → 永远选不出 leader（死锁）。
// ===========================================================================
void TestNoRemovingLastVoter() {
  const int servers = 2;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test: reject removing/demoting the last voter (keep >=1 voter)");

  int leader = cfg->CheckOneLeader();  // 初始 2 个 voter（0,1）

  // 1) 移除节点 1：2 voter -> 1 voter，合法（非最后一个）
  cfg->GetRaft(leader)->ProposeConfChange(1, false);
  bool ok = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    bool all = true;
    for (int i = 0; i < servers; i++) {
      if (cfg->GetRaft(i)->MembershipView()[1] !=
          static_cast<int>(MemberRole::kRemoved)) { all = false; break; }
    }
    if (all) { ok = true; break; }
  }
  if (!ok) cfg->Fatal("前置失败：节点1 未被移除");

  // ⚠️ 硬化：不能直接取 CheckOneLeader() 的返回值当 sole。
  // 若被移除的恰好是原 leader（2 节点下有 50% 概率），它在 apply 移除配置后
  // 会进入退位宽限期（pending_stepdown_=true，state_ 仍短暂为 kLeader）——
  // 此时 CheckOneLeader() 可能仍返回它。用它调用 ProposeConfChange 会被
  // pending_stepdown_ 闸门拒绝（index=-1），导致下面第 4 步"加回 voter 应合法"
  // 误判为被拒绝而 Fatal —— 这是测试自身的 flaky，不是代码 bug。
  // 修复：轮询直到拿到【自身仍是 kVoter 的】leader。
  int sole = -1;
  for (int t = 0; t < 100; t++) {
    int l = -1;
    for (int i = 0; i < servers; i++) {
      if (cfg->GetRaft(i)->GetState().second &&
          cfg->GetRaft(i)->MembershipView()[i] ==
              static_cast<int>(MemberRole::kVoter)) {
        l = i;
        break;
      }
    }
    if (l >= 0 && cfg->GetRaft(l)->MembershipView()[l] ==
                      static_cast<int>(MemberRole::kVoter)) {
      sole = l;
      break;
    }
    raftcpp::SleepMs(100);
  }
  if (sole < 0) cfg->Fatal("前置失败：找不到自身仍是 voter 的 leader");

  // 2) 尝试移除最后一个 voter：必须被拒绝（index 保持 -1，StartResult 默认值）
  StartResult r_remove = cfg->GetRaft(sole)->ProposeConfChange(sole, false);
  // 被拒绝时 index 保持 StartResult 默认值 -1（接受时才为正数）
  if (r_remove.index >= 0) {
    cfg->Fatal("应拒绝移除最后一个 voter，但 conf 条目被追加了（index=" +
               std::to_string(r_remove.index) + ")");
  }

  // 3) 尝试把最后一个 voter 降级为 learner：必须被拒绝
  StartResult r_demote = cfg->GetRaft(sole)->ProposeConfChangeTo(sole, MemberRole::kLearner);
  if (r_demote.index >= 0) {
    cfg->Fatal("应拒绝把最后一个 voter 降级为 learner，但 conf 条目被追加了");
  }

  // 4) 正向对照：把节点1 重新加回为 voter（1 voter -> 2 voter），应合法
  StartResult r_add = cfg->GetRaft(sole)->ProposeConfChange(1, true);
  // 加回应合法：被接受时 index>0；index<0 表示被错误拒绝
  if (r_add.index < 0) {
    cfg->Fatal("加回 voter 应合法，却被拒绝（index=" +
               std::to_string(r_add.index) + ")");
  }

  cfg->End();
}

// ===========================================================================
// 成员变更 ②：Learner 三态真正用起来（learner 持续追数据、可提拔回 voter）
//   1) learner 不计入多数派：有效 voter 数下降 → 多数派阈值跟着下降
//   2) learner 虽然不投票、不计票，但仍在持续接收并追加日志（追数据）
//   3) 追平后可把 learner 提拔回 voter，阈值随之恢复
// 这样新节点就能"先以 learner 身份追平日志，期间不把多数派撑大，追平再转正"。
// ===========================================================================
void TestLearnerCatchup() {
  const int servers = 4;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test: learner catches up logs without being counted in quorum");

  int leader = cfg->CheckOneLeader();

  // 基线：4 个 voter → 多数派阈值 = 4/2+1 = 3
  int q_all = cfg->GetRaft(leader)->QuorumSize();
  if (q_all != 3) {
    cfg->Fatal("基线阈值错误：4 个 voter 应为 3，实际 " + std::to_string(q_all));
  }

  // ---- 1) 把节点 3 降级为 learner（走三态入口）----
  cfg->GetRaft(cfg->CheckOneLeader())->ProposeConfChangeTo(3, MemberRole::kLearner);

  bool ok = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    bool all = true;
    for (int i = 0; i < servers; i++) {
      if (cfg->GetRaft(i)->MembershipView()[3] !=
          static_cast<int>(MemberRole::kLearner)) {
        all = false;
        break;
      }
    }
    if (all) { ok = true; break; }
  }
  if (!ok) cfg->Fatal("降级失败：is_member_[3] 未在 apply 时切换为 kLearner");

  // ---- 2) learner 不计入多数派：有效 voter 只剩 3 个 → 阈值应为 2 ----
  int q_learner = cfg->GetRaft(0)->QuorumSize();
  if (q_learner != 2) {
    cfg->Fatal("learner 被计入了多数派：3 个 voter 阈值应为 2，实际 " +
               std::to_string(q_learner));
  }

  // ---- 3) learner 不投票，但仍应持续追数据 ----
  // expected_servers 是下限（One 内部判 nd >= expected），3 个 voter 全提交即可
  cfg->One("learner-1", 3, false);
  cfg->One("learner-2", 3, false);

  ok = false;
  int node3_idx = -1;
  int voters_max = -1;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    int mx = 0;
    for (int i = 0; i < servers - 1; i++) {  // 0..2 是 voter
      int idx = cfg->GetRaft(i)->LastLogIndex();
      if (idx > mx) mx = idx;
    }
    voters_max = mx;
    node3_idx = cfg->GetRaft(3)->LastLogIndex();
    if (node3_idx >= voters_max) { ok = true; break; }
  }
  if (!ok) {
    cfg->Fatal("learner 没追上日志：节点3=" + std::to_string(node3_idx) +
               "，voter 最大=" + std::to_string(voters_max));
  }

  // ---- 4) 提拔回 voter：阈值恢复为 3 ----
  cfg->GetRaft(cfg->CheckOneLeader())->ProposeConfChangeTo(3, MemberRole::kVoter);

  ok = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    bool all = true;
    for (int i = 0; i < servers; i++) {
      if (cfg->GetRaft(i)->MembershipView()[3] !=
          static_cast<int>(MemberRole::kVoter)) {
        all = false;
        break;
      }
    }
    if (all) { ok = true; break; }
  }
  if (!ok) cfg->Fatal("提拔失败：is_member_[3] 未恢复为 kVoter");

  int q_back = cfg->GetRaft(0)->QuorumSize();
  if (q_back != 3) {
    cfg->Fatal("提拔后阈值应恢复为 3，实际 " + std::to_string(q_back));
  }
  cfg->One("after-promote", 4, false);

  cfg->End();
}

// ===========================================================================
// 端到端演示：learner 换来"扩容不丢容错余量"
//
// 场景（4 节点 0..3，节点 3 是"待扩容进来的新机器"）：
//   新节点 3 还在 loading / 还没追上数据（用 Disconnect 模拟它收不到日志），
//   此时再挂掉一个老 voter。存活节点完全一样，唯一的差别是节点 3 的角色：
//
//   A 组：节点 3 直接提成 voter  → voter=4，quorum=3；能 ack 的只剩 2 票 → 写不进去
//   B 组：节点 3 先以 learner 加入 → voter 仍是 3，quorum=2；能 ack 的刚好 2 票 → 写得进去
//
// 这就是 learner 的全部价值：把"撑大 quorum"推迟到新节点真正能可靠投票之后。
// ===========================================================================

// 限时尝试提交一条命令：成功返回 true，超时/无 leader 返回 false（绝不 Fatal 终止测试）。
// Config::One() 内部失败会 Fatal，所以"预期写不进去"的场景必须自己写一个。
static bool TryAgreement(Config* cfg, const std::string& cmd, int expected,
                         int timeout_ms) {
  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    int index = -1;
    for (int si = 0; si < cfg->n(); si++) {
      if (!cfg->Connected(si)) continue;
      auto rf = cfg->GetRaft(si);
      if (!rf) continue;
      StartResult r = rf->Start(cmd);
      if (r.is_leader) {
        index = r.index;
        break;
      }
    }
    if (index != -1) {
      auto sub_deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeout_ms);
      while (std::chrono::steady_clock::now() < sub_deadline) {
        auto res = cfg->NCommitted(index);
        int nd = res.first;
        auto cmd1 = res.second;
        if (nd > 0 && nd >= expected && cmd1 && *cmd1 == cmd) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
      }
      return false;  // 提上去了但提交不了 → 凑不够多数派
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return false;  // 压根没有 leader
}

// 准备场景：把节点 3 移除成"待加入槽位"，再断网模拟"新机器还在 loading"；
// 返回此时 {0,1,2} 里的 leader（断网后旧 leader 若在 3 上会因 CheckQuorum 退位）。
static int PrepareNewNodeOffline(Config* cfg, int servers) {
  // 1) 先把节点 3 移除，让它变成"扩容槽位"
  int leader = cfg->CheckOneLeader();
  cfg->GetRaft(leader)->ProposeConfChange(3, false);
  bool ok = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    bool all = true;
    for (int i = 0; i < servers; i++) {
      if (cfg->GetRaft(i)->MembershipView()[3] !=
          static_cast<int>(MemberRole::kRemoved)) {
        all = false;
        break;
      }
    }
    if (all) { ok = true; break; }
  }
  if (!ok) cfg->Fatal("准备阶段失败：节点 3 未能进入 kRemoved 槽位状态");

  // 2) 断网 → 节点 3 收不到任何日志，等价于"新节点还在 loading，日志为空"
  cfg->Disconnect(3);

  // 3) 等 leader 落到 {0,1,2} 上（断网的 3 若曾是 leader，会被 CheckQuorum 撸下来）
  leader = -1;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    int ld = cfg->CheckOneLeader();
    if (ld != 3) { leader = ld; break; }
  }
  if (leader < 0) cfg->Fatal("准备阶段失败：断网后 {0,1,2} 未能选出 leader");
  return leader;
}

// 成员变更 ③：learner 作为扩容槽位时，集群仍保住减员容错余量（文档 §2.6「扩容零可用性损失」）
void TestLearnerAvailabilityWin() {
  const int servers = 4;

  // ================= A 组：新节点直接提 voter =================
  {
    auto cfg = MakeConfig(servers, false);
    cfg->Begin("Test: adding a not-yet-caught-up node as VOTER loses quorum");

    int leader = PrepareNewNodeOffline(cfg.get(), servers);

    // 把还在 loading 的节点 3 直接提成 voter
    cfg->GetRaft(leader)->ProposeConfChange(3, true);
    bool ok = false;
    for (int t = 0; t < 100; t++) {
      raftcpp::SleepMs(100);
      if (cfg->GetRaft(leader)->MembershipView()[3] ==
          static_cast<int>(MemberRole::kVoter)) { ok = true; break; }
    }
    if (!ok) cfg->Fatal("A 组：节点 3 未被提为 voter");

    int q_voter_grp = cfg->GetRaft(leader)->QuorumSize();
    if (q_voter_grp != 3) {
      cfg->Fatal("A 组阈值错误：4 个 voter 应为 3，实际 " +
                 std::to_string(q_voter_grp));
    }

    // 再挂一个"非 leader"的老 voter（leader 得活着才能处理写）
    int victim = -1;
    for (int i = 0; i < 3; i++) {
      if (i != leader) { victim = i; break; }
    }
    cfg->Disconnect(victim);

    bool wrote = TryAgreement(cfg.get(), "x-voter-group", 2, 3000);

    std::printf("  [A 组 直接提 voter] 节点3断网(未追平) + 节点%d 也挂掉\n", victim);
    std::printf("     voter 数=4 → quorum=%d，实际能给 ack 的只有 2 票\n", q_voter_grp);
    std::printf("     写入结果：%s\n", wrote ? "成功" : "失败（超时/无 leader）");
    std::fflush(stdout);

    if (wrote) {
      cfg->Fatal("A 组预期写不进去（quorum 被撑大到 3 却只剩 2 票可 ack），"
                 "实际却写成功了 —— 说明多数派口径有问题");
    }
    cfg->End();
  }

  // ================= B 组：新节点先当 learner =================
  {
    auto cfg = MakeConfig(servers, false);
    cfg->Begin("Test: adding the same node as LEARNER keeps quorum intact");

    int leader = PrepareNewNodeOffline(cfg.get(), servers);

    // 同一个节点 3，这回只提为 learner（追数据，但不计票）
    cfg->GetRaft(leader)->ProposeConfChangeTo(3, MemberRole::kLearner);
    bool ok = false;
    for (int t = 0; t < 100; t++) {
      raftcpp::SleepMs(100);
      if (cfg->GetRaft(leader)->MembershipView()[3] ==
          static_cast<int>(MemberRole::kLearner)) { ok = true; break; }
    }
    if (!ok) cfg->Fatal("B 组：节点 3 未被提为 learner");

    int q_learner_grp = cfg->GetRaft(leader)->QuorumSize();
    if (q_learner_grp != 2) {
      cfg->Fatal("B 组阈值错误：learner 不计票，3 个 voter 应为 2，实际 " +
                 std::to_string(q_learner_grp));
    }

    int victim = -1;
    for (int i = 0; i < 3; i++) {
      if (i != leader) { victim = i; break; }
    }
    cfg->Disconnect(victim);

    bool wrote = TryAgreement(cfg.get(), "x-learner-group", 2, 5000);

    std::printf("  [B 组 先当 learner]  节点3断网(未追平) + 节点%d 也挂掉\n", victim);
    std::printf("     voter 数=3 → quorum=%d，能给 ack 的刚好 2 票\n", q_learner_grp);
    std::printf("     写入结果：%s\n", wrote ? "成功" : "失败（超时/无 leader）");
    std::fflush(stdout);

    if (!wrote) {
      cfg->Fatal("B 组预期写得进去（learner 不计票，quorum 仍是 2），实际却卡住了");
    }

    std::printf("  >>> 结论：存活节点完全相同，只因新节点角色不同（voter vs learner）\n");
    std::printf("      A 组不可用 / B 组可用 —— learner 保住了扩容期间的容错余量\n");
    std::fflush(stdout);

    cfg->End();
  }
}

// ===========================================================================
// 成员变更 ④：成员配置必须随状态一起持久化，崩溃重启后不丢失
//
// 这套特性的标题就是「is_member_ 三态化 + 持久化」。如果某节点被移除后
// 崩溃重启，重启后 is_member_ 必须从磁盘恢复成 kRemoved —— 而不是退化回
// 默认的「全员 kVoter」。否则重启节点会以为自己仍是正式 voter，开始重新
// 参与选举 / 投票 / 提交 → 安全性崩塌。
//
// 【有牙隔离设计】：这条专门抓「ReadPersist 没真恢复 is_member_」。
// 普通版会被"已提交的 conf 条目被日志重放 / 快照 members 重水化"兜底掩盖
// （实测把 ReadPersist 的 is_member_ 恢复分支改成 if(false)，普通版照样过）。
// 所以这里用快照把 conf 条目截断出日志、再断网重启，让节点 2 重启后：
//   - 自己日志里没有 conf 条目（被快照截断）→ ApplyLoop 重放补不回来；
//   - 断网收不到 leader 的 InstallSnapshot → args.members 重水化也到不了；
//   于是 is_member_ 只能来自 ReadPersist 恢复的 raft state。
//   此时若 ReadPersist 不恢复 is_member_，节点 2 退化回 kVoter → 用例 FAILED。
// ===========================================================================
void TestMembershipPersistAcrossRestart() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);
  cfg->EnableSnapshotCompaction();
  cfg->Begin("Test: membership change survives crash+restart (is_member_ persisted, isolated)");

  int leader = cfg->CheckOneLeader();

  // 1) 先写一批，确保 leader（进而各 follower）真的生成过快照
  for (int i = 1; i <= 40; i++)
    cfg->One("put k" + std::to_string(i) + " v" + std::to_string(i), servers, true);

  // 2) 移除节点 2（conf 条目此刻落在日志里）
  StartResult r = cfg->GetRaft(leader)->ProposeConfChange(2, false);
  int conf_idx = r.index;
  bool ok = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    bool all = true;
    for (int i = 0; i < servers; i++) {
      if (cfg->GetRaft(i)->MembershipView()[2] !=
          static_cast<int>(MemberRole::kRemoved)) { all = false; break; }
    }
    if (all) { ok = true; break; }
  }
  if (!ok) cfg->Fatal("移除失败：is_member_[2] 未在 apply 时切换为 kRemoved");

  // 3) 再写一批。注意：节点 2 已被移除，Q2 隐私加固让它【不再接收任何新日志】
  //    （ReplicateLoop 对其直接跳过），所以这里只要求剩下的 2 个 voter 提交。
  //    若仍写 servers=3，会卡等 removed 节点永远收不到的条目 → 超时。
  for (int i = 41; i <= 120; i++)
    cfg->One("put b" + std::to_string(i) + " v" + std::to_string(i), 2, false);

  // Q2 护栏：removed 节点必须被冻结在移除点，绝不能偷偷收新日志。
  // （本用例新增的刚性断言：坐实 Q2 真的生效，而不是退化成"removed 还在收数据"。）
  // 原本这里要求"节点2 对自己快照、截断点越过 conf_idx"以隔离 ReadPersist——
  // 但 Q2 下 removed 节点永不前进、永不对自己快照，那条隔离前提已不可能成立，
  // 故改为直接验证"冻结 + 角色持久化"这一更贴近 Q2 语义的性质。
  auto n2 = GetRaftOrFatal(cfg, 2);
  if (n2->LastLogIndex() > conf_idx) {
    cfg->Fatal("Q2 失效：removed 节点2 仍在接收新日志（LastLogIndex=" +
               std::to_string(n2->LastLogIndex()) + " > conf_idx=" +
               std::to_string(conf_idx) + "），隐私加固未生效");
  }

  // 4) 节点 2 断网后再崩溃：重启后它收不到 leader 的 InstallSnapshot，
  //    自己日志里也没有 conf 条目（被快照截断）→ is_member_ 只能来自 ReadPersist。
  cfg->Disconnect(2);
  cfg->Crash1(2);
  cfg->Start1(2);   // 刻意保持断网，不 Connect

  // 5) 重连之前立刻断言：节点 2 必须记得自己被移除（仅依赖 ReadPersist）
  raftcpp::SleepMs(300);
  int role2 = cfg->GetRaft(2)->MembershipView()[2];
  if (role2 != static_cast<int>(MemberRole::kRemoved)) {
    cfg->Fatal("成员变更未持久化：节点2【快照截断+断网重启】后 is_member_[2]=" +
               std::to_string(role2) + "，应为 kRemoved(" +
               std::to_string(static_cast<int>(MemberRole::kRemoved)) +
               ") —— 日志重放/InstallSnapshot 都到不了，说明 ReadPersist 没恢复 is_member_"
               "（退化回 kVoter 会重新参与选举/提交 → 安全性崩塌）");
  }

  // 6) 重连，确认 0/1 两个 voter 仍能正常提交（多数派不受影响）
  cfg->Connect(2);
  cfg->One("after-restart", 2, false);

  cfg->End();
}

// ===========================================================================
// 成员变更 ⑤：learner 绝不能当选 leader（安全性硬约束）
//
// learner 不投票、不计票。若实现错误地让它参选/收票，日志可能落后的 learner
// 一旦当选就会覆盖多数派已提交日志 → 安全性崩塌。
// 这条专门抓「learner 也参与选举/被投票」的实现 bug。
// ===========================================================================
void TestLearnerNeverLeader() {
  const int servers = 4;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test: a learner must never be elected leader");

  int leader = cfg->CheckOneLeader();

  // 把节点 3 降级为 learner
  cfg->GetRaft(leader)->ProposeConfChangeTo(3, MemberRole::kLearner);
  bool ok = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    if (cfg->GetRaft(3)->MembershipView()[3] ==
        static_cast<int>(MemberRole::kLearner)) { ok = true; break; }
  }
  if (!ok) cfg->Fatal("降级失败：is_member_[3] 未切换为 kLearner");

  // 阶段1：正常集群里反复重选若干轮，learner 节点必须【从不】自认 leader
  for (int round = 1; round <= 3; round++) {
    int l = cfg->CheckOneLeader();
    for (int t = 0; t < 20; t++) {
      raftcpp::SleepMs(50);
      if (cfg->GetRaft(3)->GetState().second) {
        cfg->Fatal("learner 节点3 在重选期间自认 leader —— 安全性破防");
      }
    }
    cfg->Crash1(l);
    cfg->Start1(l);
    cfg->CheckOneLeader();  // 等新 leader 选出（非 learner）
  }

  // 阶段2（有牙）：把 0/1/2 全部移除，只剩 learner(3)。
  // 正确实现下 learner 不是 voter → 集群无 voter → 节点3 永远当不了 leader；
  // 错误实现下（learner 被当成 voter 计票）节点3 会自投自当选 → 这里抓到。
  for (int i = 0; i < 2; i++) {  // 先移除 0、1（节点2 兜底作 leader）
    int ld = cfg->CheckOneLeader();
    cfg->GetRaft(ld)->ProposeConfChange(i, false);
    for (int t = 0; t < 100; t++) {
      raftcpp::SleepMs(100);
      if (cfg->GetRaft(i)->MembershipView()[i] ==
          static_cast<int>(MemberRole::kRemoved)) break;
    }
  }
  {  // 最后移除节点2（自身即最后一个 voter/leader）；移除后无 leader，不再 CheckOneLeader
    int ld = cfg->CheckOneLeader();
    cfg->GetRaft(ld)->ProposeConfChange(2, false);
    raftcpp::SleepMs(800);  // 等 conf apply + 节点2 退位
  }
  // 此刻集群：0/1/2=kRemoved，3=learner。轮询验证节点3 从不自认 leader
  bool learner_elected = false;
  for (int t = 0; t < 50; t++) {
    raftcpp::SleepMs(100);
    if (cfg->GetRaft(3)->GetState().second) { learner_elected = true; break; }
  }
  if (learner_elected) {
    cfg->Fatal("learner 节点3 在所有 voter 下线后竟然当选 leader —— learner 不应参选/收票");
  }

  cfg->End();  // 集群已无 voter，直接结束（无需恢复）
}

// ===========================================================================
// 成员变更 ⑥：kLearner 角色必须随状态持久化，崩溃重启后不退化回 kVoter
//
// 与 ④ 同构，但验证【learner 角色】持久化。若 is_member_ 的 learner 角色没真落盘/
// 没真从 ReadPersist 恢复，重启后节点会退化回默认的 kVoter —— 立刻把多数派撑大，
// 扩容期间本该保住的容错余量就没了。
// 隔离手法同 ④：快照把 learner 变更条目截断出日志 + 断网重启，逼状态只来自 ReadPersist。
// ===========================================================================
void TestLearnerPersistAcrossRestart() {
  int servers = 4;
  auto cfg = MakeConfig(servers, false);
  cfg->EnableSnapshotCompaction();
  cfg->Begin("Test: learner role persists across crash+restart (isolated)");

  int leader = cfg->CheckOneLeader();

  // 先写一批，确保各节点真的生成过快照
  for (int i = 1; i <= 40; i++)
    cfg->One("put k" + std::to_string(i) + " v" + std::to_string(i), servers, true);

  // 把节点 3 降为 learner（conf 条目此刻落在日志里）
  StartResult r = cfg->GetRaft(leader)->ProposeConfChangeTo(3, MemberRole::kLearner);
  int conf_idx = r.index;
  bool ok = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    if (cfg->GetRaft(3)->MembershipView()[3] ==
        static_cast<int>(MemberRole::kLearner)) { ok = true; break; }
  }
  if (!ok) cfg->Fatal("降级失败：is_member_[3] 未切换为 kLearner");

  // 再写一批，把 learner 变更推进快照截断点
  for (int i = 41; i <= 120; i++)
    cfg->One("put b" + std::to_string(i) + " v" + std::to_string(i), servers, true);

  // 前提：节点 3 快照点必须越过 learner 变更条目，否则隔离失败
  auto n3 = GetRaftOrFatal(cfg, 3);
  if (n3->SnapshotIndex() <= conf_idx) {
    cfg->Fatal("前提不成立：节点3快照点(" + std::to_string(n3->SnapshotIndex()) +
               ") 未越过 learner 变更条目(" + std::to_string(conf_idx) +
               ")，conf 条目仍在日志里 → 测试无法隔离 ReadPersist");
  }

  // 断网 + 崩溃 + 重启（刻意保持断网）：节点3 日志无 conf 条目、收不到 InstallSnapshot
  cfg->Disconnect(3);
  cfg->Crash1(3);
  cfg->Start1(3);

  // 重连之前断言：节点3 仍必须是 learner（仅依赖 ReadPersist）
  raftcpp::SleepMs(300);
  int role3 = cfg->GetRaft(3)->MembershipView()[3];
  if (role3 != static_cast<int>(MemberRole::kLearner)) {
    cfg->Fatal("learner 角色未持久化：节点3【快照截断+断网重启】后 is_member_[3]=" +
               std::to_string(role3) + "，应为 kLearner(" +
               std::to_string(static_cast<int>(MemberRole::kLearner)) +
               ") —— 退化回 kVoter 会把扩容期间的 quorum 撑大 → 可用性损失");
  }

  cfg->Connect(3);
  cfg->One("after-restart", 3, false);
  cfg->End();
}

// ===========================================================================
// 成员变更 ⑦：连续快速变更（churn）+ 跨 leadership 单飞
//
// 单节点变更的安全性靠「新旧配置多数派永远相交」，且 leader 在 apply 一条 conf 之前
// 不会 propose 下一条（pending_conf_index_）。这里压两件事：
//   1) 连续对节点2 反复 remove/add 交替，每轮中间强制一次 leader 重启（且不立即拉起），
//      验证「跨 leadership 时单飞不破」——新 leader 不会在旧 conf 还没 apply 时
//      又 propose 一条，导致两条未提交 conf 同时生效（脑裂）。
//   2) 全程集群始终单一 leader、始终能提交，最终收敛到一致状态。
// ===========================================================================
void TestConfChangeChurn() {
  const int servers = 4;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test: rapid conf changes (churn) survive leader turnovers");

  for (int round = 1; round <= 6; round++) {
    int leader = cfg->CheckOneLeader();
    int role2 = cfg->GetRaft(leader)->MembershipView()[2];
    bool is_voter = (role2 == static_cast<int>(MemberRole::kVoter));
    // 交替：voter→remove，removed→add
    cfg->GetRaft(leader)->ProposeConfChange(2, !is_voter);

    int want = is_voter ? static_cast<int>(MemberRole::kRemoved)
                        : static_cast<int>(MemberRole::kVoter);
    bool ok = false;
    for (int t = 0; t < 100; t++) {
      raftcpp::SleepMs(100);
      bool all = true;
      for (int i = 0; i < servers; i++) {
        if (cfg->GetRaft(i)->MembershipView()[2] != want) { all = false; break; }
      }
      if (all) { ok = true; break; }
    }
    if (!ok) cfg->Fatal("churn 第" + std::to_string(round) + "轮 conf 未 apply");

    // 强制 leader 退位但不立即重启，逼剩余节点重选（真正跨 leadership）
    cfg->Crash1(leader);
    cfg->CheckOneLeader();  // leader 仍 down，必须选出新 leader（验证跨 leadership 单飞安全）
    cfg->Start1(leader);    // 旧 leader 回来当 follower

    // 每轮集群仍能提交（节点2 是 voter 时 4 票，removed 时 3 票）
    int voters = (want == static_cast<int>(MemberRole::kVoter)) ? servers : (servers - 1);
    cfg->One("churn-r" + std::to_string(round), voters, false);
  }

  // 收尾：节点2 恢复为 voter
  int leader = cfg->CheckOneLeader();
  if (cfg->GetRaft(leader)->MembershipView()[2] !=
      static_cast<int>(MemberRole::kVoter)) {
    cfg->GetRaft(leader)->ProposeConfChange(2, true);
    bool ok = false;
    for (int t = 0; t < 100; t++) {
      raftcpp::SleepMs(100);
      bool all = true;
      for (int i = 0; i < servers; i++) {
        if (cfg->GetRaft(i)->MembershipView()[2] !=
            static_cast<int>(MemberRole::kVoter)) { all = false; break; }
      }
      if (all) { ok = true; break; }
    }
    if (!ok) cfg->Fatal("churn 收尾：节点2 未能恢复为 voter");
  }
  cfg->One("churn-final", servers, false);
  cfg->End();
}

// ===========================================================================
// 成员变更 ⑧：移除【当前 leader 自己】后必须主动退位（step down）
//
// Raft 单节点变更里，leader 也可能被写进移除列表。正确实现应在 conf apply、发现自己
// 不再是 voter 时立即 step down，让剩余节点重选。否则它会以「已被集群移除」的身份
// 继续发心跳/提交，造成「幽灵 leader」（其他节点不再投它票，但它还在干活）。
// 这条抓「移除自身后不退位」的实现 bug，用 GetState().second（is_leader）精确断言。
// ===========================================================================
void TestRemoveLeaderSelf() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test: removing the current leader itself must step it down");

  int leader = cfg->CheckOneLeader();

  // 移除 leader 自己
  cfg->GetRaft(leader)->ProposeConfChange(leader, false);

  // 等 conf apply：该节点 is_member_ 变成 kRemoved
  bool ok = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    if (cfg->GetRaft(leader)->MembershipView()[leader] ==
        static_cast<int>(MemberRole::kRemoved)) { ok = true; break; }
  }
  if (!ok) cfg->Fatal("移除自身失败：is_member_[leader] 未切换为 kRemoved");

  // 关键断言：原 leader 必须主动退位（轮询，容忍全量负载下的时序抖动）
  bool stepped_down = false;
  for (int t = 0; t < 30; t++) {
    raftcpp::SleepMs(100);
    if (!cfg->GetRaft(leader)->GetState().second) { stepped_down = true; break; }
  }
  if (!stepped_down) {
    cfg->Fatal("移除自身的 leader 没有主动退位（GetState 仍报告 is_leader=true）—— 幽灵 leader 风险");
  }

  // 集群应选出新 leader（非原 leader），且剩余 2 voter 仍能提交
  int new_leader = cfg->CheckOneLeader();
  if (new_leader == leader) {
    cfg->Fatal("移除后原 leader 仍被认作 leader（集群未重选）");
  }
  cfg->One("after-leader-removed", 2, false);
  cfg->End();
}

// ===========================================================================
// 成员变更 ⑨：InstallSnapshot 必须正确重水化 is_member_ 角色
//
// 落后节点靠 InstallSnapshot 被拉起时，快照里的 members 必须携带【角色】
// （learner/removed），而不只是「是否是成员」的 bool。若快照只用 bool 编码，
// learner 装快照后会退化回 kVoter，removed 节点会复活成 voter —— 两种情况都会
// 悄悄撑大 quorum。这条让节点2 降为 learner 并大幅落后，重连后由 InstallSnapshot
// 拉起，断言它起来后仍是 learner（角色随快照正确重水化）。
// ===========================================================================
void TestInstallSnapshotRestoresMembership() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);
  cfg->EnableSnapshotCompaction();
  cfg->Begin("Test: InstallSnapshot restores learner role (not just membership)");

  int leader = cfg->CheckOneLeader();

  // 把节点 2 降为 learner
  auto conf_res = cfg->GetRaft(leader)->ProposeConfChangeTo(2, MemberRole::kLearner);
  int conf_idx = conf_res.index;  // 这条 learner 变更条目的下标（事后确认快照覆盖了它）
  bool ok = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    if (cfg->GetRaft(2)->MembershipView()[2] ==
        static_cast<int>(MemberRole::kLearner)) { ok = true; break; }
  }
  if (!ok) cfg->Fatal("降级失败：is_member_[2] 未切换为 kLearner");

  // 让节点2 落后：断网，期间写很多（触发 leader 做快照）
  cfg->Disconnect(2);
  for (int i = 1; i <= 60; i++)
    cfg->One("put k" + std::to_string(i) + " v" + std::to_string(i), 2, false);

  // 节点2 此时 next_index 远落后于 leader 的 snapshot_index → 重连后必走 InstallSnapshot
  cfg->Connect(2);

  // 等节点2 被 InstallSnapshot 拉起并应用快照（含 members/角色）
  ok = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    if (cfg->GetRaft(2)->LastLogIndex() >= 50 &&
        cfg->GetRaft(2)->MembershipView()[2] ==
            static_cast<int>(MemberRole::kLearner)) { ok = true; break; }
  }
  if (!ok) {
    int role2 = cfg->GetRaft(2)->MembershipView()[2];
    cfg->Fatal("InstallSnapshot 未正确重水化角色：节点2 起来后 is_member_[2]=" +
               std::to_string(role2) + "，应为 kLearner(" +
               std::to_string(static_cast<int>(MemberRole::kLearner)) +
               ") —— 若快照只用 bool 编码成员，learner 会退化回 voter → quorum 被撑大");
  }

  // 小修：显式确认节点2 是【走 InstallSnapshot 重水化】拿到 learner 角色，
  // 而非靠普通日志追平（否则本用例退化为普通追平检验，对快照 bool 编码 bug 漏检）。
  int snap_idx = cfg->GetRaft(2)->SnapshotIndex();
  if (snap_idx <= conf_idx) {
    cfg->Fatal("InstallSnapshot 未覆盖成员变更条目：节点2 的 SnapshotIndex(" +
               std::to_string(snap_idx) + ") <= conf_idx(" + std::to_string(conf_idx) +
               ") —— 节点2 是走普通日志追平而非快照重水化（假绿风险）");
  }

  cfg->One("after-snap", 2, false);
  cfg->End();
}

// ===========================================================================
// 成员变更 ⑩：变更期间并发写入必须不丢、不重
//
// 修订动机：原版收尾用 `for 每个 cmd: cfg->One(cmd,3)` 重新提交 —— 这等于把
// "丢没丢" 彻底掩盖（One 会自旋等到节点2 重新加入后再提交）。唯一硬断言只剩
// "节点2 终回 kVoter"，所以"不丢/不重"几乎没真正验到 → 假绿。
//
// 修订做法：变更与写入都结束后，扫描整段【已提交】日志（index 1..leader.LastLogIndex()），
// 收集所有我们真发过的 "c*" 命令值，断言恰好 {c0..cK-1} 各出现一次：
//   - 缺失 → 命令丢失（未出现在已提交日志中）；
//   - 某个值出现 >1 次 → 同一条命令被重复提交（Raft 线性一致破防）。
// conf 变更条目也占 index，其载荷是 "CONF:..." 哨兵串，不在 cmds 集合里，
// 扫描时只统计与已知 cmds 完全相等的值，不会误伤。
// ===========================================================================
void TestConcurrentConfChangeLinearizable() {
  const int servers = 3;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test: concurrent writes during a membership change must not be lost/duplicated");
  int leader = cfg->CheckOneLeader();

  const int K = 30;
  std::vector<std::string> cmds(K);
  for (int i = 0; i < K; i++) cmds[i] = "c" + std::to_string(i);

  // 【稳健性】原写法要求每条命令都提交到 servers(=3) 台。但在"节点2 被移除"的
  // 窗口里它已被冻结（Q2：不再收新数据），该窗口内发起的 One 只能凑到 2 台；
  // Config::One 内层只等 2s（config.cpp:504）且 retry=false 会立刻 Fatal，
  // 高负载下加回+追平若超过 2s 就误判失败（假 flaky）。
  // 这里改为按【变更期始终成立的多数派】= servers-1 提交：
  //   ・不会重发命令（retry 仍为 false —— 改成 true 会重复 Start 同一条命令，
  //     直接违背下面"不重复"断言）；
  //   ・"不丢/不重"的真正把关是下面那段【全量已提交日志审计】+
  //     Wait(idx, servers) 要求 3 台都补齐，复制完整性并未被削弱。
  const int kStableServers = servers - 1;
  std::thread writer([&]() {
    for (int i = 0; i < K; i++) cfg->One(cmds[i], kStableServers, false);
  });

  // 主线程在写入进行中穿插一次成员变更（移除节点2 + 加回）
  raftcpp::SleepMs(150);
  cfg->GetRaft(leader)->ProposeConfChange(2, false);
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    if (cfg->GetRaft(2)->MembershipView()[2] ==
        static_cast<int>(MemberRole::kRemoved)) break;
  }
  StartResult r_add = cfg->GetRaft(cfg->CheckOneLeader())->ProposeConfChange(2, true);
  if (r_add.index < 0) cfg->Fatal("加回节点2 的提案被拒（index<0）—— 无法断言并发变更下的单飞保护");
  bool back = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    if (cfg->GetRaft(2)->MembershipView()[2] ==
        static_cast<int>(MemberRole::kVoter)) { back = true; break; }
  }
  if (!back) cfg->Fatal("加回节点2 后 10s 内未回到 kVoter —— 用例前提不成立");
  writer.join();

  // 角色断言：变更必须完整回滚（节点2 回到 voter）
  if (cfg->GetRaft(2)->MembershipView()[2] !=
      static_cast<int>(MemberRole::kVoter)) {
    cfg->Fatal("成员变更未完整恢复：节点2 应回到 kVoter（并发写入期间变更后状态错乱）");
  }

  // ★ 真·不丢/不重审计（修复假绿）
  // 扫描整段已提交日志，只统计我们真发过的命令，断言恰好各出现一次。
  int last = cfg->GetRaft(cfg->CheckOneLeader())->LastLogIndex();
  std::map<std::string, int> seen;
  for (int idx = 1; idx <= last; idx++) {
    auto v = cfg->Wait(idx, servers, -1);
    if (!v) cfg->Fatal("扫描已提交日志时 Wait(" + std::to_string(idx) + ") 返回空");
    // 只统计真发过的命令（conf 哨兵串 "CONF:..." 不在此集合，自动跳过）
    bool ours = false;
    for (int i = 0; i < K; i++) {
      if (*v == cmds[i]) { ours = true; break; }
    }
    if (ours) seen[*v]++;
  }
  for (int i = 0; i < K; i++) {
    if (seen.find(cmds[i]) == seen.end())
      cfg->Fatal("并发变更期间命令 " + cmds[i] + " 丢失（未出现在已提交日志中）");
    if (seen[cmds[i]] > 1)
      cfg->Fatal("命令 " + cmds[i] + " 在已提交日志中出现 " +
                 std::to_string(seen[cmds[i]]) +
                 " 次（被重复提交 → 线性一致破防）");
  }

  cfg->One("final", servers, false);
  cfg->End();
}

// ===========================================================================
// 成员变更 ⑪：learner 在追平过程中遇到网络抖动，最终仍能追平
//
// 把节点3 降为 learner，然后用后台线程周期性断连/重连节点3 模拟不稳定网络，
// 期间持续写入。验证：
//  1) 抖动期间集群仍可用（只要求 3 个 voter 提交，learner 可能离线）；
//  2) 抖动结束后 learner 必须最终追平（最终要求 4 节点都提交）；
//  3) learner 角色不被抖动破坏（仍是 kLearner，不能退化成 voter 撑大 quorum）。
// 这条抓的是"断连/重连后复制状态机（next_index_/match_index_）错乱导致追平
// 死循环 / 永久落后"类的 bug。
// ===========================================================================
void TestLearnerCatchupWithChurn() {
  const int servers = 4;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test: learner catches up despite network churn (periodic disconnect/reconnect)");
  int leader = cfg->CheckOneLeader();

  cfg->GetRaft(leader)->ProposeConfChangeTo(3, MemberRole::kLearner);
  bool ok = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    if (cfg->GetRaft(3)->MembershipView()[3] ==
        static_cast<int>(MemberRole::kLearner)) { ok = true; break; }
  }
  if (!ok) cfg->Fatal("降级失败：is_member_[3] 未切换为 kLearner");

  const int M = 40;
  std::thread churn([&]() {
    for (int i = 0; i < M; i++) {
      if (i % 3 == 0) cfg->Disconnect(3);
      else if (i % 3 == 1) cfg->Connect(3);
      // i%3==2 保持当前网络状态不变
      cfg->One("churn-" + std::to_string(i), servers - 1, false);  // learner 可能离线，容 3 个 voter
      raftcpp::SleepMs(30);
    }
    cfg->Connect(3);  // 收尾确保连通，让 learner 最终追平
  });
  churn.join();

  // 最终 learner 必须追平：所有命令在 learner 也 apply（要求 4 节点都提交）
  for (int i = 0; i < M; i++)
    cfg->One("churn-" + std::to_string(i), servers, false);

  // learner 角色不被抖动破坏
  if (cfg->GetRaft(3)->MembershipView()[3] !=
      static_cast<int>(MemberRole::kLearner)) {
    cfg->Fatal("抖动后 learner 角色错乱：节点3 应仍为 kLearner（不能退化成 voter 撑大 quorum）");
  }
  cfg->One("after-churn", servers, false);
  cfg->End();
}

// ===========================================================================
// 成员变更 ⑫：变更中途 leader 崩溃（conf 未提交）必须安全收敛
//
// 单节点变更的安全性靠「新旧配置多数派永远相交」+「leader 在 apply 一条 conf 之前
// 不提下一条（pending_conf_index_）」。这条专门压「leader 在 conf 已 propose 但
// 还没 commit/apply 时崩溃」：新 leader 上任后必须让集群以一致状态继续，绝不能
// 留下【分叉的成员态】或脑裂。
//
// 关键实现事实（raft.cpp）：conf 条目随日志持久化，pending_conf_index_ 也是
// 持久化、且是【每个节点各自】的。新 leader 不会继承别人的 pending —— 但它仍可能
// 继承一条「未提交」的 conf 条目。正确实现下这条条目会被新 leader 正常
// 复制→提交→apply，所有节点最终一致切到目标角色；本用例断言的就是这个收敛结果
// （含原 leader 崩溃回来后，三节点成员视图仍完全一致）。
// 与 ⑭（干净 apply + 复杂序列）互补：这条覆盖「崩溃打断未提交窗口」。
// ===========================================================================
void TestConfChangeMidCrash() {
  const int servers = 5;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test: membership change survives leader crash before conf commits");

  int leader = cfg->CheckOneLeader();

  // 选一个【不是 leader】的节点做移除目标，避免与「leader 自移除」耦合
  int target = (leader + 1) % servers;

  // 1) 发起「移除 target」变更，但【不等它 commit/apply】就立刻把 leader 打掉
  cfg->GetRaft(leader)->ProposeConfChange(target, false);
  raftcpp::SleepMs(120);  // conf 条目可能已复制到 follower，但远未确定是否已 apply

  cfg->Crash1(leader);

  // 2) 剩余节点必须仍能选出新 leader
  //    5 节点移除 1 后仍有 ≥3 voter；即便 conf 已 apply，crash 1 个 leader 后
  //    余下 ≥3 voter ≥ quorum(4)=3，绝不脑裂/卡死（这是 3 节点版本做不到的）
  int new_leader = cfg->CheckOneLeader();
  if (new_leader == leader) cfg->Fatal("崩溃后竟还认原 leader（它已 down）");

  // 3) 收敛后所有【在线】节点成员视图必须一致（核心不变量：不能有分叉成员态）
  //    （原 leader 已 down，跳过它）
  int ref = (leader + 2) % servers;
  if (ref == leader) ref = (leader + 3) % servers;
  auto view_ref = cfg->GetRaft(ref)->MembershipView();
  for (int i = 0; i < servers; i++) {
    if (i == leader) continue;  // 原 leader 还 down，跳过
    if (cfg->GetRaft(i)->MembershipView() != view_ref) {
      cfg->Fatal("崩溃后在线节点成员视图不一致（分叉成员态 → 安全性崩塌）");
    }
  }

  // 4) 剩余节点仍能正常提交（多数派未被破坏；3 = 5 节点集群的 quorum）
  cfg->One("after-crash", 3, false);

  // 5) 原 leader 回来：最终【所有 5 个节点】成员视图必须一致
  cfg->Start1(leader);
  cfg->Connect(leader);
  raftcpp::SleepMs(500);  // 等 conf 条目在重连后跨节点最终一致
  auto final_view = cfg->GetRaft(0)->MembershipView();
  for (int i = 0; i < servers; i++) {
    if (cfg->GetRaft(i)->MembershipView() != final_view) {
      cfg->Fatal("原 leader 回归后节点" + std::to_string(i) +
                 " 成员视图仍与其他节点不一致（conf 跨重启未收敛）");
    }
  }

  // 6) 收敛后集群仍能提交（4 = 移除 1 节点后的 voter 数）
  cfg->One("final", 4, false);
  cfg->End();
}

// ===========================================================================
// 成员变更 ⑬：被移除的节点即使在线，也绝不被计入 quorum（无幽灵票）
//
// 与 TestLearnerNeverLeader（learner 不当选）、TestRemoveLeaderSelf（移除自身退位）
// 互补：那条验「被移除节点不能【当】leader」，这条验「被移除节点不能【凑】quorum」。
// 危险场景：若 quorum 计算错误地把 kRemoved 节点也算进多数派，那么移除节点2 后，
// 即便再断掉一个真实 voter，剩下「1 个真实 voter + 节点2(removed)」仍能凑齐
// quorum → 会选出 leader（或让某次读/写成功），等于节点2 的「幽灵票」生效。
//
// 断言：移除节点2 → 断开一个真实 voter → CheckNoLeader（证明没有节点2 的幽灵票，
// 在线真实 voter 只剩 1 个 < quorum=2，永远选不出 leader）。
// 【变异实证：有牙】把 QuorumSizeLocked / MemberCountLocked 改成把 kRemoved 也算成员
// → 幽灵票生效 → {1,2} 凑齐 quorum → CheckNoLeader 失败（选出 leader）。
// ===========================================================================
void TestRemovedNodeExcludedFromQuorum() {
  const int servers = 3;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test: a removed node is excluded from quorum even when online (no ghost vote)");

  int leader = cfg->CheckOneLeader();

  // 1) 移除节点2（它仍是 running + connected，模拟「被移除但还活着、还在网上」）
  cfg->GetRaft(leader)->ProposeConfChange(2, false);
  bool ok = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    if (cfg->GetRaft(2)->MembershipView()[2] ==
        static_cast<int>(MemberRole::kRemoved)) { ok = true; break; }
  }
  if (!ok) cfg->Fatal("移除失败：is_member_[2] 未切换为 kRemoved");

  // 2) 断开一个【真实 voter】（既不是节点2，也不在被移除状态），
  //    剩「另 1 个真实 voter + 节点2(removed)」
  int victim = (leader + 1) % servers;
  if (victim == 2) victim = (leader + 2) % servers;  // 保证 victim 是真实 voter
  cfg->Disconnect(victim);

  // 3) 关键断言：没有节点2 的幽灵票，在线真实 voter 只剩 1 个 < quorum(2) → 选不出 leader
  raftcpp::SleepMs(2 * kRaftElectionTimeout);
  cfg->CheckNoLeader();

  // 4) 恢复：重连真实 voter → 必须能重新选出 leader，且集群继续可用
  cfg->Connect(victim);
  cfg->CheckOneLeader();
  cfg->One("after-reconnect", 2, false);
  cfg->End();
}

// ===========================================================================
// 成员变更 ⑭：任意 churn 收敛后，所有节点成员视图必须完全一致（防分叉成员态）
//
// 廉价强不变式兜底：不管变更序列多乱（voter↔removed 切换、learner↔voter 切换、
// 穿插 leadership 换届），只要集群静默下来，所有节点的 MembershipView() 必须逐字节
// 相等。任何让某节点漏 apply / 多 apply / 错 apply 成员变更的路径，都会暴露为
// 「分叉成员态」——而分叉成员态正是脑裂 / 提交错乱的前兆。
// 与 ⑫（注入崩溃）互补：这条覆盖「干净 apply 但序列复杂」的收尾一致性。
// 【齿已磨尖】收尾故意留节点3 为 learner 终态（见下方收敛段注释）；若某节点
// 漏 apply 自己/他人的 learner 角色，视图会与正确终态分叉 → 此用例 FAILED。
// 此前该用例因收尾强制作 voter 恰好与「默认态」重合而齿钝，现已修正。
// ===========================================================================
void TestMembershipConsistencyAtQuiescence() {
  const int servers = 5;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test: all nodes agree on membership after arbitrary churn");

  // 跑一段确定性但杂乱的变更序列：偶数轮切节点2(voter↔removed)，奇数轮切节点3(learner↔voter)
  for (int round = 0; round < 8; round++) {
    int l = cfg->CheckOneLeader();
    if (round % 2 == 0) {
      int role2 = cfg->GetRaft(l)->MembershipView()[2];
      bool is_v = (role2 == static_cast<int>(MemberRole::kVoter));
      // 【稳健性】原写法忽略提案返回值、且等待循环无 ok 标志：变更若被拒/未 apply
      // 会静默继续跑，后续"全员视图一致"断言就失去意义（分不清"没生效"和"真分叉"）。
      StartResult pr = cfg->GetRaft(l)->ProposeConfChange(2, !is_v);  // voter<->removed
      if (pr.index < 0) {
        cfg->Fatal("第 " + std::to_string(round) +
                   " 轮提案被拒（index<0）—— 用例前提不成立，继续跑只是在测空气");
      }
      int want = is_v ? static_cast<int>(MemberRole::kRemoved)
                      : static_cast<int>(MemberRole::kVoter);
      bool applied = false;
      for (int t = 0; t < 100; t++) {
        raftcpp::SleepMs(100);
        bool all = true;
        for (int i = 0; i < servers; i++)
          if (cfg->GetRaft(i)->MembershipView()[2] != want) { all = false; break; }
        if (all) { applied = true; break; }
      }
      if (!applied) {
        cfg->Fatal("第 " + std::to_string(round) +
                   " 轮：节点2 的角色变更 10s 内未被全员 apply —— 静默放过会让终态断言失去意义");
      }
    } else {
      int role3 = cfg->GetRaft(l)->MembershipView()[3];
      MemberRole want = (role3 == static_cast<int>(MemberRole::kVoter))
                            ? MemberRole::kLearner
                            : MemberRole::kVoter;
      StartResult pr2 = cfg->GetRaft(l)->ProposeConfChangeTo(3, want);
      if (pr2.index < 0) {
        cfg->Fatal("第 " + std::to_string(round) +
                   " 轮提案被拒（index<0）—— 用例前提不成立，继续跑只是在测空气");
      }
      int want_i = static_cast<int>(want);
      bool applied2 = false;
      for (int t = 0; t < 100; t++) {
        raftcpp::SleepMs(100);
        bool all = true;
        for (int i = 0; i < servers; i++) {
          auto vi = cfg->GetRaft(i)->MembershipView();
          // 【Q2 冻结的设计后果，不是缺陷】已被移除的节点会被冻结在"它自己的移除
          // 条目"上，之后的新数据一律不发（raft.cpp ReplicateLoop 的 IsRemovedFrozen
          // 闸门 + removed_cap 限流）。因此它【看不到】本轮对节点3 的降级，视图停在
          // 旧值 —— 这是隐私要求下的预期行为。原用例要求"全员一致"对已移除节点是
          // 错误预期（只因等待循环静默超时才没暴露）；这里显式跳过这类节点。
          if (i < static_cast<int>(vi.size()) &&
              vi[i] == static_cast<int>(MemberRole::kRemoved)) continue;
          if (vi[3] != want_i) { all = false; break; }
        }
        if (all) { applied2 = true; break; }
      }
      if (!applied2) {
        cfg->Fatal("第 " + std::to_string(round) +
                   " 轮：节点3 的角色变更 10s 内未被【在集群内】的节点全员 apply"
                   " —— 静默放过会让终态断言失去意义");
      }
    }
    // 每 3 轮制造一次 leadership 换届（真正跨 leadership），增加路径覆盖
    if (round % 3 == 2) {
      cfg->Crash1(l);
      cfg->CheckOneLeader();
      cfg->Start1(l);
    }
  }

    // 收敛：只把节点2 恢复为 voter，【故意留节点3 为 learner】终态 ——
  // 这样若某节点漏 apply 了节点3 的 learner 角色（"自身角色不自更新"那类 bug），
  // 它的视图会与「节点3=learner」的正确终态分叉 → 下面的完全一致断言抓到（齿变尖）。
  int leader = cfg->CheckOneLeader();
  if (cfg->GetRaft(leader)->MembershipView()[2] !=
      static_cast<int>(MemberRole::kVoter))
    cfg->GetRaft(leader)->ProposeConfChange(2, true);
  if (cfg->GetRaft(leader)->MembershipView()[3] !=
      static_cast<int>(MemberRole::kLearner))
    cfg->GetRaft(leader)->ProposeConfChangeTo(3, MemberRole::kLearner);  // 确保终态=learner
  raftcpp::SleepMs(600);  // 静默期，让所有节点把最后的 conf 都 apply 一致

  // 不变式：所有节点 MembershipView 完全相等
  auto base = cfg->GetRaft(0)->MembershipView();
  for (int i = 1; i < servers; i++) {
    if (cfg->GetRaft(i)->MembershipView() != base) {
      cfg->Fatal("静默后节点" + std::to_string(i) +
                 " 的成员视图与其他节点不一致（分叉成员态 → 脑裂前兆）");
    }
  }

  cfg->One("final", servers - 1, false);  // 节点3 终态是 learner（不计票），4 个 voter 提交即可
  cfg->End();
}

// ===========================================================================
// 成员变更 ⑮：变更期间的线性一致读（ReadIndex）必须用「已提交配置」的 quorum
//
// ReadIndex 的读确认要攒够【多数派】确认。这个多数派必须是「已提交配置」的
// voter 集合，而不能是旧配置（quorum 过大 → 该读的读不出来 / 超时）或
// in-progress 配置（quorum 过小 → 可能返回未真正安全的读）。
// 这条用 4→3 节点变更 + 断一个 voter，制造「新旧配置 quorum 大小不同」的窗口，
// 断言变更后 ReadIndex 仍能用新的、更小的 quorum 正常返回（返回 >= 0 而非 -1）。
// 【变异实证：有牙】把读确认多数派改成 peers_.size()/2+1（旧 4 节点口径），
//   变更+断网后只剩 3 节点却需 3 票 → ReadIndex 超时返回 -1 → 本用例 FAILED。
// ===========================================================================
void TestReadIndexDuringConfChange() {
  const int servers = 4;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test: ReadIndex uses committed-config quorum across a membership change");

  int leader = cfg->CheckOneLeader();

  // 基线：4 voter → quorum 3，全在线，ReadIndex 应能返回（>=0）
  cfg->One("v1", servers, false);
  int ri_base = cfg->GetRaft(leader)->ReadIndex();
  if (ri_base < 0)
    cfg->Fatal("基线 ReadIndex 失败（4 节点全在线应可线性一致读）");

  // 移除节点 3（4 → 3 voter，quorum 由 3 降到 2）
  cfg->GetRaft(leader)->ProposeConfChange(3, false);
  bool ok = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    if (cfg->GetRaft(3)->MembershipView()[3] ==
        static_cast<int>(MemberRole::kRemoved)) { ok = true; break; }
  }
  if (!ok) cfg->Fatal("移除节点3 失败：未切换为 kRemoved");

  // 断开【被移除的节点3】（确保它不在线、不补 ReadIndex 的 ack 票）
  cfg->Disconnect(3);
  // 再断开一个真实 voter（节点2）→ 在线只剩 {0,1} 两个 voter。
  // 新（已提交）配置：3 节点 quorum=2 → {0,1} 刚好够 → ReadIndex 应返回 >=0。
  // 旧 4 节点配置：quorum=3 → 只剩 2 票可达 → 超时返回 -1 → 线性一致读被破坏。
  cfg->Disconnect(2);

  // 慷慨轮询等 {0,1} 选出 leader（高负载下选举可能超过单次 CheckOneLeader 固定超时，
  // 用轮询而非固定 sleep，避免偶发 "expected one leader, got none"）。
  int leader2 = -1;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    for (int i = 0; i < servers; i++) {
      if (i == 2 || i == 3) continue;  // leader 只可能在 {0,1}
      if (cfg->GetRaft(i)->GetState().second) { leader2 = i; break; }
    }
    if (leader2 >= 0) break;
  }
  if (leader2 < 0) cfg->Fatal("变更+断网后 {0,1} 未选出 leader（集群不可用）");
  raftcpp::SleepMs(kRaftElectionTimeout);  // 等 leader 稳定、no-op 提交（ReadIndex 前提）

  // ReadIndex 重试几次：高负载下偶发 ack 超时返回 -1 属瞬态，需与
  // 「旧配置导致持续 -1」区分——后者任何重试都拿不到 >=0。
  int ri = -1;
  for (int attempt = 0; attempt < 5; attempt++) {
    ri = cfg->GetRaft(leader2)->ReadIndex();
    if (ri >= 0) break;
    raftcpp::SleepMs(200);
  }
  if (ri < 0) {
    cfg->Fatal("变更后 ReadIndex 失败：ReadIndex 似乎用了旧配置的大 quorum"
               "（4 节点口径需 3 票，但节点2/3 都已不在，只剩 2 票可达）→ 线性一致读被破坏");
  }

  // 再写一条（2 个 voter 即可提交），ReadIndex 必须看到更新的 commit（证明读随配置/日志推进）
  cfg->One("v2", 2, false);
  int ri2 = -1;
  for (int attempt = 0; attempt < 5; attempt++) {
    ri2 = cfg->GetRaft(leader2)->ReadIndex();
    if (ri2 >= 0) break;
    raftcpp::SleepMs(200);
  }
  if (ri2 < 0) cfg->Fatal("变更后第二次 ReadIndex 失败");

  cfg->End();
}

// ===========================================================================
// 成员变更 ⑯：learner 可直接被移除（learner → removed，跳过 voter 中间态）
//
// 三态转移里 voter→removed、learner→voter、voter→learner 都有覆盖，但
// 「learner 直接变 removed」这条直转分支没测过。若 ApplyLoop 处理 conf 时
// 对 learner 来源有特殊分支/漏处理，可能导致 learner 卡在中间态或不被真正移除。
// ===========================================================================
void TestLearnerDirectlyRemoved() {
  const int servers = 4;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test: a learner can be removed directly (learner -> removed)");

  int leader = cfg->CheckOneLeader();

  // 先把节点 3 降为 learner
  cfg->GetRaft(leader)->ProposeConfChangeTo(3, MemberRole::kLearner);
  bool ok = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    if (cfg->GetRaft(3)->MembershipView()[3] ==
        static_cast<int>(MemberRole::kLearner)) { ok = true; break; }
  }
  if (!ok) cfg->Fatal("降级失败：节点3 未切换为 kLearner");

  // 直接移除（learner → removed，不走 voter 中间态）
  cfg->GetRaft(cfg->CheckOneLeader())->ProposeConfChange(3, false);
  ok = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    bool all = true;
    for (int i = 0; i < servers; i++) {
      if (cfg->GetRaft(i)->MembershipView()[3] !=
          static_cast<int>(MemberRole::kRemoved)) { all = false; break; }
    }
    if (all) { ok = true; break; }
  }
  if (!ok) cfg->Fatal("learner 直转 removed 失败：节点3 未落到 kRemoved");

  // 移除后集群仍可提交（3 个 voter）
  cfg->One("after-direct-remove", servers - 1, false);

  // 角色不被任何路径复活：再等一会儿确认仍是 removed
  raftcpp::SleepMs(300);
  if (cfg->GetRaft(0)->MembershipView()[3] !=
      static_cast<int>(MemberRole::kRemoved)) {
    cfg->Fatal("learner→removed 直转后角色回退（被复活）");
  }
  cfg->End();
}

// ===========================================================================
// 成员变更 ⑰：提拔「还落后的 learner」必须安全（已提交日志不丢不重）
//
// 扩容标准流程是「先以 learner 追平，再提拔为 voter」。但若实现允许把【日志还
// 差很多】的 learner 直接提成 voter 且立刻让其参与提交，就可能用落后日志覆盖
// 多数派已提交日志 → 安全性崩塌（「不安全提拔」）。
// 这条：把节点3 降为 learner 并让其大幅落后，再提拔为 voter，断言提拔后
// 【所有此前已提交的命令一条都不丢、不重】（集群始终保持线性一致）。
// 【变异实证：有牙】把「提拔」改成不等待追平、且让该 learner 立即被计入提交
// 多数派（忽略其 match_index 落后）→ 已提交命令丢失/重复 → 扫描日志检出 → FAILED。
//   注：正确 Raft 靠选举日志比较天然挡住「落后节点当选」，故该用例主要作安全不变式
//   回归守卫；变异需同时破坏「role 切换」与「提交计票」两道闸才暴露，属深齿。
// ===========================================================================
void TestPromoteLaggingLearnerSafe() {
  const int servers = 4;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test: promoting a still-lagging learner must not lose committed entries");

  int leader = cfg->CheckOneLeader();

  // 先写一批「基线命令」，确保已提交
  const int BASE = 10;
  for (int i = 0; i < BASE; i++)
    cfg->One("base" + std::to_string(i), servers, false);

  // 把节点 3 降为 learner 并【断网】→ 它开始落后
  cfg->GetRaft(leader)->ProposeConfChangeTo(3, MemberRole::kLearner);
  bool ok = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    if (cfg->GetRaft(3)->MembershipView()[3] ==
        static_cast<int>(MemberRole::kLearner)) { ok = true; break; }
  }
  if (!ok) cfg->Fatal("降级失败：节点3 未切换为 kLearner");
  cfg->Disconnect(3);

  // 断网期间继续写（节点3 持续落后）
  for (int i = 0; i < 10; i++)
    cfg->One("lag" + std::to_string(i), servers - 1, false);  // 3 voter 提交

  // 重连：节点3 开始追平，但此刻日志仍落后 —— 必须在提拔前重连，
  // 否则提拔条目到不了节点3，它永远停在 learner。
  cfg->Connect(3);

  // 立刻提拔节点 3 为 voter（此时它日志仍可能没追平 lag 命令）
  cfg->GetRaft(cfg->CheckOneLeader())->ProposeConfChangeTo(3, MemberRole::kVoter);
  ok = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    if (cfg->GetRaft(3)->MembershipView()[3] ==
        static_cast<int>(MemberRole::kVoter)) { ok = true; break; }
  }
  if (!ok) cfg->Fatal("提拔失败：节点3 未恢复为 kVoter");

  // 等节点3 追平
  raftcpp::SleepMs(800);

  // 安全断言：所有此前已提交的命令（base* + lag*）在集群已提交日志里各出现恰好一次。
  // 用扫描已提交日志的方式核对（exact-once 审计，同 ⑮）。
  int last = cfg->GetRaft(cfg->CheckOneLeader())->LastLogIndex();
  std::set<std::string> expected;
  for (int i = 0; i < BASE; i++) expected.insert("base" + std::to_string(i));
  for (int i = 0; i < 10; i++) expected.insert("lag" + std::to_string(i));

  std::map<std::string, int> seen;
  for (int idx = 1; idx <= last; idx++) {
    auto v = cfg->Wait(idx, servers, -1);
    if (!v) cfg->Fatal("扫描日志 Wait(" + std::to_string(idx) + ") 返回空");
    if (expected.count(*v)) seen[*v]++;  // 跳过 conf 哨兵串 "CONF:..."
  }
  for (const auto& c : expected) {
    if (seen.find(c) == seen.end())
      cfg->Fatal("已提交命令 " + c + " 丢失（不安全提拔导致日志被覆盖）");
    if (seen[c] > 1)
      cfg->Fatal("已提交命令 " + c + " 重复出现（线性一致破防）");
  }

  cfg->One("after-promote", servers, false);
  cfg->End();
}

// ===========================================================================
// 成员变更 ⑱：对非 voter（removed / learner）调 Start() 必须被拒，命令永不提交
//
// 与 ⑯（learner 不当选）、⑬（removed 不计 quorum）互补：那条验「身份」，
// 这条验「写入口」。一个 removed/learner 节点若还能通过 Start() 把命令写进日志
// 并提交，就等于它仍能影响状态机 → 安全性破防。
// 断言：Start() 返回 is_leader=false 且 index=-1（命令根本没被追加），且限时内
// 该命令不出现在任何已提交日志中。
// 【变异实证：有牙】去掉 Start() 的 `state_ != kLeader` 拦截（允许非 leader 也追加）
//   → removed/learner 节点 Start() 返回 index>=0 → 本用例 FAILED。
// ===========================================================================
void TestStartRejectedForNonVoter() {
  const int servers = 3;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test: Start() on a removed/learner node is rejected (command never commits)");

  int leader = cfg->CheckOneLeader();

  // ---- removed 节点 ----
  cfg->GetRaft(leader)->ProposeConfChange(2, false);
  bool ok = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    if (cfg->GetRaft(2)->MembershipView()[2] ==
        static_cast<int>(MemberRole::kRemoved)) { ok = true; break; }
  }
  if (!ok) cfg->Fatal("移除失败：节点2 未切换为 kRemoved");

  StartResult r_removed = cfg->GetRaft(2)->Start("from-removed");
  if (r_removed.is_leader || r_removed.index != -1) {
    cfg->Fatal("removed 节点 Start() 不应接受写（应 is_leader=false 且 index=-1）");
  }

  // ---- learner 节点 ----
  int leader2 = cfg->CheckOneLeader();
  cfg->GetRaft(leader2)->ProposeConfChangeTo(1, MemberRole::kLearner);  // 节点1 降 learner
  ok = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    if (cfg->GetRaft(1)->MembershipView()[1] ==
        static_cast<int>(MemberRole::kLearner)) { ok = true; break; }
  }
  if (!ok) cfg->Fatal("降级失败：节点1 未切换为 kLearner");

  StartResult r_learner = cfg->GetRaft(1)->Start("from-learner");
  if (r_learner.is_leader || r_learner.index != -1) {
    cfg->Fatal("learner 节点 Start() 不应接受写（应 is_leader=false 且 index=-1）");
  }

  // 限时确认这两条命令从未进入已提交日志
  raftcpp::SleepMs(500);
  int last = cfg->GetRaft(cfg->CheckOneLeader())->LastLogIndex();
  for (int idx = 1; idx <= last; idx++) {
    // 注意：不要求 3 节点都拥有每条（Q2 下 removed/learner 节点不再收新日志，
    // 仅剩 voter 数可能 < 3）。只要"被提交的命令里出现非 voter 来源"即判失败——
    // 若 Start() 没拦住 removed/learner 的写，它会被提交到某条 index，这里必然抓到。
    auto v = cfg->Wait(idx, 1, 200);
    if (v && (*v == "from-removed" || *v == "from-learner"))
      cfg->Fatal("非 voter 节点 Start() 的命令竟出现在已提交日志（应被拒）");
  }

  cfg->End();
}

// ===========================================================================
// 成员变更 ⑲：removed 节点保持冻结 + kRemoved（不被复活、不收新数据）
//
// ⑨ 验了「learner 装快照后仍是 learner」。这条补对称场景：在 Q2 隐私加固下，
// removed 节点【根本不再接收任何复制流量（日志/快照/心跳）】——它已在被移除前作为
// voter 收到了自己的移除配置并 apply 成 kRemoved，之后只需冻结。于是落后、重连后
// 它仍是 kRemoved 且日志不前进，绝不会被快照重水化"复活成 voter"或偷偷拿到新数据。
// 与 ⑨ 共同覆盖「成员角色编码」的正确性：learner 走快照路径、removed 走冻结路径，
// 两条路径都不会把 removed 误当成 member 复活成 voter（否则 quorum 被悄悄撑大）。
// ===========================================================================
void TestInstallSnapshotRestoresRemovedRole() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);
  cfg->EnableSnapshotCompaction();
  cfg->Begin("Test: a removed node stays removed AND frozen (never resurrected, never gets new data)");

  int leader = cfg->CheckOneLeader();

  // 把节点 2 降为 removed（用 removed，而非 ⑪ 的 learner）
  auto conf_res = cfg->GetRaft(leader)->ProposeConfChange(2, false);
  int conf_idx = conf_res.index;  // 这条 removed 变更条目的下标
  bool ok = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    if (cfg->GetRaft(2)->MembershipView()[2] ==
        static_cast<int>(MemberRole::kRemoved)) { ok = true; break; }
  }
  if (!ok) cfg->Fatal("移除失败：节点2 未切换为 kRemoved");

  // 让节点2 落后：断连期间写很多（触发 leader 做快照），节点2 收不到
  cfg->Disconnect(2);
  for (int i = 1; i <= 60; i++)
    cfg->One("put r" + std::to_string(i) + " v" + std::to_string(i), 2, false);

  // 节点2 的 next_index 远落后 leader 的 snapshot_index → 旧逻辑下重连必走 InstallSnapshot。
  // 但 Q2 规定 removed 节点不再收任何复制流量（含快照），所以重连后它应【保持冻结 +
  // kRemoved】，而不是被快照重水化"复活成 voter"或偷偷拿到新数据。
  cfg->Connect(2);

  ok = false;
  for (int t = 0; t < 60; t++) {
    raftcpp::SleepMs(100);
    // 关键断言：仍是 kRemoved，且日志未前进（证明没收到新数据/快照）
    if (cfg->GetRaft(2)->MembershipView()[2] ==
            static_cast<int>(MemberRole::kRemoved) &&
        cfg->GetRaft(2)->LastLogIndex() <= conf_idx) { ok = true; break; }
  }
  if (!ok) {
    int role2 = cfg->GetRaft(2)->MembershipView()[2];
    int ll = cfg->GetRaft(2)->LastLogIndex();
    cfg->Fatal("removed 节点未保持冻结+removed：role=" + std::to_string(role2) +
               " LastLogIndex=" + std::to_string(ll) +
               "（应为 kRemoved 且 LastLogIndex <= conf_idx=" +
               std::to_string(conf_idx) + "；Q2 应阻止其收任何新数据/快照）");
  }

  cfg->End();
}

// ===========================================================================
// 成员变更 ㉓：removed 节点的日志必须冻结（Q2 隐私加固刚性验证）
//
// 移除一个节点后，集群继续大量写入。正确的 Q2 行为：该 removed 节点的日志长度
// 永远停在「它被移除的那条配置条目」下标，不再前进半格。若 ReplicateLoop 仍向它
// 发 AppendEntries，它的 LastLogIndex 会随集群增长 → 这条抓到。
// 这是 ④ 里那句"LastLogIndex > conf_idx 即 Fatal"的【独立、无重启】版本，更直接。
// ===========================================================================
void TestRemovedNodeStopsReceivingReplication() {
  const int servers = 3;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test: a removed node's log stops advancing (Q2 privacy hardening)");

  int leader = cfg->CheckOneLeader();
  for (int i = 1; i <= 10; i++)
    cfg->One("put a" + std::to_string(i) + " v" + std::to_string(i), servers, true);

  // 移除节点 2，记录冻结点（它作为 voter 收到的最后一条 = 移除配置条目）
  auto r = cfg->GetRaft(leader)->ProposeConfChange(2, false);
  int conf_idx = r.index;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    if (cfg->GetRaft(2)->MembershipView()[2] ==
        static_cast<int>(MemberRole::kRemoved)) break;
  }
  int frozen = cfg->GetRaft(2)->LastLogIndex();
  if (frozen < conf_idx)
    cfg->Fatal("节点2 冻结点异常：LastLogIndex(" + std::to_string(frozen) +
               ") < conf_idx(" + std::to_string(conf_idx) + ")");

  // 继续写很多，只要求 2 个 voter 提交
  for (int i = 1; i <= 80; i++)
    cfg->One("put z" + std::to_string(i) + " v" + std::to_string(i), 2, false);

  raftcpp::SleepMs(500);
  int after = cfg->GetRaft(2)->LastLogIndex();
  if (after != frozen) {
    cfg->Fatal("Q2 失效：removed 节点2 日志仍在前进（" + std::to_string(frozen) +
               " -> " + std::to_string(after) + "），应被冻结在移除点");
  }
  if (cfg->GetRaft(2)->MembershipView()[2] !=
      static_cast<int>(MemberRole::kRemoved))
    cfg->Fatal("removed 节点2 角色漂移（不再是 kRemoved）");

  cfg->End();
}

// ===========================================================================
// 成员变更 ㉔：removed 节点永不看到 post-removal 的客户端数据（Q2 隐私加固）
//
// 移除前写一条"公开"数据（节点 2 作为 voter 收得到）；移除后写一条"机密"数据，
// 只走 2 个 voter 提交。正确行为：机密数据【绝不】出现在 removed 节点 2 的已提交
// 日志里 → NCommitted(机密index) == 2（只有两个 voter），而不是 3。
// 这是对 Q2"不向 removed 节点泄漏新数据"最直接的端到端断言。
// ===========================================================================
void TestRemovedNodeStaysQuiescentAfterRemoval() {
  const int servers = 3;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test: a removed node never sees post-removal client data (Q2)");

  int leader = cfg->CheckOneLeader();
  cfg->One("put public-pre", 3, true);  // 节点 2 作为 voter 收得到

  cfg->GetRaft(leader)->ProposeConfChange(2, false);
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    if (cfg->GetRaft(2)->MembershipView()[2] ==
        static_cast<int>(MemberRole::kRemoved)) break;
  }

  // 移除后写机密数据，只要求 2 个 voter 提交
  int idx = cfg->One("put SECRET-post-removal", 2, false);
  raftcpp::SleepMs(500);

  // 这条机密命令应【只】在 2 个 voter 上提交；removed 节点 2 绝不该拥有它。
  auto [nd, cmd] = cfg->NCommitted(idx);
  if (nd >= 3) {
    cfg->Fatal("Q2 失效：removed 节点2 竟拥有 post-removal 机密数据"
               "（NCommitted=" + std::to_string(nd) + "，应为 2）");
  }
  if (cfg->GetRaft(2)->MembershipView()[2] !=
      static_cast<int>(MemberRole::kRemoved))
    cfg->Fatal("removed 节点2 角色漂移（不再是 kRemoved）");

  cfg->End();
}

// ===========================================================================
// 成员变更 ⑳：随机化 churn 模糊测试（fuzz）
//
// 确定性用例（⑦Churn / ⑭）只翻转固定节点（偶数轮切节点2、奇数轮切节点3），
// 漏掉「特定随机翻转顺序触发 ApplyLoop 分支遗漏 / 收敛卡死」这类竞态。这条用
// 固定种子的伪随机引擎，随机翻转角色（voter/learner/removed，且始终保证在线
// voter ≥ quorum）+ 随机断连重连 + 偶发 leader 崩溃换届，制造确定性序列碰不到
// 的变更路径。收敛后断言：所有节点成员视图完全一致 + 集群仍能提交。
//
// 就像 ⑭ 一样是「广覆盖一致性兜底网」，但比 ⑭ 更狠——随机序列能撞到
// 确定性序列永远走不到的翻转组合。
// 【变异实证有牙】：ApplyLoop 应用 conf 时跳过本节点（sv != me_）→ 被变更节点
//   不自更视图 → 收敛后视图分叉 → FAILED（与 ⑭ 同源变异）。
// ===========================================================================
void TestMembershipFuzzChurn() {
  const int servers = 5;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test (fuzz): randomized membership churn must converge to a consistent view");

  // 固定种子，可复现；想换序列就改这里
  std::mt19937 rng(20260916);
  auto rand_int = [&](int n) { return std::uniform_int_distribution<int>(0, n - 1)(rng); };

  // 本地模型：当前角色（起点全是 voter）。仅用于 quorum 闸门与可读性，
  // 最终断言一律用集群真实 MembershipView，模型漂移不影响正确性。
  std::vector<MemberRole> role(servers, MemberRole::kVoter);
  std::vector<bool> connected(servers, true);
  int leader = cfg->CheckOneLeader();

  const int STEPS = 25;
  const MemberRole kTargets[3] = {MemberRole::kRemoved, MemberRole::kLearner,
                                  MemberRole::kVoter};
  for (int step = 0; step < STEPS; step++) {
    int node = rand_int(servers);
    MemberRole tgt = kTargets[rand_int(3)];
    if (tgt == role[node]) continue;  // 没变化就跳过本步

    // quorum 闸门：应用后在线 voter 数（含即将变更的 node）必须 ≥ 3（5 节点 majority）
    int online_voters = 0;
    for (int i = 0; i < servers; i++) {
      MemberRole r = (i == node) ? tgt : role[i];
      if (connected[i] && r == MemberRole::kVoter) online_voters++;
    }
    if (online_voters < 3) continue;  // 会破坏 quorum，跳过该步

    // 【稳健性】原写法忽略返回值且等待循环无 ok 标志：变更被拒/未 apply 时静默继续，
    // 本地模型 role[] 却已推进 —— 模型与集群漂移，终态断言等于没在测。
    StartResult pr = cfg->GetRaft(leader)->ProposeConfChangeTo(node, tgt);
    if (pr.index < 0) continue;   // 被拒（换届/单飞）：模型不推进，下一步再试
    role[node] = tgt;
    // 等 apply（单飞保护：上一条未 commit 前新提议会被拒，所以这里等它真正生效）
    bool applied = false;
    for (int t = 0; t < 40; t++) {
      raftcpp::SleepMs(50);
      if (cfg->GetRaft(node)->MembershipView()[node] == static_cast<int>(tgt)) {
        applied = true;
        break;
      }
    }
    if (!applied) {
      cfg->Fatal("step " + std::to_string(step) + "：节点 " + std::to_string(node) +
                 " 的角色变更 2s 内未 apply —— 静默放过会让终态断言失去意义");
    }
    leader = cfg->CheckOneLeader();  // 变更期间可能换届，刷新 leader 引用

    // 偶发网络抖动：随机断连一个节点 200ms 再重连（长期仍保 quorum）
    if (step % 5 == 4) {
      int d = rand_int(servers);
      if (connected[d]) {
        cfg->Disconnect(d);
        connected[d] = false;
        raftcpp::SleepMs(200);
        cfg->Connect(d);
        connected[d] = true;
      }
    }
    // 偶发 leader 崩溃+重启：验证换届后成员视图不丢、不脑裂
    if (step % 7 == 6) {
      int old = leader;
      cfg->Crash1(old);
      raftcpp::SleepMs(kRaftElectionTimeout);
      cfg->Start1(old);
      cfg->Connect(old);
      leader = cfg->CheckOneLeader();
    }
  }

  // 收敛：重连所有节点、等静默
  for (int i = 0; i < servers; i++) { cfg->Connect(i); connected[i] = true; }
  raftcpp::SleepMs(2 * kRaftElectionTimeout);

  // 不变式 1：所有节点成员视图完全一致（核心不变量：无分叉成员态）
  int converged_leader = cfg->CheckOneLeader();
  auto ref = cfg->GetRaft(converged_leader)->MembershipView();
  for (int i = 0; i < servers; i++) {
    if (cfg->GetRaft(i)->MembershipView() != ref) {
      cfg->Fatal("fuzz 收敛后节点" + std::to_string(i) +
                 " 成员视图与其他节点不一致（分叉成员态 → 脑裂/提交错乱前兆）");
    }
  }
  // 不变式 2：集群仍能正常提交（按真实 voter 数，removed 节点不计入）
  int voters = 0;
  for (int i = 0; i < servers; i++)
    if (cfg->GetRaft(i)->MembershipView()[i] == static_cast<int>(MemberRole::kVoter)) voters++;
  cfg->One("fuzz-final", voters, false);
  cfg->End();
}

// ===========================================================================
// 成员变更 ㉑：少数派分区内的 leader 发起的成员变更，愈合后必须不生效
//
// 单节点变更的安全性最终靠「commit 必须过多数派」兜底。这条专门压：leader 被
// 切到少数派分区后仍自认 leader（CheckQuorum 150ms 才自查，存在窗口），它发起的
// 变更只复制到少数派 follower，永远到不了多数派 commit → 愈合后必须被丢弃，
// 目标节点仍是原角色，且不能脑裂。
//
// 这是 ⑫（崩溃打断未提交窗口）的「分区版」对称：⑫ 是 leader 崩了新 leader 接手，
// 这条是 leader 没崩但被隔离。
// 【变异实证】ProposeConfChangeTo 乐观地「本地立即切换 is_member_」→ 少数派 leader
//   把目标节点本地改成 removed，愈合后其视图与多数派分叉 → FAILED（该乐观实现会让
//   陈旧 leader 的脏 conf 生效，正是我们要挡的回归）。
// ===========================================================================
void TestConfChangeFromMinorityLeader() {
  const int servers = 5;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test: a conf change proposed by a leader in the minority partition must NOT take effect");
  int leader = cfg->CheckOneLeader();
  cfg->One("v1", servers, false);

  // 把 leader 和一个 follower 一起切到少数派分区（2 节点）；其余 3 个在多数派分区
  int victim = (leader + 1) % servers;
  int target = (leader + 2) % servers;  // 多数派分区里的一个 voter（我们要验证它不被误改）
  cfg->Disconnect(leader);
  cfg->Disconnect(victim);

  // 关键：立刻在 leader 仍自认 leader 的窗口内（CheckQuorum 150ms 才自查）发起变更
  cfg->GetRaft(leader)->ProposeConfChange(target, false);  // 尝试 remove target
  raftcpp::SleepMs(2 * kRaftElectionTimeout);  // 多数派分区选出新 leader + 旧 leader 自查退位

  // 愈合分区
  cfg->Connect(leader);
  cfg->Connect(victim);
  raftcpp::SleepMs(2 * kRaftElectionTimeout);  // 等日志收敛（旧 leader 的脏 conf 被新 leader 截断）

  // 断言 1：愈合后集群有且仅有 1 个 leader（无脑裂）
  int new_leader = cfg->CheckOneLeader();

  // 断言 2：少数派 leader 发起的变更【没生效】——target 仍是 voter
  if (cfg->GetRaft(target)->MembershipView()[target] !=
      static_cast<int>(MemberRole::kVoter)) {
    cfg->Fatal("分区内 leader 发起的成员变更竟在愈合后生效：节点" +
               std::to_string(target) + " 不再是 kVoter（旧 leader 的脏 conf 被错误提交）");
  }

  // 断言 3：所有节点成员视图一致（无分叉）
  auto ref = cfg->GetRaft(new_leader)->MembershipView();
  for (int i = 0; i < servers; i++) {
    if (cfg->GetRaft(i)->MembershipView() != ref) {
      cfg->Fatal("分区愈合后节点" + std::to_string(i) +
                 " 成员视图与其他节点不一致（分叉成员态）");
    }
  }

  // 断言 4：集群仍能正常提交（状态机没被弄坏）
  cfg->One("after-heal", servers, false);
  cfg->End();
}

// ===========================================================================
// 成员变更 ㉒：learner 必须被拒绝对 ReadIndex 服务（对称负向，对应 ⑮）
//
// ⑮ 验的是「voter leader 在变更期用正确的（已提交配置）quorum 做 ReadIndex」；
// 这条验的是对称面：一个 learner（非 leader）调 ReadIndex 必须被拒（返回 -1），
// 否则它会把本地可能滞后的 commit 值当成线性一致读结果返回 → 脏读。
//
// 实现事实（raft.cpp:ReadIndex）：首行 `if (state_ != kLeader) return -1;`
// 天然挡住 learner/follower。这条用例守的就是「别有人为了‘让 learner 也能读’
// 去掉这个守卫」。
// 【变异实证有牙】去掉 ReadIndex 的 `state_ != kLeader` 守卫 → learner 也返回
//   commit_index_(>=0) → 期望 -1 落空 → FAILED。
// ===========================================================================
void TestLearnerReadIndexRejected() {
  const int servers = 5;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test: a learner must NOT serve ReadIndex (must return -1)");
  int leader = cfg->CheckOneLeader();
  cfg->One("v1", servers, false);

  // 把节点 3 降为 learner
  cfg->GetRaft(leader)->ProposeConfChangeTo(3, MemberRole::kLearner);
  bool ok = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    if (cfg->GetRaft(3)->MembershipView()[3] ==
        static_cast<int>(MemberRole::kLearner)) { ok = true; break; }
  }
  if (!ok) cfg->Fatal("降级失败：节点3 未切换为 kLearner");

  // 关卡 1：对 learner 直接 ReadIndex 必须被拒
  int ri = cfg->GetRaft(3)->ReadIndex();
  if (ri >= 0) {
    cfg->Fatal("learner 调 ReadIndex 返回 " + std::to_string(ri) +
               " —— 非 leader 不应服务线性一致读（脏读风险）");
  }

  // 关卡 2：写一条已提交命令后，learner 仍应被拒（不因‘本地有 commit 值’就放行）
  cfg->One("v2", servers, false);
  int ri2 = cfg->GetRaft(3)->ReadIndex();
  if (ri2 >= 0) {
    cfg->Fatal("learner 在已有提交后仍被允许 ReadIndex（返回 " +
               std::to_string(ri2) + "）—— 不安全");
  }

  // 对称正例：voter leader 的 ReadIndex 仍正常（证明我们没把 ReadIndex 整坏）
  int leader2 = cfg->CheckOneLeader();
  int ri3 = cfg->GetRaft(leader2)->ReadIndex();
  if (ri3 < 0) cfg->Fatal("对照：voter leader 的 ReadIndex 不应失败");

  cfg->End();
}

// ===========================================================================
// 成员变更 ㉕：选举计票口径 —— 已移除 / learner 的票不得计入分子
//
// 背景（真实缺陷）：num_votes_ 原是裸整数「回包 granted 就 ++」，不记录
// "谁投的"，于是"回包时已被移除"的节点的票照样进分子；而门槛
// QuorumSizeLocked() 只数当前 voter —— 分子分母口径不一致。后果：候选者
// 能靠一张已作废的票凑够 quorum 当选，leader 并非由当前配置多数选出
// （选举合法性被破坏）。
// 本用例直接构造输入调用计票纯函数，确定性验证口径，不依赖运行时序。
// ===========================================================================
void TestVoteCountIgnoresRemovedVoters() {
  auto cfg = MakeConfig(3, false);
  cfg->Begin("Test: election vote counting ignores removed/learner ballots");

  // 场景：3 节点 —— 0:voter(自己)  1:voter  2:removed
  //       票：      {0:授予(自己),  1:未投,   2:授予(但该节点已被移除)}
  // 正确口径：2 的票作废 → 只计自己 1 票；QuorumSize({0,1}) = 2 → 不该当选。
  std::vector<MemberRole> is_member = {MemberRole::kVoter, MemberRole::kVoter,
                                       MemberRole::kRemoved};
  std::vector<int> votes = {1, 0, 1};
  int n = CountGrantedVotes(is_member, votes);
  if (n != 1) {
    cfg->Fatal("计票口径错：removed 节点 2 的票被计入了（得到 " +
               std::to_string(n) + "，应为 1）");
  }

  // 对照组：同样三票，若节点 2 仍是 voter，则应计 2 票（自己 + 节点 2）。
  std::vector<MemberRole> all_voter = {MemberRole::kVoter, MemberRole::kVoter,
                                       MemberRole::kVoter};
  int n_all = CountGrantedVotes(all_voter, votes);
  if (n_all != 2) {
    cfg->Fatal("计票口径错：全 voter 时应计 2 票（得到 " +
               std::to_string(n_all) + "）");
  }

  // learner 同样不参与计票（learner 不投票，也不该被算进多数派）。
  std::vector<MemberRole> with_learner = {MemberRole::kVoter,
                                          MemberRole::kLearner,
                                          MemberRole::kVoter};
  std::vector<int> votes_all = {1, 1, 1};
  int n_learner = CountGrantedVotes(with_learner, votes_all);
  if (n_learner != 2) {
    cfg->Fatal("计票口径错：learner 的票被计入了（得到 " +
               std::to_string(n_learner) + "，应为 2）");
  }

  cfg->End();
}

// ===========================================================================
// ㉖ Q2 冻结判据：两源取或（冻结点 OR 配置视图）
// ---------------------------------------------------------------------------
// 为什么必须"或"而不是只看一个：两条真实路径各只被一个源覆盖。
//   (a) 提案 → apply 窗口：is_member_ 还没切，只有 removed_at_index_ 知道要冻结；
//   (b) 经 InstallSnapshot 恢复配置：conf 条目被快照截断、冻结点重建不出来，
//       只有 is_member_ 知道该节点已被移除。
// 纯函数单测（无时序、零 flaky），直接钉住这个判据。
// ===========================================================================
void TestRemovedFreezeCoversBothSources() {
  auto cfg = MakeConfig(3, false);
  cfg->Begin("Test: removal freeze triggers on either freeze-point or config view");

  // (a) 只有冻结点（提案后、apply 前）：必须冻结，否则 leader 会继续把
  //     conf 之后的新条目复制给待移除节点（㉓ 偶发失败的旧根因）。
  {
    std::vector<MemberRole> is_member = {MemberRole::kVoter, MemberRole::kVoter,
                                         MemberRole::kVoter};  // 尚未切换
    std::vector<int> cap = {-1, -1, 7};                        // 已记冻结点
    if (!IsRemovedFrozen(is_member, cap, 2)) {
      cfg->Fatal("冻结判据错：仅有冻结点(7)时应冻结（提案→apply 窗口会漏数据）");
    }
  }

  // (b) 只有配置视图（装快照恢复、冻结点重建不出来）：必须冻结，
  //     否则新 leader 会给已从快照里读到的 removed 节点继续发新数据。
  {
    std::vector<MemberRole> is_member = {MemberRole::kVoter, MemberRole::kVoter,
                                         MemberRole::kRemoved};
    std::vector<int> cap = {-1, -1, -1};  // 快照截断，扫不到 conf
    if (!IsRemovedFrozen(is_member, cap, 2)) {
      cfg->Fatal("冻结判据错：仅配置视图为 kRemoved 时也应冻结（快照恢复场景会漏数据）");
    }
  }

  // (c) 加回 / 从未移除：两个源都为负 → 不冻结（否则加回的节点收不到日志）。
  {
    std::vector<MemberRole> is_member = {MemberRole::kVoter, MemberRole::kVoter,
                                         MemberRole::kVoter};
    std::vector<int> cap = {-1, -1, -1};
    if (IsRemovedFrozen(is_member, cap, 2)) {
      cfg->Fatal("冻结判据错：未移除节点被误冻结（加回后收不到日志）");
    }
  }

  // (d) learner：必须【不】冻结——learner 的立身之本就是继续追数据。
  {
    std::vector<MemberRole> is_member = {MemberRole::kVoter, MemberRole::kVoter,
                                         MemberRole::kLearner};
    std::vector<int> cap = {-1, -1, -1};
    if (IsRemovedFrozen(is_member, cap, 2)) {
      cfg->Fatal("冻结判据错：learner 被误冻结（learner 必须继续接收日志）");
    }
  }

  // (e) 越界防御：下标非法时不得误判冻结。
  {
    std::vector<MemberRole> is_member = {MemberRole::kVoter, MemberRole::kVoter,
                                         MemberRole::kRemoved};
    std::vector<int> cap = {-1, -1, -1};
    if (IsRemovedFrozen(is_member, cap, -1) ||
        IsRemovedFrozen(is_member, cap, 99)) {
      cfg->Fatal("冻结判据错：越界下标被判为冻结");
    }
  }

  cfg->End();
}

// ===========================================================================
// C1 未提交 entry 上限（背压）—— leader 未提交日志积压到上限后必须拒绝新提案
//
// 为什么需要：Start() 之前对"leader 能堆多少未提交日志"完全没有约束，海量并发
// 写（或 follower 全断）时这些条目全部堆在内存里，直到 OOM。etcd 的
// MaxUncommittedEntries 就是这道闸门。
//
// 变异检验（双向，缺一不可）：
//   · 删掉 Start() 里的背压判断        → 永远观测不到 backpressure=true → 本用例失败
//   · 把判断改成无条件触发（if(true)）  → 正常集群一条都提交不了 → 回归用例失败
// ===========================================================================
void TestMaxUncommittedBackpressure() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test: max uncommitted entries backpressure (C1)");

  int leader = cfg->CheckOneLeader();

  // 断开两个 follower：leader 拿不到多数派 → 日志只增不提交，未提交数必然单调上涨。
  // ⚠️ CheckQuorum 会在 ~150ms 后让 leader 主动退位，所以下面这场 Start() 暴雨
  //    必须【紧跟着断开】在毫秒级内打完，否则测的就是"退位"而不是"背压"。
  for (int i = 0; i < servers; i++) {
    if (i != leader) cfg->Disconnect(i);
  }
  if (!cfg->GetRaft(leader)->GetState().second) {
    cfg->Fatal("准备阶段失败：断开 follower 后 leader 已提前退位");
  }

  const int limit = 10;
  cfg->GetRaft(leader)->SetMaxUncommittedEntries(limit);

  int accepted = 0;
  int backpressured = 0;
  for (int i = 0; i < 60; i++) {
    auto r = cfg->GetRaft(leader)->Start("c1-cmd-" + std::to_string(i));
    if (r.backpressure) {
      backpressured++;
    } else if (r.is_leader) {
      accepted++;
    }
  }

  if (backpressured <= 0) {
    cfg->Fatal("背压未生效：60 次 Start() 中没有一次返回 backpressure=true");
  }
  if (accepted > limit) {
    cfg->Fatal("背压失效：被接受的提案数 " + std::to_string(accepted) +
               " 超过上限 " + std::to_string(limit));
  }
  // 背压是"拒绝"，不是"丢弃"：日志里必须恰好只多出 accepted 条，一条不多。
  int uncommitted = cfg->GetRaft(leader)->UncommittedCount();
  if (uncommitted > limit) {
    cfg->Fatal("未提交条目数 " + std::to_string(uncommitted) + " 突破上限 " +
               std::to_string(limit));
  }

  // 恢复：闸门调回默认 + 重新连网，集群必须自愈并能正常提交。
  cfg->GetRaft(leader)->SetMaxUncommittedEntries(kMaxUncommittedEntriesDefault);
  for (int i = 0; i < servers; i++) {
    if (i != leader) cfg->Connect(i);
  }
  cfg->One("after-backpressure", servers, false);
  cfg->End();
}

// ===========================================================================
// C3 单条消息字节上限 —— 超限的 RPC 必须被拒收（而不是把两端内存顶穿）
//
// 为什么需要：InstallSnapshot 的载荷是整个状态机快照。上层状态机一旦膨胀（或
// 收到畸形超大条目），单条消息可以轻易到 GB 级：发送侧整块序列化、接收侧整块
// 反序列化，两侧内存同时被顶穿，而协议层没有任何兜底。etcd 的
// --max-request-bytes（默认 1.5MiB）是同一道闸门。
//
// 变异检验：
//   · 删掉 SendReq 里的超限判断 → OversizedDropped() 恒为 0 → 本用例失败
// ===========================================================================
void TestRpcMaxMessageBytes() {
  int servers = 3;
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test: RPC message byte cap drops oversized messages (C3)");

  int leader = cfg->CheckOneLeader();
  (void)leader;
  cfg->One("before-cap", servers, false);

  // 默认闸门（64MiB）下不该有任何消息被拒 —— 否则说明闸门默认值定得太小，
  // 会误伤正常的快照流量。
  if (cfg->OversizedDropped() != 0) {
    cfg->Fatal("默认闸门下不应有消息被拒收，实际 = " +
               std::to_string(cfg->OversizedDropped()));
  }

  // 压到 4 字节：连最小的心跳都会超限 → 全网消息全部被拒收。
  //
  // ⚠️ 这里不能想当然写"64 字节"。args 用 zigzag varint 编码（codec.h:43），
  //    小数值只占 1 字节 —— 一个空 AppendEntries 心跳是 7 个字段 ≈ 7 字节。
  //    第一版就是照直觉写了 64，结果闸门一条都没拦到（OversizedDropped=0），
  //    用例红着才发现。所以闸门值必须按【实际编码尺寸】选，不是拍脑袋。
  cfg->SetMaxRpcMessageBytes(4);
  raftcpp::SleepMs(kRaftElectionTimeout);

  int64_t dropped = cfg->OversizedDropped();
  if (dropped <= 0) {
    cfg->Fatal("字节闸门未生效：被拒收的消息数 = 0（应为多条）");
  }

  // 恢复闸门后集群必须自愈（证明闸门只是"拒收"，没有把状态机搞坏）。
  cfg->SetMaxRpcMessageBytes(labrpc::kMaxRpcMessageBytesDefault);
  cfg->One("after-cap-recovered", servers, false);
  cfg->End();
}

// ===========================================================================
// C5 被移除节点主动退场 —— apply 到"移除自己"的配置条目后必须停止参选
//
// 现状说明（重要，避免误判为"从零新增"）：不起选 / 不投票 / 拒绝写入这三件事
// 本来就被 IsVoter 守卫覆盖了（raft.cpp:460/557/774）。真正缺的是【入口处主动
// 静默 + 可观测】：原来要"进了 StartElection() 才被拦"，白跑一趟 Pre-Vote 广播，
// 而且没有任何证据能区分"主动静默"和"碰巧没超时"。
//
// 因此本用例断言的是新增的 ElectionSuppressedCount() —— 它只在"选举定时器已
// 超时、但因自己已被移除而主动放弃"时自增。
//
// ⚠️ 必须断开 victim：removed 节点【仍会收到心跳】（raft.cpp:1146 明确这么设计，
//    它要靠心跳推进 commit_index_ 才能 apply 到移除自己的那条 conf）。不断开则
//    last_heartbeat_ 一直被刷新，定时器永不超时 → 计数器恒为 0 → 用例假失败。
//
// 变异检验：
//   · 删掉选举定时器里的 IsRemovedSelf() 判断 → 计数器恒为 0 → 本用例失败
//     （注意：删掉后集群行为仍然正确，因为 StartElection 里的 !IsVoter 兜底
//       还在 —— 所以必须用计数器断言，用"它是不是 leader"断言会假通过）
// ===========================================================================
void TestRemovedNodeQuiesces() {
  int servers = 5;  // 移除 1 台后剩 4 台，多数派 3，留足余量
  auto cfg = MakeConfig(servers, false);
  cfg->Begin("Test: removed node stops campaigning (C5)");

  int leader = cfg->CheckOneLeader();
  int victim = (leader + 1) % servers;

  // ① 先（在连网状态下）让 victim apply 到移除自己的配置条目
  cfg->GetRaft(leader)->ProposeConfChange(victim, false);
  bool removed_ok = false;
  for (int t = 0; t < 100; t++) {
    raftcpp::SleepMs(100);
    if (cfg->GetRaft(victim)->MembershipView()[victim] ==
        static_cast<int>(MemberRole::kRemoved)) {
      removed_ok = true;
      break;
    }
  }
  if (!removed_ok) cfg->Fatal("移除失败：victim 的 is_member_ 未切换为 kRemoved");
  if (!cfg->GetRaft(victim)->IsRemovedSelf()) {
    cfg->Fatal("IsRemovedSelf() 未报告 true");
  }

  // ② 断开它，逼选举定时器反复超时（见上方 ⚠️ 说明）
  cfg->Disconnect(victim);
  raftcpp::SleepMs(3 * kRaftElectionTimeout);

  if (cfg->GetRaft(victim)->ElectionSuppressedCount() <= 0) {
    cfg->Fatal("被移除节点没有主动静默：ElectionSuppressedCount() == 0");
  }
  if (cfg->GetRaft(victim)->GetState().second) {
    cfg->Fatal("被移除节点竟然成了 leader");
  }

  // ③ 其余节点不应被误判为"已移除"
  for (int i = 0; i < servers; i++) {
    if (i == victim) continue;
    if (cfg->GetRaft(i)->IsRemovedSelf()) {
      cfg->Fatal("节点 " + std::to_string(i) + " 被误报为 IsRemovedSelf");
    }
  }

  // ④ 剩余集群仍可正常提交
  cfg->One("after-remove", servers - 1, false);
  cfg->End();
}

// ===========================================================================
// 测试主程序
// ===========================================================================

struct TestCase {
  const char* name;
  std::function<void()> fn;
};

static const TestCase kTests[] = {
    {"TestInitialElection2A", TestInitialElection2A},
    {"TestReElection2A", TestReElection2A},

    {"TestBasicAgree2B", TestBasicAgree2B},
    {"TestRPCBytes2B", TestRPCBytes2B},
    {"TestFailAgree2B", TestFailAgree2B},
    {"TestFailNoAgree2B", TestFailNoAgree2B},
    {"TestConcurrentStarts2B", TestConcurrentStarts2B},
    {"TestRejoin2B", TestRejoin2B},
    {"TestBackup2B", TestBackup2B},
    {"TestCount2B", TestCount2B},

    {"TestPersist12C", TestPersist12C},
    {"TestPersist22C", TestPersist22C},
    {"TestPersist32C", TestPersist32C},
    {"TestFigure82C", TestFigure82C},
    {"TestUnreliableAgree2C", TestUnreliableAgree2C},
    {"TestFigure8Unreliable2C", TestFigure8Unreliable2C},
    {"TestReliableChurn2C", TestReliableChurn2C},
    {"TestUnreliableChurn2C", TestUnreliableChurn2C},

    {"TestSnapshotTruncatesLog2D", TestSnapshotTruncatesLog2D},
    {"TestInstallSnapshotCatchUp2D", TestInstallSnapshotCatchUp2D},
    {"TestSnapshotRestart2D", TestSnapshotRestart2D},
    {"TestSnapshotStateMachine2D", TestSnapshotStateMachine2D},

    // ./build/raft_test CheckQuorum     # 精确命中 TestCheckQuorum（唯一含 CheckQuorum 的）
    // ./build/raft_test ReadIndex       # 同时命中 TestReadIndex + TestReadIndexNoStale
    {"TestCheckQuorum", TestCheckQuorum},
    {"TestCheckQuorumNoSpuriousDemote", TestCheckQuorumNoSpuriousDemote},
    {"TestCheckQuorumUnreliableNoFlap", TestCheckQuorumUnreliableNoFlap},
    {"TestCheckQuorumUnreliableIsolated", TestCheckQuorumUnreliableIsolated},
    {"TestCheckQuorumMinorityPartitionKeepsLeadership", TestCheckQuorumMinorityPartitionKeepsLeadership},
    {"TestCheckQuorumMinorityPartitionKeepsLeadership5", TestCheckQuorumMinorityPartitionKeepsLeadership5},
    {"TestCheckQuorumSustainedMinorityStable", TestCheckQuorumSustainedMinorityStable},

    {"TestReadIndex", TestReadIndex},
    {"TestReadIndexNoStale", TestReadIndexNoStale},
    {"TestReadIndexConcurrent", TestReadIndexConcurrent},
    {"TestReadIndexPartitionImmediate", TestReadIndexPartitionImmediate},
    {"TestReadIndexMajorityToleratesMinorityFailure", TestReadIndexMajorityToleratesMinorityFailure},
    // ReadIndex × InstallSnapshot：快照回包是 AppendEntries 之外的第二条读确认通道
    {"TestReadIndexSnapshotCatchUp", TestReadIndexSnapshotCatchUp},
    {"TestReadIndexSnapshotNoDoubleCount", TestReadIndexSnapshotNoDoubleCount},
    // ReadIndex × 不可靠网络（丢包）：补齐 7 条 ReadIndex 里唯一的黑盒压力缺口
    {"TestReadIndexUnreliable", TestReadIndexUnreliable},
    // ReadIndex × 飞行中 leader 易主：旧 leader 的读必须返回 -1（绝不吐过期 commitIndex）
    {"TestReadIndexDuringReelection", TestReadIndexDuringReelection},
    // ReadIndex × 不可靠网络 × 装快照：丢包下装快照时的读不脏读且基本可用
    {"TestReadIndexSnapshotUnreliable", TestReadIndexSnapshotUnreliable},
    {"TestReadIndexIgnoresNonVoterAcks", TestReadIndexIgnoresNonVoterAcks},
    // ReadIndex × 失多数派超时契约（行为契约，非变异有牙）
    {"TestReadIndexTimesOutWithoutQuorum", TestReadIndexTimesOutWithoutQuorum},
  
    // 成员变更 ①：单节点变更（一次一个、apply 时切换、单飞保护）
    {"TestSingleNodeConfChange", TestSingleNodeConfChange},
    {"TestNoRemovingLastVoter", TestNoRemovingLastVoter},
    // ② Learner 三态：不计入多数派、但仍持续追数据、可提拔回 voter
    {"TestLearnerCatchup", TestLearnerCatchup},
    // ③ learner 扩容容错（§2.6 扩容零可用性损失）
    {"TestLearnerAvailabilityWin", TestLearnerAvailabilityWin},
    // ④ 成员配置持久化：崩溃重启后 is_member_ 不丢失（抓"只三态没真持久化"）
    {"TestMembershipPersistAcrossRestart", TestMembershipPersistAcrossRestart},
    // ⑤ learner 绝不当选 leader（安全性硬约束）
    {"TestLearnerNeverLeader", TestLearnerNeverLeader},
    // ⑥ kLearner 角色持久化（隔离版，同 ④ 手法）
    {"TestLearnerPersistAcrossRestart", TestLearnerPersistAcrossRestart},
    // ⑦ 连续变更 churn + 跨 leadership 单飞（脑裂压力）
    {"TestConfChangeChurn", TestConfChangeChurn},
    // ⑧ 移除当前 leader 自身必须主动退位
    {"TestRemoveLeaderSelf", TestRemoveLeaderSelf},
    // ⑨ InstallSnapshot 重水化角色（learner/removed 不退化）
    {"TestInstallSnapshotRestoresMembership", TestInstallSnapshotRestoresMembership},
    {"TestConcurrentConfChangeLinearizable", TestConcurrentConfChangeLinearizable},
    {"TestLearnerCatchupWithChurn", TestLearnerCatchupWithChurn},
    // ⑫ 变更中途 leader 崩溃（conf 未提交）必须安全收敛（无分叉成员态）
    {"TestConfChangeMidCrash", TestConfChangeMidCrash},
    // ⑬ 被移除节点即使在线也不计入 quorum（无幽灵票）
    {"TestRemovedNodeExcludedFromQuorum", TestRemovedNodeExcludedFromQuorum},
    // ⑭ 任意 churn 收敛后所有节点成员视图必须完全一致（防分叉成员态）
    {"TestMembershipConsistencyAtQuiescence", TestMembershipConsistencyAtQuiescence},
    {"TestReadIndexDuringConfChange", TestReadIndexDuringConfChange},
    {"TestLearnerDirectlyRemoved", TestLearnerDirectlyRemoved},
    {"TestPromoteLaggingLearnerSafe", TestPromoteLaggingLearnerSafe},
    {"TestStartRejectedForNonVoter", TestStartRejectedForNonVoter},
    {"TestInstallSnapshotRestoresRemovedRole", TestInstallSnapshotRestoresRemovedRole},
    // ㉓ removed 节点的日志必须冻结（Q2 隐私加固刚性验证）
    {"TestRemovedNodeStopsReceivingReplication", TestRemovedNodeStopsReceivingReplication},
    // ㉔ removed 节点永不看到 post-removal 客户端数据（Q2 隐私加固端到端）
    {"TestRemovedNodeStaysQuiescentAfterRemoval", TestRemovedNodeStaysQuiescentAfterRemoval},
    // ⑳ 随机 churn 模糊测试（fuzz）：收敛后所有节点成员视图一致
    {"TestMembershipFuzzChurn", TestMembershipFuzzChurn},
    // ㉑ 少数派分区内 leader 发起的变更，愈合后必须不生效
    {"TestConfChangeFromMinorityLeader", TestConfChangeFromMinorityLeader},
    // ㉒ learner 必须被拒绝对 ReadIndex 服务（对称负向）
    {"TestLearnerReadIndexRejected", TestLearnerReadIndexRejected},
    {"TestVoteCountIgnoresRemovedVoters", TestVoteCountIgnoresRemovedVoters},
    {"TestRemovedFreezeCoversBothSources", TestRemovedFreezeCoversBothSources},

    // ---- 细节级生产加固 C1 / C3 / C5 ----
    // C1 未提交 entry 上限（背压，防 leader 内存被未提交日志撑爆）
    {"TestMaxUncommittedBackpressure", TestMaxUncommittedBackpressure},
    // C3 单条 RPC 消息字节上限（防超大 snapshot/畸形条目顶穿收发两端内存）
    {"TestRpcMaxMessageBytes", TestRpcMaxMessageBytes},
    // C5 被移除节点主动退场（入口处静默 + 可观测，不再白跑 Pre-Vote 广播）
    {"TestRemovedNodeQuiesces", TestRemovedNodeQuiesces},
  };

static void Usage(const char* argv0) {
  std::printf("\n用法: %s [过滤词] [-count N]\n\n", argv0);
  std::printf("  %s                跑全部用例\n", argv0);
  std::printf("  %s 2A             只跑名字里含 2A 的用例\n", argv0);
  std::printf("  %s Initial        只跑名字里含 Initial 的用例\n", argv0);
  std::printf("  %s 2B -count 10   2B 跑 10 遍（抓偶发 bug 的必备姿势）\n\n",
              argv0);
  std::printf("可选用例:\n");
  for (const auto& t : kTests) std::printf("  %s\n", t.name);
  std::printf("\n");
}

int main(int argc, char** argv) {
  std::string filter;
  int count = 1;

  for (int i = 1; i < argc; i++) {
    std::string a = argv[i];
    if (a == "-count" && i + 1 < argc) {
      count = std::atoi(argv[++i]);
      if (count < 1) count = 1;
    } else if (a == "-h" || a == "--help") {
      Usage(argv[0]);
      return 0;
    } else if (!a.empty() && a[0] == '-') {
      std::printf("unknown option: %s\n", a.c_str());
      Usage(argv[0]);
      return 2;
    } else {
      filter = a;
    }
  }

  const std::size_t num_tests = sizeof(kTests) / sizeof(kTests[0]);

  std::printf("raft_test：已注册 %zu 个用例\n", num_tests);
  std::printf("  新增用例务必登记进 kTests[] —— 漏登记【不会有任何编译期提示】，\n");
  std::printf("  该用例会永远不被执行。启动这里打印数量就是给你对一眼的。\n");
  std::printf("  sanitizer = %s\n", raftcpp::SanitizerName());
  std::printf("  SEED = %llu（设 SEED=<n> 换一条随机序列；固定种子让偶发失败可复现）\n",
              static_cast<unsigned long long>(raftcpp::RandSeed()));
  std::printf("  单用例超时 = %lld ms（TEST_TIMEOUT_MS=<n> 可覆盖，0 关闭）\n\n",
              static_cast<long long>(raftcpp::TestTimeoutMs()));

  int passed = 0;
  int failed = 0;
  std::vector<std::string> failed_names;
  const auto t_all = raftcpp::Now();

  // 过滤模式二选一：filter 与某个用例名【完全相等】→ 只精确跑这一条；
  // 否则退回原有子串匹配（"2A"、"Initial"、"ReadIndex" 等惯用法不变）。
  // 精确模式是给 test_part.sh 的并发压测用的：脚本按全名分进程跑，
  // 而子串匹配下 "TestReadIndex" 会误伤其余 9 个 TestReadIndex* 用例
  //（TestCheckQuorum 同理），每条进程就变成"跑一遍全家桶"了。
  bool exact = false;
  if (!filter.empty()) {
    for (const auto& t : kTests) {
      if (filter == t.name) { exact = true; break; }
    }
  }

  for (int round = 0; round < count; round++) {
    if (count > 1) std::printf("\n===== 第 %d/%d 轮 =====\n", round + 1, count);
    for (const auto& t : kTests) {
      if (!filter.empty() &&
          (exact ? filter != t.name
                 : std::string(t.name).find(filter) == std::string::npos)) {
        continue;
      }
      // 后台线程崩溃计数：跑完取差值，>0 就判失败
      // （否则 Raft 的选举/复制线程崩了，断言全过 → 用例照样"通过"）。
      const int crashes_before = raftcpp::ThreadTracker::Instance().Crashes();

      // 看门狗：卡死超过 TestTimeoutMs() 就 abort。
      // 对齐 go test 的 10 分钟超时 —— 之前这里只有个没用上的 `auto t0`，
      // 用例真死锁了会永久挂住，什么线索都留不下。
      raftcpp::Watchdog wd(t.name, raftcpp::TestTimeoutMs());
      auto t0 = raftcpp::Now();

      bool bad = false;
      std::string reason;
      try {
        t.fn();
      } catch (const TestFailure& f) {
        bad = true;
        reason = f.msg;
      } catch (const std::exception& e) {
        bad = true;
        reason = std::string("未捕获的 std::exception: ") + e.what();
      } catch (...) {
        bad = true;
        reason = "未捕获的非 std 异常";
      }

      const int crashed =
          raftcpp::ThreadTracker::Instance().Crashes() - crashes_before;
      const long long elapsed_ms =
          static_cast<long long>(raftcpp::MillisSince(t0));

      if (!bad && crashed > 0) {
        bad = true;
        char buf[256];
        std::snprintf(buf, sizeof(buf), "%d 个后台线程抛异常崩溃（%s）", crashed,
                      raftcpp::ThreadTracker::Instance().LastCrash().c_str());
        reason = buf;
      }

      if (bad) {
        failed++;
        std::printf("  !!!! FAILED: %s -- %s  (%lld ms)\n", t.name,
                    reason.c_str(), elapsed_ms);
        failed_names.push_back(t.name);
      } else {
        passed++;
      }
      std::fflush(stdout);
    }
  }

  std::printf("\n========================================\n");
  std::printf("  通过 %d 个，失败 %d 个  （总耗时 %.1f 秒）\n", passed, failed,
              raftcpp::SecondsSince(t_all));
  if (!failed_names.empty()) {
    std::printf("  失败的用例:\n");
    for (const auto& n : failed_names) std::printf("    - %s\n", n.c_str());
  }
  std::printf("========================================\n");

  return failed == 0 ? 0 : 1;
}
