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

  const int n = 50;
  int ok = 0, fail = 0, stale = 0;
  for (int i = 0; i < n; i++) {
    int ri = cfg->GetRaft(leader)->ReadIndex();
    if (ri < 0) {
      fail++;            // 丢包超时 / 瞬态退位，保守返回 -1，可接受
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
  const int nthreads = 8, ncalls = 20;
  std::vector<std::thread> ts;
  std::atomic<int> ok{0}, fail{0}, stale{0};
  for (int t = 0; t < nthreads; t++) {
    ts.emplace_back([&]() {
      for (int i = 0; i < ncalls; i++) {
        int ri = cfg->GetRaft(leader)->ReadIndex();
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

  // (b) 可用性：丢包 + 装快照环境下多数派心跳仍能凑齐，成功数应多于失败数
  if (ok <= fail) {
    cfg->Fatal("不可靠网装快照期间 ReadIndex 成功(" + std::to_string(ok) +
               ") 未超过失败(" + std::to_string(fail) +
               ") —— 线性一致读基本不可用（liveness 回归）");
  }

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
