// test_raft.cpp —— 所有测试用例（Go 版 src/raft/test_test.go 的移植）
//
// ===========================================================================
// 这个文件【原则上不用改】。它是你的验收标准。
//
// 想自己加测试？看 doc/05-如何写C++测试.md，末尾也留了两个
// TODO 空位（TestMyOwnScenario）供你练手。
//
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
// TODO(练手)：自己写一个场景测试
// ---------------------------------------------------------------------------
// 照着上面随便一个抄，改改参数就行。比如：
//   - 5 台机器，随机断 1 台，同时持续提交命令，最后检查所有日志一致
//   - 只留 2 台，确认提交不了；再连回 3 台，确认能提交
// 写好后加进下面的 kTests 数组就能跑。
// ===========================================================================

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

  for (int round = 0; round < count; round++) {
    if (count > 1) std::printf("\n===== 第 %d/%d 轮 =====\n", round + 1, count);
    for (const auto& t : kTests) {
      if (!filter.empty() && std::string(t.name).find(filter) ==
                                 std::string::npos) {
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
