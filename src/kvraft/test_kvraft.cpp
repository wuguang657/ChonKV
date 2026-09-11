// test_kvraft.cpp —— KV 服务的测试（对应 Go 版 src/kvraft/test_test.go 的 3A / 3B）
//
// 跑法（和 raft_test 一致）：
//   ./kv_test           跑全部
//   ./kv_test 3A        只跑名字里含 3A 的
//   ./kv_test 3B -count 3
//
// ===========================================================================
// 3A / 3B 各在考什么
// ===========================================================================
//   3A：KV 服务本身 —— Get/Put/Append 语义、exactly-once（客户端重试不重复执行）、
//       分区后各副本最终一致、不可靠网络下仍能推进。此时 maxraftstate = -1（不快照）。
//   3B：日志压缩 —— raft 状态超过 maxraftstate 就得生成快照并截断日志；
//       落后太多的 follower 靠 InstallSnapshot RPC 追上；快照 + 崩溃重启后数据还在。
//
// 和 Go 版唯一的结构差异：Go 用 goroutine + channel，这里用 std::thread +
// atomic flag。语义完全对齐。

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>  // std::getenv (KV_MINI_LIN 调试开关)
#include <functional>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "../common/util.h"      // raftcpp::RandInt / RandSeed（让 SEED 管得到测试随机序列）
#include "../common/watchdog.h"  // 单用例超时看门狗（对齐 go test 的 10min 超时）
#include "client.h"
#include "config.h"
#include "server.h"
#include "test_status.h"  // 共享 g_failed / Fatal() 给 Config.cpp 用

namespace kvraft {
namespace {

// ===========================================================================
// 全局失败标记 + 辅助函数
// ===========================================================================
//
// g_failed / Fatal() 的定义挪到 test_status.{h,cpp}，方便 Config.cpp 也调用。
// 否则 Config::CheckLinearizability 失败时只 Config::Fail()，test runner 这边
// 的 g_failed 还是 false，会把失败的用例算成"通过"。

// 复现客户端 Append 出来的期望值：把每次 append 的内容依次拼起来
std::string NextValue(const std::string& prev, const std::string& val) {
  return prev + val;
}

std::string ClntValue(int cli, int j) {
  std::string v;
  for (int i = 0; i < j; i++) {
    v = NextValue(v, "x " + std::to_string(cli) + " " + std::to_string(i) + " y");
  }
  return v;
}

// ---- 带计数 + 断言的客户端操作封装 ----
std::string DoGet(Config* cfg, Clerk* ck, const std::string& key) {
  std::string v = ck->Get(key);
  cfg->Op();
  return v;
}

void DoPut(Config* cfg, Clerk* ck, const std::string& key,
           const std::string& value) {
  ck->Put(key, value);
  cfg->Op();
}

void DoAppend(Config* cfg, Clerk* ck, const std::string& key,
              const std::string& value) {
  ck->Append(key, value);
  cfg->Op();
}

void DoCheck(Config* cfg, Clerk* ck, const std::string& key,
             const std::string& want) {
  std::string v = ck->Get(key);
  cfg->Op();
  if (v != want) {
    char buf[512];
    std::snprintf(buf, sizeof(buf), "key=%s wanted=[%s] got=[%s]", key.c_str(),
                  want.c_str(), v.c_str());
    Fatal(buf);
  }
}

// 校验"第 cli 号客户端 append 了 count 次之后，值应该是什么"
void CheckClntAppends(int cli, const std::string& v, int count) {
  std::string want = ClntValue(cli, count);
  if (want != v) {
    char buf[512];
    std::snprintf(buf, sizeof(buf),
                  "client %d: wrong appends, want len=%zu got len=%zu", cli,
                  want.size(), v.size());
    Fatal(buf);
  }
}

// 对应 Go 的 checkConcurrentAppends：逐客户端、逐次校验
//   (1) 该次 append 的内容必须出现（漏执行 → missing）
//   (2) 只能出现一次（重复执行 → duplicate，find 与 rfind 位置不同）
//   (3) 同一个客户端的各次 append 必须按 j 递增顺序出现（乱序 → wrong order）
// 比只查"在不在"强得多，能抓出 exactly-once 被破坏（重复 Append / 漏执行）。
void CheckConcurrentAppends(int nclients, const std::string& v,
                            const std::vector<int>& counts) {
  for (int cli = 0; cli < nclients; cli++) {
    int lastoff = -1;
    for (int j = 0; j < counts[cli]; j++) {
      std::string wanted =
          "x " + std::to_string(cli) + " " + std::to_string(j) + " y";
      size_t off = v.find(wanted);
      if (off == std::string::npos) {
        char buf[256];
        std::snprintf(buf, sizeof(buf),
                      "client %d: missing element %s in append result", cli,
                      wanted.c_str());
        Fatal(buf);
        return;
      }
      size_t off1 = v.rfind(wanted);
      if (off1 != off) {
        char buf[256];
        std::snprintf(buf, sizeof(buf),
                      "client %d: duplicate element %s in append result", cli,
                      wanted.c_str());
        Fatal(buf);
        return;
      }
      if (static_cast<int>(off) <= lastoff) {
        char buf[256];
        std::snprintf(buf, sizeof(buf),
                      "client %d: wrong order for element %s in append result",
                      cli, wanted.c_str());
        Fatal(buf);
        return;
      }
      lastoff = static_cast<int>(off);
    }
  }
}

// KV_MINI_LIN=1 才跑那些"迷你"调试用例（缩短 iter / sleep，压小 history）。
// 正式全量跑不该被它们污染 —— 之前 kv_mini_lin_dup 混在 kTests 里，
// 每次全量都多跑一个用例。
bool MiniLinEnabled() {
  const char* p = std::getenv("KV_MINI_LIN");
  return p != nullptr && p[0] != '\0' && p[0] != '0';
}

int RandInt(int n) {
  // ⚠️ 原来这里是 `std::mt19937 rng(std::random_device{}())` —— 拿真随机源
  //    播种，SEED 环境变量【完全管不到】。后果：启动横幅那句"固定种子让偶发
  //    失败可复现"是假承诺 —— 客户端选 Append 还是 Get、选哪个 key，走的都
  //    是这条流，每次运行都换一套，偶发失败根本复现不出来。
  //
  //    改成转发 raftcpp::RandInt：它用 MixSeed(RandSeed(), 线程流编号) 播种，
  //    SEED 固定 → 序列固定。两个附带好处：
  //      * 内部是 thread_local 引擎，不需要这里的 mutex；
  //      * 用 uniform_int_distribution 而不是 `rng() % n`，分布更均匀
  //        （原始引擎低位规律性会被取模放大）。
  //
  //    可复现到什么程度（见 common/util.h:99-105 的诚实说明）：主线程严格可
  //    复现，子线程是"大概率"—— 线程领号顺序仍受调度影响。想做到严格可复现
  //    要用 raftcpp::RandIntFor(逻辑编号, n) 把流绑到逻辑实体而非 OS 线程。
  return raftcpp::RandInt(n);
}

// 前向声明：3B 章节定义的通用混沌测试驱动器。
// Go 版 TestBasic3A / TestConcurrent3A / TestUnreliable3A 也是直接调它，
// 所以 C++ 侧的 3A 分区用例（TestManyPartitions*3A）同样复用。
void GenericTest(const std::string& part, int nclients, bool unreliable,
                 bool crash, bool partitions, int maxraftstate,
                 bool linearizability = false);

// 线性一致性版本：跨 client 随机选 key，3 轮 iter 后交给 porcupine 判定。
// 完全对齐 Go 版 GenericTestLinearizability（test_test.go:411）。
void GenericTestLinearizability(const std::string& part, int nclients,
                                int nservers, bool unreliable, bool crash,
                                bool partitions, int maxraftstate);

// 和 Go 版 test_test.go 顶部的常量保持一致
constexpr int kElectionTimeoutMs = 1000;  // Go: electionTimeout = 1s

// ===========================================================================
// 后台跑一个客户端操作，并能查询"它是否已经返回"
// ===========================================================================
// Go 版用 goroutine + channel + select(time.After) 来断言"这个操作在 N 秒内
// 不许返回"。C++ 没有 select，用 atomic flag + 轮询实现同样的语义。
//
//   AsyncOp op([&]{ DoPut(&cfg, cku.get(), "1", "15"); });
//   if (op.WaitDone(1000)) Fatal("少数派里的 Put 竟然返回了");
//   cfg.ConnectAll();
//   if (!op.WaitDone(3000)) Fatal("heal 之后 Put 还是没完成");
//
// ⚠️ 析构时会 join —— 所以必须确保操作最终能返回，否则析构会把测试挂死。
//   上面的用法里 heal 之后操作一定会返回，安全。
class AsyncOp {
 public:
  template <typename F>
  explicit AsyncOp(F&& fn)
      : th_([this, fn] {
          fn();
          done_.store(true);
        }) {}

  AsyncOp(const AsyncOp&) = delete;
  AsyncOp& operator=(const AsyncOp&) = delete;

  // 等到操作返回；超时返回 false
  bool WaitDone(int timeout_ms) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
      if (done_.load()) return true;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return done_.load();
  }

  bool done() const { return done_.load(); }

  ~AsyncOp() {
    if (th_.joinable()) th_.join();
  }

 private:
  std::atomic<bool> done_{false};
  std::thread th_;
};

// ===========================================================================
// 3A：基础功能
// ===========================================================================

// 最简单的一个：Put 进去、Get 出来；Append 追加。
void TestBasic3A() {
  // 对齐 Go 版 test_test.go:448 —— TestBasic3A 就是 GenericTest 的
  // nclients=1 版本（跑 3 轮，每轮 5 秒并发随机 Append/Get，
  // 结束调 CheckClntAppends 自检）。
  //
  // ⚠️ 之前这里是手写的 3 个固定 op（Put + 2×Append + 查不存在的 key），
  //    单线程、无并发、无重试 —— 完全不锻炼"客户端重试去重 / 并发自校验"
  //    路径。一个只在【并发重试】下才暴露的 exactly-once bug 会直接漏过。
  //    改回调 GenericTest 补回覆盖（与 TestConcurrent3A 的做法一致）。
  GenericTest("3A", /*nclients=*/1, /*unreliable=*/false, /*crash=*/false,
              /*partitions=*/false, /*maxraftstate=*/-1);
}

// 多个客户端并发写【各自的 key】。
// 考的是：线性读、exactly-once、各副本最终一致。
// 对齐 Go 版：Go 的 TestConcurrent3A 走 GenericTest(t,"3A",5,false,false,false,-1)，
// 跑 3 轮且 GenericTest 自带 SnapshotSize==0 自检（test_test.go:282-286）。
// 原手写单轮版本既少 2/3 压力又缺该自检，故直接复用 C++ 既有 GenericTest。
void TestConcurrent3A() {
  GenericTest("3A", /*nclients=*/5, /*unreliable=*/false, /*crash=*/false,
              /*partitions=*/false, /*maxraftstate=*/-1, /*linearizability=*/false);
}

// 不可靠网络：丢请求、丢回包、乱序。
// 考的是：客户端重试 + exactly-once 必须能扛住"同一个请求被提交两次"。
// 对齐 Go 版：Go 的 TestUnreliable3A 走 GenericTest(t,"3A",5,true,false,false,-1)，
// 跑 3 轮且自带 SnapshotSize==0 自检。复用 C++ 既有 GenericTest 保持同构。
void TestUnreliable3A() {
  GenericTest("3A", /*nclients=*/5, /*unreliable=*/true, /*crash=*/false,
              /*partitions=*/false, /*maxraftstate=*/-1, /*linearizability=*/false);
}

// 多个客户端并发 append【同一个 key】。
// 这是 exactly-once 最狠的一条：任何一次"重复执行"都会让最终值多出一截，
// 任何一次"漏执行"都会少一截 —— 最后的字符串长度和内容是精确的。
void TestUnreliableOneKey3A() {
  // 对齐 Go 版 test_test.go:463-493：
  //   nservers=3、nclient=5、upto=10（每个客户端【固定】append 10 次后停）。
  //
  // ⚠️ 之前是 nservers=5 / nclients=10 / 主线程 sleep 5 秒（次数不定）——
  //    把 Go 的"确定性 50 次 append 校验"改成了"时长驱动的随机量"，
  //    每次跑的负载都不同、出问题难复现。改回固定次数与 Go 一致。
  const int nservers = 3;
  const int nclients = 5;
  const int upto = 10;
  const std::string key = "k";  // Go 版用 "k"

  Config cfg(nservers, true, -1);  // unreliable = true
  auto ck = cfg.MakeClient(cfg.All());

  cfg.Begin("Test: concurrent append to same key, unreliable (3A)");

  // Go 版第一步：先把 key 初始化成空串，之后各客户端往【同一个】key 追加
  DoPut(&cfg, ck.get(), key, "");

  std::vector<std::thread> threads;
  for (int cli = 0; cli < nclients; cli++) {
    threads.emplace_back([&, cli] {
      auto myck = cfg.MakeClient(cfg.All());
      for (int n = 0; n < upto; n++) {
        // 每个客户端 append 的内容自带编号，最后可以精确还原出执行了几次
        std::string nv =
            "x " + std::to_string(cli) + " " + std::to_string(n) + " y";
        DoAppend(&cfg, myck.get(), key, nv);
      }
    });
  }
  for (auto& t : threads) t.join();

  // 校验：每个客户端的每次 append 都恰好生效一次、不重复、有序
  // （对应 Go 的 checkConcurrentAppends，能抓出 exactly-once 被破坏）
  // Go 版 counts 直接填 upto —— 每个客户端一定成功 append 了 10 次。
  std::vector<int> counts(nclients, upto);
  std::string v = DoGet(&cfg, ck.get(), key);
  CheckConcurrentAppends(nclients, v, counts);

  cfg.End();
  cfg.Cleanup();
}

// 分区：把集群切成多数派 / 少数派，验证三件事（严格对齐 Go 版三段式）。
//
// 这是 3A 里唯一一个"断言【不该发生】的事没有发生"的用例 —— 前几个用例都是
// 验证"该发生的发生了"，而它专门验证"少数派写必须卡住"。少了这一段，
// 一个"不检查任期、少数派也敢提交"的实现是能全绿的。
void TestOnePartition3A() {
  const int nservers = 5;
  Config cfg(nservers, false, -1);

  auto ck = cfg.MakeClient(cfg.All());
  DoPut(&cfg, ck.get(), "1", "13");

  // ---------- 第一段：多数派能推进 ----------
  cfg.Begin("Test: progress in majority (3A)");

  std::vector<int> p1, p2;
  cfg.MakePartition(&p1, &p2);  // p2 是少数派（含旧 leader），只切了集群的网

  auto ckp1 = cfg.MakeClient(p1);  // 客户端出生时只打开到 p1 的端点
  auto ckp2a = cfg.MakeClient(p2);  // 客户端出生时只打开到 p2 的端点
  auto ckp2b = cfg.MakeClient(p2);

  DoPut(&cfg, ckp1.get(), "1", "14");
  DoCheck(&cfg, ckp1.get(), "1", "14");

  cfg.End();

  // ---------- 第二段：少数派不许推进 ----------
  // 在后台发起 Put / Get，然后断言它们在 1 秒内【都不许返回】。
  // 少数派凑不齐多数派，命令不可能被提交 → 一个正确的实现必须让客户端一直重试。
  cfg.Begin("Test: no progress in minority (3A)");

  AsyncOp op_put([&] { DoPut(&cfg, ckp2a.get(), "1", "15"); });
  AsyncOp op_get([&] { DoGet(&cfg, ckp2b.get(), "1"); });

  // ⚠️ 注意这两个是"不该返回"的断言，和 op_get/op_put 的析构顺序无关：
  //    下面第三段会先 ConnectAll + ConnectClient 让它们返回，才析构。
  if (op_put.WaitDone(kElectionTimeoutMs)) {
    Fatal("Put in minority completed");
  }
  if (op_get.WaitDone(kElectionTimeoutMs)) {
    Fatal("Get in minority completed");
  }

  // 同时确认多数派没被少数派拖住，仍然能正常读写
  DoCheck(&cfg, ckp1.get(), "1", "14");
  DoPut(&cfg, ckp1.get(), "1", "16");
  DoCheck(&cfg, ckp1.get(), "1", "16");

  cfg.End();

  // ---------- 第三段：heal 之后刚才卡住的请求必须完成 ----------
  cfg.Begin("Test: completion after heal (3A)");

  cfg.ConnectAll();    // 连上集群间的网
  cfg.ConnectClient(ckp2a.get(), cfg.All()); // 客户端ckp2a 连上集群间的网
  cfg.ConnectClient(ckp2b.get(), cfg.All()); // 客户端ckp2b 连上集群间的网

  // 给新任期一点时间产生（Go 版这里是 sleep(electionTimeout)）
  std::this_thread::sleep_for(std::chrono::milliseconds(kElectionTimeoutMs));

  // 少数派里挂着的 Put / Get 现在必须能完成。
  //
  // ⚠️ Go 版这里只等 3 秒（time.After(30 * 100 * time.Millisecond)），
  //    C++ 版必须放宽，否则会随机失败：
  //    ckp2a / ckp2b 在分区期间只连着 2 台，另外 3 台是断连的 —— 而发往
  //    断连节点的 RPC 会被网络层延迟 rand()%7000 ms 才返回失败
  //    （labrpc.cpp:188，与 Go 版 labrpc.go:296 一致）。heal 之后，那个
  //    "已经在途"的 RPC 仍要按原定延迟走完（最多 7 秒）才轮得到下一台。
  //    等 3 秒不够，等 10 秒够。
  if (!op_put.WaitDone(10000)) {
    Fatal("Put did not complete");
  }
  if (!op_get.WaitDone(10000)) {
    Fatal("Get did not complete");
  }

  // ⭐ 核心断言：heal 之后，少数派发出的那条 Put("1","15") 必须真正生效。
  //    它比第二段多数派的 "16" 更晚被提交，所以最终值必须是 "15" 而不是 "16"。
  //    少了这一行，一个"heal 后把旧请求直接丢弃"的实现照样能全绿。
  DoCheck(&cfg, ck.get(), "1", "15");

  cfg.End();
  cfg.Cleanup();
}

// 随机分区 + 不崩溃（对应 Go 的 TestManyPartitions*3A）。
// 和 TestPersistPartition3A 的区别：不重启，纯粹考"边分区边服务"的能力。
void TestManyPartitionsOneClient3A() {
  GenericTest("3A", 1, false, false, true, -1);
}

void TestManyPartitionsManyClients3A() {
  GenericTest("3A", 5, false, false, true, -1);
}

// ===========================================================================
// 3B：日志压缩（快照）
// ===========================================================================

// 通用的"混沌测试"驱动器。参数含义和 Go 版 GenericTest 一一对应：
//   unreliable：开不可靠网络
//   crash：      每轮结束把全部 server 崩掉再重启
//   partitions：边跑边随机分区
//   maxraftstate：快照阈值
//   linearizability：跑完后用 porcupine 验线性一致性（只对 *Linearizable
//                   用例开；代价：检查本身可能跑几十秒）
void GenericTest(const std::string& part, int nclients, bool unreliable,
                 bool crash, bool partitions, int maxraftstate,
                 bool linearizability) {
  std::string title = "Test: ";
  if (unreliable) title += "unreliable net, ";
  if (crash) title += "restarts, ";
  if (partitions) title += "partitions, ";
  if (maxraftstate != -1) title += "snapshots, ";
  title += (nclients > 1 ? "many clients" : "one client");
  title += " (" + part + ")";

  const int nservers = 5;
  Config cfg(nservers, unreliable, maxraftstate, linearizability);
  cfg.Begin(title);

  for (int iter = 0; iter < 3; iter++) {
    std::atomic<bool> done_clients{false};
    std::atomic<bool> done_partitioner{false};
    std::vector<int> counts(nclients, 0);
    std::vector<std::thread> threads;

    for (int cli = 0; cli < nclients; cli++) {
      // 当场构造一个线程，每个线程对应一个客户端，每个客户端随机50% Put / Get操作
      threads.emplace_back([&, cli] {
        auto myck = cfg.MakeClient(cfg.All());
        int j = 0;
        std::string last;
        std::string key = std::to_string(cli);
        DoPut(&cfg, myck.get(), key, last);
        while (!done_clients.load()) {
          if (RandInt(1000) < 500) {
            std::string nv =
                "x " + std::to_string(cli) + " " + std::to_string(j) + " y";
            DoAppend(&cfg, myck.get(), key, nv);
            last = NextValue(last, nv);
            j++;
          } else {
            std::string v = DoGet(&cfg, myck.get(), key);
            if (v != last) {
              Fatal("get wrong value, key " + key + " wanted [" + last +
                    "] got [" + v + "]");
            }
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        counts[cli] = j;
      });
    }

    std::thread partitioner;
    if (partitions) {
      // 先让客户端在无干扰的环境下跑 1 秒（Go 版原话：
      // "Allow the clients to perform some operations without interruption"）
      std::this_thread::sleep_for(std::chrono::seconds(1));
      partitioner = std::thread([&] {
        while (!done_partitioner.load()) {
          std::vector<int> p1, p2;
          // 对齐 Go 版 test_test.go:126 partitioner()：每台 server 独立 50%。
          // 用固定比例的 MakePartition 会把形态锁死成 4 vs 3（n=7），
          // 12 次分区几乎一模一样，测不到 1v6 / 全员连通。
          cfg.MakePartitionRandom(&p1, &p2);
          // ⚠️ 间隔必须 ≈1 个选举超时（对齐 Go 版：
          // electionTimeout + rand%200 ms）。
          // 之前这里是 200~500ms，而选出一个 leader 本身就要 ~1 秒 ——
          // 结果就是 leader 刚上位网络就被切开，客户端几乎没有推进，
          // counts[i] 全是 0，最后 CheckClntAppends(cli, v, 0)
          // "什么都不检查就直接通过"，用例变成了摆设。
          std::this_thread::sleep_for(std::chrono::milliseconds(
              kElectionTimeoutMs + RandInt(200)));
        }
        cfg.ConnectAll();
      });
    }

    std::this_thread::sleep_for(std::chrono::seconds(5));

    done_clients.store(true);
    done_partitioner.store(true);

    if (partitions) {
      if (partitioner.joinable()) partitioner.join();
      // 重连后要等一会儿，让被隔离的旧 leader 发现新 term 并退位
      // 对齐 Go 版 test_test.go:242 —— time.Sleep(electionTimeout) = 1000ms。
      // （之前是 500ms，短于一个选举超时：旧 leader 可能还没退位就开始校验，
      //   正确实现也会被判失败 → flaky。）
      cfg.ConnectAll();
      std::this_thread::sleep_for(std::chrono::milliseconds(kElectionTimeoutMs));
    }
    // 先断网杀死全部 5 台（留下持久化的"盘"）→ 静默 1 秒 → 拿旧盘重建全新实例并组网 → 让在途客户端重试完成
    if (crash) {
      for (int i = 0; i < nservers; i++) cfg.ShutdownServer(i);
      // 同上，对齐 Go 版 test_test.go:252 的 time.Sleep(electionTimeout)
      std::this_thread::sleep_for(std::chrono::milliseconds(kElectionTimeoutMs));
      for (int i = 0; i < nservers; i++) cfg.StartServer(i);
      cfg.ConnectAll();
    }
    // 等待所有线程都结束
    for (auto& t : threads) t.join();

    // ---- 进度自检：防止用例"空转通过" ----
    // CheckClntAppends(cli, v, 0) 在 count==0 时是恒真的 —— 什么都不检查就过。
    // 所以必须先确认这一轮客户端真的干成了活儿，否则这个用例等于没跑。
    int total_appends = 0;
    for (int c : counts) total_appends += c;
    if (total_appends == 0) {
      Fatal("no client made progress: 一轮 5 秒内没有任何 append 成功，"
            "CheckClntAppends 会恒真通过 —— 这个用例等于没跑"
            "（常见原因：分区过于频繁 / 网络线程池饥饿 / 集群选不出 leader）");
    } else if (total_appends < nclients) {
      char buf[256];
      std::snprintf(buf, sizeof(buf),
                    "(warning) 第 %d 轮只有 %d 次 append / %d 个客户端，进度偏低",
                    iter, total_appends, nclients);
      std::printf("    %s\n", buf);
      std::fflush(stdout);
    }

    // 崩溃重启后，每台的状态机都必须还留着完整的 append 历史
    auto ck = cfg.MakeClient(cfg.All());
    for (int cli = 0; cli < nclients; cli++) {
      std::string v = DoGet(&cfg, ck.get(), std::to_string(cli));
      CheckClntAppends(cli, v, counts[cli]);
    }

    if (maxraftstate > 0) {
      int sz = cfg.LogSize();
      if (sz > 8 * maxraftstate) {
        char buf[256];
        std::snprintf(buf, sizeof(buf), "logs were not trimmed (%d > 8*%d)", sz,
                      maxraftstate);
        Fatal(buf);
      }
    }
    if (maxraftstate < 0) {
      int ssz = cfg.SnapshotSize();
      if (ssz > 0) {
        Fatal("snapshot should not be used when maxraftstate = -1");
      }
    }
  }

  cfg.End();
  cfg.Cleanup();
}

// ===========================================================================
// 线性一致性版本的混沌测试驱动器（对应 Go 版 GenericTestLinearizability）
// ===========================================================================
//
// 和 GenericTest 的核心区别：
//   1. 客户端循环里【随机选 key】（key := rand.Int() % nclients）
//      —— 跨 client 互相写同一个 key，构造最强的并发交错，让最容易出 bug 的
//      序列（lost update、stale read、append non-atomic 等）以几何级数触发。
//   2. 不做 client 端 self-check（单 client 不再独占自己的 key，没法维护
//      本地"expected last"）。
//   3. cfg.End() 内部 CheckLinearizability 触发 porcupine 判定（因为 Config
//      构造时传了 linearizability=true）。这一步之前 Go 版是显式调
//      porcupine.CheckOperationsVerbose，这里 C++ 把判定收敛到 cfg.End() 内。
//
// 测试规模与 Go 版 1:1：
//   TestPersistPartitionUnreliableLinearizable3A  →  15 clients / 7 servers
//   TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B
//                                                  →  15 clients / 7 servers
//
// ⚠️ 注意，Go 版 client_id 是用 cli 编号固定下来的（操作记录的 ClientId 字段
// 就是 goroutine 编号 0..nclients-1）。C++ 端 Clerk 拿全局 NRand 当 client_id，
// 每次 outer iter 都新 MakeClient → 新 client_id 串。这不影响线性一致性
// 判定（porcupine 的 client_id 只用于可视化），但会让 dump 不那么直观。
void GenericTestLinearizability(const std::string& part, int nclients,
                                int nservers, bool unreliable, bool crash,
                                bool partitions, int maxraftstate) {
  std::string title = "Test: ";
  if (unreliable) title += "unreliable net, ";
  if (crash) title += "restarts, ";
  if (partitions) title += "partitions, ";
  if (maxraftstate != -1) title += "snapshots, ";
  title += (nclients > 1 ? "many clients" : "one client");
  title += ", linearizability checks (" + part + ")";

  // 🐞 调试开关：KV_MINI_LIN=1 时把 outer iter / sleep 拉短，
  // 让 dump 出的 history 尽量小 (≤几十条 ops)，方便手算锁定非法 op 对。
  // 不影响原 3A/3B 入口（它们都是用默认 3 iter × 5 秒跑）。
  static const bool kMiniLin = MiniLinEnabled();
  const int kIterCount = kMiniLin ? 1 : 3;
  const int kSleepSec  = kMiniLin ? 2 : 5;

  Config cfg(nservers, unreliable, maxraftstate, /*linearizability=*/true);
  cfg.Begin(title);

  for (int iter = 0; iter < kIterCount; iter++) {
    std::atomic<bool> done_clients{false};
    std::atomic<bool> done_partitioner{false};
    std::vector<std::thread> threads;

    for (int cli = 0; cli < nclients; cli++) {
      threads.emplace_back([&, cli] {
        auto myck = cfg.MakeClient(cfg.All());
        int j = 0;
        // 关键：随机 key —— 跨 client 互相写同一个 key。
        //
        // 本实现的比例：50% Append / 10% Put / 40% Get。
        //
        // ⚠️ 这和 Go 版【并不相同】，别被"对齐"的说法误导。
        //    Go 版 test_test.go:352/356 是两次【独立】取随机数：
        //        if      (rand.Int()%1000) < 500   → Append  50%
        //        else if (rand.Int()%1000) < 100   → Put     50% × 10% = 5%
        //        else                              → Get     50% × 90% = 45%
        //    即 Go 的真实比例是 50 / 5 / 45。这里用单次 `r` 判断，Put 拿到
        //    的是 500..600 那一段（10%），是 Go 的两倍。
        //
        //    保留 10% 是有意的：Put 是覆盖而非追加，比例越高越容易撞出
        //    lost update。代价是出问题时没法用 Go 版交叉验证。
        //    想严格对齐 Go，把下面的条件改成再取一次随机数即可：
        //        } else if (RandInt(1000) < 100) {
        while (!done_clients.load()) {
          std::string key = std::to_string(RandInt(nclients));
          std::string nv =
              "x " + std::to_string(cli) + " " + std::to_string(j) + " y";
          int r = RandInt(1000);
          if (r < 500) {
            DoAppend(&cfg, myck.get(), key, nv);
            j++;
          } else if (r < 600) {
            // 500..600 → 10% 的概率做 Put（<500 已经吃掉 50%）
            DoPut(&cfg, myck.get(), key, nv);
            j++;
          } else {
            DoGet(&cfg, myck.get(), key);
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
      });
    }

    std::thread partitioner;
    if (partitions) {
      // 【mini 模式】跳过 1 秒"自由跑"，让 partition 立刻开始抢 server
      // —— 不然 sleep(2) 总时间被 partition 准备吃掉一大半。
      if (!kMiniLin) std::this_thread::sleep_for(std::chrono::seconds(1));
      partitioner = std::thread([&] {
        while (!done_partitioner.load()) {
          std::vector<int> p1, p2;
          // 同 GenericTest：对齐 Go 版 partitioner()，每台独立 50%
          cfg.MakePartitionRandom(&p1, &p2);
          std::this_thread::sleep_for(std::chrono::milliseconds(
              kElectionTimeoutMs + RandInt(200)));
        }
        cfg.ConnectAll();
      });
    }

    std::this_thread::sleep_for(std::chrono::seconds(kSleepSec));

    done_clients.store(true);
    done_partitioner.store(true);

    if (partitions) {
      if (partitioner.joinable()) partitioner.join();
      cfg.ConnectAll();
      std::this_thread::sleep_for(std::chrono::milliseconds(kElectionTimeoutMs));
    }

    if (crash) {
      for (int i = 0; i < nservers; i++) cfg.ShutdownServer(i);
      std::this_thread::sleep_for(std::chrono::milliseconds(kElectionTimeoutMs));
      for (int i = 0; i < nservers; i++) cfg.StartServer(i);
      cfg.ConnectAll();
    }

    for (auto& t : threads) t.join();

    if (maxraftstate > 0) {
      int sz = cfg.LogSize();
      if (sz > 8 * maxraftstate) {
        char buf[256];
        std::snprintf(buf, sizeof(buf),
                      "logs were not trimmed (%d > 8*%d)", sz, maxraftstate);
        Fatal(buf);
      }
    }
    if (maxraftstate < 0) {
      int ssz = cfg.SnapshotSize();
      if (ssz > 0) {
        Fatal("snapshot should not be used when maxraftstate = -1");
      }
    }
  }

  cfg.End();  // 触发 CheckLinearizability → porcupine 判定
  cfg.Cleanup();
}

// 核心 3B 用例：follower 落后太多 → 必须靠 InstallSnapshot RPC 才能追上。
void TestSnapshotRPC3B() {
  const int nservers = 3;
  const int maxraftstate = 1000;
  Config cfg(nservers, false, maxraftstate);
  auto ck = cfg.MakeClient(cfg.All());

  cfg.Begin("Test: InstallSnapshot RPC (3B)");

  DoPut(&cfg, ck.get(), "a", "A");
  DoCheck(&cfg, ck.get(), "a", "A");

  // 把 2 号隔离，让 {0,1} 疯狂写 → 日志被反复压缩
  cfg.Partition({0, 1}, {2});
  {
    auto ck1 = cfg.MakeClient({0, 1});
    for (int i = 0; i < 50; i++) {
      DoPut(&cfg, ck1.get(), std::to_string(i), std::to_string(i));
    }
    // Go 版这里是 time.Sleep(electionTimeout) = 1s（test_test.go:639），
    // 原来 C++ 版写成 500ms —— 短于一个选举超时。这条路径上它只影响"多数派
    // 有多少时间把日志压缩掉"，正常情况不会改变结果；但和 GenericTest 里
    // 那两处 500ms 属于同一类偏差，一并对齐成 1000ms。
    std::this_thread::sleep_for(std::chrono::milliseconds(kElectionTimeoutMs));
    DoPut(&cfg, ck1.get(), "b", "B");
  }

  // 多数派应该已经把日志截断得差不多了
  int sz = cfg.LogSize();
  if (sz > 8 * maxraftstate) {
    char buf[256];
    std::snprintf(buf, sizeof(buf), "logs were not trimmed (%d > 8*%d)", sz,
                  maxraftstate);
    Fatal(buf);
  }

  // 换一个必须拉上落后节点 2 才能提交的分组：2 号只能靠快照追上来
  cfg.Partition({0, 2}, {1});
  {
    auto ck1 = cfg.MakeClient({0, 2});
    DoPut(&cfg, ck1.get(), "c", "C");
    DoPut(&cfg, ck1.get(), "d", "D");
    DoCheck(&cfg, ck1.get(), "a", "A");
    DoCheck(&cfg, ck1.get(), "b", "B");
    DoCheck(&cfg, ck1.get(), "1", "1");
    DoCheck(&cfg, ck1.get(), "49", "49");
  }

  // 全网恢复
  cfg.Partition({0, 1, 2}, {});
  DoPut(&cfg, ck.get(), "e", "E");
  DoCheck(&cfg, ck.get(), "c", "C");
  DoCheck(&cfg, ck.get(), "e", "E");
  DoCheck(&cfg, ck.get(), "1", "1");

  cfg.End();
  cfg.Cleanup();
}

// 快照不能太大：我们只存了几个 key，500 字节是很宽松的上限。
void TestSnapshotSize3B() {
  const int nservers = 3;
  const int maxraftstate = 1000;
  const int maxsnapshotstate = 500;
  Config cfg(nservers, false, maxraftstate);
  auto ck = cfg.MakeClient(cfg.All());

  cfg.Begin("Test: snapshot size is reasonable (3B)");

  for (int i = 0; i < 200; i++) {
    DoPut(&cfg, ck.get(), "x", "0");
    DoCheck(&cfg, ck.get(), "x", "0");
    DoPut(&cfg, ck.get(), "x", "1");
    DoCheck(&cfg, ck.get(), "x", "1");
  }

  int sz = cfg.LogSize();
  if (sz > 8 * maxraftstate) {
    char buf[256];
    std::snprintf(buf, sizeof(buf), "logs were not trimmed (%d > 8*%d)", sz,
                  maxraftstate);
    Fatal(buf);
  }

  int ssz = cfg.SnapshotSize();
  if (ssz > maxsnapshotstate) {
    char buf[256];
    std::snprintf(buf, sizeof(buf), "snapshot too large (%d > %d)", ssz,
                  maxsnapshotstate);
    Fatal(buf);
  }

  cfg.End();
  cfg.Cleanup();
}

// ===========================================================================
// 测试注册表（照抄 raft_test 的范式）
// ===========================================================================

struct TestEntry {
  const char* name;
  void (*fn)();
};

const TestEntry kTests[] = {
    // ---- 3A ----
    {"TestBasic3A", TestBasic3A},
    {"TestConcurrent3A", TestConcurrent3A},
    {"TestUnreliable3A", TestUnreliable3A},
    {"TestUnreliableOneKey3A", TestUnreliableOneKey3A},
    {"TestOnePartition3A", TestOnePartition3A},
    // ---- 3A 分区但不重启（对应 Go 的 TestManyPartitions*3A）----
    {"TestManyPartitionsOneClient3A", TestManyPartitionsOneClient3A},
    {"TestManyPartitionsManyClients3A", TestManyPartitionsManyClients3A},
    // ---- 3A 持久化专项（对应 Go 的 TestPersist*3A，专门打"重启后状态机还在"）----
    {"TestPersistOneClient3A",
     [] { GenericTest("3A", 1, false, true, false, -1); }},
    {"TestPersistConcurrent3A",
     [] { GenericTest("3A", 5, false, true, false, -1); }},
    {"TestPersistConcurrentUnreliable3A",
     [] { GenericTest("3A", 5, true, true, false, -1); }},
    {"TestPersistPartition3A",
     [] { GenericTest("3A", 5, false, true, true, -1); }},
    {"TestPersistPartitionUnreliable3A",
     [] { GenericTest("3A", 5, true, true, true, -1); }},
    // ---- 3A 线性一致性：用 porcupine 验证 op 历史能排成线性化序列 ----
    // 对应 Go 的 TestPersistPartitionUnreliableLinearizable3A。
    // 这是 Lab 3 最强的不变式："所有副本一致"之外还要"客户端视角线性一致"。
    //
    // 【与 GenericTest 区别】这里调的是 GenericTestLinearizability：
    //   * 客户端循环里【随机选 key】（key := rand.Int() % nclients）—— 跨 client
    //     互相写同一个 key，构造最强的并发交错。
    //   * 不做 client 端 self-check（client 不固定 key），最后由 porcupine 判定。
    //   * 规模：15 clients / 7 servers（与 Go 版完全一致）。
    {"TestPersistPartitionUnreliableLinearizable3A",
     [] { GenericTestLinearizability("3A", /*nclients=*/15, /*nservers=*/7,
                                     /*unreliable=*/true, /*crash=*/true,
                                     /*partitions=*/true, /*maxraftstate=*/-1); }},
    // ---- 3B ----
    {"TestSnapshotRPC3B", TestSnapshotRPC3B},
    {"TestSnapshotSize3B", TestSnapshotSize3B},
    {"TestSnapshotRecover3B",
     [] { GenericTest("3B", 1, false, true, false, 1000); }},
    {"TestSnapshotRecoverManyClients3B",
     [] { GenericTest("3B", 20, false, true, false, 1000); }},
    {"TestSnapshotUnreliable3B",
     [] { GenericTest("3B", 5, true, false, false, 1000); }},
    {"TestSnapshotUnreliableRecover3B",
     [] { GenericTest("3B", 5, true, true, false, 1000); }},
    {"TestSnapshotUnreliableRecoverConcurrentPartition3B",
     [] { GenericTest("3B", 5, true, true, true, 1000); }},
    // 3B 线性一致性版：对应 Go 的 TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B。
    // 【快照 + 不稳定 leader】叠加，构造更复杂的并发交错：
    //   15 clients / 7 servers / unreliable / crash / partitions / maxraftstate=1000。
    {"TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B",
     [] { GenericTestLinearizability("3B", /*nclients=*/15, /*nservers=*/7,
                                     /*unreliable=*/true, /*crash=*/true,
                                     /*partitions=*/true, /*maxraftstate=*/1000); }},

    // ---- 🐞 调试用最小复现入口（KV_MINI_LIN=1 时把 outer iter / sleep 拉短，
    //         让 dump 出的 op history 控制在 ~30 条以内，方便手算锁定违法 op 对）。
    // ---- 平时不会跑出 mini 行为，不影响上面 3A / 3B 正式用例。
    {"kv_mini_lin_dup",
     [] {
       if (!MiniLinEnabled()) {
         std::printf("  (skip) kv_mini_lin_dup：设 KV_MINI_LIN=1 才跑"
                     "（迷你版，history 小，方便手算定位非法 op）\n");
         std::fflush(stdout);
         return;
       }
       GenericTestLinearizability("3A", /*nclients=*/2, /*nservers=*/3,
                                  /*unreliable=*/true, /*crash=*/true,
                                  /*partitions=*/true, /*maxraftstate=*/-1);
     }},
};

void Usage(const char* argv0) {
  std::printf("\n用法: %s [过滤词] [-count N]\n\n", argv0);
  std::printf("  %s                跑全部用例\n", argv0);
  std::printf("  %s 3A             只跑名字里含 3A 的用例\n", argv0);
  std::printf("  %s 3B -count 3    3B 跑 3 遍（抓偶发 bug 的必备姿势）\n\n",
              argv0);
  std::printf("可选用例:\n");
  for (const auto& t : kTests) std::printf("  %s\n", t.name);
  std::printf("\n");
}

}  // namespace
}  // namespace kvraft

int main(int argc, char** argv) {
  using namespace kvraft;

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

  std::printf("kv_test：已注册 %zu 个用例\n", num_tests);
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
      if (!filter.empty() &&
          std::string(t.name).find(filter) == std::string::npos) {
        continue;
      }
      g_failed.store(false);
      {
        std::lock_guard<std::mutex> lk(g_fail_mu);
        g_fail_msg.clear();
      }

      // 用例开始前先记下崩溃计数，跑完取差值 —— Raft 的后台线程崩了
      // 但断言全过时，靠这个才能判失败（否则"崩溃伪装成通过"）。
      const int crashes_before = raftcpp::ThreadTracker::Instance().Crashes();

      // 看门狗：卡死超过 TestTimeoutMs() 就 abort（对齐 go test 的 10min 超时）
      raftcpp::Watchdog wd(t.name, raftcpp::TestTimeoutMs());
      const auto t0 = raftcpp::Now();

      bool bad = false;
      std::string reason;
      try {
        t.fn();
      } catch (const TestFailure& f) {
        bad = true;
        reason = f.msg;
      } catch (const std::exception& e) {
        // 这一层是原来【完全没有】的。之前任何没走 Fatal() 的异常都会
        // 逃出 main → std::terminate() → 进程直接没：不打印汇总、
        // 后面的用例根本没跑、失败数被系统性少报，现场只剩一行
        // "terminate called after throwing..."，连哪个用例都不知道。
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

      if (!bad && g_failed.load()) {
        bad = true;
        std::lock_guard<std::mutex> lk(g_fail_mu);
        reason = g_fail_msg;
      }
      if (!bad && crashed > 0) {
        bad = true;
        char buf[256];
        std::snprintf(buf, sizeof(buf), "%d 个后台线程抛异常崩溃（%s）", crashed,
                      raftcpp::ThreadTracker::Instance().LastCrash().c_str());
        reason = buf;
      }

      if (bad) {
        failed++;
        std::printf("  >>> FAILED: %s -- %s  (%lld ms)\n", t.name,
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
