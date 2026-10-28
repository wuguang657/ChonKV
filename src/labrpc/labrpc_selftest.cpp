// labrpc_selftest.cpp —— 网络层自测
//
// 【这是教你写 C++ 测试的最小范例，建议先读这个文件】
//
// 写测试就三步，没有任何魔法：
//
//   1. 造输入（这里：搭一个网络、一个服务端）
//   2. 跑你要测的东西（这里：发 RPC）
//   3. 断言结果符合预期（这里：CHECK 宏）
//
// 断言就是 "if (条件不成立) { 报错; 记一笔失败; }"。
// Go 有 t.Fatalf，C++ 标准库没有测试框架，所以我们自己写一个宏，20 行搞定。

#include <arpa/inet.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "labrpc.h"
#include "transport_factory.h"  // MakeTransport：按 LABRPC_TRANSPORT 选后端（inproc/net）
#include "../common/util.h"

// ------------------------ 一个 20 行的迷你测试框架 ------------------------
static int g_checks = 0;
static int g_failures = 0;

#define CHECK(cond)                                                       \
  do {                                                                    \
    g_checks++;                                                           \
    if (!(cond)) {                                                        \
      g_failures++;                                                       \
      std::printf("  FAIL %s:%d  %s\n", __FILE__, __LINE__, #cond);        \
    }                                                                     \
  } while (0)

#define CHECK_EQ(a, b)                                                    \
  do {                                                                    \
    g_checks++;                                                           \
    if (!((a) == (b))) {                                                  \
      g_failures++;                                                       \
      std::printf("  FAIL %s:%d  %s == %s\n", __FILE__, __LINE__, #a, #b); \
    }                                                                     \
  } while (0)
// -------------------------------------------------------------------------

// 一个很笨的"服务"：把请求原样返回，外加一个前缀
static std::shared_ptr<labrpc::Service> MakeEchoService() {
  using labrpc::Service;
  return std::make_shared<Service>(
      "Echo", std::unordered_map<std::string, Service::Handler>{
                  {"Upper", [](const std::string& args) -> std::string {
                     std::string s = args;
                     for (char& c : s) {
                       if (c >= 'a' && c <= 'z') c = c - 'a' + 'A';
                     }
                     return s;
                   }}});
}

static std::shared_ptr<labrpc::Server> MakeEchoServer() {
  auto srv = std::make_shared<labrpc::Server>();
  srv->AddService(MakeEchoService());
  return srv;
}

// 带身份标记的 echo 服务：同一方法名、不同 server 返回不同前缀。
// 用来验证"这条 RPC 到底发给了哪台 server"——内容不同即目标不同。
static std::shared_ptr<labrpc::Server> MakeTaggedEchoServer(
    const std::string& tag) {
  using labrpc::Service;
  auto srv = std::make_shared<labrpc::Server>();
  srv->AddService(std::make_shared<Service>(
      "Echo", std::unordered_map<std::string, Service::Handler>{
                  {"Upper", [tag](const std::string& args) -> std::string {
                     std::string s = args;
                     for (char& c : s) {
                       if (c >= 'a' && c <= 'z') c = c - 'a' + 'A';
                     }
                     return tag + s;
                   }}}));
  return srv;
}

// 大回包 echo：请求原样返回，但回包被撑到 kBigReplySize 字节。
// 用来验证"请求很小、回包很大"时，SetMaxMessageBytes 的语义边界。
static constexpr size_t kBigReplySize = 8 * 1024;  // 8KiB

static std::shared_ptr<labrpc::Server> MakeBigReplyServer() {
  using labrpc::Service;
  auto srv = std::make_shared<labrpc::Server>();
  srv->AddService(std::make_shared<Service>(
      "Echo", std::unordered_map<std::string, Service::Handler>{
                  {"Big", [](const std::string&) -> std::string {
                     return std::string(kBigReplySize, 'Z');
                   }}}));
  return srv;
}

// ---------------------------------------------------------------------------
// 用例 1：最基本的收发
// ---------------------------------------------------------------------------
static void TestBasicCall() {
  std::printf("TestBasicCall ...\n");
  auto net = labrpc::MakeTransport();
  net->AddServer(0, MakeEchoServer());
  auto end = net->MakeEnd("c0");
  net->Connect("c0", 0);
  net->Enable("c0", true);

  std::string reply;
  bool ok = end->Call("Echo.Upper", "hello", reply);
  CHECK(ok);
  CHECK_EQ(reply, std::string("HELLO"));

  net->Cleanup();
}

// ---------------------------------------------------------------------------
// 用例 2：端点被禁用 → Call 返回 false
// ---------------------------------------------------------------------------
static void TestDisabled() {
  std::printf("TestDisabled ...\n");
  auto net = labrpc::MakeTransport();
  net->LongDelays(false);  // 断连已是立即失败；这行保留只为对齐"关掉长延迟"的旧写法
  net->AddServer(0, MakeEchoServer());
  auto end = net->MakeEnd("c0");
  net->Connect("c0", 0);
  net->Enable("c0", false);  // 没开

  std::string reply;
  bool ok = end->Call("Echo.Upper", "hello", reply);
  CHECK(!ok);  // 应该失败

  net->Cleanup();
}

// ---------------------------------------------------------------------------
// 用例 3：服务器被删掉 → 已经发出去的请求也要失败（不能返回"成功"）
// ---------------------------------------------------------------------------
static void TestDeleteServer() {
  std::printf("TestDeleteServer ...\n");
  auto net = labrpc::MakeTransport();
  net->LongDelays(false);
  net->AddServer(0, MakeEchoServer());
  auto end = net->MakeEnd("c0");
  net->Connect("c0", 0);
  net->Enable("c0", true);

  net->DeleteServer(0);
  std::string reply;
  bool ok = end->Call("Echo.Upper", "hello", reply);
  CHECK(!ok);

  net->Cleanup();
}

// ---------------------------------------------------------------------------
// 用例 4：不可靠模式确实会丢包（但不该全丢）
// ---------------------------------------------------------------------------
static void TestUnreliable() {
  std::printf("TestUnreliable ...\n");
  auto net = labrpc::MakeTransport();
  net->Reliable(false);
  net->AddServer(0, MakeEchoServer());
  auto end = net->MakeEnd("c0");
  net->Connect("c0", 0);
  net->Enable("c0", true);

  int success = 0;
  const int kTotal = 200;
  for (int i = 0; i < kTotal; i++) {
    std::string reply;
    if (end->Call("Echo.Upper", "abc", reply)) success++;
    if ((i + 1) % 20 == 0) {
      std::printf("    ... %d done\n", i + 1);
      std::fflush(stdout);
    }
  }
  std::printf("  %d/%d 成功\n", success, kTotal);
  // 每条 RPC 丢请求 10% + 丢回复 10%，成功率应该在 80% 上下
  CHECK(success > kTotal * 50 / 100);   // 不至于全丢
  CHECK(success < kTotal * 98 / 100);   // 也不可能一次都不丢

  net->Cleanup();
}

// ---------------------------------------------------------------------------
// 用例 5：字节统计和 RPC 计数（TestRPCBytes2B 靠这个）
// ---------------------------------------------------------------------------
static void TestCounters() {
  std::printf("TestCounters ...\n");
  auto net = labrpc::MakeTransport();
  net->AddServer(0, MakeEchoServer());
  auto end = net->MakeEnd("c0");
  net->Connect("c0", 0);
  net->Enable("c0", true);

  CHECK_EQ(net->GetTotalCount(), 0);
  CHECK_EQ(net->GetTotalBytes(), 0);

  std::string payload(5000, 'x');
  std::string reply;
  CHECK(end->Call("Echo.Upper", payload, reply));

  CHECK_EQ(net->GetTotalCount(), 1);
  // 请求 5000 字节 + 回复 5000 字节
  CHECK_EQ(net->GetTotalBytes(), 10000);
  CHECK_EQ(net->GetCount(0), 1);  // 0 号服务器收到 1 次

  net->Cleanup();
}

// ---------------------------------------------------------------------------
// 用例 6：并发调用同一个 ClientEnd（Go 版明确要求支持）
// ---------------------------------------------------------------------------
static void TestConcurrentCalls() {
  std::printf("TestConcurrentCalls ...\n");
  auto net = labrpc::MakeTransport();
  net->AddServer(0, MakeEchoServer());
  auto end = net->MakeEnd("c0");
  net->Connect("c0", 0);
  net->Enable("c0", true);

  std::atomic<int> ok_count{0};
  std::vector<std::thread> threads;
  for (int i = 0; i < 20; i++) {
    threads.emplace_back([&]() {
      std::string reply;
      if (end->Call("Echo.Upper", "concurrent", reply)) {
        if (reply == "CONCURRENT") ok_count.fetch_add(1);
      }
    });
  }
  for (auto& t : threads) t.join();
  CHECK_EQ(ok_count.load(), 20);

  net->Cleanup();
}

// 一个会"睡眠"的服务，用于测 DeleteServer 唤醒在途 RPC（对应 Go TestKilled）
static std::shared_ptr<labrpc::Server> MakeSleepServer() {
  using labrpc::Service;
  auto srv = std::make_shared<labrpc::Server>();
  srv->AddService(std::make_shared<Service>(
      "Sleep", std::unordered_map<std::string, Service::Handler>{
                  {"Snooze", [](const std::string& args) -> std::string {
                     std::this_thread::sleep_for(std::chrono::milliseconds(200));
                     return args;
                   }}}));
  return srv;
}

// ---------------------------------------------------------------------------
// 用例 7：多 ClientEnd 并发（对应 Go TestConcurrentMany：20 端 × 10 RPC）
// ---------------------------------------------------------------------------
static void TestConcurrentMany() {
  std::printf("TestConcurrentMany ...\n");
  auto net = labrpc::MakeTransport();
  net->AddServer(0, MakeEchoServer());
  const int nclients = 20, nrpcs = 10;
  std::atomic<int> ok_count{0};
  std::vector<std::thread> threads;
  for (int i = 0; i < nclients; i++) {
    threads.emplace_back([&, i]() {
      std::string name = "c" + std::to_string(i);
      auto e = net->MakeEnd(name);
      net->Connect(name, 0);
      net->Enable(name, true);
      for (int j = 0; j < nrpcs; j++) {
        std::string reply;
        if (e->Call("Echo.Upper", "concurrent", reply) && reply == "CONCURRENT")
          ok_count.fetch_add(1);
      }
    });
  }
  for (auto& t : threads) t.join();
  CHECK_EQ(ok_count.load(), nclients * nrpcs);
  CHECK_EQ(net->GetCount(0), nclients * nrpcs);
  net->Cleanup();
}

// ---------------------------------------------------------------------------
// 用例 8：DeleteServer 必须唤醒"在途"的 RPC（对应 Go TestKilled）
// 注意：C++ labrpc 不在 handler 睡眠中途打断，而是 handler 返回后由
// IsServerDead 作废结果 → Call 最终返回 false（不会永远卡住）。
// 这与 Go 的"中途打断"实现不同，但对外语义一致：删服务器 → 在途 RPC 失败。
// ---------------------------------------------------------------------------
static void TestKilled() {
  std::printf("TestKilled ...\n");
  auto net = labrpc::MakeTransport();
  net->AddServer(0, MakeSleepServer());
  auto end = net->MakeEnd("c0");
  net->Connect("c0", 0);
  net->Enable("c0", true);

  std::atomic<bool> ok{true};
  std::thread t([&]() {
    std::string reply;
    ok.store(end->Call("Sleep.Snooze", "x", reply));
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  net->DeleteServer(0);
  t.join();
  CHECK(!ok.load());  // 在途 RPC 必须返回 false，不能永远卡住
  net->Cleanup();
}

// ---------------------------------------------------------------------------
// 用例 9：禁用期间的 RPC 失败；重新启用后新 RPC 立即成功（对应 Go TestRegression1）
// 注意：当前两个后端在 enabled=false 时都【不派发】（SendReq 直接走失败路径，
// 见 inproc/network.cpp / socket/socket_transport.cpp 的 !enabled 分支），因此禁用期的
// RPC 不会累加 GetCount（本用例不断言计数）；对外语义"禁用即不可达"。
// （旧注释称"仍会执行 handler、计数会是 21"是早期实现的描述，已修正。）
// ---------------------------------------------------------------------------
static void TestRegression1() {
  std::printf("TestRegression1 ...\n");
  auto net = labrpc::MakeTransport();
  net->AddServer(0, MakeEchoServer());
  auto end = net->MakeEnd("c");
  net->Connect("c", 0);

  net->Enable("c", false);
  std::atomic<int> failed{0};
  std::vector<std::thread> threads;
  for (int i = 0; i < 20; i++) {
    threads.emplace_back([&]() {
      std::string reply;
      if (!end->Call("Echo.Upper", "x", reply)) failed.fetch_add(1);
    });
  }
  // 【必须 join 之后再 Enable(true)】：旧写法是主线程 sleep(100ms) 就重新启用，
  // 而断言 failed==20 隐含了"20 个线程都必须在 100ms 内进入 Call"。机器被压满
  // （比如四路并发跑全量）时线程可能还没起跑就被重新启用 → 那些 Call 走成功
  // 路径 → failed<20 → 假失败。变异实证：给线程体加 150ms 启动延迟，本用例
  // 立刻报 `failed == 20` 不成立。
  // 那个 100ms 窗口本身也没测到任何东西：两个后端在 !enabled 时都是【立即】
  // 返回失败（delay=0），不存在"重新启用那一刻还有一批禁用期请求在途"。
  for (auto& th : threads) th.join();
  CHECK_EQ(failed.load(), 20);  // 确定性：整批 Call 都落在禁用窗口内

  net->Enable("c", true);
  auto t0 = std::chrono::steady_clock::now();
  std::string reply;
  CHECK(end->Call("Echo.Upper", "fast", reply));
  auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::steady_clock::now() - t0).count();
  CHECK_EQ(reply, std::string("FAST"));
  CHECK(dur < 200);            // 重新启用后不应被之前的禁用 RPC 拖慢
  net->Cleanup();
}

// ---------------------------------------------------------------------------
// 用例 10：Connect 改指向另一台 server 后，后续 RPC 必须发到【新目标】
// 防的是一处真缺陷：socket 后端把 TCP 连接缓存在 conns_[endname] 里，
// 复用判据只有 "fd >= 0"，【没有】比对 "这条连接当初连的是不是同一台 server"。
// 于是 net.Connect("c0", 1) 之后，RPC 仍然沿旧连接发到 server 0（串台）。
// in-proc 后端每次 SendReq 都重读 connections_，天然正确 —— 两个后端语义不一致。
// 断言：两台 server 用同一方法名、不同 reply 前缀，内容不同即证明走错了目标。
// ---------------------------------------------------------------------------
static void TestRetargetServer() {
  std::printf("TestRetargetServer ...\n");
  auto net = labrpc::MakeTransport();
  net->AddServer(0, MakeTaggedEchoServer("s0:"));
  net->AddServer(1, MakeTaggedEchoServer("s1:"));
  auto end = net->MakeEnd("c0");
  net->Connect("c0", 0);
  net->Enable("c0", true);

  std::string reply;
  CHECK(end->Call("Echo.Upper", "abc", reply));
  CHECK_EQ(reply, std::string("s0:ABC"));   // 先连 server 0，确认基线

  net->Connect("c0", 1);                    // 同一 endname 改指向 server 1
  reply.clear();
  CHECK(end->Call("Echo.Upper", "abc", reply));
  CHECK_EQ(reply, std::string("s1:ABC"));   // 必须走新目标（旧连接串台 → 得到 s0:ABC）

  net->Cleanup();
}

// ---------------------------------------------------------------------------
// 用例 11：同名 endname 重建端点 → 必须回到"禁用 + 未连接"（与 in-proc 对齐）
// in-proc Network::MakeEnd 会 `enabled_[name]=false; connections_.erase(name)`，
// 而 socket 后端原先什么都不做：重建出来的端点"没 Connect 就能发 RPC"，
// 同一份测试代码在两个后端表现不同。断言：重建后第一次 Call 必须失败。
// ---------------------------------------------------------------------------
static void TestMakeEndResets() {
  std::printf("TestMakeEndResets ...\n");
  auto net = labrpc::MakeTransport();
  net->AddServer(0, MakeEchoServer());

  auto e1 = net->MakeEnd("c0");
  net->Connect("c0", 0);
  net->Enable("c0", true);
  std::string reply;
  CHECK(e1->Call("Echo.Upper", "abc", reply));  // 先确认这条链路是通的
  CHECK_EQ(reply, std::string("ABC"));

  auto e2 = net->MakeEnd("c0");  // 同名重建：新端点默认未启用/未连接
  reply.clear();
  CHECK(!e2->Call("Echo.Upper", "abc", reply));

  net->Cleanup();
}

// ---------------------------------------------------------------------------
// 用例 12：Cleanup 之后 AddServer 必须是【空操作】（停机后的网络不接受新工作）
// 防的是实测复现过的一个洞：Cleanup 幂等（stopped_.exchange 早退），所以
//     net1.Cleanup(); net1.AddServer(0, ...);
// 会在 AddServer 里重新 bind+listen 并起一个 acceptor，而 Cleanup 早已跑完、
// 不会再来 join 它 —— listen fd + acceptor 线程永久滞留。后果不是"多一个空转
// 线程"这么轻：那个 socket 一直占着 base_port+0，同进程里随后新建的网络
// （测试里每个 Config 都用同一个 base_port）bind 直接 EADDRINUSE，打印出来
// 还误导成"端口被别的进程占用"；server 起不来 → 所有 RPC 发黑洞 → 用例卡死。
// 断言不必探端口：在【同一个 base_port】上再建一张网络，它的 RPC 必须通。
// ---------------------------------------------------------------------------
static void TestAddServerAfterCleanup() {
  std::printf("TestAddServerAfterCleanup ...\n");
  {
    auto net1 = labrpc::MakeTransport();
    net1->AddServer(0, MakeEchoServer());
    net1->Cleanup();
    net1->AddServer(0, MakeEchoServer());  // 停机后误用：必须是空操作
  }  // net1 析构：若上面泄漏了监听 socket，端口会一直占着

  auto net2 = labrpc::MakeTransport();  // 同 base_port（端口被占则 bind 失败）
  net2->AddServer(0, MakeEchoServer());
  auto end = net2->MakeEnd("c0");
  net2->Connect("c0", 0);
  net2->Enable("c0", true);
  std::string reply;
  CHECK(end->Call("Echo.Upper", "abc", reply));  // 旧实现：server 起不来 → false
  CHECK_EQ(reply, std::string("ABC"));

  net2->Cleanup();
}

// ---------------------------------------------------------------------------
// 用例 13：LongReordering —— 回包可以被延迟，但不能队头阻塞
// 防的正是已修掉的真 bug：旧实现在 handler 线程里 sleep 200~2200ms，
// 同一条 TCP 连接上的后续请求全部被串行堵死（net 模式
// TestFigure8Unreliable2C 稳定失败，38.7s 达不成一致）。修复后延迟帧
// 由独立 sender 线程发送，处理循环不占线程。
//
// 两个断言：
//   ① 正确性：2/3 概率被延迟 200~2200ms，但最终全部到达、内容正确；
//   ② 无队头阻塞：并发在途请求的完成顺序必须出现过乱序——
//      旧实现单 handler 串行处理，完成顺序恒等于发送顺序（乱序恒为 0）；
//      修复后每条帧独立延迟（iid），保序概率约 1/kN!，乱序必然出现。
// ---------------------------------------------------------------------------
static void TestLongReordering() {
  std::printf("TestLongReordering ...\n");
  auto net = labrpc::MakeTransport();
  net->LongReordering(true);
  net->AddServer(0, MakeEchoServer());
  auto end = net->MakeEnd("c0");
  net->Connect("c0", 0);
  net->Enable("c0", true);

  const int kN = 20;
  std::mutex mu;
  std::condition_variable cv;
  std::vector<int> order;  // 完成顺序（只记成功且内容正确的请求序号）
  int done = 0;
  for (int i = 0; i < kN; i++) {
    end->CallAsync(
        "Echo.Upper", "abc",
        [i, &mu, &cv, &order, &done](bool ok, const std::string& reply) {
          {
            std::lock_guard<std::mutex> lk(mu);
            if (ok && reply == "ABC") order.push_back(i);
            done++;
          }
          cv.notify_one();
        });
  }
  {
    std::unique_lock<std::mutex> lk(mu);
    // 最坏每条都被延迟到上限 2200ms；30s 上限纯兜底防挂死
    cv.wait_for(lk, std::chrono::seconds(30), [&] { return done == kN; });
  }
  CHECK_EQ(done, kN);                // 全部完成（没被延迟堵死）
  CHECK_EQ((int)order.size(), kN);   // 全部成功且内容正确
  bool inverted = false;
  for (size_t i = 1; i < order.size(); i++) {
    if (order[i] < order[i - 1]) inverted = true;  // 后完成的序号更小 → 乱序
  }
  CHECK(inverted);  // 独立延迟 → 乱序必然出现；旧 bug 下恒为 false

  net->Cleanup();
}

// ---------------------------------------------------------------------------
// 用例 14：SetMaxMessageBytes(0) = 不限制——大帧必须能通过
// 防的是 B3 接收侧闸门的边界 bug：0 值若被无脑 +slack，承诺的"不限制"
// （SetMaxMessageBytes 的契约：0 = 不限制）会变成 1KiB 硬闸，合法大帧被误杀 → 客户端疯狂重连。
// 256KiB 远超 kFrameLenSlack(1KiB)，正是误杀区的靶心。
// ---------------------------------------------------------------------------
static void TestMaxMessageBytesZero() {
  std::printf("TestMaxMessageBytesZero ...\n");
  auto net = labrpc::MakeTransport();
  net->AddServer(0, MakeEchoServer());
  auto end = net->MakeEnd("c0");
  net->Connect("c0", 0);
  net->Enable("c0", true);
  net->SetMaxMessageBytes(0);  // 不限制

  const size_t kBig = 256 * 1024;  // 256KiB
  std::string big(kBig, 'x');
  std::string reply;
  bool ok = end->Call("Echo.Upper", big, reply);
  CHECK(ok);                    // 不限制 → 必须成功（误杀版这里直接挂）
  CHECK_EQ(reply.size(), kBig);  // 内容完整
  CHECK_EQ(reply, std::string(kBig, 'X'));

  net->Cleanup();
}

// ---------------------------------------------------------------------------
// 用例 15：异步调用的回调【绝不内联】跑在调用方线程上
// 防的陷阱：socket 后端早先在 SendReq 的快速失败路径（断连/丢请求/无连接）
// 里【直接】在调用方线程上调 cb——调用方可能是持锁的 Raft 线程。当前所有
// Raft 回调恰好都在 !ok 时立即返回且调用点都在锁外，内联暂未致命；但
// "cb 跑在持锁的调用方线程上"是将来谁加个会阻塞/会重入的回包处理就死锁
// 的陷阱，且与 in-proc（Finish 里 Post 到线程池）语义不一致。
// 断言：快速失败场景下，cb 必须最终收到 ok=false，且执行线程 != 调用方线程。
// ---------------------------------------------------------------------------
static void TestAsyncFastFailNotInline() {
  std::printf("TestAsyncFastFailNotInline ...\n");
  auto net = labrpc::MakeTransport();
  net->LongDelays(false);   // 关掉 0~7000ms，快速失败立即发生
  net->AddServer(0, MakeEchoServer());
  auto end = net->MakeEnd("c0");
  net->Connect("c0", 0);
  net->Enable("c0", false);  // 没开 → SendReq 必走快速失败路径

  std::mutex mu;
  std::condition_variable cv;
  bool done = false;
  bool ok_seen = true;
  std::thread::id caller_tid;
  std::thread::id cb_tid;
  // 注意：CallAsync 期间【不持 mu】。旧（内联）实现下 cb 会在 CallAsync 返回前
  // 于本线程执行；若这里持着 mu，cb 抢同一把非递归锁会自死锁（挂死而非失败），
  // 那样就不是一个好用的回归用例了。不持锁时旧实现表现为 cb_tid == caller_tid，
  // 被下面第三条断言干净抓住。
  end->CallAsync("Echo.Upper", "hello",
                 [&mu, &cv, &done, &ok_seen, &cb_tid](bool ok,
                                                      const std::string&) {
                   {
                     std::lock_guard<std::mutex> lk2(mu);
                     cb_tid = std::this_thread::get_id();
                     ok_seen = ok;
                     done = true;
                   }
                   cv.notify_all();
                 });
  caller_tid = std::this_thread::get_id();
  {
    std::unique_lock<std::mutex> lk(mu);
    // cb 由独立投递线程执行，几毫秒内必到；5s 上限纯兜底防挂死
    cv.wait_for(lk, std::chrono::seconds(5), [&] { return done; });
  }
  CHECK(done);                    // cb 必须被调用（旧实现也满足，防"cb 丢失"回归）
  CHECK(!ok_seen);                // 断连 → ok=false
  CHECK(cb_tid != caller_tid);    // 绝不内联在调用方线程（内联实现这里恒等 → 挂）

  net->Cleanup();
}

// ---------------------------------------------------------------------------
// 用例 16：SetMaxMessageBytes 只约束【请求】——超过该值的【回包】不得被掐断
//
// 契约（labrpc.h 的 C3 常量块 / kMaxRpcMessageBytesDefault）：这道闸门是"限制【单条 RPC 请求】序列化后的字节数"，
// 目的是防止 InstallSnapshot 那种 GB 级【请求】把两端内存顶穿。它从来没有说过
// 回包也受这个上限约束 —— 而且现实里完全可能出现"请求很小、回包很大"：
// 一次 Get 只有几十字节的 key，回包却是 megabytes 级的 value。
//
// socket 后端却把同一个运行期上限也用在了【接收侧】：
//   RecvReplyFrame(..., RecvFrameLenCap(max_message_bytes_))  // 旧实现
// 于是把闸门调小（比如 1KiB）之后，一个合法的小请求 + 8KiB 回包会被
// RecvReplyFrame 判成"帧非法"→ ConnReader 视作连接断开 → 撕掉 TCP 连接 →
// 在途请求全判失败。而且重连后下一次调用仍然失败（同一个大回包）→ 该 RPC
// 在 socket 后端【永久不可用】。in-proc 后端没有接收侧闸门，照常成功
// → 两个后端语义分歧。
//
// 正解：传输层的接收侧闸门只负责"防错乱长度头触发超大分配"，其下限必须是
// 默认上限（64MiB），不能被运行期调小的 SetMaxMessageBytes 拖下去。
// ---------------------------------------------------------------------------
static void TestReplyLargerThanRequestCap() {
  std::printf("TestReplyLargerThanRequestCap ...\n");
  auto net = labrpc::MakeTransport();
  net->AddServer(0, MakeBigReplyServer());
  auto end = net->MakeEnd("c0");
  net->Connect("c0", 0);
  net->Enable("c0", true);
  net->SetMaxMessageBytes(1024);  // 请求上限 1KiB（回包不应受它约束）

  std::string reply;
  CHECK(end->Call("Echo.Big", "x", reply));  // 请求 1 字节，必须发得出去
  CHECK_EQ(reply.size(), kBigReplySize);     // 8KiB 回包必须完整回来
  // 连接若被撕裂，第二次调用仍会失败（每次重连都撞同一个大回包）→ 必须也能过
  std::string reply2;
  CHECK(end->Call("Echo.Big", "x", reply2));
  CHECK_EQ(reply2.size(), kBigReplySize);

  net->Cleanup();
}

// ---------------------------------------------------------------------------
// 用例 17：网线上来的非法方法名不得杀进程（远程输入 ≠ 编程错误）
//
// in-proc 后端的方法名永远来自【本方调用点】（编译期常量），所以
// Server::Dispatch 遇到未知 service/method 时 std::abort() 是合理的
// "编程错误断言"。但 socket 后端的方法名是【从 TCP 读出来的远程输入】：
//   * 这个后端的设计目标就是能跨进程/跨机，对端完全可能是别人；
//   * 极端情况下帧错位也会把方法名变成垃圾。
// 旧实现直接把线上字符串喂给 Dispatch → 任何人连上端口发一帧
// "Echo.NoSuchMethod"（38 字节）就能让整个进程 SIGABRT 退出（实测 134）。
//
// 本用例直接开一个【裸 TCP 客户端】发三种非法帧，然后：
//   ① 每种都必须收到 ok=false 的失败帧（而不是进程消失）；
//   ② 之后同一连接上再发一个【合法】请求必须成功 —— 证明失败帧没有
//      把连接/帧流搞坏（回失败帧后继续读下一帧，而不是 break）。
// in-proc 后端没有"线上输入"这一层，直接跳过。
// ---------------------------------------------------------------------------
static bool IsSocketBackend() {
  const char* e = std::getenv("LABRPC_TRANSPORT");
  std::string k = e ? e : "inproc";
  return k == "socket" || k == "tcp" || k == "net";
}

static void PutU32BE(std::string& b, uint32_t v) {
  char t[4] = {static_cast<char>((v >> 24) & 0xFF),
               static_cast<char>((v >> 16) & 0xFF),
               static_cast<char>((v >> 8) & 0xFF), static_cast<char>(v & 0xFF)};
  b.append(t, 4);
}
static void PutU64BE(std::string& b, uint64_t v) {
  char t[8];
  for (int i = 7; i >= 0; i--) {
    t[i] = static_cast<char>(v & 0xFF);
    v >>= 8;
  }
  b.append(t, 8);
}
static bool ReadAllFd(int fd, char* buf, size_t n) {
  size_t off = 0;
  while (off < n) {
    ssize_t r = ::read(fd, buf + off, n - off);
    if (r < 0 && errno == EINTR) continue;
    if (r <= 0) return false;
    off += static_cast<size_t>(r);
  }
  return true;
}

// 按 socket/socket_transport.cpp 的线上格式发一个请求帧
static bool RawSendRequest(int fd, uint64_t corr, const std::string& svc_meth,
                           const std::string& args) {
  std::string payload;
  PutU64BE(payload, corr);
  PutU32BE(payload, static_cast<uint32_t>(svc_meth.size()));
  payload += svc_meth;
  PutU32BE(payload, static_cast<uint32_t>(args.size()));
  payload += args;
  std::string frame;
  PutU32BE(frame, static_cast<uint32_t>(payload.size()));
  frame += payload;
  size_t off = 0;
  while (off < frame.size()) {
    ssize_t w = ::write(fd, frame.data() + off, frame.size() - off);
    if (w < 0 && errno == EINTR) continue;
    if (w <= 0) return false;
    off += static_cast<size_t>(w);
  }
  return true;
}

// 读一个回包帧；返回 false = 读不到/超时（调用方 CHECK 会报出来）
static bool RawRecvReply(int fd, uint64_t& corr, bool& ok, std::string& reply) {
  char lenbuf[4];
  if (!ReadAllFd(fd, lenbuf, 4)) return false;
  uint32_t len = (static_cast<uint8_t>(lenbuf[0]) << 24) |
                 (static_cast<uint8_t>(lenbuf[1]) << 16) |
                 (static_cast<uint8_t>(lenbuf[2]) << 8) |
                 static_cast<uint8_t>(lenbuf[3]);
  if (len < 13) return false;
  std::string buf(len, '\0');
  if (!ReadAllFd(fd, &buf[0], len)) return false;
  const unsigned char* p = reinterpret_cast<const unsigned char*>(buf.data());
  corr = 0;
  for (int i = 0; i < 8; i++) corr = (corr << 8) | p[i];
  ok = (p[8] != 0);
  uint32_t rlen = (static_cast<uint32_t>(p[9]) << 24) |
                  (static_cast<uint32_t>(p[10]) << 16) |
                  (static_cast<uint32_t>(p[11]) << 8) |
                  static_cast<uint32_t>(p[12]);
  if (13 + rlen > len) return false;
  reply.assign(reinterpret_cast<const char*>(p + 13), rlen);
  return true;
}

static void TestWireBadMethodDoesNotAbort() {
  std::printf("TestWireBadMethodDoesNotAbort ...\n");
  if (!IsSocketBackend()) {
    std::printf("  (in-proc 后端没有线上输入，跳过)\n");
    return;
  }
  auto net = labrpc::MakeTransport();
  net->AddServer(0, MakeEchoServer());

  const char* b = std::getenv("LABRPC_SOCKET_BASE");
  int port = (b && b[0]) ? std::atoi(b) : 19090;  // 与 SocketNetwork 默认一致
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  CHECK(fd >= 0);
  timeval tv{};
  tv.tv_sec = 2;  // 兜底：若实现既不回失败帧也不 abort，别把测试挂死
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  sockaddr_in sa{};
  sa.sin_family = AF_INET;
  sa.sin_port = htons(static_cast<uint16_t>(port));
  sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  CHECK(::connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) == 0);

  // 三种非法输入：未知方法（service 在）、未知 service、名字里没有 '.'
  struct Bad {
    uint64_t corr;
    const char* svc_meth;
  } bads[] = {{101, "Echo.NoSuchMethod"}, {102, "NoSuchSvc.M"}, {103, "NoDot"}};
  for (auto& bs : bads) {
    CHECK(RawSendRequest(fd, bs.corr, bs.svc_meth, "x"));
    uint64_t corr = 0;
    bool ok = true;
    std::string reply = "unset";
    bool got = RawRecvReply(fd, corr, ok, reply);
    CHECK(got);            // 必须收到回包（旧实现：进程直接 abort，这里读不到）
    CHECK_EQ(corr, bs.corr);
    CHECK(!ok);            // 且必须是失败帧
  }

  // 连接仍然可用 + 帧流没被搞坏：合法请求必须照常成功
  CHECK(RawSendRequest(fd, 104, "Echo.Upper", "abc"));
  uint64_t corr = 0;
  bool ok = false;
  std::string reply;
  CHECK(RawRecvReply(fd, corr, ok, reply));
  CHECK_EQ(corr, static_cast<uint64_t>(104));
  CHECK(ok);
  CHECK_EQ(reply, std::string("ABC"));

  ::close(fd);
  net->Cleanup();
}

// ---------------------------------------------------------------------------
// 用例 18：连接 churn 不得无限堆积 handler 线程资源
//
// socket 后端是"一条 TCP 连接一个 handler 线程"，线程句柄存进
// ServerState::handlers，早先只在 DeleteServer/Cleanup 时统一 join。
// 而 std::thread 在 join 之前一直持有【已退出线程的栈】和 callable 里的
// shared_ptr<SConn> —— 于是"长期存活的服务端 + 客户端反复重连"会单调涨内存：
//   实测（3000 次 connect→close）：RSS +48.3MiB（≈16KiB/连接，线性增长），
//   手动 DeleteServer 触发 join 后立刻回落到 +0.3MiB
//   → 证明是"join 前不释放"，不是真泄漏，但边界是"整个 server 生命周期"。
//
// 修复后 AcceptLoop 定期回收（poll 超时分支 + accept 路径 250ms 节流）。
// 断言：200 次短连接结束、静置 1.5s 后，滞留的 handler 线程必须是个位数。
// 反证（把 ReapFinishedHandlers 两个调用点删掉）：left == 200，本用例必红。
// ---------------------------------------------------------------------------
static void TestHandlerThreadsReaped() {
  std::printf("TestHandlerThreadsReaped ...\n");
  if (!IsSocketBackend()) {
    std::printf("  (只有 socket 后端是 per-connection 线程，in-proc 跳过)\n");
    return;
  }
  auto net = labrpc::MakeTransport();
  auto* sn = dynamic_cast<labrpc::SocketNetwork*>(net.get());
  CHECK(sn != nullptr);
  if (sn == nullptr) return;
  net->AddServer(0, MakeEchoServer());

  const char* b = std::getenv("LABRPC_SOCKET_BASE");
  int port = (b && b[0]) ? std::atoi(b) : 19090;

  constexpr int kN = 200;
  int ok_conns = 0;
  for (int i = 0; i < kN; i++) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) break;
    sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port = htons(static_cast<uint16_t>(port));
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) == 0) {
      ok_conns++;
    }
    ::close(fd);
  }
  CHECK(ok_conns >= kN * 9 / 10);  // 端口占用/backlog 抖动时不要误报

  // 等 accept 全部排空 + handler 退出 + 下一轮回收（超时分支 500ms / 节流 250ms）
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));
  int left = sn->GetHandlerThreadCount(0);
  if (left > 8) {
    std::printf("  滞留 handler 线程 %d 个（成功连接 %d 条，预期 <= 8）\n", left,
                ok_conns);
  }
  CHECK(left <= 8);

  net->Cleanup();
}

// ---------------------------------------------------------------------------
// TestClientReaderThreadsReaped —— 客户端 reader 线程也会随"反复重连"堆积
//
// 这是上一轮 TestHandlerThreadsReaped 的【另一半】：服务端是 accept 线程池，
// 客户端是每个 endname 一条 TCP 连接 + 一个 ConnReader 线程。两者早期都只在
// Cleanup 时统一 join，而 std::thread 在 join 之前一直持有已退出线程的栈和
// callable 里捕获的 shared_ptr（这里是 Conn）。
//
// 实测（/tmp/probe_reader_leak.cpp，3000 轮解绑重连，server 全程存活）：
//   RSS +65.0 MiB（≈16 KiB/轮，线性）；静置 1s 仍 +47.6 MiB；
//   Cleanup 一 join 立刻回落到 +0.6 MiB → 是"join 前不释放"，不是真泄漏。
//
// 修复：Conn 加 atomic reader_exited（ConnReader 最后一行置位），reader_threads_
// 改存 pair<Conn, thread>，GetOrCreateConn 建连后调 ReapFinishedReaderThreads。
// 断言：200 轮"解绑→重连→调用"之后静置 1s，滞留 reader 必须是个位数。
// 反证（删掉 GetOrCreateConn 里的 ReapFinishedReaderThreads 调用）：left == 200。
// ---------------------------------------------------------------------------
static void TestClientReaderThreadsReaped() {
  std::printf("TestClientReaderThreadsReaped ...\n");
  if (!IsSocketBackend()) {
    std::printf("  (只有 socket 后端有 per-connection reader，in-proc 跳过)\n");
    return;
  }
  auto net = labrpc::MakeTransport();
  auto* sn = dynamic_cast<labrpc::SocketNetwork*>(net.get());
  CHECK(sn != nullptr);
  if (sn == nullptr) return;
  net->AddServer(0, MakeEchoServer());

  constexpr int kN = 200;
  int ok_calls = 0;
  for (int i = 0; i < kN; i++) {
    auto end = net->MakeEnd("c0");
    net->Connect("c0", 0);
    net->Enable("c0", true);
    std::string reply;
    if (end->Call("Echo.Upper", "x", reply) && reply == "X") ok_calls++;
    // 撕掉连接：ShutdownConn 唤醒 reader → 退出 → 线程句柄留在 reader_threads_，
    // 直到下一次建连时被 ReapFinishedReaderThreads 回收。
    net->RemoveClientEnd("c0");
  }
  CHECK_EQ(ok_calls, kN);  // 每一轮都必须真的建过一条新连接，否则本用例没覆盖到

  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  int left = sn->GetReaderThreadCount();
  if (left > 8) {
    std::printf("  滞留 reader 线程 %d 个（%d 轮重连，预期 <= 8）\n", left, kN);
  }
  CHECK(left <= 8);

  net->Cleanup();
}

// ---------------------------------------------------------------------------
// TestDeleteServerPrompt —— 关停一个 server 不该干等 accept 循环的 500ms 超时
//
// 背景：AcceptLoop 用 poll(500ms) 轮询 + accept_stopped 标志退出。但 poll() 只认
// "被监视的 fd 上有事件"，而 close(listen_fd) 在 macOS 上【不保证】唤醒正阻塞在
// poll(lfd) 上的线程（POSIX 对"close 与 poll 并发"也是未定义行为）。于是
// DeleteServer 结尾的 acceptor.join() 只能等满一个超时周期：
//   实测（/tmp/probe_split.cpp，acceptor 已稳定在 poll 里）：
//     无连接 DeleteServer        = 297ms（剩余超时，最多 500）
//     有 1 条空闲连接 DeleteServer = 298ms
//     3 台 server 的 Cleanup      = 501ms
// 这不是"多睡一会"的小事：raft 的每个 crash/restart（config.cpp 的 crash1/start1）
// 都走 DeleteServer，每个 Config 析构都走 Cleanup → 整个 net 模式测试套件在为
// 平台的 close/poll 语义交税。实测 200 次 crash/start 的重启风暴探针：
//   修复前 107.4s（单次 RPC 最坏 25.9s，因为 server 每次要"死" 500ms）；
//   修复后 0.83s（最坏 200ms）。
//
// 修复：每个 server 一对 self-pipe（ServerState::wake_r/wake_w），关停时先置
// accept_stopped、再往 wake_w 写一个字节 → poll 立刻返回 → 复查标志退出。
// 顺带把"poll 返回后、accept() 之前复查退出条件"补上（收窄 fd 复用窗口）。
//
// 断言：DeleteServer / Cleanup 都必须 ≤100ms（修复前 297/501ms，修复后 0/0ms）。
//       100ms 留足调度抖动余量，不会被偶发停顿打成 flaky。
// 反证（把 WakeAcceptor 的写字节删掉）：两条断言分别回到 ~300ms/~500ms，用例必红。
// ---------------------------------------------------------------------------
static void TestDeleteServerPrompt() {
  std::printf("TestDeleteServerPrompt ...\n");
  if (!IsSocketBackend()) {
    std::printf("  (in-proc 后端关 server 只是改 map，in-proc 跳过)\n");
    return;
  }
  auto net = labrpc::MakeTransport();
  CHECK(dynamic_cast<labrpc::SocketNetwork*>(net.get()) != nullptr);
  net->AddServer(0, MakeEchoServer());
  net->AddServer(1, MakeEchoServer());

  // 先建一条真实客户端连接：这样 DeleteServer(0) 除了 join acceptor，还要
  // shutdown 连接并 join 阻塞在 recv 里的 handler 线程 —— 两条路径一起计时。
  auto end = net->MakeEnd("c0");
  net->Connect("c0", 0);
  net->Enable("c0", true);
  std::string reply;
  CHECK(end->Call("Echo.Upper", "x", reply) && reply == "X");

  // 静置一会，确保两个 acceptor 都已稳定阻塞在 poll() 里（否则测到的是
  // "线程还没起来"的假快，正是我第一版探针被误导的原因）。
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  auto ms_since = [](std::chrono::steady_clock::time_point t0) {
    return static_cast<int>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - t0)
            .count());
  };

  auto t0 = std::chrono::steady_clock::now();
  net->DeleteServer(0);
  int del_ms = ms_since(t0);

  auto t1 = std::chrono::steady_clock::now();
  net->Cleanup();  // 还剩 1 台，同样要 join acceptor + 关唤醒管道
  int clean_ms = ms_since(t1);

  std::printf("  DeleteServer=%dms  Cleanup=%dms（要求都 <=100ms）\n", del_ms,
              clean_ms);
  CHECK(del_ms <= 100);
  CHECK(clean_ms <= 100);
}

// ---------------------------------------------------------------------------
// 用例 21：线上乱帧不得把 server 搞崩 / 搞死
//
// 为什么需要它：HandleClient 读到的每个字节都来自网线（可能是别的进程/机器，
// 也可能因为帧错位变成垃圾）。这条边界上任何"越界读 / 分配被打爆 / 挂死"都会把
// "对方发错一个包"升级成"我这边整个测试进程没了"。
//
// 现有用例只覆盖了 3 个手写的非法【方法名】（TestWireBadMethodDoesNotAbort）；
// 这里补的是【帧结构本身】被破坏：载荷与长度头不符、svc_len/args_len 是垃圾、
// corr 之后直接截断，再混上三种断连姿势（连上就断 / 发一个字节就断 / 半关闭）。
//
// 断言不是"进程没崩"这种弱条件：
//   ① 60 轮乱帧期间，那条【全程不断开】的合法连接每 20 轮必须能正常 RPC；
//   ② 乱帧结束后静置 400ms，滞留 handler 线程必须是个位数
//      —— 坏连接不能让"一连接一线程"变成线程泄漏；
//   ③ 收尾时 server 仍能正常服务。
//
// 实测（/tmp/probe_wire_fuzz.cpp 同款逻辑放大到 800 轮，另跑 ASan 400 轮）：
//   0 次合法 RPC 失败、0 次连接失败、0 条 ASan 报告。
// ---------------------------------------------------------------------------
static void TestWireGarbageDoesNotPoisonServer() {
  std::printf("TestWireGarbageDoesNotPoisonServer ...\n");
  if (!IsSocketBackend()) {
    std::printf("  (in-proc 后端没有线上输入，跳过)\n");
    return;
  }
  auto net = labrpc::MakeTransport();
  auto* sn = dynamic_cast<labrpc::SocketNetwork*>(net.get());
  CHECK(sn != nullptr);
  if (sn == nullptr) return;
  net->AddServer(0, MakeEchoServer());

  const char* b = std::getenv("LABRPC_SOCKET_BASE");
  int port = (b && b[0]) ? std::atoi(b) : 19090;

  // 一条全程不断的合法连接：它能不能继续干活，就是"server 是否还健康"的判据
  auto legit = net->MakeEnd("legit");
  net->Connect("legit", 0);
  net->Enable("legit", true);
  std::string reply;
  CHECK(legit->Call("Echo.Upper", "abc", reply));
  CHECK_EQ(reply, std::string("ABC"));

  // 确定性伪随机：不引 <random>（不同标准库的分布实现不同，会让"可复现"失效）
  uint32_t st = 0x12345678u;
  auto rnd = [&st](uint32_t n) {  // 返回 0..n（含）
    st = st * 1103515245u + 12345u;
    return (st >> 8) % (n + 1);
  };
  auto mk_frame = [&rnd](int kind) {
    std::string payload;
    switch (kind) {
      case 0:  // 长度头自洽、内容全随机：结构上像帧，解析必然走进边界检查
        payload.assign(24, '\0');
        for (auto& c : payload) c = static_cast<char>(rnd(255));
        break;
      case 1:  // svc_len 是垃圾（远超实际载荷）
        PutU64BE(payload, rnd(0xFFFF));
        PutU32BE(payload, 0xFFFFFFFFu);
        payload.append(3, 'x');
        break;
      case 2:  // args_len 是垃圾
        PutU64BE(payload, rnd(0xFFFF));
        PutU32BE(payload, 3);
        payload.append("abc");
        PutU32BE(payload, 0xFFFFFFFFu);
        payload.append(2, 'y');
        break;
      default:  // corr 之后直接截断（连 ok / 长度字段都没给全）
        PutU64BE(payload, rnd(0xFFFF));
        PutU32BE(payload, 1);
        payload.append("a");
        break;
    }
    std::string frame;
    // 半数写真实长度（能穿过帧长闸门、进入解析），半数写离谱值（当场判帧坏）
    PutU32BE(frame, rnd(1) ? static_cast<uint32_t>(payload.size()) : rnd(4096));
    frame.append(payload);
    return frame;
  };
  auto connect_to = [port]() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port = htons(static_cast<uint16_t>(port));
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) != 0) {
      ::close(fd);
      return -1;
    }
    return fd;
  };

  constexpr int kN = 60;
  int ok_conns = 0, ok_legit = 0;
  for (int i = 0; i < kN; i++) {
    int fd = connect_to();
    if (fd < 0) continue;
    ok_conns++;
    switch (rnd(4)) {
      case 0:  // 连上就断
        break;
      case 1:  // 发一个字节就断
        (void)::write(fd, "Z", 1);
        break;
      case 2: {  // 半关闭：只关写方向，读方向留着
        std::string f = mk_frame(static_cast<int>(rnd(3)));
        (void)::write(fd, f.data(), f.size());
        ::shutdown(fd, SHUT_WR);
        break;
      }
      default: {  // 1~2 个乱帧
        int n = 1 + static_cast<int>(rnd(1));
        for (int k = 0; k < n; k++) {
          std::string f = mk_frame(static_cast<int>(rnd(3)));
          (void)::write(fd, f.data(), f.size());
        }
        break;
      }
    }
    // 不等回包直接关：顺便覆盖"服务端往已关闭的连接写回包"（EPIPE）这条路径
    ::close(fd);
    // 每 20 轮验证一次（i=19/39/59 各一次，共 3 次）
    if (i % 20 == 19) {
      reply.clear();
      if (legit->Call("Echo.Upper", "ping", reply) && reply == "PING") ok_legit++;
    }
  }
  CHECK(ok_conns >= kN * 9 / 10);  // backlog/调度抖动时不要误报
  CHECK_EQ(ok_legit, 3);           // 60 轮里检查了 3 次，每次都必须通

  // 等回收，而不是拍一个固定 sleep：回收触发点是 AcceptLoop 的
  //   * poll 超时分支（500ms，连接静默后才会到）
  //   * accept 路径的 250ms 节流（连接风暴期间一直在跑）
  // 乱帧风暴本身只持续十几毫秒，所以最后一批要等下一个触发点。固定 sleep
  // 既容易短了变 flaky、又看不出"到底多久回收"，这里改成带上限的轮询。
  int left = -1;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (std::chrono::steady_clock::now() < deadline) {
    left = sn->GetHandlerThreadCount(0);
    if (left <= 8) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  if (left > 8) {
    std::printf("  滞留 handler 线程 %d 个（%d 条乱帧连接，预期 <= 8）\n", left, kN);
  }
  CHECK(left <= 8);

  reply.clear();
  CHECK(legit->Call("Echo.Upper", "still-alive", reply));
  CHECK_EQ(reply, std::string("STILL-ALIVE"));
  net->Cleanup();
}

// ---------------------------------------------------------------------------
// 用例 22：host 必须真的被解析，不能"解析不了就悄悄改成 loopback"
//
// 为什么需要它：AddServer 的 bind 与 GetOrCreateConn 的 connect 早先各自调
// ::inet_addr(host_)，并在结果为 INADDR_NONE 时兜底成 127.0.0.1。而 inet_addr
// 只认数字点分字面量，域名/主机名/打错一位的地址全都命中兜底 —— 配置被静默改写
// 成本机回环。单机测试完全看不出来（本来就是 loopback），多机部署时每台机器都在
// 跟自己的 loopback 说话，集群永远收不到对方的 RPC 却零报错。
// 实测（/tmp/probe_host.cpp，修复前）：
//   host="no-such-host.invalid" → RPC 成功、reply="HELLO"
//   host="example.com"          → RPC 成功（同样被打回 loopback）
//
// 断言：① 合法地址必须能干活；② 不可解析的域名必须【失败】；③ 打错的数字地址
//       （"127.0.0.999"）同样必须失败。
// 反证（把 ResolveIPv4 换回 inet_addr + loopback 兜底）：②③ 两条断言变红。
// ---------------------------------------------------------------------------
static void TestHostResolution() {
  std::printf("TestHostResolution ...\n");
  if (!IsSocketBackend()) {
    std::printf("  (in-proc 后端没有 host 概念，跳过)\n");
    return;
  }
  const char* b = std::getenv("LABRPC_SOCKET_BASE");
  int base = (b && b[0]) ? std::atoi(b) : 19090;

  // 构造期会用 LABRPC_SOCKET_HOST 覆盖显式传入的 host（这是刻意的：脚本靠它
  // 统一改地址）。本用例要测的恰恰是"显式 host 参数"，所以先摘掉环境变量，
  // 结束后还原（不改变调用方的环境）。
  const char* saved_p = std::getenv("LABRPC_SOCKET_HOST");
  std::string saved = saved_p ? saved_p : "";
  ::unsetenv("LABRPC_SOCKET_HOST");

  // ① 合法地址：照常收发
  {
    labrpc::SocketNetwork net("127.0.0.1", base + 7);
    net.AddServer(0, MakeEchoServer());
    auto e = net.MakeEnd("h0");
    net.Connect("h0", 0);
    net.Enable("h0", true);
    std::string reply;
    CHECK(e->Call("Echo.Upper", "ok", reply));
    CHECK_EQ(reply, std::string("OK"));
    net.Cleanup();
  }
  // ② 不可解析的域名：必须失败（修复前会被静默打回 loopback → 成功）
  {
    labrpc::SocketNetwork net("no-such-host.invalid", base + 8);
    net.AddServer(0, MakeEchoServer());
    auto e = net.MakeEnd("h1");
    net.Connect("h1", 0);
    net.Enable("h1", true);
    std::string reply;
    bool ok = e->Call("Echo.Upper", "ok", reply);
    if (ok) std::printf("  !! 非法域名居然 RPC 成功了（说明又退化成 loopback）\n");
    CHECK(!ok);
    net.Cleanup();
  }
  // ③ 打错的数字地址：同样不能悄悄变 loopback
  {
    labrpc::SocketNetwork net("127.0.0.999", base + 9);
    net.AddServer(0, MakeEchoServer());
    auto e = net.MakeEnd("h2");
    net.Connect("h2", 0);
    net.Enable("h2", true);
    std::string reply;
    bool ok = e->Call("Echo.Upper", "ok", reply);
    if (ok) std::printf("  !! 非法数字地址居然 RPC 成功了\n");
    CHECK(!ok);
    net.Cleanup();
  }

  if (!saved.empty()) ::setenv("LABRPC_SOCKET_HOST", saved.c_str(), 1);
}

// ---------------------------------------------------------------------------
// 用例 23：LABRPC_SOCKET_BASE 必须是【真端口】，不能被 atoi + uint16 截断改写
//
// 病根：`base_port_ = atoi(getenv("LABRPC_SOCKET_BASE"))`，随后
//       `htons(static_cast<uint16_t>(base_port_ + servername))`。三个都"不报错"：
//         "abc"     → 0     → bind(0) 让内核随便挑临时端口，客户端仍去连 base+i
//                             → 全部失败，而且【一行日志都没有】
//         "-5"      → 65531 → 监听/连接一个完全无关的端口
//         "70000"   → 4464  → 同上；两端截断方式一致，于是 RPC 居然"成功"
//   实测（/tmp/probe_baseport.cpp，修复前）：
//         base=70000 → RPC ok=1 reply="HI"，且 4464 端口确实有人在听
//         base=-5    → RPC ok=1，端口 65531
//         base=abc   → RPC 全失败且零诊断
//   这类"配置被静默改写成另一个值"的故障，排查成本远高于启动即报错
//   （和上一轮 host 被改写成 loopback 是同一个病，只是维度从地址换成了端口）。
//
// 修复：严格解析（十进制整串、1..65535），非法即 port_ok_=false → 不监听不建连，
//       构造期把错因打出来；base+servername 越界也单独拒绝（不靠 uint16 回绕）。
//
// 断言：四种非法值下面 RPC 都必须【失败】。修复前 70000 / -5 / 19090xyz
//       三条会红（它们会被 atoi 解析成"看起来能用"的数字）。
// ---------------------------------------------------------------------------
static void TestBasePortValidation() {
  std::printf("TestBasePortValidation ...\n");
  if (!IsSocketBackend()) {
    std::printf("  (in-proc 后端没有端口，跳过)\n");
    return;
  }
  // 环境变量只在构造期读一次，所以每个非法值都新建一张网络；结束后还原。
  const char* saved_p = std::getenv("LABRPC_SOCKET_BASE");
  std::string saved = saved_p ? saved_p : "";
  const bool had = (saved_p != nullptr);

  const char* bads[] = {"70000", "-5", "abc", "19090xyz"};
  for (const char* bad : bads) {
    ::setenv("LABRPC_SOCKET_BASE", bad, 1);
    labrpc::SocketNetwork net("127.0.0.1", 19090);
    net.AddServer(0, MakeEchoServer());
    auto e = net.MakeEnd("p0");
    net.Connect("p0", 0);
    net.Enable("p0", true);
    std::string reply;
    const bool ok = e->Call("Echo.Upper", "x", reply);
    if (ok) {
      std::printf("  !! LABRPC_SOCKET_BASE=\"%s\" 居然 RPC 成功了（端口被改写）\n",
                  bad);
    }
    CHECK(!ok);
    net.Cleanup();
  }

  // ---- 阳性对照（第 33 轮补）----
  // 上面四条断言全是 "CHECK(!ok)"，也就是"**必须失败**"。四条全写成单向断言，
  // 等于给了一个可以被"整个网络彻底坏掉"满足的集合：
  //    变异实证：把 AddServer 改成一开始就 return（或 GetOrCreateConn 恒返回
  //    nullptr），四条 CHECK(!ok) 依旧全绿 —— 端口校验用例不但没抓到故障，
  //    还替故障盖了个"我检查过了"的章。
  // 所以先把环境变量还原成进来时的样子，再用一个【合法】端口跑一次真 RPC。
  if (had) {
    ::setenv("LABRPC_SOCKET_BASE", saved.c_str(), 1);
  } else {
    ::unsetenv("LABRPC_SOCKET_BASE");
  }
  {
    labrpc::SocketNetwork net("127.0.0.1", 19090);  // base 由（已还原的）环境决定
    net.AddServer(0, MakeEchoServer());
    auto e = net.MakeEnd("ctrl");
    net.Connect("ctrl", 0);
    net.Enable("ctrl", true);
    std::string reply;
    const bool ok = e->Call("Echo.Upper", "x", reply);
    if (!ok) {
      std::printf("  !! 阳性对照失败：合法端口也发不出 RPC（说明坏的不是端口校验）\n");
    }
    CHECK(ok);
    CHECK_EQ(reply, std::string("X"));
    net.Cleanup();
  }
}

// ---------------------------------------------------------------------------
// 用例 24：accept() 的【瞬时】错误不得把 acceptor 永久打死
//
// 病根：AcceptLoop 里 `if (cfd < 0) { if (errno == EINTR) continue; break; }`
//   —— 除了 EINTR，任何失败都 break。而 EMFILE（fd 打满）、ENFILE、
//   ECONNABORTED（客户端 connect 后立刻 close/RST，BSD/macOS 会这么返回）、
//   ENOBUFS 全都是【瞬时】的，下一秒就可能好了。
//   一旦 break，这台 server 的死法是最难查的一种：监听 fd 还开着 → 客户端
//   connect() 仍然成功（内核在 backlog 里完成握手）→ write 也成功 → 然后永远
//   没有回包。上层看到的是"节点失联/用例超时"，而不是"进程崩了"，日志里一个字
//   都没有。
//
// 复现手法（确定性，不靠调度碰运气）：把本进程 fd 表填满，再让客户端接一条
// 连接 —— 服务端 accept() 必然 EMFILE。释放 fd 后立刻【撕掉旧连接、重新建连】，
// 看服务端还能不能服务。
//   实测（/tmp/probe_accept_err.cpp，修复前）：accept<0 errno=24 → break →
//   重连后的 RPC 2042ms 超时失败；修复后 0ms 成功。
//   第一版探针之所以误判"没问题"，是因为它复用了基线那条早已被 accept 过的
//   连接 —— acceptor 死没死根本看不出来。必须逼客户端重新建连。
//
// 断言：fd 耗尽之后，一条全新连接的 RPC 必须成功（2s 看门狗兜底，防挂死）。
// 反证（把 accept 的 break 改回"非 EINTR 一律退出"）：这条断言变红。
// ---------------------------------------------------------------------------
namespace {
// fd 表的"占满器"：析构时全部归还，任何提前 return 都不会把 fd 泄漏给后续用例
struct FdHoard {
  std::vector<int> fds;
  ~FdHoard() {
    for (int fd : fds) ::close(fd);
  }
  bool Fill() {
    int src = ::open("/dev/null", O_RDONLY);
    if (src < 0) return false;
    int fd;
    while (fds.size() < 100000 && (fd = ::dup(src)) >= 0) fds.push_back(fd);
    // 源 fd 也一起占着：否则它在 dup 循环结束后被 close，表里就【多出一个空槽】，
    // accept() 会正好用掉它 —— 探针就测不到 EMFILE 了（第一版用例正是这样假绿的：
    // 关了 src(13) 又关了最后一个 dup，只补回一个，于是还剩一个空槽）。
    fds.push_back(src);
    return !fds.empty();
  }
};
}  // namespace

static void TestAcceptorSurvivesFdExhaustion() {
  std::printf("TestAcceptorSurvivesFdExhaustion ...\n");
  if (!IsSocketBackend()) {
    std::printf("  (in-proc 后端没有 accept 循环，跳过)\n");
    return;
  }
  const char* b = std::getenv("LABRPC_SOCKET_BASE");
  const int base = (b && b[0]) ? std::atoi(b) : 19090;
  const int port = base + 11;
  // 构造期会用 LABRPC_SOCKET_BASE 覆盖显式传入的 base（这是刻意的：脚本靠它统一
  // 改端口）。本用例要显式指定 port，所以先摘掉它，结束时还原。
  const char* saved_p = std::getenv("LABRPC_SOCKET_BASE");
  const std::string saved_base = saved_p ? saved_p : "";
  const bool had_base = (saved_p != nullptr);
  ::unsetenv("LABRPC_SOCKET_BASE");

  labrpc::SocketNetwork net("127.0.0.1", port);
  net.AddServer(0, MakeEchoServer());
  auto end = net.MakeEnd("fd0");
  net.Connect("fd0", 0);
  net.Enable("fd0", true);

  std::string reply;
  CHECK(end->Call("Echo.Upper", "a", reply));  // 基线：先证明这台 server 本来是好的
  CHECK_EQ(reply, std::string("A"));

  {
    FdHoard hoard;
    if (!hoard.Fill()) {
      std::printf("  (fd 表填不满，跳过)\n");
      net.Cleanup();
      if (had_base) ::setenv("LABRPC_SOCKET_BASE", saved_base.c_str(), 1);
      return;
    }
    // 留一个空槽给客户端 socket()：内核会照常完成握手、把连接放进 backlog，
    // 而服务端 accept() 此时拿不到 fd → EMFILE
    ::close(hoard.fds.back());
    hoard.fds.pop_back();
    int c = ::socket(AF_INET, SOCK_STREAM, 0);
    if (c < 0) {
      std::printf("  (留不出空槽，跳过)\n");
      net.Cleanup();
      if (had_base) ::setenv("LABRPC_SOCKET_BASE", saved_base.c_str(), 1);
      return;
    }
    sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port = htons(static_cast<uint16_t>(port));
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    const bool connected =
        ::connect(c, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) == 0;
    // 给 acceptor 足够时间撞上 EMFILE（poll 立刻返回 → accept 立刻失败）
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    CHECK(connected);
    ::close(c);
  }  // hoard 析构：fd 全部归还

  // 逼客户端重建连接 —— 否则它会复用上面那条早已被 accept 过的连接，
  // acceptor 死没死看不出来（这正是第一版探针的坑）。
  net.RemoveClientEnd("fd0");
  net.Connect("fd0", 0);
  net.Enable("fd0", true);

  std::atomic<bool> cancel{false};
  std::thread dog([&cancel] {
    std::this_thread::sleep_for(std::chrono::seconds(2));
    cancel.store(true);
  });
  reply.clear();
  auto t0 = std::chrono::steady_clock::now();
  const bool ok = end->Call("Echo.Upper", "b", reply, &cancel);
  const int ms = static_cast<int>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - t0)
          .count());
  dog.join();
  if (!ok) {
    std::printf("  !! fd 耗尽之后重连的 RPC 失败（%dms）—— acceptor 已被打死\n", ms);
  }
  CHECK(ok);
  CHECK_EQ(reply, std::string("B"));

  net.Cleanup();
  if (had_base) ::setenv("LABRPC_SOCKET_BASE", saved_base.c_str(), 1);
}

// ---------------------------------------------------------------------------
// 用例 25：被取消（Kill）的异步 RPC，回包字节【一个都不算】——两个后端必须一致
//
// 契约出处：in-proc 的 Network::Finish 第一句就是
//     if (e->cancel && e->cancel->load()) return;   // 对方已被 Kill：结果作废
//     if (e->ok) bytes_.fetch_add(e->reply.size());
// 而 socket 后端旧实现是在 ConnReader 里【先记账、再投递】：
//     if (ok) bytes_.fetch_add(reply.size());
//     DeliverResult(req, ok, std::move(reply));   // ← 里面才检查 cancel
// 于是同一条"发出去、回了包、但调用方已经 Kill 掉"的 RPC，in-proc 记 0 字节回包，
// socket 记满一个回包长度。实测（/tmp/probe_cancel_bytes.cpp，4 字节 args+4 字节
// 回包）：in-proc GetTotalBytes()=4，socket=8。
//
// 危害方向不是"多算几个字节"这么轻：GetTotalBytes() 是上层用来反推"有没有重复
// 发送"的仪表（TestRPCBytes2B 就是拿它断言每个命令只发一次），两个后端给出不同
// 读数等于让这条断言在 net 模式下悄悄变松。而 raft 的四个 RPC（AppendEntries /
// RequestVote / PreVote / InstallSnapshot）全都走 CallAsyncTyped + &cancel_rpcs_，
// 所以"被取消的回包"在真实路径上一直存在。
// ---------------------------------------------------------------------------
static void TestCanceledReplyNotCounted() {
  std::printf("TestCanceledReplyNotCounted ...\n");
  auto net = labrpc::MakeTransport();
  net->AddServer(0, MakeSleepServer());  // Sleep.Snooze：睡 200ms 再原样返回
  auto end = net->MakeEnd("c0");
  net->Connect("c0", 0);
  net->Enable("c0", true);
  CHECK_EQ(net->GetTotalBytes(), 0);

  const std::string args(4, 'a');  // 回包长度同为 4
  std::atomic<bool> cancel{false};
  std::atomic<int> cb_called{0};
  end->CallAsync("Sleep.Snooze", args,
                 [&cb_called](bool, const std::string&) { cb_called.fetch_add(1); },
                 &cancel);
  // 请求已发出（handler 正在睡）；50ms 后取消，模拟 raft 的 Kill
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  cancel.store(true);
  // 等得比 handler 的 200ms 长，确保回包真的回来过（只是被作废了）
  std::this_thread::sleep_for(std::chrono::milliseconds(400));

  CHECK_EQ(cb_called.load(), 0);           // 取消之后 cb 不该被调用
  // 契约：作废的结果"一个字节都不算" → 只剩请求本身的 4 字节
  CHECK_EQ(net->GetTotalBytes(), static_cast<int64_t>(args.size()));

  net->Cleanup();
}

// ---------------------------------------------------------------------------
// 用例 26：客户端连接缓存的复用判据（三子句缺一不可）
//
// 为什么单独钉这个纯函数：第 32 轮发现 GetOrCreateConn 的复用判据少了
// "!closed" 这一条 —— ShutdownConn 先置 closed，fd 由 ConnReader 稍后才 close，
// 这段窗口里 "fd >= 0" 仍然成立，于是 crash→restart 之后那台 server 的连接
// 会被当成"可复用"，下一条 RPC 直接凭空失败（in-proc 没有连接缓存，没这个洞）。
//
// 端到端症状是【间歇】的（reader 调度快就测不到：空载 100 轮里偶发 0~6 次），
// 拿成功率当断言只会变成 flaky 赌局。所以这里固化判据本身：
//   删掉 !closed       → 第 2 条断言红
//   删掉 fd >= 0       → 第 3 条断言红
//   删掉 servername 比较 → 第 4 条断言红
// ---------------------------------------------------------------------------
static void TestConnReusePredicate() {
  std::printf("TestConnReusePredicate ...\n");
  if (!IsSocketBackend()) {
    std::printf("  (in-proc 后端没有连接缓存，跳过)\n");
    return;
  }
  using SN = labrpc::SocketNetwork;
  CHECK(SN::ConnReusable(5, false, 0, 0));    // 活的、同一目标 → 可复用
  CHECK(!SN::ConnReusable(5, true, 0, 0));    // 已 closed（fd 还没置 -1）→ 绝不复用
  CHECK(!SN::ConnReusable(-1, false, 0, 0));  // fd 已关
  CHECK(!SN::ConnReusable(5, false, 1, 0));   // 目标变了（Connect 改指向）
}

int main() {
  // 关掉 stdout 缓冲：程序万一被杀掉，也能看到它跑到了哪一步
  std::setvbuf(stdout, nullptr, _IONBF, 0);

  TestBasicCall();
  TestDisabled();
  TestDeleteServer();
  TestUnreliable();
  TestCounters();
  TestConcurrentCalls();
  TestConcurrentMany();
  TestKilled();
  TestRegression1();
  TestRetargetServer();
  TestMakeEndResets();
  TestAddServerAfterCleanup();
  TestLongReordering();
  TestMaxMessageBytesZero();
  TestAsyncFastFailNotInline();
  TestReplyLargerThanRequestCap();
  TestWireBadMethodDoesNotAbort();
  TestHandlerThreadsReaped();
  TestClientReaderThreadsReaped();
  TestDeleteServerPrompt();
  TestHostResolution();
  TestWireGarbageDoesNotPoisonServer();
  TestBasePortValidation();
  TestAcceptorSurvivesFdExhaustion();
  TestCanceledReplyNotCounted();
  TestConnReusePredicate();

  // 最后一道闸：后端名拼错时，MakeTransport 会回退成 in-proc（不能对库函数
  // abort），于是上面所有 socket-only 用例都会被 skip 掉，整套自测"全绿"，
  // 而实际一条 net 用例都没跑。横幅里还会照抄用户写的那个错名字。
  // 断言这条标志，就把"配置拼错"从静默假绿变成一条真正的 FAIL 行。
  // 实测（第 33 轮，修复前）：
  //   $ LABRPC_TRANSPORT=socekt ./test_part.sh 2A
  //   OK：2A 全部 2 条用例 × 1 轮通过（后端 net=socekt persist=memory）
  //   $ echo $?  → 0
  // 而那条 "[labrpc] 无法识别" 的 warning 被 run_one 重定向进了每个用例的
  // 日志文件，控制台上一个字都没有。
  if (labrpc::TransportKindFellBack()) {
    std::printf(
        "  （注意：LABRPC_TRANSPORT 的取值无法识别，本次实际跑的是 in-proc）\n");
  }
  CHECK(!labrpc::TransportKindFellBack());

  std::printf("\n========================================\n");
  std::printf("  断言 %d 条，失败 %d 条\n", g_checks, g_failures);
  std::printf("========================================\n");
  return g_failures == 0 ? 0 : 1;
}
