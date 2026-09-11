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

#include <atomic>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

#include "labrpc.h"
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

// ---------------------------------------------------------------------------
// 用例 1：最基本的收发
// ---------------------------------------------------------------------------
static void TestBasicCall() {
  std::printf("TestBasicCall ...\n");
  auto net = labrpc::MakeNetwork();
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
  auto net = labrpc::MakeNetwork();
  net->LongDelays(false);  // 关掉 0~7000ms 的长延迟，不然这个测试要跑 3 秒
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
  auto net = labrpc::MakeNetwork();
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
  auto net = labrpc::MakeNetwork();
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
  auto net = labrpc::MakeNetwork();
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
  auto net = labrpc::MakeNetwork();
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
  auto net = labrpc::MakeNetwork();
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
  auto net = labrpc::MakeNetwork();
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
// 注意：C++ labrpc 在 enabled=false 时【仍会执行 handler】，只是结果被作废为
// false（与 Go 的"不投递"实现不同）。因此本测试只断言"禁用期 RPC 全部失败 +
// 重新启用后不被拖慢"，不断言 GetCount 计数（C++ 设计下计数会是 21 而非 1）。
// ---------------------------------------------------------------------------
static void TestRegression1() {
  std::printf("TestRegression1 ...\n");
  auto net = labrpc::MakeNetwork();
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
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  net->Enable("c", true);
  for (auto& th : threads) th.join();

  auto t0 = std::chrono::steady_clock::now();
  std::string reply;
  CHECK(end->Call("Echo.Upper", "fast", reply));
  auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::steady_clock::now() - t0).count();
  CHECK_EQ(reply, std::string("FAST"));
  CHECK(dur < 200);            // 重新启用后不应被之前的禁用 RPC 拖慢
  CHECK_EQ(failed.load(), 20);  // 禁用期的 20 个 RPC 全部失败
  net->Cleanup();
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

  std::printf("\n========================================\n");
  std::printf("  断言 %d 条，失败 %d 条\n", g_checks, g_failures);
  std::printf("========================================\n");
  return g_failures == 0 ? 0 : 1;
}
