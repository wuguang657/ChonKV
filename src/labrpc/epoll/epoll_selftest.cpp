// epoll_selftest.cpp —— EpollNetwork 的练习用自测入口（不在 build.sh 的用例清单里）
//
// 用法（Linux）：
//     cmake -S . -B build && make -C build epoll_selftest -j8
//     LABRPC_TRANSPORT=epoll ./build/epoll_selftest
//
// 退出码约定（防止"假绿"）：
//     0 = 全部通过
//     1 = 有失败（含超时）
//     2 = 本平台没有 epoll，只跑了 wire 层用例，网络用例【未验证】
//         —— 刻意用 2 而不是 0：不能让人把"什么也没验"当成"通过"。
//
// 用例分组：
//   A. wire 层（纯内存，任何平台都能跑）        —— 已实现，应该立刻全绿
//   B. epoll 网络层（需要你先实现 TODO(1)~(9)）—— 现在是红的，正是你的目标
//
// 每个网络用例都带看门狗：一个没实现的 SendReq 会让 Call() 永远等不到回包，
// 而不带超时的测试只会表现为"卡住"，最后被外层看门狗当成"用例超时"，
// 与真正的原因（哪一步没实现）完全无关。这里直接把"超时"和 Hint 打在脸上。

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "labrpc/epoll/epoll_transport.h"
#include "labrpc/epoll/poller.h"
#include "labrpc/epoll/wire.h"
#include "labrpc/labrpc.h"

namespace {

int g_pass = 0;
int g_fail = 0;

void Check(bool cond, const char* what) {
  if (cond) {
    g_pass++;
  } else {
    g_fail++;
    std::printf("    [FAIL] %s\n", what);
  }
}

// 带看门狗的用例执行器。
// 为什么必须这样：C++ 里一个卡在 condition_variable 上的线程没法被"取消"，
// 唯一安全的退出方式就是主线程直接 _Exit —— 那也正是真实测试里看到的现象
// （整个进程被外层看门狗杀掉）。在这里主动做，能把原因写清楚。
void RunGuarded(const char* name, int timeout_ms, std::function<bool()> fn) {
  std::printf("  %-44s", name);
  std::fflush(stdout);
  auto fut = std::async(std::launch::async, [fn]() { return fn(); });
  if (fut.wait_for(std::chrono::milliseconds(timeout_ms)) !=
      std::future_status::ready) {
    std::printf("超时\n");
    std::printf("    [FAIL] %s 在 %dms 内没有返回。\n", name, timeout_ms);
    std::printf("    Hint：Call() 等的是 ReplySlot 的 cv，它永远不会被通知 ——\n");
    std::printf("          说明 SendReq / 事件循环 / OnClientFrame 里至少有一处还没实现。\n");
    std::fflush(stdout);
    std::_Exit(1);  // 卡住的线程无法回收，直接退出，别让析构再挂一次
  }
  const bool ok = fut.get();
  std::printf("%s\n", ok ? "通过" : "失败");
  if (ok) g_pass++; else g_fail++;
}

// ---------------------------------------------------------------------------
// A. wire 层
// ---------------------------------------------------------------------------
void TestWireRoundTrip() {
  const std::string svc = "Raft.AppendEntries";
  const std::string args(300, 'x');
  const std::string frame = labrpc::wire::EncodeRequestFrame(42, svc, args);

  // 帧头 4 字节 + 8(corr) + 4(svc_len) + svc + 4(args_len) + args
  Check(frame.size() == 4 + 8 + 4 + svc.size() + 4 + args.size(),
        "请求帧长度 = 4 + 8 + 4 + svc + 4 + args");
  Check(static_cast<unsigned char>(frame[0]) == 0,
        "帧头是大端（高位在前）");

  labrpc::wire::FrameReader rd(1 << 20);
  rd.Append(frame.data(), frame.size());
  std::string body;
  Check(rd.Next(body), "整帧能切出来");
  uint64_t corr = 0;
  std::string s, a;
  Check(labrpc::wire::DecodeRequest(body, corr, s, a), "请求体能解码");
  Check(corr == 42 && s == svc && a == args, "corr/svc/args 与编码前一致");
  Check(!rd.Next(body), "切完之后没有第二帧");
}

void TestWireHalfFrame() {
  // 一个字节一个字节喂：模拟 TCP 把一帧切成很多段（真实网络的常态）。
  const std::string frame = labrpc::wire::EncodeReplyFrame(7, true, "pong");
  labrpc::wire::FrameReader rd(1 << 20);
  std::string body;
  int frames = 0;
  for (size_t i = 0; i < frame.size(); i++) {
    rd.Append(frame.data() + i, 1);
    if (rd.Next(body)) {
      frames++;
      uint64_t corr = 0;
      bool ok = false;
      std::string reply;
      Check(labrpc::wire::DecodeReply(body, corr, ok, reply), "逐字节喂也能解出回包体");
      Check(corr == 7 && ok && reply == "pong", "逐字节喂出的内容正确");
    }
    if (i + 1 < frame.size()) Check(frames == 0, "帧没齐之前不能切出东西");
  }
  Check(frames == 1, "整帧到达时恰好切出一帧（不多不少）");
}

void TestWireMultipleFrames() {
  // 一次 Append 里塞 5 帧：事件循环必须把这 5 帧都处理掉，不能只处理第一帧。
  labrpc::wire::FrameReader rd(1 << 20);
  std::string all;
  for (uint64_t i = 1; i <= 5; i++) all += labrpc::wire::EncodeReplyFrame(i, true, "r");
  rd.Append(all.data(), all.size());
  std::string body;
  uint64_t expect = 1;
  int n = 0;
  while (rd.Next(body)) {
    uint64_t corr = 0;
    bool ok = false;
    std::string reply;
    Check(labrpc::wire::DecodeReply(body, corr, ok, reply) && corr == expect,
          "批量到达的帧按顺序切出");
    expect++;
    n++;
  }
  Check(n == 5, "5 帧全部切出");
}

void TestWireLengthGate() {
  // 长度头写 4GiB：必须在切帧阶段就被挡掉，绝不能先 allocate 再说。
  std::string evil;
  labrpc::wire::PutU32(evil, 0xFFFFFFFFu);
  evil.append(16, 'x');
  labrpc::wire::FrameReader rd(1024);  // 上限 1KiB
  rd.Append(evil.data(), evil.size());
  std::string body;
  Check(!rd.Next(body), "超过闸门的长度头不切帧");
  Check(rd.broken(), "并且把 reader 标记为 broken（调用方据此关连接）");
}

// ---------------------------------------------------------------------------
// B. epoll 网络层
// ---------------------------------------------------------------------------
class EchoService : public labrpc::Service {
 public:
  EchoService()
      : Service("Echo", {{"Ping",
                          [](const std::string& args) { return args; }}}) {}
};

bool TestOneRpc() {
  auto net = std::make_shared<labrpc::EpollNetwork>("127.0.0.1", 19600);
  auto srv = std::make_shared<labrpc::Server>();
  srv->AddService(std::make_shared<EchoService>());
  net->AddServer(0, srv);
  auto end = net->MakeEnd("e0");
  net->Connect("e0", 0);
  net->Enable("e0", true);

  std::string reply;
  const bool ok = end->Call("Echo.Ping", "hello", reply);
  return ok && reply == "hello";
}

bool TestConcurrentRpc() {
  auto net = std::make_shared<labrpc::EpollNetwork>("127.0.0.1", 19700);
  auto srv = std::make_shared<labrpc::Server>();
  srv->AddService(std::make_shared<EchoService>());
  net->AddServer(0, srv);

  // 8 个端点并发打同一台 server：这是"事件循环到底有没有真的并发处理多连接"
  // 的最小判据。只测单连接的话，把事件循环写成"每轮只处理一个 fd"也能过。
  auto worker = [net](int id) {
    const std::string name = "c" + std::to_string(id);
    auto end = net->MakeEnd(name);
    net->Connect(name, 0);
    net->Enable(name, true);
    for (int i = 0; i < 20; i++) {
      std::string reply;
      const std::string payload = name + ":" + std::to_string(i);
      if (!end->Call("Echo.Ping", payload, reply)) return false;
      if (reply != payload) return false;
    }
    return true;
  };
  std::vector<std::thread> ts;
  std::vector<int> results(8, 0);
  for (int i = 0; i < 8; i++) {
    ts.emplace_back([&worker, &results, i]() { results[i] = worker(i) ? 1 : 0; });
  }
  for (auto& t : ts) t.join();

  bool all_ok = true;
  for (int i = 0; i < 8; i++) {
    if (results[i] != 1) all_ok = false;
  }
  if (!all_ok) std::printf("    [FAIL] 有端点失败（见上面的连接数）\n");
  if (net->ConnCount() <= 0) {
    std::printf("    [FAIL] 事件循环里没有注册任何连接\n");
    return false;
  }
  return all_ok;
}

bool TestCleanupIdempotent() {
  // Cleanup 跑两次不能崩（Config 析构 + 显式 Cleanup 会撞在一起）。
  auto net = std::make_shared<labrpc::EpollNetwork>("127.0.0.1", 19800);
  net->Cleanup();
  net->Cleanup();
  bool ok = net->IsStopped();
  // 停机之后的网络是终态：AddServer 必须是空操作（否则会漏 fd + 漏线程）。
  auto srv = std::make_shared<labrpc::Server>();
  net->AddServer(0, srv);
  if (net->ConnCount() != 0) ok = false;
  return ok;
}

}  // namespace

int main() {
  std::printf("=== EpollNetwork 自测 ===\n");
  std::printf("平台 Poller 后端: %s（Supported=%d）\n",
              labrpc::Poller::BackendName(),
              static_cast<int>(labrpc::EpollNetwork::Supported()));

  std::printf("\n[A] wire 层（纯内存，与平台无关）\n");
  TestWireRoundTrip();
  TestWireHalfFrame();
  TestWireMultipleFrames();
  TestWireLengthGate();
  std::printf("  wire 层小计: 通过 %d，失败 %d\n", g_pass, g_fail);

  if (!labrpc::EpollNetwork::Supported()) {
    std::printf(
        "\n[B] epoll 网络层 —— SKIP\n"
        "    本平台没有 epoll（Linux 专属）。请在 Linux 上跑这一组；\n"
        "    macOS 上可以用 inproc / socket 后端跑 raft 与 kv 的全套用例。\n");
    std::printf("\n=== 未完整验证（退出码 2）===\n");
    return 2;  // 刻意不是 0：见文件头的退出码约定
  }

  std::printf("\n[B] epoll 网络层（需要你先实现 TODO(1)~(9)）\n");
  const int before = g_fail;
  RunGuarded("单连接一次 RPC", 5000, TestOneRpc);
  if (g_fail == before) {
    RunGuarded("8 端点 × 20 次并发 RPC", 15000, TestConcurrentRpc);
  } else {
    // 第一条都不过就别往下跑：后面只会是一堆超时噪声，看不出真正原因。
    std::printf("  8 端点 × 20 次并发 RPC                       跳过（上一条未通过）\n");
  }
  RunGuarded("Cleanup 幂等 / 停机后不收新连接", 3000, TestCleanupIdempotent);

  std::printf("\n=== 通过 %d，失败 %d ===\n", g_pass, g_fail);
  return g_fail == 0 ? 0 : 1;
}
