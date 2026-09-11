// watchdog.h —— 单用例超时看门狗
//
// 为什么需要它？
//
// Go 版靠 `go test` 运行时兜底：默认 10 分钟超时 → `panic: test timed out`
// 并且把【所有 goroutine 的栈】一起 dump 出来。排查死锁时，这堆栈是最有用的
// 信息 —— 你能直接看见哪个 goroutine 卡在哪一行 lock / channel 上。
//
// C++ 自研 runner 原本完全没有这层保护：用例一旦死锁就永久挂住，
// 既不报错也不退出，CI 直接卡死，人回来只看到一个还在跑的进程，
// 什么线索都没有。这是"bug 伪装成挂死"的最高风险缺口。
//
// 用法（栈对象，离开作用域自动解除）：
//
//   {
//     raftcpp::Watchdog wd("TestFoo2B", raftcpp::TestTimeoutMs());
//     RunTestFoo();
//   }   // ← 析构时置 done，看门狗线程退出
//
// 【C++ 知识点】
//   * 看门狗线程必须 detach：它需要和被测用例【并发】跑，join 就变成串行了。
//   * done 标志用 shared_ptr<atomic<bool>> 持有：栈对象析构后线程仍可能
//     再读一次，裸引用会 use-after-free。
//   * 超时后 abort() 而不是抛异常：异常只能在单线程栈上展开，跨线程传递不了；
//     而 Go 的 test timeout 也是直接终止整个测试二进制，语义一致。
//     abort() 会产生 core / 崩溃报告，lldb 挂上去就能看所有线程的栈。

#pragma once

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "thread_tracker.h"
#include "util.h"

namespace raftcpp {

// 单用例超时上限（毫秒）。
//
// 默认 10 分钟：对齐 `go test` 的默认 -timeout（Go 官方 6.824 就是靠它兜底）。
// 实测最慢的用例（3B 线性一致性、2C churn）非 TSan 约 40~60 秒；
// TSan 版会慢数倍，10 分钟才留得出足够余量，避免把"跑得慢"误判成"死锁"。
// 想临时放宽/收紧就设环境变量，例如：TEST_TIMEOUT_MS=900000 ./kv_test 3B
inline int64_t TestTimeoutMs() {
  const char* s = std::getenv("TEST_TIMEOUT_MS");
  if (s != nullptr && *s != '\0') {
    const long long v = std::strtoll(s, nullptr, 10);
    if (v > 0) return static_cast<int64_t>(v);
  }
  return 10 * 60 * 1000;  // 10 分钟（对齐 go test 默认）
}

class Watchdog {
 public:
  Watchdog(std::string test_name, int64_t timeout_ms)
      : done_(std::make_shared<std::atomic<bool>>(false)) {
    if (timeout_ms <= 0) return;  // 0 / 负数 = 关闭看门狗

    auto done = done_;
    std::string name = std::move(test_name);
    const int64_t limit = timeout_ms;

    std::thread([done, name, limit]() {
      const auto start = std::chrono::steady_clock::now();
      while (!done->load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        if (done->load(std::memory_order_relaxed)) return;

        const auto elapsed =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start)
                .count();
        if (elapsed < limit) continue;

        // ---- 判定卡死。把能拿到的现场信息尽量打全再 abort ----
        std::fflush(stdout);
        std::fprintf(stderr, "\n");
        std::fprintf(stderr,
                     "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");
        std::fprintf(stderr, "!!!! 看门狗触发：用例 [%s] 超过 %lld ms 未返回\n",
                     name.c_str(), static_cast<long long>(limit));
        std::fprintf(stderr, "!!!! 判定为死锁 / 永久阻塞，强制 abort。\n");
        std::fprintf(stderr, "!!!! 当前 ThreadTracker 里还有 %d 个后台线程活着。\n",
                     ThreadTracker::Instance().Alive());
        std::fprintf(stderr, "!!!!\n");
        std::fprintf(stderr,
                     "!!!! 这等价于 Go 版 `go test` 10 分钟超时后的\n"
                     "!!!!   \"panic: test timed out\" —— 只是 Go 会把所有\n"
                     "!!!!   goroutine 的栈直接打出来，C++ 需要你自己挂调试器。\n");
        std::fprintf(stderr, "!!!!\n");
        std::fprintf(stderr, "!!!! 拿栈的办法（任选其一）：\n");
        std::fprintf(stderr,
                     "!!!!   1) 另开终端挂调试器：\n"
                     "!!!!        lldb -p $(pgrep -f '%s' | head -1)\n"
                     "!!!!      然后 (lldb) bt all —— 相当于 Go dump 的全 goroutine 栈\n",
                     name.c_str());
        std::fprintf(stderr,
                     "!!!!   2) 让它自己留 core：ulimit -c unlimited 后再跑，\n"
                     "!!!!      abort 后去 /cores 找 core 文件\n");
        std::fprintf(stderr,
                     "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");
        std::fflush(stderr);
        std::abort();
      }
    }).detach();
  }

  ~Watchdog() { done_->store(true, std::memory_order_relaxed); }

  Watchdog(const Watchdog&) = delete;
  Watchdog& operator=(const Watchdog&) = delete;

 private:
  std::shared_ptr<std::atomic<bool>> done_;
};

}  // namespace raftcpp
