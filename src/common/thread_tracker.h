// thread_tracker.h —— 被跟踪的一次性线程
//
// 为什么需要这个东西？
//
// Go 版 Raft 到处都是 `go func(){ ... }()`：拉票时给每个 peer 开一个
// goroutine，心跳也是。goroutine 很便宜，而且 Go 从不在乎它什么时候结束。
//
// C++ 的 std::thread 不行：
//   * 必须在销毁前 join() 或 detach()，否则 std::terminate() 直接崩；
//   * detach() 之后，如果线程还持有 this 指针而对象已被析构 → 野指针。
//
// 所以这里做一个折中：启动即 detach，但用计数器记录"还有多少线程活着"，
// 程序（或测试框架）需要收尾时调用 WaitAll() 等它们跑完，再释放对象。
//
// 【C++ 知识点】
//   * detach() 后线程变成"后台线程"，资源由运行时回收，但对象生命周期
//     要你自己保证 —— 这就是这个 Tracker 存在的意义。
//   * 捕获裸指针的 lambda 交给 detached 线程 = 高危动作，必须有配对的等待。

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <functional>
#include <mutex>
#include <thread>

#include "util.h"

namespace raftcpp {

class ThreadTracker {
 public:
  static ThreadTracker& Instance() {
    static ThreadTracker t;    // 全局只构造一次该对象，多次调用返回同一个对象
    return t;
  }

  // 启动一个后台线程，会自动计数
  void Spawn(std::function<void()> fn) {
    alive_.fetch_add(1); // 原子增加计数器
    std::thread([this, fn]() {
      try {
        fn();
      } catch (const std::exception& e) {
        // 光打日志是不够的：之前这里只 fprintf 一行就完了，既不置失败标志
        // 也不计入失败数 —— Raft 的选举/复制/应用线程崩了，用例照样判
        // "通过"。典型的「崩溃伪装成通过」。
        // 现在记进 crashes_，runner 在每个用例前后取差值，>0 就判失败。
        RecordCrash(e.what());
        std::fprintf(stderr, "thread crashed: %s\n", e.what());
      } catch (...) {
        RecordCrash("unknown exception");
        std::fprintf(stderr, "thread crashed: unknown exception\n");
      }
      // 线程跑完，计数 -1，返回减之前的值，如果减之前是1，说明是最后一个线程，通知所有等待者，去析构
      if (alive_.fetch_sub(1) == 1) cv_.notify_all();
    }).detach(); //线程后台运行，不阻塞主线程
  }

  // 累计有多少个后台线程因异常提前退出。
  // runner 的用法：用例开始前记下 Crashes()，结束后再取一次，差值 > 0
  // 说明这个用例期间有线程崩了 → 判失败（即使断言全过）。
  int Crashes() const { return crashes_.load(); }

  // 最后一次崩溃的原因（只留一条，供 runner 打印）
  std::string LastCrash() const {
    std::lock_guard<std::mutex> lk(mu_);
    return last_crash_;
  }

  // 等所有线程退出。返回 true 表示真的等到了 0。
  bool WaitAll(int64_t timeout_ms) {
    auto deadline = Now() + std::chrono::milliseconds(timeout_ms);
    std::unique_lock<std::mutex> lk(mu_);
    while (alive_.load() > 0) {
      if (Now() >= deadline) {
        std::fprintf(stderr,
                     "[warn] ThreadTracker::WaitAll timeout, %d threads still "
                     "alive\n",
                     alive_.load());
        return false;
      }
      cv_.wait_for(lk, std::chrono::milliseconds(50));
    }
    return true;
  }

  int Alive() const { return alive_.load(); }

 private:
  ThreadTracker() = default;

  void RecordCrash(const char* what) {
    crashes_.fetch_add(1);
    std::lock_guard<std::mutex> lk(mu_);
    last_crash_ = what;
  }

  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::atomic<int> alive_{0};
  std::atomic<int> crashes_{0};
  std::string last_crash_;
};

}  // namespace raftcpp
