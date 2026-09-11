// test_status.cpp —— 全局测试失败状态的定义（见 test_status.h）

#include "test_status.h"

#include <cstdio>
#include <thread>

namespace kvraft {

std::atomic<bool> g_failed{false};
std::mutex g_fail_mu;
std::string g_fail_msg;

namespace {

// 主线程 id：在【静态初始化期】抓，那一刻跑的必然是 main 线程。
// 不能写成函数内的 static 懒加载 —— 万一第一个调 Fatal() 的是后台线程，
// 它就把自己登记成"主线程"了。
const std::thread::id kMainThreadId = std::this_thread::get_id();

}  // namespace

void Fatal(const std::string& msg) {
  {
    std::lock_guard<std::mutex> lk(g_fail_mu);
    if (g_fail_msg.empty()) g_fail_msg = msg;
  }
  g_failed.store(true);
  std::printf("    !!! %s\n", msg.c_str());
  std::fflush(stdout);

  // 见 test_status.h 的注释：只有主线程才抛，子线程靠 g_failed 记账。
  if (std::this_thread::get_id() == kMainThreadId) {
    throw TestFailure{msg};
  }
}

}  // namespace kvraft
