// test_status.h —— 跨文件共享的"测试失败"状态
//
// 之前 g_failed / Fatal() 都在 test_kvraft.cpp 的匿名 namespace 里，
// Config::CheckLinearizability（位于 config.cpp）够不着，于是 End() 里
// 打印了 "FAILED" 但测试 runner 那边 g_failed 还是 false，结果把
// 失败的用例算成"通过"。
//
// 这里把失败状态提到一个能被 Config.cpp 也 #include 的公共头里：
//   - g_failed     : 主 runner 用的全局失败标记（test_kvraft.cpp main() 检查）
//   - g_fail_mu    : 保护 g_fail_msg 的互斥锁
//   - g_fail_msg   : 第一条失败原因（后续 Fail 调用不覆盖）
//   - Fatal()      : 一次调用同时置 g_failed 并打印
//
// 注意：放在 namespace kvraft 内，因为 Config 也是这个 namespace。
#pragma once

#include <atomic>
#include <exception>
#include <mutex>
#include <string>
#include <utility>

namespace kvraft {

// 全局失败标记：test_kvraft.cpp main() 用它计数通过 / 失败。
// Config::CheckLinearizability / DoCheck 之类的检查也应 flip 它。
extern std::atomic<bool> g_failed;
extern std::mutex g_fail_mu;
extern std::string g_fail_msg;

// 测试失败异常 —— 对齐 Lab 2 的 raft::TestFailure（raft/config.h:40）。
//
// 有了它，kvraft 的断言失败才能像 Go 的 t.Fatalf 一样"立刻中断当前用例"，
// 而不是"记个标记然后继续往下跑"（跑下去可能二次崩溃或输出误导信息）。
// 与 raft::TestFailure（raft/config.h:40）保持同构：同样继承 std::exception，
// 否则 porcupine.cpp:388 的 catch (std::exception) 抓不到它，只会落到
// catch (...) 报一句"未知异常"，真实 msg 全丢。
struct TestFailure : std::exception {
  explicit TestFailure(std::string m) : msg(std::move(m)) {}
  const char* what() const noexcept override { return msg.c_str(); }
  std::string msg;
};

// 标记测试失败：设 g_failed、记录第一条原因（不覆盖）、打印。
// 调用后无需再 Config::Fail —— Config 那边的 failed_ 由 End() 自己看。
//
// 【抛异常的范围：只限主线程】
//   主线程调用 → 抛 TestFailure，立刻中断当前用例，runner 接住后继续跑
//                下一个（等价于 Go 的 t.Fatalf → runtime.Goexit）。
//   子线程调用 → 只记录 + 打印，**不抛**。
//
//   为什么子线程不能抛？GenericTest 里的客户端线程是裸 std::thread
//   （test_kvraft.cpp:253 / 304 等），lambda 外面没人 catch，异常逃出线程
//   函数会直接 std::terminate() 干掉整个进程 —— 汇总都打不出来，
//   比"继续跑完"糟糕得多。这类断言由 g_failed 记账，一样会判失败。
//
//   所以本函数是"条件 [[noreturn]]"：不能真的标 [[noreturn]]，
//   否则编译器会假定子线程分支不可达。
void Fatal(const std::string& msg);

}  // namespace kvraft
