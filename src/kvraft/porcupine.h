// porcupine.h —— 线性一致性检查器（移植自 MIT 6.824 src/porcupine/）
//
// porcupine 是 MIT 自己写的"历史是否可线性化"判定器：
//   给定一个操作历史（每个 op 包含 input/output + 调用/返回时间戳），
//   它搜索是否存在一个全序排列 σ，使得状态机从初态依次应用 σ 中每个
//   op 的 input，都能在对应位置产生 output。
//
// Go 版用 interface{} 在运行时分发；C++ 版用模板，三个核心类型
// 由调用方指定（KV 测试用 KvInput / KvOutput / KvState = std::string）。
//
// ⚠️ 算法的正确性关键在于两个不变式：
//   (1) 链表按时间排序：同 op 的 call 一定在 return 之前
//   (2) lift/unlift 对称：把 (call, return) 一对从链表同时摘下 / 同时挂回
//   任何破坏这两条的改动都会让线性化结果变成 false negative 或 false positive。
// Client A: Put("k","1")   [0, 10ms]   ← Put 必须生效（必须在0-10ms之间拿到server返回到client A的结果），没得选
// Client B: Get("k")        [5, 15ms]  返回 "1"   ← B 的读只能看到 A 生效之后，也就是说A生效在5ms之前可以读到1
// Client C: Get("k")        [12, 20ms] 返回 "1"   ← C 的读看到 A 生效之前，必然读到1

#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

namespace porcupine {

enum class CheckResult {
  Ok,        // 找到合法的全序排列 → 这次历史是线性一致的
  Illegal,   // 搜遍整个搜索空间都找不到 → 不是线性一致的（肯定有 bug）
  Unknown,   // 超时没搜完 → 不下结论（false positive 不能报警）
};

// 一次客户端操作（对应 Go 的 porcupine.Operation）
//
//   client_id : 哪个客户端发起的（0 起点，仅可视化用）
//   input     : 调用参数（如 KvInput{Op=1, Key="k", Value="v"}）
//   output    : 返回结果（如 KvOutput{Value="v"}）；对于 Put/Append 通常是空
//   call_time / return_time : 调用发起 / 返回的真实时间（单调时钟，纳秒）
//
// 必须保证 call_time < return_time —— 否则一次操作还没发出去就拿到返回值，
// 算法会判 Illegal。
template <typename Input, typename Output>
struct Operation {
  int client_id = 0;
  // 🐞 调试用：客户端的 seq 号（同 client 单调递增）。不进 porcupine 算法。
  // 只供 Config::CheckLinearizability 的 dump 用，方便定位"是不是去重漏了一条"
  // 这类问题。注意：因为是最后加的，老的 history 走默认 0（不影响）。
  int64_t seq_id = 0;
  Input input{};
  Output output{};
  int64_t call_time_ns = 0;
  int64_t return_time_ns = 0;
};

// 系统模型：定义状态机怎么跑 + 怎么把历史切成可独立检查的分片
//
//   Partition : 把 history 切成多个 sub-history，每个 sub-history 内部
//               独立判定（KV 测试按 key 切，因为不同 key 之间不互斥）
//               必须设置；不设置就用 NoPartition（全部 op 在一个分片）
//   Init      : 状态机初始状态（KV 测试是空字符串）
//   Step      : 给定当前状态 + 一个 op 的 (input, output)，返回
//               (ok, new_state)。ok=false 表示这条 op 在当前状态下
//               不可能产生这个 output —— 算法立刻否决这条线性化路径
//   Equal     : 状态等价比较（必须设置，模板版没法做默认 ShallowEqual）
//   DescribeOperation / DescribeState : 可选，仅供调试 / 可视化输出
template <typename Input, typename Output, typename State>
struct Model {
  std::function<std::vector<std::vector<int>>(const std::vector<Operation<Input, Output>>&)>
      Partition;
  std::function<State()> Init;
  std::function<std::pair<bool, State>(const State&, const Input&, const Output&)>
      Step;
  std::function<bool(const State&, const State&)> Equal;
  std::function<std::string(const Input&, const Output&)> DescribeOperation;
  std::function<std::string(const State&)> DescribeState;
};

// 顶层入口：检查整个 history 是否线性一致
//   timeout <= 0 表示不超时（搜到天荒地老）
//   返回 Ok / Illegal / Unknown；Unknown 不算失败（只是没搜完）
template <typename Input, typename Output, typename State>
CheckResult CheckOperations(const Model<Input, Output, State>& model,
                            const std::vector<Operation<Input, Output>>& history,
                            std::chrono::milliseconds timeout =
                                std::chrono::milliseconds::zero());

// 显式实例化声明：实际机器码由 porcupine.cpp 生成。
// 注意：必须放在调用方能看到完整 KvInput/KvOutput 定义的位置，
// 否则 Operation<Input, Output> 没法实例化（output 字段类型不完整）。
// 我们把这部分留到 kv_model.h 末尾，kv_model.h 里同时 #include 了本头。

}  // namespace porcupine