// kv_model.cpp —— KV 模型的实现（对应 Go 版 src/models/kv.go）

#include "kv_model.h"

#include <algorithm>
#include <cstdio>
#include <utility>
#include <vector>

namespace kvraft {

namespace {

// 按 key 分桶：不同 key 的 op 之间没有顺序约束，可以并行检查。
std::vector<std::vector<int>> KvPartition(
    const std::vector<KvOperation>& history) {
  // key → op ids
  std::vector<std::pair<std::string, std::vector<int>>> buckets;
  for (size_t i = 0; i < history.size(); ++i) {
    const std::string& k = history[i].input.key;
    auto it = std::find_if(buckets.begin(), buckets.end(),
                           [&](const auto& p) { return p.first == k; });
    if (it == buckets.end()) {
      buckets.push_back({k, {static_cast<int>(i)}});
    } else {
      it->second.push_back(static_cast<int>(i));
    }
  }
  std::vector<std::vector<int>> out;
  out.reserve(buckets.size());
  for (auto& b : buckets) out.push_back(std::move(b.second));
  return out;
}

KvState KvInit() { return KvState{}; }

std::pair<bool, KvState> KvStep(const KvState& state, const KvInput& inp,
                                const KvOutput& out) {
  switch (inp.op) {
    case KV_OP_GET:
        // Get：output.value 必须等于当前 state，否则非法
        return {out.value == state, state};
    case KV_OP_PUT:
        // Put：直接覆盖
        return {true, inp.value};
    case KV_OP_APPEND:
        // Append：拼接
        return {true, state + inp.value};
    default:
        return {false, state};
  }
}

bool KvEqual(const KvState& a, const KvState& b) { return a == b; }

std::string KvDescribeOp(const KvInput& inp, const KvOutput& out) {
  char buf[256];
  switch (inp.op) {
    case KV_OP_GET:
      std::snprintf(buf, sizeof(buf), "get('%s') -> '%s'", inp.key.c_str(),
                    out.value.c_str());
      break;
    case KV_OP_PUT:
      std::snprintf(buf, sizeof(buf), "put('%s', '%s')", inp.key.c_str(),
                    inp.value.c_str());
      break;
    case KV_OP_APPEND:
      std::snprintf(buf, sizeof(buf), "append('%s', '%s')", inp.key.c_str(),
                    inp.value.c_str());
      break;
    default:
      std::snprintf(buf, sizeof(buf), "<invalid op %u>", inp.op);
      break;
  }
  return buf;
}

std::string KvDescribeState(const KvState& s) {
  return "'" + s + "'";
}

}  // namespace

porcupine::Model<KvInput, KvOutput, KvState> MakeKvModel() {
  porcupine::Model<KvInput, KvOutput, KvState> m;
  m.Partition = KvPartition;
  m.Init = KvInit;
  m.Step = KvStep;
  m.Equal = KvEqual;
  m.DescribeOperation = KvDescribeOp;
  m.DescribeState = KvDescribeState;
  return m;
}

// 显式实例化由 porcupine.cpp 完成（这里 extern 已在 porcupine.h 声明）
// 不需要再写一次。

}  // namespace kvraft