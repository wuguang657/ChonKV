// kv_model.h —— KV 服务的线性一致性模型（对应 Go 版 src/models/kv.go）
//
// KV 模型的核心：
//   Init : 空字符串（每个 key 单独一个模型实例，KV 测试按 key 分桶）
//   Step : get 必须返回"当前值"；put 替换；append 拼接
//          op==GET 必须满足 out.Value == state，否则非法（不一致）
//
// 这个 model 之所以只需要"每个 key 一个 string"就够了：
//   porcupine 把 history 按 key 切成多个 sub-history，每条 sub-history
//   单独跑 Step(state, ...)。state 只承载"这个 key 当前的值"即可。

#pragma once

#include <cstdint>
#include <string>

#include "porcupine.h"

namespace kvraft {

// Op 类型必须和 Go 版 models.KvInput.Op 一致
constexpr uint8_t KV_OP_GET = 0;
constexpr uint8_t KV_OP_PUT = 1;
constexpr uint8_t KV_OP_APPEND = 2;

struct KvInput {
  uint8_t op = 0;
  std::string key;
  std::string value;

  bool operator==(const KvInput& o) const {
    return op == o.op && key == o.key && value == o.value;
  }
};

struct KvOutput {
  std::string value;

  bool operator==(const KvOutput& o) const { return value == o.value; }
};

using KvState = std::string;  // 单 key 当前的值

// 拿一个 KV model。调用方需要在用之前设置好 Partition / Init / Step / Equal。
// 默认 Init 是空串、Step 实现 put/append/get 三种语义、Equal 是 ==
// Partition 默认按 key 分桶。
porcupine::Model<KvInput, KvOutput, KvState> MakeKvModel();

// 便捷别名
using KvOperation = porcupine::Operation<KvInput, KvOutput>;
using KvCheckResult = porcupine::CheckResult;

// CheckOperations 的显式实例化在 porcupine.cpp 里做（链接时只生成一份
// 机器码）。调用方只要 #include "kv_model.h" 就够，编译器不会再生成副本。

}  // namespace kvraft