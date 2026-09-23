// client.h —— KV 客户端 Clerk（对应 Go 版 src/kvraft/client.go）
//
// Clerk 要和一群 KVServer 打交道，但只有 leader 能处理写请求，所以核心逻辑
// 就是"找对人"：挨个试，碰到 ErrWrongLeader / 网络不通就换下一台，退避 20ms。
//
// exactly-once 语义靠两个字段：
//   client_id_：建 Clerk 时随机生成，标识"我是谁"
//   seq_id_   ：每次请求 +1，标识"这是我的第几个请求"
// 服务端用 last_seq_[client_id] 去重，重复请求（客户端重试）不会被执行两次。

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "../labrpc/labrpc.h"
#include "common.h"
#include "kv_model.h"

namespace kvraft {

// 生成一个随机 id（对应 Go 的 nrand()，用 crypto/rand 取 0..2^62）
int64_t NRand();

class Clerk {
 public:
  explicit Clerk(std::vector<std::shared_ptr<labrpc::ClientEnd>> servers);

  Clerk(const Clerk&) = delete;
  Clerk& operator=(const Clerk&) = delete;

  // 取一个 key 的值；不存在返回 ""
  std::string Get(const std::string& key);
  void Put(const std::string& key, const std::string& value);
  void Append(const std::string& key, const std::string& value);

  int client_id() const { return client_id_; }
  int seq_id() const { return seq_id_; }

  // 本 Clerk 是否曾在 Put/Append 上超时放弃（Get 放弃不算 —— 读不改状态，
  // 少记一条 history 不影响线性一致性判定）。
  //
  // 用途：Config::CheckLinearizability 据此判断 history 是否可信。
  // 放弃过的写可能已经在服务端生效却没进 history → porcupine 会把它判成
  // Illegal（"值是凭空出现的"），那是假失败。
  bool gave_up_on_write() const { return gave_up_on_write_.load(); }

  // 把 Clerk 内部记录的全部 op 历史取走（一次性转移所有权）。
  // 调用后内部 history_ 清空 —— 让 Config 多次调不会重复追加。
  std::vector<KvOperation> DrainHistory();

 private:
  // Put / Append 共用
  void PutAppend(const std::string& key, const std::string& value,
                 const std::string& op);

  // 把一次成功返回的 op 追加到 history_。
  // Get 拿真实 value；Put/Append 用空 KvOutput（KV 协议对 Put 不返回 value）。
  void RecordGet(const std::string& key, const std::string& value,
                 int64_t call_time_ns, int64_t return_time_ns);
  void RecordPutAppend(const std::string& key, const std::string& value,
                       uint8_t op_code, int64_t call_time_ns,
                       int64_t return_time_ns);

  std::vector<std::shared_ptr<labrpc::ClientEnd>> servers_;
  int client_id_ = 0;
  int seq_id_ = 0;
  int leader_id_ = 0;  // 上次成功那台的编号，下次优先试它
  // 重定向兜底用的 round-robin 游标：保证"一定能遍历到所有节点"。
  // 客户端每次失败都让 rr_ 前移 1，所以即便 hint 全是陈旧的（在 0<->2 之间
  // 互指、绕开真正的 leader），也能靠 rr_ 兜底覆盖到真正的 leader。
  // used_hint_：是否已经采纳过 hint 快路。逻辑是"首次错 leader 且 hint 有效
  // 就直连它走一步；一旦采纳过，就【永久抑制后续 hint、强制纯轮询】直到本操作
  // 成功"。原因：某个节点若持续返回陈旧 hint 指向非 leader 节点，朴素"一直信
  // hint"会把客户端锁死在 1<->2 互指（Concurrent3A 实测卡死 60s GiveUp）。
  // 绝大多数情况下首条 hint 即真 leader 一步直达，只有选举抖动期才退化轮询。
  int rr_ = 0;
  int used_hint_ = 0;

  // 是否在 Put/Append 上触发过 GiveUp（供 CheckLinearizability 判断 history 可信度）
  std::atomic<bool> gave_up_on_write_{false};

  // 操作历史（供 Config::CheckLinearizability 使用）。单 Clerk 单线程访问，
  // 不需要锁；但 DrainHistory 在外部线程调用，所以用 mutex 兜底。
  std::mutex history_mu_;
  std::vector<KvOperation> history_;
};

}  // namespace kvraft
