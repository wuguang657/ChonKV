// client.cpp —— Clerk 实现（对应 Go 版 src/kvraft/client.go）
//
// 关键改动（相对 Lab 2 时期的版本）：
//   1. 每次成功 Get / Put / Append 都记录到 history_ 里 —— 给 Config::End()
//      收集起来跑 porcupine 做线性一致性检查。
//   2. t_call 在 RPC 发起前抓、t_return 在拿到 reply 后抓，单位纳秒。
//      单调时钟，不受系统时间跳变影响。

#include "client.h"

#include <chrono>
#include <cstdio>
#include <random>
#include <thread>
#include <utility>

#include "../common/util.h"  // raftcpp::RandEngine（让 NRand 受 SEED 控制）

namespace kvraft {

// ---------------------------------------------------------------------------
// Clerk 放弃前的总预算
// ---------------------------------------------------------------------------

#if defined(__SANITIZE_THREAD__) || \
    (defined(__has_feature) && __has_feature(thread_sanitizer))
constexpr std::chrono::seconds kClerkGiveUpTimeout{300};
#else
constexpr std::chrono::seconds kClerkGiveUpTimeout{60};
#endif

namespace {

// 放弃前打一条醒目日志 —— 让人一眼看出是"Clerk 主动放弃"而不是 Raft 丢数据。
void ReportGiveUp(const char* op, int64_t client_id, int64_t seq_id,
                  const std::string& key) {
  std::fprintf(stderr,
               "\n[kv-clerk] GIVE UP: %s(client=%lld seq=%lld key=\"%s\") "
               "超过 %llds 未完成，Clerk 主动放弃。\n"
               "           这可能是真的分区不可达；也可能是 window 太窄造成的"
               "假失败 —— 排查时先确认集群当时是否还在正常选主。\n",
               op, (long long)client_id, (long long)seq_id, key.c_str(),
               (long long)kClerkGiveUpTimeout.count());
  std::fflush(stderr);
}

}  // namespace

int64_t NRand() {
  std::uniform_int_distribution<int64_t> dist(0, (int64_t(1) << 62) - 1);
  return dist(raftcpp::RandEngine());
}

Clerk::Clerk(std::vector<std::shared_ptr<labrpc::ClientEnd>> servers)
    : servers_(std::move(servers)) {
  client_id_ = static_cast<int>(NRand() & 0x7fffffff);
  seq_id_ = 0;
  // 首跳随机化：Config::MakeClient 传的 servers_ 是原始顺序（下标 j ↔ server j，
  // 保证 hint 回填 reply.leader_id 后 servers_[leader_id] 连到的就是 leader 本身）。
  // "让所有客户端首跳不挤向同一台"的随机性在这里补 —— 与 Go 版
  // MakeClerk(random_handles(ends)) 的随机首跳等效，且不分裂下标空间。
  if (!servers_.empty()) {
    std::uniform_int_distribution<int> dist(0,
                                            static_cast<int>(servers_.size()) - 1);
    int start = dist(raftcpp::RandEngine());
    leader_id_ = start;
    rr_ = start;
  } else {
    leader_id_ = 0;
    rr_ = 0;
  }
}

// steady_clock::time_point → 纳秒（单调时间）
static int64_t MonoNs(std::chrono::steady_clock::time_point tp) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             tp.time_since_epoch())
      .count();
}

std::string Clerk::GetStale(const std::string& key) {
  if (servers_.empty()) return "";

  seq_id_++;
  GetArgs args;
  args.key = key;
  args.client_id = client_id_;
  args.seq_id = seq_id_;

  // 总超时兜底：防止"集群全挂/全不可达"时永不返回（与 Get 同款防护）。
  const auto deadline =
      std::chrono::steady_clock::now() + kClerkGiveUpTimeout;

  int leader = leader_id_;
  while (true) {
    if (std::chrono::steady_clock::now() >= deadline) {
      ReportGiveUp("GetStale", client_id_, seq_id_, key);
      return "";
    }
    GetReply reply;
    // 任一可达节点都能做陈旧读（无需 leader）。节点挂了（!ok）就换下一台轮询。
    bool ok = servers_[leader]->CallTyped("KVServer.GetStale", args, reply);
    if (!ok) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      leader = (leader + 1) % static_cast<int>(servers_.size());
      continue;
    }
    // GetStale 服务端只会返回 kOK / kNoKey（server.cpp:326 直读 kv_store_，无
    // leader/quorum/超时/会话/背压逻辑，构造上不可能返 kWrongLeader/kTimeout/
    // kBusy/kSessionGone）。节点不可达已由上面的 !ok 分支兜底，这里只处理成功
    // 返回的两种正常语义；其余枚举值显式分类，避免未来服务端演进时被"沉默吞噬"。
    switch (reply.err) {
      case Err::kOK:
        // 记住这台可达的：GetStale 不承诺 leader 语义，但"最近一次成功
        // 读到的那台"通常是最佳首跳候选（它活着且已 apply 到较新下标）。
        // 不更新的话，首跳节点挂掉后每次调用都要从同一台开始空转一轮。
        leader_id_ = leader;
        return reply.value;
      case Err::kNoKey:
        leader_id_ = leader;
        return "";
      case Err::kBusy:
        // 当前服务端不会返回；若将来 GetStale 加了背压，应与 Get 一致重试【同一台】
        // （轮询会让请求在节点间空转，甚至触发 GiveUp 丢写），故 leader 不变。
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        continue;  // 重试同一台
      case Err::kWrongLeader:
      case Err::kTimeout:
      case Err::kSessionGone:
      default:
        // 这些 GetStale 服务端都不可能返回。退化为换台重试，避免挂死。
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        leader = (leader + 1) % static_cast<int>(servers_.size());
        continue;
    }
  }
}

std::string Clerk::Get(const std::string& key) {
  if (servers_.empty()) return "";

  seq_id_++;
  int leader = leader_id_;

  GetArgs args;
  args.key = key;
  args.client_id = client_id_;
  args.seq_id = seq_id_;

  // history 记录：call_time 在 RPC 发起前抓（对应 Go 版的 time.Since(begin)）
  const auto t_call = std::chrono::steady_clock::now();

  // 总超时：兜底防止"永久分区 / 集群全挂"时客户端永不返回。取值依据见
  // 文件头 kClerkGiveUpTimeout 的注释（原来是 30s，偏窄，会造成假失败）。
  const auto deadline = std::chrono::steady_clock::now() + kClerkGiveUpTimeout;

  while (true) {
    if (std::chrono::steady_clock::now() >= deadline) {
      ReportGiveUp("Get", client_id_, seq_id_, key);
      return "";
    }
    GetReply reply;
    bool ok = servers_[leader]->CallTyped("KVServer.Get", args, reply);

    if (!ok) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      leader = (leader + 1) % static_cast<int>(servers_.size());
      continue;
    }

    const auto t_return = std::chrono::steady_clock::now();
    switch (reply.err) {
      case Err::kOK:
        leader_id_ = leader;
        rr_ = (leader + 1) % static_cast<int>(servers_.size());
        used_hint_ = 0;
        // 记录成功的 Get（只有这条进了 history；ErrWrongLeader/kTimeout 那
        // 些次不算操作 —— Go 版同样不记）
        RecordGet(key, reply.value, MonoNs(t_call), MonoNs(t_return));
        return reply.value;
      case Err::kNoKey:
        leader_id_ = leader;
        rr_ = (leader + 1) % static_cast<int>(servers_.size());
        used_hint_ = 0;
        RecordGet(key, "", MonoNs(t_call), MonoNs(t_return));
        return "";
      case Err::kBusy:
        // 背压：本节点就是 leader 但被限流，应稍后重试【同一台】，不要换 leader，
        // 也不要走 hint（hint 指向别处只会让请求在节点间空转、甚至触发 GiveUp 丢写）。
        used_hint_ = 0;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        continue;
      case Err::kWrongLeader:
      case Err::kTimeout:
      case Err::kSessionGone: {  // Get 不会被服务端 fencing（读不改会话），万一出现按重试处理
        // 重定向："首次"错 leader 时若带有效 hint（且不是当前节点自己），直连它走快路；
        // 但一旦采纳过 hint，就【抑制后续 hint、强制纯 round-robin】直到本操作成功。
        // 原因：某个节点若持续返回陈旧 hint（指向一个根本不是 leader 的节点），
        // 每轮都信它就会把客户端锁死在"1<->2"互指里（Concurrent3A 实测卡死）。
        // 抑制 hint 后，rr_ 游标会坚定不移地遍历全部节点，一定能覆盖到真 leader。
        // 绝大多数情况下首条 hint 就是真 leader，一步直达；只有选举抖动期才会退化成轮询。
        int n = static_cast<int>(servers_.size());
        int next;
        if (reply.leader_id >= 0 && reply.leader_id != leader && !used_hint_) {
          next = reply.leader_id;   // 一次性尝试 hint 指向的 leader
          used_hint_ = 1;           // 之后抑制 hint，强制轮询直到成功
        } else {
          next = rr_;               // 纯轮询兜底：保证覆盖全部节点
          // 注意：此处不把 used_hint_ 清零——保持抑制状态，直到 kOK/kBusy 才放行
        }
        rr_ = (next + 1) % n;
        leader = next;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        continue;
      }
    }
  }
}

Err Clerk::PutAppend(const std::string& key, const std::string& value,
                      const std::string& op) {
  if (servers_.empty()) return Err::kOK;

  seq_id_++;
  PutAppendArgs args;
  args.key = key;
  args.value = value;
  args.op = op;
  args.client_id = client_id_;
  args.seq_id = seq_id_;

  // history 记录
  const auto t_call = std::chrono::steady_clock::now();
  const uint8_t op_code =
      (op == "Put") ? KV_OP_PUT : KV_OP_APPEND;

  // 总超时：见文件头 kClerkGiveUpTimeout 的注释（原来是 30s，偏窄）。
  const auto deadline = std::chrono::steady_clock::now() + kClerkGiveUpTimeout;

  while (true) {
    if (std::chrono::steady_clock::now() >= deadline) {
      ReportGiveUp(op.c_str(), client_id_, seq_id_, key);
      // 标记 history 不可信：这次写可能已经在服务端生效，但没进 history。
      // Config::CheckLinearizability 会据此跳过判定，避免假 Illegal。
      gave_up_on_write_.store(true);
      return Err::kOK;
    }
    PutAppendReply reply;
    bool ok = servers_[leader_id_]->CallTyped("KVServer.PutAppend", args, reply);

    if (!ok) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      leader_id_ = (leader_id_ + 1) % static_cast<int>(servers_.size());
      continue;
    }

    const auto t_return = std::chrono::steady_clock::now();
    switch (reply.err) {
      case Err::kOK:
        rr_ = (leader_id_ + 1) % static_cast<int>(servers_.size());
        used_hint_ = 0;
        RecordPutAppend(key, value, op_code, MonoNs(t_call), MonoNs(t_return));
        return Err::kOK;
      case Err::kSessionGone:
        // 服务端协议兜底触发：本会话已被 fence（淘汰）。
        client_id_ = static_cast<int>(NRand() & 0x7fffffff);
        seq_id_ = 0;
        used_hint_ = 0;
        rr_ = (leader_id_ + 1) % static_cast<int>(servers_.size());
        return Err::kSessionGone;
      case Err::kNoKey:
        // Put/Append 不该返回这个；当成"换台重试"处理，避免死循环
        rr_ = (leader_id_ + 1) % static_cast<int>(servers_.size());
        used_hint_ = 0;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        leader_id_ = (leader_id_ + 1) % static_cast<int>(servers_.size());
        continue;
      case Err::kBusy:
        // 背压：本节点就是 leader 但被限流，重试【同一台】，不换 leader、不走 hint。
        used_hint_ = 0;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        continue;
      case Err::kWrongLeader:
      case Err::kTimeout: {
        // 重定向：与 Get 同一策略——"首次"错 leader 且带有效 hint 时直连快路，
        // 一旦采纳过 hint 就抑制后续 hint、强制纯 round-robin 直到成功，
        // 避免被某个节点的陈旧 hint 锁死在互指循环里。
        int n = static_cast<int>(servers_.size());
        int next;
        if (reply.leader_id >= 0 && reply.leader_id != leader_id_ && !used_hint_) {
          next = reply.leader_id;   // 一次性尝试 hint 指向的 leader
          used_hint_ = 1;           // 之后抑制 hint，强制轮询直到成功
        } else {
          next = rr_;               // 纯轮询兜底：保证覆盖全部节点
          // 不把 used_hint_ 清零（保持抑制，直到 kOK/kBusy 才放行）
        }
        rr_ = (next + 1) % n;
        leader_id_ = next;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        continue;
      }
    }
  }
}

Err Clerk::Put(const std::string& key, const std::string& value) {
  return PutAppend(key, value, "Put");
}

Err Clerk::Append(const std::string& key, const std::string& value) {
  return PutAppend(key, value, "Append");
}

// ---------------------------------------------------------------------------
// history 记录
// ---------------------------------------------------------------------------

void Clerk::RecordGet(const std::string& key, const std::string& value,
                      int64_t call_time_ns, int64_t return_time_ns) {
  KvOperation op;
  op.client_id = client_id_;
  op.seq_id = seq_id_;
  op.input.op = KV_OP_GET;
  op.input.key = key;
  op.input.value = "";
  op.output.value = value;
  op.call_time_ns = call_time_ns;
  op.return_time_ns = return_time_ns;
  std::lock_guard<std::mutex> lk(history_mu_);
  history_.push_back(std::move(op));
}

void Clerk::RecordPutAppend(const std::string& key, const std::string& value,
                            uint8_t op_code, int64_t call_time_ns,
                            int64_t return_time_ns) {
  KvOperation op;
  op.client_id = client_id_;
  op.seq_id = seq_id_;
  op.input.op = op_code;
  op.input.key = key;
  op.input.value = value;
  op.output.value = "";  // Put/Append 不返回 value
  op.call_time_ns = call_time_ns;
  op.return_time_ns = return_time_ns;
  std::lock_guard<std::mutex> lk(history_mu_);
  history_.push_back(std::move(op));
}

std::vector<KvOperation> Clerk::DrainHistory() {
  std::lock_guard<std::mutex> lk(history_mu_);
  std::vector<KvOperation> out;
  out.swap(history_);
  return out;
}

}  // namespace kvraft