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
// Go 版 Clerk 是【永不放弃】的（client.go:57-86 / :107-134 的 for 循环没有
// 任何退出条件）—— 分区期间它就一直重试，直到分区 heal。C++ 这边保留一个兜底
// 上限，是为了"集群真的全挂了"时不至于把整个测试进程永久挂住。
//
// ⚠️ 这个值不能设小了，否则会制造 **Go 基准不会复现的假失败**。算一下账：
//    发往【断连】节点的 RPC 在网络层会被模拟延迟 rand()%7000 ms 才返回失败
//    （labrpc.cpp:188-190，与 Go labrpc.go:294-300 逐字一致；kvraft 建网时
//    开了 LongDelays(true)，config.cpp:31）。
//    5 台机、断了 3 台时，光走完一圈的上限就是 3×7000 ≈ 21s。
//    原来给 30s —— 只够一圈多一点，很容易在"集群其实马上就能服务"的时候
//    提前放弃。给 60s ≈ 2.8 圈余量，正常分区 heal 的时间尺度绰绰有余。
//
//    另外注意：超时后 Get 返回 ""、PutAppend 静默 return，而那条命令其实有可能
//    已经在服务端生效了 —— 客户端以为没做成、状态机里却有，于是 CheckClntAppends
//    报 want/got 不一致。这类假失败排查起来极具迷惑性，所以放弃时必须打日志
//    （见 ReportGiveUp），不能真"静默"。
constexpr std::chrono::seconds kClerkGiveUpTimeout{60};

namespace {

// 放弃前打一条醒目日志 —— 让人一眼看出是"Clerk 主动放弃"而不是 Raft 丢数据。
void ReportGiveUp(const char* op, int64_t client_id, int64_t seq_id,
                  const std::string& key) {
  std::fprintf(stderr,
               "\n[kv-clerk] ⚠️ GIVE UP: %s(client=%lld seq=%lld key=\"%s\") "
               "超过 %llds 未完成，Clerk 主动放弃。\n"
               "           这可能是真的分区不可达；也可能是 window 太窄造成的"
               "假失败 —— 排查时先确认集群当时是否还在正常选主。\n",
               op, (long long)client_id, (long long)seq_id, key.c_str(),
               (long long)kClerkGiveUpTimeout.count());
  std::fflush(stderr);
}

}  // namespace

int64_t NRand() {
  // ⚠️ 原来是 `static std::mt19937_64 rng(std::random_device{}())` + 一把 mutex。
  //    和 test_kvraft.cpp 里那个本地 RandInt 是同一类问题：拿真随机源播种，
  //    SEED 环境变量【完全管不到】它 —— "固定种子可复现"对 client_id 不成立，
  //    偶发的去重 bug 复现不出来。
  //    改走 raftcpp::RandEngine()：splitmix64 派生流 + thread_local 引擎，
  //    既受 SEED 控制，又顺带省掉了锁。
  std::uniform_int_distribution<int64_t> dist(0, (int64_t(1) << 62) - 1);
  return dist(raftcpp::RandEngine());
}

Clerk::Clerk(std::vector<std::shared_ptr<labrpc::ClientEnd>> servers)
    : servers_(std::move(servers)) {
  // ⚠️ 原来是 static_cast<int>(NRand())：NRand 返回 0..2^62-1，截断到 32 位 int
  //    可能得到【负数】。服务端用 last_seq_[client_id] 做去重，负数当 map key
  //    虽然也能工作，但取值范围被无谓地砍到一半（碰撞概率翻倍），且看着像 bug。
  //    这里掩码到非负 31 位。
  //
  //    另外注意：client_id 是【服务端去重用的身份】，必须每个 Clerk 唯一。
  //    不要为了"对齐 Go 的 porcupine ClientId"把它改成 0..nclients-1 ——
  //    Go 里 porcupine 的 ClientId 是 goroutine 编号（只用于画图，
  //    porcupine/model.go:6 写着 "optional, unless you want a visualization"），
  //    与 Clerk 自己的 nrand() 去重 id 是两回事。真改成固定编号的话，
  //    第 2 轮新建的 Clerk 会复用编号 0 且 seq 从 1 重新计数，撞上第 1 轮残留的
  //    last_seq_[0]，所有写都被当成重复请求丢弃 —— 那才是灾难。
  client_id_ = static_cast<int>(NRand() & 0x7fffffff);
  seq_id_ = 0;
  leader_id_ = 0;
}

// steady_clock::time_point → 纳秒（单调时间）
static int64_t MonoNs(std::chrono::steady_clock::time_point tp) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             tp.time_since_epoch())
      .count();
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
      case Err::kTimeout: {
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

void Clerk::PutAppend(const std::string& key, const std::string& value,
                      const std::string& op) {
  if (servers_.empty()) return;

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
      return;
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
        return;
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

void Clerk::Put(const std::string& key, const std::string& value) {
  PutAppend(key, value, "Put");
}

void Clerk::Append(const std::string& key, const std::string& value) {
  PutAppend(key, value, "Append");
}

// ---------------------------------------------------------------------------
// history 记录
// ---------------------------------------------------------------------------

void Clerk::RecordGet(const std::string& key, const std::string& value,
                      int64_t call_time_ns, int64_t return_time_ns) {
  KvOperation op;
  op.client_id = client_id_;
  op.seq_id = seq_id_;          // 🐞 给 dump 字段加料（去重 bug 排查用）
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
  op.seq_id = seq_id_;          // 🐞 给 dump 字段加料（去重 bug 排查用）
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