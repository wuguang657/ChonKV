// server.cpp —— KV 服务器实现（对应 Go 版 src/kvraft/server.go）
//
// ===========================================================================
// 【本文件最要紧的一处：快照 "raft 已装 / KV 未装" 窗口】
// ===========================================================================
//
// Follower 收到 leader 的 InstallSnapshot 之后，要做两件【分属两把锁】的事：
//
//   ① raft 侧（raft 锁）：截断日志、推进 last_applied_ / snapshot_index_、
//      把 blob 与 raft state 落盘
//   ② KV 侧（KV 锁）：把快照字节反序列化，灌进 kv_store_ 状态机
//
// 如果崩在 ① 之后、② 之前：
//   * raft 重启后认为自己"快照已装"（snapshot_index_ 已经是新的），
//     再也不会重发这条 snapshot 消息
//   * 但 kv_store_ 还是旧的 → 状态机永久缺数据
//
// 本文件用两层把这个窗口闭合：
//
//   【第 1 层 · 顺序与幂等】（ApplierLoop 的 snapshot_valid 分支）
//     raft 侧已在 InstallSnapshot 内用 last_applied_ 守卫拦下陈旧快照，
//     所以 KV 侧【无条件安装】，只用自身的 last_cmd_index_ 防回退；
//     ApplySnapshotLocked 是幂等的（整体替换，重复装结果一样）。
//
//   【第 2 层 · 重启恢复】（构造函数）
//     启动时经 rf_->GetSnapshotData() 把已提交的快照装回状态机。
//     这是补刀：即便真的崩在 ① 和 ② 之间，重启时 KV 也能从磁盘恢复出来。
//     ⚠️ 必须走 raft 的接口而不是直接读 persister —— 盘上存的是带
//     index/term 头的磁盘格式，且可能是"阶段1 已落盘、阶段2 未提交"的 blob，
//     只有 raft 剥头校验过才知道该不该采用。
//     这一层对应 Go 版 StartKVServer 里的
//     `if kv.persister.ReadSnapshot() != nil { kv.installSnapshot(...) }`。
//
// 两层叠加后，无论崩在哪一刻，重启后都是自洽的。
// ===========================================================================

#include "server.h"

#include <chrono>
#include <utility>

namespace kvraft {

// ---------------------------------------------------------------------------
// 构造 / 析构
// ---------------------------------------------------------------------------

KVServer::KVServer(std::vector<std::shared_ptr<labrpc::ClientEnd>> peers, int me,
                   std::shared_ptr<raft::Persister> persister, int maxraftstate)
    : me_(me), maxraftstate_(maxraftstate), persister_(persister) {
  apply_ch_ = std::make_shared<raftcpp::Chan<raft::ApplyMsg>>();
  rf_ = std::make_shared<raft::Raft>(peers, me, persister, apply_ch_);

  // ======== T2/T3 修复 · 第 2 层：重启恢复 ========
  // 详见文件头注释。没有这一行，崩在"raft 已落盘 / KV 未安装"之间就会永久丢数据。
  // 【顺序必须早于 rf_->Start()】这是 3B 偶发丢数据的根因：Start() 一起后台线程，
  // ApplyLoop 立刻就能把 index > snapshot_index_ 的条目推进 apply_ch_，
  // ApplierLoop 在【kv_store_ 还是空】的状态下执行了这批 Put/Append；
  // 紧接着才用快照整体替换 kv_store_，把这批刚写进去的修改回滚掉，
  // 而 last_applied_ 已经推过去不会重放 → 表现为 key 消失 / got []。
  if (persister_) {
    // ⚠️ 必须走 raft 的 GetSnapshotData()，不要再直接 persister_->ReadSnapshot()：
    //   盘上存的是【带 index/term 头的磁盘格式】，而且可能是"阶段1 已落盘、
    //   阶段2 未提交"的 blob。raft 已经剥头并校验过"是否已提交"，这里拿到
    //   的才是可信的裸状态机数据。
    std::string snap = rf_->GetSnapshotData();
    int snap_idx = rf_->SnapshotIndex();
    // 判据必须是 snap_idx > 0，不能是 snap 非空：
    //   若 blob 被判为未提交而丢弃，snap 会是空，但 last_cmd_index_ 仍必须
    //   推到 snap_idx —— 否则 ReadIndex 路径（等 last_cmd_index_ >= ri）
    //   会永久卡住，因为快照覆盖的下标永远不会再被 apply 上来。
    if (snap_idx > 0) {
      std::lock_guard<std::mutex> lk(mu_);
      if (!snap.empty()) ApplySnapshotLocked(snap);
      last_cmd_index_ = snap_idx;
    }
  }

  rf_->Start();  // ✅ 状态机恢复完毕，才允许 Raft 派发新条目
}

KVServer::~KVServer() {
  if (!dead_.load()) Kill();
}

void KVServer::Start() {
  applier_ = std::thread([this] { ApplierLoop(); });
}

void KVServer::Kill() {
  dead_.store(true);

  // 先关 channel：applier 会把队列里剩下的消息消费完，然后 Pop 返回 false 退出
  apply_ch_->Close();

  // 唤醒所有卡在 WaitOp 里的 RPC handler，让它们尽快返回（否则客户端 RPC 挂死）
  {
    std::lock_guard<std::mutex> lk(mu_);
    for (auto& p : msg_replies_) {
      p.second.done = true;
      p.second.ok = false;
    }
  }
  apply_cv_.notify_all();

  // 只有既未 join 也未 detach 的线程才可 join，否则会抛异常。
  if (applier_.joinable()) applier_.join();
  if (rf_) rf_->Kill();
}

// ---------------------------------------------------------------------------
// 快照编解码
// ---------------------------------------------------------------------------

// 格式：[kv 条数][k,v]*[lastSeq 条数][cid,sid]*
// 全部用 length-prefixed 的 Bytes，value 里含任意字节都不会解析错。
std::string KVServer::EncodeSnapshotLocked() const {
  labrpc::Encoder e;
  e.Int(static_cast<int>(kv_store_.size()));
  for (const auto& p : kv_store_) {
    e.Bytes(p.first).Bytes(p.second);
  }
  e.Int(static_cast<int>(last_seq_.size()));
  for (const auto& p : last_seq_) {
    e.Int(p.first).Int(p.second);
  }
  return e.Take();
}

// 幂等：整体替换，重复 apply 同一个 blob 结果完全一样。
// 解码中途失败就整体丢弃（保留当前状态机），绝不让半截数据污染。
void KVServer::ApplySnapshotLocked(const std::string& blob) {
  labrpc::Decoder d(blob);

  int n = 0;
  if (!d.Int(n) || n < 0) return;
  std::map<std::string, std::string> store;
  for (int i = 0; i < n; i++) {
    std::string k, v;
    if (!d.Bytes(k) || !d.Bytes(v)) return;
    store[k] = v;
  }

  int m = 0;
  if (!d.Int(m) || m < 0) return;
  std::map<int, int> seq;
  for (int i = 0; i < m; i++) {
    int cid = 0, sid = 0;
    if (!d.Int(cid) || !d.Int(sid)) return;
    seq[cid] = sid;
  }

  if (!d.Ok()) return;

  kv_store_ = std::move(store);
  last_seq_ = std::move(seq);
}

// 在 KV 锁内判断要不要压缩、并把快照字节编码好。
// 【为什么必须在锁内编码】否则编码到一半状态机被 applier 改了，
// 编出来的快照就是一个"撕裂"的中间状态。
//
// 对齐 Go 版 kvraft server.go 的 saveSnapshot：
//   只在 Raft 状态大小超过阈值时才真正生成快照（控制落盘频率）。
//   raft 的 snapshot_index_（lastIncludedIndex）始终由已【应用】的命令推进，
//   与是否落盘无关——这点由 Raft::Snapshot 的 index 参数保证。
bool KVServer::PrepareSnapshotLocked(int index, int* snap_index,
                                     std::string* blob) {
  if (maxraftstate_ == -1) return false;               // 关闭快照
  if (!persister_) return false;
  if (persister_->RaftStateSize() < maxraftstate_) return false;   // 阈值门控

  *snap_index = index;
  *blob = EncodeSnapshotLocked();
  return true;
}

std::map<std::string, std::string> KVServer::SnapshotStore() const {
  std::lock_guard<std::mutex> lk(mu_);
  return kv_store_;
}

// ---------------------------------------------------------------------------
// RPC handler
// ---------------------------------------------------------------------------

// Get 走 ReadIndex（线性一致读优化，不再为只读请求写一遍 raft 日志）：
//   1) rf_->ReadIndex() 返回"已提交下标"的线性化点 ri。仅在 leader 且凑齐多数派时
//      ri >= 0；非 leader / 凑不齐多数派返回 -1，让 Clerk 换台重试。
//   2) 等状态机 apply 到那个下标（last_cmd_index_ >= ri），再【持锁直读】本地状态机。
//   注意 value / err 必须在【持锁临界区】内拷出：释放锁后再读 kv_store_ 会被
//   后续已提交的 Put 改写 → 读到比自身 linearization point 更晚的值 →
//   porcupine 判 Illegal。
void KVServer::Get(const GetArgs& args, GetReply& reply) {
  // 线性化点：ReadIndex 内部已保证"仅 leader 且凑齐多数派"才返回 >-1 的下标。
  // 非 leader / 凑不齐多数派返回 -1，让 Clerk 换台。
  int ri = rf_->ReadIndex();
  if (ri < 0) {
    // 非 leader：把认知到的 leader 编号回填，供 Clerk 重定向直连。
    // 但若是"自认为是 leader 却被隔离"（ri<0 仅因凑不齐多数派），GetLeaderId()
    // 会返回 me_ 自己 —— 回填会让 Clerk 重定向回本节点死循环，故置 -1 退回轮询。
    int lid = rf_->GetLeaderId();
    reply.leader_id = (lid == me_ || lid < 0) ? -1 : lid;
    reply.err = Err::kWrongLeader;
    return;
  }

  std::unique_lock<std::mutex> lk(mu_);

  const auto kPoll = std::chrono::milliseconds(100);
  // ⚠️ 总超时：被隔离在少数派的旧 leader 仍认为自己是 leader，但 ReadIndex 永远
  // 凑不齐多数派 → ri 之后的状态机永不推进 → Get 会死等，客户端 RPC 永不返回。
  // 1s 超时即视为"我不是有效 leader"，返回 WrongLeader 让 Clerk 换台机器重试。
  const auto kWaitTimeout = std::chrono::milliseconds(1000);
  auto wait_start = std::chrono::steady_clock::now();

  while (true) {
    // ★ 状态机已追上线性化点：持锁直读并拷出（坑1：临界区内拷，防并发改写）
    if (last_cmd_index_ >= ri) {
      auto it = kv_store_.find(args.key);
      if (it != kv_store_.end()) {
        reply.value = it->second;
        reply.err = Err::kOK;
      } else {
        reply.value.clear();
        reply.err = Err::kNoKey;
      }
      return;
    }

    if (dead_.load()) { reply.err = Err::kWrongLeader; return; }

    // 等 applier 推进 last_cmd_index_：wait_for 期间自动释放 mu_，
    // 被 notify 或 100ms 到点都会醒来重新检查条件。
    apply_cv_.wait_for(lk, kPoll, [&] {
      return last_cmd_index_ >= ri || dead_.load();
    });

    if (std::chrono::steady_clock::now() - wait_start > kWaitTimeout) {
      // 超时：本节点自认为是 leader 但被隔离（凑不齐多数派）—— 此时 leader_id_
      // 指向的是"自己"，若回填会让 Clerk 重定向回同一台死循环。故显式置 -1，
      // 让 Clerk 退回盲目轮询去另找活着的 leader。
      reply.leader_id = -1;
      reply.err = Err::kWrongLeader;
      return;
    }
  }
}

void KVServer::PutAppend(const PutAppendArgs& args, PutAppendReply& reply) {
  Op op;
  op.key = args.key;
  op.value = args.value;
  op.method = args.op;  // "Put" or "Append"
  op.client_id = args.client_id;
  op.seq_id = args.seq_id;

  int leader = -1;
  reply.err = WaitOp(op, &leader);
  reply.leader_id = leader;  // 仅 kWrongLeader 分支会填成有效编号，其余为 -1
}

// ---------------------------------------------------------------------------
// WaitOp：提交命令并等它被 apply
// ---------------------------------------------------------------------------
//
// 为什么必须带超时轮询，而不能死等？
//   一个少数派 leader 提交了日志但永远凑不齐多数派 → 这条日志永远不会被提交。
//   如果 RPC handler 死等，客户端就永久挂住。Go 版的注释也写了这一点：
//   "当Start发给少数派领导在分区修复后卸任时，阻塞的RPC处理器将永远无法解除阻塞"。
Err KVServer::WaitOp(const Op& op, int* out_leader_id) {
  if (out_leader_id) *out_leader_id = -1;  // 默认"不知道 leader"

  raft::StartResult r = rf_->Start(op.Serialize());
  // ---- C1 背压：我是 leader 但被限流 → 返回 kBusy ----
  // 关键点：之前这里只判 !r.is_leader，会把"背压限流"也当成 kWrongLeader，
  // 客户端于是换台重试 —— 但背压恰恰发生在 leader 身上，换台毫无意义，
  // 反而可能把本应稍后重试的请求在节点间打转、甚至触发 GiveUp 丢写。
  // 现在区分开：backpressure=true 专指"我是 leader、稍后重试同一台即可"。
  if (r.backpressure) return Err::kBusy;
  if (!r.is_leader) {
    // 真不是 leader：把认知到的 leader 编号回填，客户端据此重定向直连。
    // 自环守卫：若本节点误以为自己是 leader（leader_id_==me_，选举瞬间的
    // 陈旧认知），回填会让客户端重定向回自己死循环 —— 置 -1 退回轮询。
    if (out_leader_id) {
      int lid = rf_->GetLeaderId();
      *out_leader_id = (lid == me_ || lid < 0) ? -1 : lid;
    }
    return Err::kWrongLeader;
  }

  std::unique_lock<std::mutex> lk(mu_);
  msg_replies_[r.index] = NotifyMsg{false, false, op.client_id, op.seq_id};

  const auto kPoll = std::chrono::milliseconds(100);
  // ⚠️ 总超时：被隔离在少数派的"旧 leader"仍认为自己是 leader（isLeader 一直
  // 为 true），但它提交的日志永远凑不齐多数派 → 命令永不 apply → WaitOp 会死等、
  // 客户端 RPC 永不返回，分区用例（3A progress-in-majority / 3B partition）直接卡死。
  // 1s 超时即视为"我不是有效 leader"，返回 WrongLeader 让 Clerk 换台机器重试
  // （连多数派的新 leader 就能成功）。正常命令几十 ms 内 commit+apply，远小于 1s。
  const auto kWaitTimeout = std::chrono::milliseconds(1000);
  auto wait_start = std::chrono::steady_clock::now();

  while (true) {
    apply_cv_.wait_for(lk, kPoll, [&] {
      auto it = msg_replies_.find(r.index);
      return it != msg_replies_.end() && it->second.done;
    });

    if (dead_.load()) {  // 服务器被 Kill 了
      msg_replies_.erase(r.index);
      return Err::kWrongLeader;
    }

    auto it = msg_replies_.find(r.index);
    if (it != msg_replies_.end() && it->second.done) {
      // ok=false 说明这个 index 上的命令已被新 leader 覆盖成别的内容，
      // 返回 WrongLeader 让 Clerk 换台重试。
      bool ok = it->second.ok;
      msg_replies_.erase(it);
      if (ok) return Err::kOK;
      // 被新 leader 覆盖了：把新 leader 编号回填，客户端重定向直连。
      // 同样加自环守卫（见上方 !r.is_leader 分支），避免把"自己"当重定向地址。
      if (out_leader_id) {
        int lid = rf_->GetLeaderId();
        *out_leader_id = (lid == me_ || lid < 0) ? -1 : lid;
      }
      return Err::kWrongLeader;
    }

    // ⚠️ 总超时（见上方说明）：被隔离的旧 leader 上 WaitOp 不能无限等。
    if (std::chrono::steady_clock::now() - wait_start > kWaitTimeout) {
      msg_replies_.erase(r.index);
      return Err::kWrongLeader;
    }
    // 没超时，继续等下一轮
  }
}

// ---------------------------------------------------------------------------
// ApplierLoop：消费 applyCh
// ---------------------------------------------------------------------------
void KVServer::ApplierLoop() {
  raft::ApplyMsg m;
  while (apply_ch_->Pop(m)) {
    if (m.command_valid) {
      // no-op 空条目：raft 新 leader 上任提交的占位命令，本身不写状态机，
      // 只用来推进 commitIndex。
      // ⚠️ 但必须照样推进 last_cmd_index_：ReadIndex() 返回的线性化点 ri 可能
      // 正好落在 no-op 的下标上，若这里直接跳过，Get 的"等 last_cmd_index_ >= ri"
      // 就会永远差 1 卡死（实测 lci=836 / ri=837，恰好差一个 no-op）。
      // 安全性：no-op 不写状态机，不存在"下标已推进但 kv_store_ 还没改"的窗口。
      if (m.command.empty()) {
        {
          std::lock_guard<std::mutex> lk(mu_);
          last_cmd_index_ = m.command_index;
        }
        apply_cv_.notify_all();
        continue;
      }

      Op op;
      // 真实命令：空命令已在上面处理。
      if (!op.Deserialize(m.command)) continue;

      bool need_snap = false;
      int snap_index = 0;
      std::string snap_blob;

      {
        std::lock_guard<std::mutex> lk(mu_);

        // ---- exactly-once 语义 ----
        // 客户端重试会产生重复命令，靠 (client_id, seq_id) 去重。
        // 只有严格更大的 seq 才执行，保证"至少一次提交 + 至多一次执行"。
        if (op.seq_id > last_seq_[op.client_id]) {
          if (op.method == "Put") {
            kv_store_[op.key] = op.value;
          } else if (op.method == "Append") {
            kv_store_[op.key] += op.value;
          }
          // 只有 Put / Append 两种：Get 走 ReadIndex 直读，不会进这个分支。
          last_seq_[op.client_id] = op.seq_id;
        }

        // ---- 通知等待者 ----
        // 必须校验身份：这个 index 上的命令可能已经被新 leader 覆盖成别的了。
        // 身份不符就告诉等待者"你这条没生效"，让它换 leader 重试。
        auto it = msg_replies_.find(m.command_index);
        if (it != msg_replies_.end()) {
          it->second.done = true;
          it->second.ok = (it->second.client_id == op.client_id &&
                           it->second.seq_id == op.seq_id);
        }

        // ---- 准备快照（在锁内编码，保证编出来的是一个一致的瞬间）----
        need_snap =
            PrepareSnapshotLocked(m.command_index, &snap_index, &snap_blob);

        // ⚠️ 必须放在状态机突变【之后】：ReadIndex 路径靠它判定"已追上线性化点"，
        // 顺序反了会让 Get 读到旧值。
        last_cmd_index_ = m.command_index;
      }  // ---- 释放 KV 锁 ----

      apply_cv_.notify_all();

      // ⚠️ 锁外再调 Raft：避免"持 KV 锁跨进 raft 锁"
      if (need_snap) {
        rf_->Snapshot(snap_index, snap_blob);
      }

    } else if (m.snapshot_valid) {
      // InstallSnapshot 已在 raft 侧用 last_applied_ 守卫拦下陈旧快照，
      // 所以 KV 这里【必须】安装 —— 否则会出现「raft 已把 last_applied_ 推进到
      // idx，但 KV 拒装 → 被快照覆盖的 [last_cmd+1, idx] 那段 entry 再无来源 →
      // 整键空 / got []」的丢数据。
      // 防回退只靠 KV 自身的 last_cmd_index_：比已应用过的更旧才跳过。
      {
        std::lock_guard<std::mutex> lk(mu_);
        if (m.snapshot_index >= last_cmd_index_) {
          ApplySnapshotLocked(m.snapshot);
          last_cmd_index_ = m.snapshot_index;
        }
      }
      // 与 no-op / command 分支保持一致：装快照同样可能让某条 ReadIndex 读成立。
      apply_cv_.notify_all();
    }
  }
}

// ---------------------------------------------------------------------------
// 工厂函数
// ---------------------------------------------------------------------------
std::shared_ptr<KVServer> StartKVServer(
    std::vector<std::shared_ptr<labrpc::ClientEnd>> peers, int me,
    std::shared_ptr<raft::Persister> persister, int maxraftstate) {
  auto kv = std::make_shared<KVServer>(peers, me, persister, maxraftstate);
  kv->Start();  // 构造完才能起 applier 线程
  return kv;
}

}  // namespace kvraft
