// server.cpp —— KV 服务器实现（对应 Go 版 src/kvraft/server.go）
//
// ===========================================================================
// 【本文件最重要的两处：T2/T3 窗口修复】
// ===========================================================================
//
// 先回顾一下 Lab 2 里那个致命窗口是什么。Follower 收到 leader 的
// InstallSnapshot 之后，要做两件【分属两把锁】的事：
//
//   ① raft 侧（raft 锁）：CondInstallSnapshot() 截断日志、推进
//      last_applied_/snapshot_index_、PersistLocked() 落盘
//   ② KV 侧（KV 锁）：把快照字节反序列化，灌进 kv_store_ 状态机
//
// 如果崩在 ① 之后、② 之前：
//   * raft 重启后认为自己"快照已装"（snapshot_index_ 已经是新的），
//     再也不会重发这条 snapshot 消息
//   * 但 kv_store_ 还是旧的 → 状态机永久缺数据
//
// 本文件用两层把这个窗口彻底闭合：
//
//   【第 1 层 · 顺序与幂等】（ApplierLoop 的 snapshot_valid 分支）
//     让 raft 先裁决（CondInstallSnapshot 内部会拒绝任何"回退"的快照：
//     index <= last_applied_ 或 index <= snapshot_index_ 直接返回 false），
//     raft 说"可以"之后 KV 才装。这样绝不会出现"装了更旧的快照"，
//     且 ApplySnapshotLocked 是幂等的（整体替换，重复装结果一样）。
//
//   【第 2 层 · 重启恢复】（构造函数末尾）
//     启动时主动 persister_->ReadSnapshot() 把快照装回状态机。
//     这是补刀：即便真的崩在 ① 和 ② 之间，重启时 KV 也能从磁盘上的
//     blob 恢复出来 —— 因为 InstallSnapshot 的 RPC handler 在推消息给
//     applier 【之前】就已经 SaveStateAndSnapshot() 落盘了。
//     这一层正是 Go 版 server.go 的做法（StartKVServer 里的
//     `if kv.persister.ReadSnapshot() != nil { kv.installSnapshot(...) }`）。
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
    std::string snap = persister_->ReadSnapshot();
    if (!snap.empty()) {
      std::lock_guard<std::mutex> lk(mu_);
      ApplySnapshotLocked(snap);
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

  if (applier_.joinable()) applier_.join();  // 对非join,detach的线程可以join，否则会报错
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
  std::string blob = e.Take();
  return blob;
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

std::string KVServer::EncodeSnapshot() const {
  std::lock_guard<std::mutex> lk(mu_);
  return EncodeSnapshotLocked();
}

std::map<std::string, std::string> KVServer::SnapshotStore() const {
  std::lock_guard<std::mutex> lk(mu_);
  return kv_store_;
}

// ---------------------------------------------------------------------------
// RPC handler
// ---------------------------------------------------------------------------

// Get 也要走一遍 raft（不能直读本地状态机）：
// 否则会读到 stale 数据（leader 已经切走了，本地还没同步）。
void KVServer::Get(const GetArgs& args, GetReply& reply) {
  Op op;
  op.key = args.key;
  op.method = "Get";
  op.client_id = args.client_id;
  op.seq_id = args.seq_id;

  // value / err 在 WaitOp 内部由 ApplierLoop 在持锁的临界区捕获并拷出，
  // 本函数【不要】在 WaitOp 返回后再读 kv_store_，否则中间窗口可能被
  // 后续已提交的 Put 改写，Get 会读到比它自身 linearization point 更晚的值
  // → porcupine 判 Illegal（见 server.h NotifyMsg 注释）。
  Err e = WaitOp(op, &reply.value, &reply.err);
  if (e != Err::kOK) {
    reply.err = e;
    return;
  }
  // reply.value / reply.err 已由 WaitOp 灌好（kNoKey / kOK + 对应 value）
}

void KVServer::PutAppend(const PutAppendArgs& args, PutAppendReply& reply) {
  Op op;
  op.key = args.key;
  op.value = args.value;
  op.method = args.op;  // "Put" or "Append"
  op.client_id = args.client_id;
  op.seq_id = args.seq_id;

  reply.err = WaitOp(op, nullptr, nullptr);
}

// ---------------------------------------------------------------------------
// WaitOp：提交命令并等它被 apply
// ---------------------------------------------------------------------------
//
// 为什么必须带超时轮询，而不能死等？
//   一个少数派 leader 提交了日志但永远凑不齐多数派 → 这条日志永远不会被提交。
//   如果 RPC handler 死等，客户端就永久挂住。Go 版的注释也写了这一点：
//   "当Start发给少数派领导在分区修复后卸任时，阻塞的RPC处理器将永远无法解除阻塞"。
//   所以每 100ms 醒一次，检查"我还是不是 leader"，不是就赶紧返回让客户端换台机器。
Err KVServer::WaitOp(const Op& op, std::string* out_value, Err* out_err) {
  raft::StartResult r = rf_->Start(op.Serialize());
  if (!r.is_leader) return Err::kWrongLeader;

  std::unique_lock<std::mutex> lk(mu_);
  msg_replies_[r.index] = NotifyMsg{false, false, op.client_id, op.seq_id, "", Err::kOK};

  const auto kPoll = std::chrono::milliseconds(100);
  // ⚠️ 总超时：被隔离在少数派的"旧 leader"仍认为自己是 leader（isLeader 一直
  // 为 true），但它提交的日志永远凑不齐多数派 → 命令永不 apply → WaitOp 会死等、
  // 客户端 RPC 永不返回，分区用例（3A progress-in-majority / 3B partition）直接卡死。
  // 1s 超时即视为"我不是有效 leader"，返回 WrongLeader 让 Clerk 换台机器重试
  // （连多数派的新 leader 就能成功）。正常命令几十 ms 内 commit+apply，远小于 1s。
  const auto kWaitTimeout = std::chrono::milliseconds(1000);
  auto wait_start = std::chrono::steady_clock::now();

  while (true) {
    // & 按引用捕获 lambda 体内用到的所有外部变量，这里用到了r和msg_replies_
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
      bool ok = it->second.ok;
      // ⚠️ 关键：在持锁临界区内把 value / err 拷出来，调用方收到后再用，
      // 完全避免"WaitOp 释放锁后，调用方再读 kv_store_"这条有数据竞争的路径。
      if (out_value) *out_value = it->second.value;
      if (out_err) *out_err = it->second.err;
      msg_replies_.erase(it);
      return ok ? Err::kOK : Err::kWrongLeader;
    }

    // 超时：确认自己还是不是 leader。
    // ⚠️ GetState() 要抢 raft 的锁 —— 必须先释放 mu_ 再调，
    //    绝不能"持 KV 锁跨进 raft 锁"。
    lk.unlock();
    bool still_leader = rf_->GetState().second;
    lk.lock();

    if (!still_leader) {
      msg_replies_.erase(r.index);
      return Err::kWrongLeader;
    }
    // ⚠️ 总超时（见上方说明）：被隔离的旧 leader 上 WaitOp 不能无限等。
    if (std::chrono::steady_clock::now() - wait_start > kWaitTimeout) {
      msg_replies_.erase(r.index);
      return Err::kWrongLeader;
    }
    // 还是 leader 且没超时，继续等下一轮
  }
}

// ---------------------------------------------------------------------------
// ApplierLoop：消费 applyCh
// ---------------------------------------------------------------------------
void KVServer::ApplierLoop() {
  raft::ApplyMsg m;
  while (apply_ch_->Pop(m)) {
    if (m.command_valid) {
      Op op;
      // raft 新 leader 上任可能提交 no-op 空条目（Command 为空字符串），
      // 这是占位条目，只用来推进 commitIndex，绝不能当真实命令执行。
      // 先显式跳过空命令，避免依赖 Deserialize 失败这个隐式副作用（更稳健）。
      if (m.command.empty()) continue;
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
          // "Get" 不改状态机，走 raft 只是为了线性读
          last_seq_[op.client_id] = op.seq_id;
        }

        // ---- 给 Get waiter 准备返回值（修复 porcupine 抓到的 Illegal）----
        //
        // 必须在持锁临界区把 value/err 灌进 msg_replies_，
        // 因为 WaitOp 释放锁后调用方读 kv_store_ 的窗口里，另一个已 apply 的 op
        // 可能已经把状态改了 —— Get 就会读到比它自身 linearization point 更晚的值。
        // 这里先把状态固化进 NotifyMsg，WaitOp 再原样拷给调用方。
        //
        // 注意：哪怕是 dedup（seq_id <= last_seq_）也要做，
        // 因为这是一个新 index 的新 waiter，需要"此刻 K 的值"作为返回值。
        {
          auto reply_it = msg_replies_.find(m.command_index);
          if (reply_it != msg_replies_.end() && op.method == "Get") {
            auto it_state = kv_store_.find(op.key);
            if (it_state != kv_store_.end()) {
              reply_it->second.value = it_state->second;
              reply_it->second.err = Err::kOK;
            } else {
              reply_it->second.value.clear();
              reply_it->second.err = Err::kNoKey;
            }
          }
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

        last_cmd_index_ = m.command_index;  // 排错用
      }  // ---- 释放 KV 锁 ----

      apply_cv_.notify_all();

      // ⚠️ 锁外再调 Raft：避免"持 KV 锁跨进 raft 锁"
      if (need_snap) {
        rf_->Snapshot(snap_index, snap_blob);
      }

    } else if (m.snapshot_valid) {
      // ======== 对齐 Go 版 kvraft（raft.go:239-244）========
      // InstallSnapshot 已在 raft 锁内用 last_applied_ 守卫拦下陈旧快照
      // （raft.cpp:980），所以这里【必须】无条件安装——否则会出现
      // 「raft 已把 last_applied_ 推进到 idx，但 CondInstallSnapshot 的门控把
      // 快照拒了 → 被快照覆盖的 [last_cmd+1, idx] 那段 entry 再无来源 →
      // 整键空 / got []」的丢数据。
      // 仅用 KV 自身的 last_cmd_index_ 防回退：快照比已应用过的更旧才跳过
      // （不会出现「装旧 snap 覆盖新状态」）。
      if (m.snapshot_index >= last_cmd_index_) {
        std::lock_guard<std::mutex> lk(mu_);
        ApplySnapshotLocked(m.snapshot);
        last_cmd_index_ = m.snapshot_index;
      }
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
