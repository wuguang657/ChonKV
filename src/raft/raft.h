// raft.h —— Raft 共识算法的 C++ 骨架（对应 Go 版 src/raft/raft.go）
//
// ===========================================================================
// 给你的任务：只改 raft.cpp，不动 raft.h / config.* / test_raft.cpp / labrpc
// ===========================================================================
//
// 分三个阶段，和 MIT 6.824 Lab 2 一一对应：
//
//   2A 领导者选举   StartElection(), RequestVote(), AppendEntries(),
//                   ElectionTimerLoop()          → 测试：*2A*
//   2B 日志复制     Start(), ApplyLoop(), ReplicateLoop(),
//                   AppendEntries 的日志部分     → 测试：*2B*
//   2C 持久化       Persist(), ReadPersist()     → 测试：*2C*
//
// 【日志下标的约定 —— 这个必须先搞清楚，否则 2B 一定写错】
//
//   logs_ 是一个 vector，logs_[0] 是"哨兵"，它的 index 是 0，term 是 0。
//   真实日志从 index 1 开始。这样：
//
//       logs_[k].index == k          （下标 = 日志索引，不用到处 ±1）
//       LastLogIndex() == logs_.size() - 1
//       Start() 返回的下标 = LastLogIndex() + 1
//
//   注意：官方测试 TestBasicAgree2B 要求"第一条命令的下标是 1"。
//   所以【不要在当选时追加 no-op 空日志】，否则第一条命令会变成下标 2，
//   测试直接挂。
//
// 【三条铁律，C++ 版尤其重要】
//
//   1. 绝不持锁发 RPC。mu_ 只能保护内存里的状态；网络调用可能耗时几秒，
//      持锁会让整个状态机停摆（心跳、选举、回包处理全部饿死）。
//   2. 绝不在锁内 sleep 或阻塞等待。
//   3. RPC 回包回来后，必须重新检查"我还是不是我以为的那个状态"
//      （term 变了没？还是 candidate 吗？），再改状态。
//      这是 split vote / term 暴增类 bug 的头号来源。

#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "../common/chan.h"
#include "../common/thread_tracker.h"
#include "../common/util.h"
#include "../labrpc/labrpc.h"
#include "persister.h"

namespace raft {

// 日志里存的命令。Go 版是 interface{}，C++ 里我们用字符串，
// 到了 Lab 3（KV 服务）你可以把它塞成任意序列化的字节。
using Command = std::string;

// ---------------------------------------------------------------------------
// 常量：选举超时 / 心跳间隔
// ---------------------------------------------------------------------------

// 官方 tester 要求：选举必须在 1 秒内完成，心跳每秒不超过 10 次。
// 取 150~300ms 是经典的取值（和 Go 版一致）。
constexpr int kElectionTimeoutMin = 150;  // ms
constexpr int kElectionTimeoutMax = 300;  // ms
constexpr int kHeartbeatInterval = 50;    // ms

// 同一个 follower，两次 AppendEntries 之间的最小间隔。
// 复制是"发射后不管"的，没有这个节流，follower 一旦慢/断线，
// leader 会在一个 while 循环里疯狂往外打 RPC。
constexpr int kMinSendIntervalMs = 10;
// 带日志的 AppendEntries 发出去后，超过这个时间还没回音就重发
// （回信丢了也要能恢复，否则日志永远推不过去）
constexpr int kLogRetryMs = 300;

// ---------------------------------------------------------------------------
// 数据结构
// ---------------------------------------------------------------------------

enum class ServerState {
  kFollower = 0,
  kCandidate,
  kLeader,
};
// 多个.cpp文件同时引入该头文件，调用该函数时，得加inline，否则会报错
inline const char* StateName(ServerState s) {
  switch (s) {
    case ServerState::kFollower: return "Follower";
    case ServerState::kCandidate: return "Candidate";
    case ServerState::kLeader: return "Leader";
  }
  return "???";
}

struct LogEntry {
  int term = 0;
  int index = 0;
  Command command;
};

// 提交给上层状态机的消息
struct ApplyMsg {
  bool command_valid = false;
  Command command;
  int command_index = 0;

  // Lab 3（快照）才会用到，这里先留着
  bool snapshot_valid = false;
  std::string snapshot;
  int snapshot_term = 0;
  int snapshot_index = 0;
};

// ---- RPC 消息 ----
// 每个结构都有 Serialize()/Deserialize()，网络层靠它们把对象变成字节流。
// 顺序必须严格一致：编码写进去几个字段、什么顺序，解码就怎么读出来。

struct RequestVoteArgs {
  int term = 0;
  int candidate_id = 0;
  int last_log_index = 0;
  int last_log_term = 0;

  std::string Serialize() const;
  bool Deserialize(const std::string& s);
};

struct RequestVoteReply {
  int term = 0;
  bool vote_granted = false;

  std::string Serialize() const;
  bool Deserialize(const std::string& s);
};

// ---- 预投票 RPC（lab 增强：治 term 暴涨）----
struct RequestPreVoteArgs {
  int term = 0;
  int candidate_id = 0;
  int last_log_index = 0;
  int last_log_term = 0;

  std::string Serialize() const;
  bool Deserialize(const std::string& s);
};

struct RequestPreVoteReply {
  int term = 0;
  bool vote_granted = false;

  std::string Serialize() const;
  bool Deserialize(const std::string& s);
};

struct AppendEntriesArgs {
  int term = 0;
  int leader_id = 0;
  int prev_log_index = 0;
  int prev_log_term = 0;
  std::vector<LogEntry> entries;   // 空 = 纯心跳
  int leader_commit = 0;

  std::string Serialize() const;
  bool Deserialize(const std::string& s);
};

struct AppendEntriesReply {
  int term = 0;
  bool success = false;
  // 快速回退优化：失败时告诉 leader "你下次从这个下标开始试"
  // 没有这个优化，TestBackup2B 会跑很久（leader 一次只退一格）
  int next_index = 0;

  std::string Serialize() const;
  bool Deserialize(const std::string& s);
};

// 快照 RPC（Lab 3 日志压缩）：leader 给落后太多的 follower 直接发整块快照，
// 跳过漫长的日志重放。data 是上层状态机的序列化 blob，Raft 不解析。
struct InstallSnapshotArgs {
  int term = 0;                   // leader 的 currentTerm
  int leader_id = 0;              // 便于 follower 重定向客户端
  int last_included_index = 0;    // 快照覆盖的最后一条日志下标的逻辑 index
  int last_included_term = 0;     // 该下标的 term
  std::string data;               // 快照内容（状态机 blob）

  std::string Serialize() const;
  bool Deserialize(const std::string& s);
};

struct InstallSnapshotReply {
  int term = 0;  // follower 当前 term，leader 据以退位

  std::string Serialize() const;
  bool Deserialize(const std::string& s);
};

// Start() 的返回值。Go 版返回 (index, term, isLeader) 三元组，
// C++ 里没有多返回值，包成结构体更清楚。
struct StartResult {
  bool is_leader = false;
  int index = -1;
  int term = 0;
};

// ---------------------------------------------------------------------------
// Raft
// ---------------------------------------------------------------------------

// 继承 enable_shared_from_this类模版 是为了让 RPC 的异步回信回调能安全地持有
// 一个 shared_ptr<Raft>。
//
// 为什么必须这样？回信是网络线程池调过来的，时间点完全不可控：
// 万一这时候 Raft 已经被测试框架释放了（crash1 之后），回调里再摸
// this 就是赤裸裸的野指针。持有一份 shared_ptr，就能保证"只要还有
// 在途的 RPC，对象就还活着"。
class Raft : public std::enable_shared_from_this<Raft> {
 public:
  Raft(std::vector<std::shared_ptr<labrpc::ClientEnd>> peers, int me,
       std::shared_ptr<Persister> persister,
       std::shared_ptr<raftcpp::Chan<ApplyMsg>> apply_ch);
  ~Raft();

  Raft(const Raft&) = delete;
  Raft& operator=(const Raft&) = delete;

  // ---- 以下四个是测试框架和上层服务会调用的（已实现，不用改）----

  // 启动后台线程（选举定时器 / 复制线程 / 提交循环）。
  //
  // 为什么不在构造函数里直接起线程？
  // 因为 ReplicateLoop 要用 shared_from_this()，而在构造函数执行期间
  // 对象还没被 shared_ptr 接管，调用它只会抛出 std::bad_weak_ptr。
  // 所以拆成两步：make_shared 构造完 → 再调 Start()。
  void Start();

  // 返回 (currentTerm, 我是否是 leader)
  std::pair<int, bool> GetState();

  // 上层服务（比如 KV 服务器）提交一条命令。
  // 不是 leader 就返回 is_leader=false。
  StartResult Start(const Command& command);

  // 关掉这个 Raft 实例：停掉后台线程、取消所有在途 RPC。
  // 注意：持久化状态保存在 persister 里，不会被清掉。
  void Kill();
  bool killed() const { return killed_.load(); }

  // 调试用：一行文字描述当前状态
  std::string LogStatus();
  int me() const { return me_; }

  // ---- 快照测试 / 调试访问器（不影响生产逻辑）----
  // ⚠️ TSan 修复：这四个调试访问器原本是无锁 inline 读，会被 main 线程的
  // CheckConsistency() 并发读到（worker 线程在 mu_ 锁内写 last_applied_ 等），
  // 触发 data race（raft.cpp:885 ApplyLoop 写 last_applied_）。改为在锁内读
  // （mu_ 是 mutable，const 方法可锁，无重入死锁）。
  int LogSize() const {
    std::lock_guard<std::mutex> lk(mu_);
    return static_cast<int>(logs_.size());
  }
  int SnapshotIndex() const {
    std::lock_guard<std::mutex> lk(mu_);
    return snapshot_index_;
  }
  int CommitIndex() const {
    std::lock_guard<std::mutex> lk(mu_);
    return commit_index_;
  }
  int LastApplied() const {
    std::lock_guard<std::mutex> lk(mu_);
    return last_applied_;
  }

  // ---- RPC handler：由网络层在收到消息时调用 ----
  // 这两个函数【必须自己加锁】，因为网络线程会并发调用它们。

  // TODO(2A)：实现投票逻辑
  void RequestVote(const RequestVoteArgs& args, RequestVoteReply& reply);
  void RequestPreVote(const RequestPreVoteArgs& args, RequestPreVoteReply& reply);
  void StartRealElection();  // 预投票拿到多数派后，真正自增 term 发起真实投票
  void SendRequestVoteRPCs();  // 真实投票 RPC 发送(StartRealElection 与 candidate 超时重发共用)
  // TODO(2A)：实现心跳；TODO(2B)：追加日志冲突检测
  void AppendEntries(const AppendEntriesArgs& args, AppendEntriesReply& reply);

  // ---- 快照（Lab 3 日志压缩）----
  // 上层状态机（KV 服务）调用：到 index 为止的状态机已持久化，
  // 让 Raft 截断日志、把快照存盘。index 必须 <= commit_index_。
  void Snapshot(int index, const std::string& snapshot);

  // 上层在收到 snapshot ApplyMsg 后调用，确认可以安全安装。
  // 返回 false 表示状态机已经 apply 了更新的，别回退。
  bool CondInstallSnapshot(int index, int term, const std::string& snapshot);

  // 快照 RPC：leader 给落后太多的 follower 发送。
  void InstallSnapshot(const InstallSnapshotArgs& args, InstallSnapshotReply& reply);

 private:
  // ---- 后台线程（骨架已经帮你起好了，你需要实现循环体）----

  // 选举定时器 + leader 心跳定时器（2A）
  void ElectionTimerLoop();

  // 发起一轮选举：自提 → 并行拉票（2A）
  void StartElection();

  // 给第 server 号 follower 同步日志 / 发心跳（2A 心跳，2B 日志）
  void ReplicateLoop(int server);

  // 把已提交的日志投递到 apply_ch_（2B）
  void ApplyLoop();

  // ---- 持久化（2C）----

  // 把需要持久化的状态写进 persister_。调用前必须持有 mu_。
  void PersistLocked();
  // 从字节流恢复状态。Make() 时调用。
  void ReadPersist(const std::string& data);

  // ---- 小工具。约定：名字以 Locked 结尾的，调用时必须已经持有 mu_ ----
  int LastLogIndexLocked() const {
    return logs_.empty() ? 0 : logs_.back().index;
  }
  int LastLogTermLocked() const {
    return logs_.empty() ? 0 : logs_.back().term;
  }
  // 任期变大了 → 退回 follower。调用前必须持有 mu_。
  void ConvertToFollowerLocked(int new_term);

  mutable std::mutex mu_;

  // ---- 构造时定下来的，之后不变（读它们不需要加锁）----
  std::vector<std::shared_ptr<labrpc::ClientEnd>> peers_;
  std::shared_ptr<Persister> persister_;
  int me_ = 0;

  std::atomic<bool> killed_{false};
  // Kill() 时置 true，让所有在途的 RPC 立即返回失败，
  // 这样后台线程能迅速退出，不会卡在 7 秒的模拟延迟里。
  std::atomic<bool> cancel_rpcs_{false};

  // ---- 2A：所有 server 都有的状态 ----
  ServerState state_ = ServerState::kFollower;
  int current_term_ = 0;
  int voted_for_ = -1;    // 本任期把票投给了谁，-1 = 还没投
  int num_votes_ = 0;     // 本轮选举收到的票数
  int num_prevotes_ = 0;  // 预投票

  // 最近一次"听到合法 leader / 给别人投了票"的时刻。
  // 选举超时是相对它来算的。
  raftcpp::TimePoint last_heartbeat_;
  // 用来唤醒选举定时器（收到心跳 / 被 Kill 时 notify）
  std::condition_variable tick_cv_;

  // ---- 2B：日志 ----
  std::vector<LogEntry> logs_;    // logs_[0] 是哨兵，index = snapshot_index_
  int commit_index_ = 0;
  int last_applied_ = 0;
  std::shared_ptr<raftcpp::Chan<ApplyMsg>> apply_ch_;

  // ---- 2D：快照（日志压缩）----
  // 快照覆盖到最后这条日志（逻辑 index）。logs_[0].index 始终 == snapshot_index_，
  // 因此"逻辑 index - snapshot_index_" = 在 logs_ 中的下标。
  int snapshot_index_ = 0; // 快照覆盖到最后这条日志（逻辑 index）
  int snapshot_term_ = 0;  // 快照覆盖到最后这条日志（逻辑 term）
  std::string snapshot_data_;      // 快照内容（状态机 blob，Raft 不解析）
  bool snapshot_dirty_ = false;   // 快照 blob 自上次落盘后变过没

  // applyCh 单发送者：InstallSnapshot 在锁内把 snapshot 挂到 pending_snapshot_，
  // 由 ApplyLoop 在锁内取出、锁外 push。绝不允许 InstallSnapshot 自己也
  // push，否则会和 ApplyLoop 派发的 log entries 互相穿插，KVServer 状态错乱。
  // 仿 Go 版 src/raft/raft.go:133 的 pendingSnapshot 字段。
  std::optional<ApplyMsg> pending_snapshot_;

  // ---- 2B：只有 leader 用的状态 ----
  std::vector<int> next_index_;   // 下一个要发给该 follower 的日志下标
  std::vector<int> match_index_;  // 已经确认该 follower 复制到的最高下标

  // 心跳计数器：leader 每 kHeartbeatInterval 自增一次。
  // 每个复制线程记下自己上次处理到几号心跳（last_heartbeat_seq_），
  // 两者不一样就该发一次心跳了。
  //
  // 为什么需要它？复制线程的等待条件必须是"有没有活儿干"：
  //   - 有日志没发出去 → 立刻发（Start() 之后零延迟）
  //   - 心跳该发了     → 发
  //   - 都没有         → 睡
  // 如果条件只写"我是 leader"，wait 会永远立即返回 → 空转，
  // 一秒钟能打出几万次 RPC，TestCount2B 直接挂。
  // leader 只需 heartbeat_seq_++ + notify_all() 这一行，所有 n-1 个 ReplicateLoop 都被唤醒，各自比对发现"序号变了"就发心跳。leader 不用维护"每个 follower 该不该发"的状态，也不用手动调每个线程——序号广播 + 每线程自比，自动去中心化分发。
  uint64_t heartbeat_seq_ = 0;
  std::vector<uint64_t> last_heartbeat_seq_;

  // 每个 follower：上次真正发出 AppendEntries 的时刻（用于节流）
  std::vector<raftcpp::TimePoint> last_send_time_;
  // 是否已经有一份"带日志"的 AppendEntries 在途（在途就先只发空心跳，
  // 避免同一批日志被重复发送 —— TestRPCBytes2B 会数字节）
  std::vector<bool> inflight_log_;
  std::vector<raftcpp::TimePoint> inflight_log_time_;

  // Start() 追加日志、或心跳计数变化时 notify_all() 唤醒所有复制线程
  std::condition_variable replicator_cv_;
  std::condition_variable apply_cv_;
};

// 把 Raft 对象包装成一个可以被 labrpc 调用的 Service。
// 相当于 Go 版的 labrpc.MakeService(rf)，只是 C++ 没有反射，得手写方法表。
inline std::shared_ptr<labrpc::Service> MakeRaftService(
    std::shared_ptr<Raft> rf) {
  using labrpc::Service;

  // lambda对象会随着net一直存活，rf引用计数+1
  Service::Handler request_vote = [rf](const std::string& args) -> std::string {
    RequestVoteArgs a;
    a.Deserialize(args);
    RequestVoteReply r;
    rf->RequestVote(a, r);
    return r.Serialize();
  };
  Service::Handler request_prevote = [rf](const std::string& args) -> std::string {
    RequestPreVoteArgs a;
    a.Deserialize(args);
    RequestPreVoteReply r;
    rf->RequestPreVote(a, r);
    return r.Serialize();
  };
  Service::Handler append_entries =
      [rf](const std::string& args) -> std::string {
    AppendEntriesArgs a;
    a.Deserialize(args);
    AppendEntriesReply r;
    rf->AppendEntries(a, r);
    return r.Serialize();
  };
  Service::Handler install_snapshot =
      [rf](const std::string& args) -> std::string {
    InstallSnapshotArgs a;
    a.Deserialize(args);
    InstallSnapshotReply r;
    rf->InstallSnapshot(a, r);
    return r.Serialize();
  };
  return std::make_shared<Service>(
      "Raft", std::unordered_map<std::string, Service::Handler>{
                  {"RequestVote", request_vote},
                  {"RequestPreVote", request_prevote},
                  {"AppendEntries", append_entries},
                  {"InstallSnapshot", install_snapshot},
              });
}

}  // namespace raft
