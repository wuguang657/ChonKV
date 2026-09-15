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
#include <unordered_set>

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
// CheckQuorum：leader 每隔这么久自查一次"我还能不能联系上多数派"。
// 取选举超时的下界，保证被分区隔离的旧 leader 能在一个选举超时内主动退位，
// 而不是傻等到被新 leader 的心跳赶下台（那样它还会继续以旧身份应答读请求）。
// 如果我在 follower 会等不及、跑去选新 leader 的那段时间（150ms）里，都没确认联系上多数派 → 我被分区了 → 我主动退位。
constexpr int kCheckQuorumIntervalMs = kElectionTimeoutMin;  // 150ms
constexpr int kClockDriftBoundMs = 25;
constexpr int kLeaderLeaseMs = kElectionTimeoutMin - kClockDriftBoundMs;  // 125ms
// CheckQuorum：判定某个 follower "真断连" 所需的【连续】RPC 失败次数上限。
// 不可靠网络有 10% 丢包，单条回包丢失（ok=false）是常态，绝不能当成断连；
// 只有"连续 N 次 RPC 都失败"才说明对方真的联系不上（被分区 / 宕机）。
// 取 5：10% 丢包下"连续 5 次全丢"的概率 ≈ 1e-5，几乎不会误杀；而真断连时
// leader 每 ~50ms 发一次心跳，250ms 内就累计到 5 → 及时退位（远小于 Isolated 测试的 1000ms 观察窗）。
constexpr int kCQMaxConsecFail = 5;

// CheckQuorum 的【第二路存活信号】：last_ack_time_（follower 最近一次 ok=true 回包时刻）的陈旧窗口。
// 这是一条【不依赖回调计数】的权威可达性判断：只要最近一个窗口内收到过 ok=true，就说明链路通。
// 真断连时 ok=true 永远到不了 → last_ack_time_ 被"冻住"，越过本窗口即判死，150ms 远小于 Isolated 测试的
// 1000ms 观察窗。
// 注：labrpc.cpp SendReq 的断连分支现已改为【快速失败】（~100ms 回 ok=false），所以主信号 cq_consec_fail_
// 也能在 ~350ms 累计到阈值抓住真断连；本窗口是 belt-and-suspenders，二者用"或"关系互补，互不打架：
//   · cq_consec_fail_ 容忍【回包延迟但终会到达】的长重排（Figure8 不可靠场景）；
//   · last_ack_time_ 窗口兜住【回包永远不到】的真断连（Disconnect 隔离场景）。
constexpr int kCQStaleWindowMs = 150;
// 可开关：true = 读走 LeaseRead 快路径（租约内跳过心跳确认）；
//         false = 每轮都用心跳确认（安全，默认）。
constexpr bool kEnableLeaseRead = false;
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
  int read_ctx = 0;

  std::string Serialize() const;
  bool Deserialize(const std::string& s);
};

struct AppendEntriesReply {
  int term = 0;
  bool success = false;
  // 快速回退优化：失败时告诉 leader "你下次从这个下标开始试"
  // 没有这个优化，TestBackup2B 会跑很久（leader 一次只退一格）
  int next_index = 0;
  int read_ctx = 0;

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

struct ReadIndexCtx {
  int term = 0;        // 发起读时的 currentTerm，term 一变该 ctx 作废
  int read_index = 0;  // 发起读那一刻的 commitIndex（本次读至少要看到它）
  int ack_count = 0;   // 已收到的确认票数（含 leader 自己那一票）
  bool done = false;   // 是否已攒够多数派
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
  int ReadIndex();
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
  // 【新增】取「已提交且有效」的快照 blob（已剥掉磁盘格式头，裸状态机数据）。
  // KVServer 重启恢复应走这里，不要直接 persister_->ReadSnapshot()——
  // 后者拿到的是带 header 的磁盘格式，且可能是「尚未提交」的 blob。
  std::string GetSnapshotData() const {
    std::lock_guard<std::mutex> lk(mu_);
    return snapshot_data_;
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

  // ---- snapshot blob 落盘（两阶段中的阶段1）----
  //
  // 把 (index, term, raw_blob) 编码后写入 persister 的 snapshot 槽。
  //
  // ★★ 本函数存在的全部意义：让 blob 的慢 I/O 发生在 raft 主锁 mu_ 之外 ★★
  //   真实磁盘上这里就是 write()+fsync()，几百 MB 的快照要几百 ms～几秒。
  //   若放在 mu_ 内（旧实现就是如此，因为只有 SaveStateAndSnapshot 能写
  //   blob），期间心跳、读心跳、AppendEntries 全部拿不到锁 → follower 收不
  //   到心跳而超时选举、leader 被 CheckQuorum 判"联系不上多数派"而误退位。
  //   etcd 的选主风暴就是这条链路。
  //
  // 本函数【不需要调用方持有 mu_】，内部会短暂持锁做单调性检查：
  //   snap_io_mu_ 串行化所有 blob 写（避免两个并发写互相覆盖成更旧的），
  //   mu_ 只用来读写 saved_blob_index_（O(1)，纳秒级）。
  // 返回 false 表示「盘上已有 index >= 本 index 的 blob，无需重写」。
  bool SaveSnapshotBlob(int index, int term, const std::string& raw_blob);

  // ---- 小工具。约定：名字以 Locked 结尾的，调用时必须已经持有 mu_ ----
  int LastLogIndexLocked() const {
    return logs_.empty() ? 0 : logs_.back().index;
  }
  int LastLogTermLocked() const {
    return logs_.empty() ? 0 : logs_.back().term;
  }
  // 任期变大了 → 退回 follower。调用前必须持有 mu_。
  void ConvertToFollowerLocked(int new_term);
  // 统计"最近 window_ms 毫秒内确认过我"的节点数（含 leader 自己）。
  // CheckQuorum（我还能联系上多数派吗）和 Leader Lease（租约还成立吗）
  // 都基于这同一个计数。调用前必须持有 mu_。
  //
  // 为什么 leader 自己要特殊处理？因为 leader 不会给自己发 AppendEntries，
  // last_ack_time_[me_] 不会像 follower 那样被心跳回包刷新，所以它必须
  // 无条件算一票，否则刚当选的 leader 会把自己判成"联系不上多数派"而退位。
  int CountRecentAcksLocked(int window_ms) const; // 承诺该函数不会修改对象状态
  // 当前配置里的成员个数。所有"多数派"判定（选举计票、日志提交、
  // CheckQuorum、ReadIndex）都必须用它，而不是 peers_.size() ——
  // 因为 peers_ 里可能还挂着"已移除"或"尚未加入"的节点，把它们算进
  // 多数派会直接破坏安全性（已移除的节点不该有能力凑出多数派）。
  // 调用前必须持有 mu_。
  int MemberCountLocked() const;

    // 给所有 follower 发一轮"带 read_ctx 的空心跳"，用于 ReadIndex 的身份确认。
  // 必须在锁外调用（RPC 异步，不能持 mu_ 发网络）。
  void BroadcastReadHeartbeat(int ctx);
  // 回包到达时按 ctx 把这一票记到对应读请求上；攒够多数派就标记 done 并唤醒。
  // 调用前必须持有 mu_。
  void RecordReadAckLocked(int ctx);

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
  // 裸的状态机 blob（不含磁盘头）。恢复时由 persister 读出、剥头校验后填入。
  std::string snapshot_data_;

  // ---- snapshot blob 两阶段落盘：把大 blob 的 I/O 挪出 raft 主锁 ----
  //
  // 背景：原来 blob 只能经 Persister::SaveStateAndSnapshot() 跟 state 一起
  // 原子写，而它是在 raft 主锁 mu_ 内调的 → 真盘上一个几百 MB 的快照
  // fsync（几百 ms～几秒）会把心跳/读心跳/选举全饿死（etcd 踩过的生产事故）。
  //
  // 改成两阶段：
  //   阶段1（mu_ 外）：只落 blob（自带 index/term 头）  ← 慢 I/O 在这里
  //   阶段2（mu_ 内）：截断日志 + 推进 snapshot_index_ + 持久化 state
  // 崩溃一致性由「blob 自带 index」保证：恢复时若 blob.index != state 里的
  // snapshot_index_，说明阶段2 没走完，blob 视为未提交丢弃，从完整日志重放。
  //
  // saved_blob_index_：盘上 blob 对应的 index，保证「盘上 blob 的 index 单调
  // 不减」。若放任旧 blob 覆盖新 blob，而 state 已截断到更新 index，恢复时
  // 丢弃该 blob 就再也重放不出来（日志已截断）→ 真丢数据。
  int saved_blob_index_ = 0;

  // 只用于串行化 blob 落盘 I/O。★ 关键：慢 I/O 发生在 mu_ 之外，
  // 期间不占 raft 主锁。锁序恒为 snap_io_mu_ → mu_，且全项目仅
  // SaveSnapshotBlob 一处按此序取锁，无反向路径，不会死锁。
  std::mutex snap_io_mu_;

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

  // ---- 生产级扩展 ①：CheckQuorum / Leader Lease / ReadIndex ----
  // 【last_ack_time_】leader 侧记录：第 i 个 follower 最近一次"成功回复"
  // AppendEntries 的时刻。
  //   * CheckQuorum 靠它数"最近一个选举超时窗口内我还能联系上几个"；
  //   * Leader Lease 靠它算租约起点（多数派里第 ⌈n/2⌉ 新的那个 ack 时刻）。
  // 只有 leader 会写它，且一律在 mu_ 锁内；follower/candidate 侧无意义。
  std::vector<raftcpp::TimePoint> last_ack_time_;
  raftcpp::TimePoint last_quorum_check_;
  // 【cq_consec_fail_】CheckQuorum liveness：第 i 个 follower 的【连续】RPC 失败计数。
  //   * 某次 AppendEntries/InstallSnapshot 回包 ok=true（哪怕很迟才到）→ 清零；
  //   * 回包 ok=false（丢包 / 真断连）→ 累加。
  // 用它（而非 last_ack_time_ 的"到达时刻"）判定存活，才能区分两种本质不同的情况：
  //   · 长重排导致的"回包延迟"——回包终会 ok=true 到达 → 清零 → 视为可达（不误杀）；
  //   · 真断连/宕机——回包持续 ok=false → 累计到 kCQMaxConsecFail → 视为不可达（退位）。
  std::vector<int> cq_consec_fail_;

  // 【lease_expire_】租约到期时刻 = 多数派确认时刻 + kLeaderLeaseMs。
  // 只有 LeaseRead 打开时才被用来跳过心跳确认。
  raftcpp::TimePoint lease_expire_;

  // 【ReadIndex 一轮"读确认"的状态机】
  // 唤醒"正在等多数派确认"的读线程
  std::condition_variable read_cv_;

    // 【ReadIndex 读确认：per-request ctx 设计】
  std::atomic<int> next_read_ctx_{1};            // 全局唯一 ctx 号（单调递增，永不复用）
  std::unordered_map<int, ReadIndexCtx> read_ctxs_;  // ctx -> 该条读的状态，受 mu_ 保护

  // ReadIndex 线性一致读的硬性前提：本 leader 必须先把一条自己 term 的 entry
  // （no-op，见 become-leader 块）提交，commit_index_ 才能覆盖上任前已提交的 entry。
  // 否则 commit_index_ 是旧值（甚至 0），据此读的会是"还没把 append 应用进状态机"
  // 的 stale 状态（分区用例偶发 wrong appends / 空读）。
  // read_safe_commit_ = 本 term 必须 commit 到的最小下标，一个 term 内只设一次
  // （当选时）；之后 commit_index_ 只会更大，始终 >= 它。ReadIndex 据此拦截"还没
  // 安全"的读，让上层 Clerk 重试，等 no-op 提交后自然满足。

  // 客户端在旧 leader 上 Append 成功（写到了 index ~100，已提交、已 apply、客户端拿到 OK）。然后旧 leader 挂了，新 leader 上任。
  // 95（已提交）， 96-100（未提交）, 101（no-op）
  // 新 leader 的 commit_index_ 还停在 95
  // 新 leader 不能靠"数副本个数"来提交旧 term 的 entry。​ 它必须先提交一条自己 term 的 entry，这条一提交，前面所有 entry 才被"顺带"间接提交。
  // 此时来一个 Get：
  // ReadIndex() 返回 ri = commit_index_ = 95
  // Get 等 last_cmd_index_ >= 95 → 早就成立（applier 只 apply 到 commit_index_，所以它停在 95）
  // 持锁直读 kv_store_ → 那个 index 100 的 Append 根本还没进状态机
  // 客户端拿到一个比自己上一次读还要旧的值 → 违反线性一致性
  int read_safe_commit_ = 0;
   // ---- 生产级扩展 ③：成员变更 ----
  // 【is_member_】is_member_[i] == true 表示节点 i 在当前配置 C 里。
  // 只有成员才参与：选举计票、日志提交的多数派判定、CheckQuorum 计数、
  // ReadIndex 确认。非成员（已被移除的 / 还没正式加入的）一律不计数。
  // 这个数组就是论文里"C_old 与 C_new 的多数派必有交集"那条论证的落地点。
  std::vector<bool> is_member_;

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
