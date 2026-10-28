// raft.h —— Raft 共识算法的 C++ 骨架（对应 Go 版 src/raft/raft.go）
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
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <unordered_set>
#ifdef RAFT_BENCH_METRICS
#include <unordered_map>  // propose_ts_（仅埋点构建需要）
#endif

#include "../common/chan.h"
#include "../common/thread_tracker.h"
#include "../common/util.h"
#include "../labrpc/labrpc.h"
#include "../persist/state/persister.h"
#include "raft_protocol.h"
#include "raft_util.h"
#include "../common/metrics.h"  // 性能埋点（未定义 RAFT_BENCH_METRICS 时为空头，零开销）

namespace raft {

// 线上协议数据结构（RPC 消息 / 日志条目 / apply 消息 / 成员角色）见 raft_protocol.h；
// CRC32 / 快照 blob 编解码 / Trace 调试日志 / 计票纯函数见 raft_util.h。

using Persister = persist::Persister;
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
constexpr int kLogRetryMs = 300;
// 流水线（滑动窗口）：单条 AppendEntries 携带的日志条目上限。
// 同时修复"单条 AE 条目无上限"的缺口 —— follower 落后 100 万条时不再一条 RPC 全塞。
constexpr int kMaxEntriesPerRpc = 1024;
// 流水线窗口：允许在途（已发未确认）的日志条目上限。
// 在途条目数 = next_index_[s] - 1 - match_index_[s]，超过则暂停发新批，
// 直到收到 ACK（match_index_ 推进）或超时重发。设为 kMaxEntriesPerRpc 整数倍
// → 即"8 批在途"，跨 10ms RTT 时吞吐从 1/RTT 提升到 8/RTT。
constexpr int kPipelineMaxInFlight = 8 * kMaxEntriesPerRpc;

// §5.2 生产加固：单条 AppendEntries 字节上限（双上限之二，条目数上限见 kMaxEntriesPerRpc）
// follower 落后时若单条 command 很大（非 lab 短 value），单 RPC 仍可能偏大 → 序列化/网络压力。
// 与 kMaxEntriesPerRpc 共同构成"单条 AE 双上限"：任一先到即截断。
constexpr size_t kMaxBytesPerRpc = 1 * 1024 * 1024;  // 1 MiB

// 快照分块流式传输：单条 InstallSnapshot RPC 携带的快照字节上限。
// 生产上状态机几十 GB 时，若整块塞一条 RPC → 内存爆 / 连接占死 / 超时重传整块。
// 切成固定大小的 chunk，follower 按 offset 追加写、done 时校验 CRC 后安装。
constexpr size_t kSnapshotChunkSize = 512 * 1024;  // 512 KiB / chunk
// §2.1 重组缓冲硬上限（P0-3 生产加固，防 OOM DoS）：follower 接收分块快照时，
// recv_snap_buf_ 按 offset 扩容。伪造 term 的 InstallSnapshot RPC（超大 offset / 超大数据）
// 可令 resize 撑爆内存。合法快照状态机远小于此值，超过即视为恶意/异常、拒绝并复位在途缓冲。
constexpr size_t kMaxRecvSnapBuf = 2ULL * 1024 * 1024 * 1024;  // 2 GiB

// ---------------------------------------------------------------------------
// C1 生产加固：未提交 entry 上限（背压 / 防 OOM）
// ---------------------------------------------------------------------------
// leader 侧允许积压的【未提交日志条目】数上限。
//   未提交条目数 = LastLogIndex() - commit_index_
// 超过则 Start() 拒绝新提案（返回 backpressure=true），形成背压，
// 防止海量并发写时 leader 把未提交日志无限堆在内存里撑爆（etcd 的
// MaxUncommittedEntries 同款语义，etcd 按字节算，这里按条目数算更贴合 lab）。
//
// 默认 10000 条：lab 规模永远触发不到 → 现有 67 个用例零行为变更。
// 生产部署或测试可用 SetMaxUncommittedEntries() 调低（<=0 表示不限制）。
constexpr int kMaxUncommittedEntriesDefault = 10000;

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

// 前置声明：Raft log 持久化后端的抽象接口（依赖反转，只认接口不认具体类）。
// 完整定义见 persist/log_store.h（接口），段文件/内存两后端在 persist/log/。
class LogStorage;


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
  std::pair<int64_t, bool> GetState();

  // 重定向用：返回本节点"认知到的 leader 编号"。
  // 收到合法 AppendEntries（含心跳）时记下 args.leader_id；自己当选时记 me_。
  // 初始 -1（尚未认知任何 leader）。KVServer 据此在 kWrongLeader 的 reply 里
  // 回填 leader 地址，让客户端直连、省掉一轮盲目轮询。
  int GetLeaderId() const;

  // 上层服务（比如 KV 服务器）提交一条命令。
  // 不是 leader 就返回 is_leader=false。
  StartResult Start(const Command& command);

  // #2.2 磁盘水位保护：设置 raft state（term/votedFor/log）序列化后允许的最大字节数。
  // 设为 0（默认）= 不限制，保持与现有行为完全一致。超过则 Start() 返回 disk_full=true，
  // 调用方应触发 Snapshot() 压缩后再重试。
  void SetMaxRaftStateBytes(size_t n) { max_raft_state_bytes_ = n; }
  // 当前 raft state 序列化字节数（与 persister 写入的 RaftState 大小一致，不含快照 blob）。
  size_t RaftStateSize() const;
  int64_t ReadIndex();
  // 提议一次【单节点】成员变更（一次只加/减一个，非 lab 原生）。
  // add=true  → 把 server 设为 kVoter；add=false → 设为 kRemoved。
  // 内部会追加一条 conf 配置条目，节点在【apply 到该条目时】才切换本地配置；
  // 若已有未提交的变更在途（pending_conf_index_ != 0）则拒绝（单飞保护）。
  // 非 leader / 目标越界 / 有在途变更 → 返回 is_leader=false 且不做事。
  StartResult ProposeConfChange(int server, bool add);
  // 把 server 设为【指定角色】（kRemoved / kLearner / kVoter 三态）。
  // 与 ProposeConfChange 走完全相同的"追加配置条目 → apply 时切换"路径，
  // 差别只是目标角色由调用方指定，因此支持 kLearner：
  //   kLearner = 在集群里【只追数据、不投票、不计入多数派】，
  //   用来做"先让新节点追平日志、追平后再提拔为 voter"的安全扩容
  //   （避免新节点一进来就把多数派阈值撑大 → 扩容窗口内可用性下降）。
  // 非 leader / 目标越界 / 角色非法 / 有在途变更 → 拒绝。
  StartResult ProposeConfChangeTo(int server, MemberRole target);
  // 关掉这个 Raft 实例：停掉后台线程、取消所有在途 RPC。
  // 注意：持久化状态保存在 persister 里，不会被清掉。
  void Kill();
  bool killed() const { return killed_.load(); }
  // 调试/测试：返回各节点当前角色（MemberRole 整型值），read-only。
  // 0=kRemoved, 1=kLearner, 2=kVoter。用于验证成员变更是否按预期 apply。
  std::vector<int> MembershipView() const;
  // 调试/测试：当前多数派阈值（只数 kVoter）。
  // 用来验证"learner 不计入多数派"：4 节点里 1 个是 learner 时，
  // 有效 voter=3 → 阈值 2（而不是 4 节点的 3）。
  int QuorumSize() const;

  // ---- C1 未提交 entry 上限（背压）----
  // 设置上限（<=0 表示不限制）。默认 kMaxUncommittedEntriesDefault，
  // 生产/测试可下调以验证背压行为。
  void SetMaxUncommittedEntries(int n);
  int MaxUncommittedEntries() const;
  // 当前未提交条目数 = LastLogIndex() - commit_index_（调试/测试用）。
  int64_t UncommittedCount() const;

  // ---- C5 被移除节点主动退场 ----
  // 本节点是否已被配置变更移除（apply 到"移除自己"那条 conf 条目之后为真）。
  // 被移除的节点会主动静默：不发起选举、不接受客户端写、不提供线性一致读，
  // 防止它继续当 candidate 干扰多数派（etcd 同款行为）。
  // 若后续配置又把本节点加回（kLearner / kVoter），本值自动恢复为 false。
  bool IsRemovedSelf();
  // 因"已被移除"而被主动抑制的选举次数（证明定时器确实在静默，而非偶然没触发）。
  int ElectionSuppressedCount() const;
  // 调试/测试：本节点日志最新下标。
  // 用来验证"learner 虽不投票，但仍在持续接收并追加日志（追数据）"。
  int64_t LastLogIndex() const;

  // §5.2 生产加固：单条 AppendEntries 字节上限的观测接口（测试/调试用，不影响生产逻辑）。
  // 因"单条 RPC 字节数超过 kMaxBytesPerRpc"而截断的批次数（已发至少一条仍超时不截断）。
  long long AEByteTruncatedCount() const;
  // 历史单条 AE 携带的 entry payload 字节数最大值（不含 RPC header），用于断言不超上限。
  long long MaxAppendEntriesBytes() const;

  // 调试用：一行文字描述当前状态
  std::string LogStatus();
  int me() const { return me_; }

  // ---- 快照测试 / 调试访问器（不影响生产逻辑）----
  int LogSize() const {
    std::lock_guard<std::mutex> lk(mu_);
    return static_cast<int>(logs_.size());
  }
  int64_t SnapshotIndex() const {
    std::lock_guard<std::mutex> lk(mu_);
    return snapshot_index_;
  }
  // 【新增】取「已提交且有效」的快照 blob（已剥掉磁盘格式头，裸状态机数据）。
  // KVServer 重启恢复应走这里，不要直接 persister_->ReadSnapshot()——
  // 后者拿到的是带 header 的磁盘格式，且可能是「尚未提交」的 blob。
  std::string GetSnapshotData() const {
    std::lock_guard<std::mutex> lk(mu_);
    return *snapshot_data_;
  }
  int64_t CommitIndex() const {
    std::lock_guard<std::mutex> lk(mu_);
    return commit_index_;
  }
  int64_t LastApplied() const {
    std::lock_guard<std::mutex> lk(mu_);
    return last_applied_;
  }

  // ---- RPC handler：由网络层在收到消息时调用 ----
  // 这两个函数【必须自己加锁】，因为网络线程会并发调用它们。

  void RequestVote(const RequestVoteArgs& args, RequestVoteReply& reply);
  void RequestPreVote(const RequestPreVoteArgs& args, RequestPreVoteReply& reply);
  void StartRealElection();  // 预投票拿到多数派后，真正自增 term 发起真实投票
  void SendRequestVoteRPCs();  // 真实投票 RPC 发送(StartRealElection 与 candidate 超时重发共用)
  void AppendEntries(const AppendEntriesArgs& args, AppendEntriesReply& reply);
  // Follower ReadIndex 的服务端入口：follower 发来的读请求在这里被 leader 处理。
  // 内部复用 leader 本地 ReadIndex()（多数派心跳确认 + 等 apply），把 commit_index_ 回给 follower。
  void ReadIndexRPC(const ReadIndexArgs& args, ReadIndexReply& reply);

  // ---- 快照（Lab 3 日志压缩）----
  // 上层状态机（KV 服务）调用：到 index 为止的状态机已持久化，
  // 让 Raft 截断日志、把快照存盘。index 必须 <= commit_index_。
  void Snapshot(int64_t index, const std::string& snapshot);

  // 快照 RPC：leader 给落后太多的 follower 发送。
  void InstallSnapshot(const InstallSnapshotArgs& args, InstallSnapshotReply& reply);

  // 在【持有本节点 mu_】的前提下克隆持久化目录（测试框架的"崩溃重启"专用）。
  //
  // 为什么需要持锁：文件模式下 raft.log.seg 的截断是原地 RewriteFile
  // （ftruncate(0) + 逐条 pwrite，无 rename 原子性）。若 CopyDir 与后台线程的
  // 截断并发，可能克隆出半截段文件 → 新实例重启后日志空洞。
  // 持 mu_ 后，所有持锁写盘路径（SaveRaftState / log_store_ Append·Truncate*）
  // 都被挡住；唯一漏网的 snapshot blob 是 tmp+rename 原子写，读到即完整旧/新文件。
  //
  // 调用方必须已通过 shared_ptr 持有本对象（克隆期间对象不得被析构）。
  std::shared_ptr<Persister> CopyPersisterUnderLock();

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
  // 锁以 unique_lock 引用传入：file 模式下 group commit 等待落盘时需要它释放 mu_
  // （见下方 persist_* 成员处的设计说明）。
  void PersistLocked(std::unique_lock<std::mutex>& lk);
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
  bool SaveSnapshotBlob(int64_t index, int64_t term, const std::string& raw_blob);

  // ---- 小工具。约定：名字以 Locked 结尾的，调用时必须已经持有 mu_ ----
  int64_t LastLogIndexLocked() const {
    return logs_.empty() ? 0 : logs_.back().index;
  }
  int64_t LastLogTermLocked() const {
    return logs_.empty() ? 0 : logs_.back().term;
  }
  // 任期变大了 → 退回 follower。调用前必须持有 mu_。
  void ConvertToFollowerLocked(int64_t new_term, std::unique_lock<std::mutex>& lk);
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
  int QuorumSizeLocked() const { return MemberCountLocked() / 2 + 1; }
  // 本轮（预）投票的赞成票数：只数"当前仍是 voter"的节点，
  // 与 QuorumSizeLocked() 同口径（已移除 / learner 的票作废）。调用前持 mu_。
  int CountGrantedVotesLocked() const;
  int CountGrantedPrevotesLocked() const;
  // 当选判定 + 上任初始化：够 quorum 就上任，否则什么也不做。调用前持 mu_。
  // 回包处与 StartRealElection 发完票后都要调一次（详见 raft.cpp 注释）。
  void BecomeLeaderIfQuorumLocked(std::unique_lock<std::mutex>& lk);
  // 通用：n 个节点的多数派阈值（不依赖本节点配置）。
  int MajorityOf(int n) const { return n / 2 + 1; }
  // 给所有 follower 发一轮"带 read_ctx 的空心跳"，用于 ReadIndex 的身份确认。
  // 必须在锁外调用（RPC 异步，不能持 mu_ 发网络）。
  void BroadcastReadHeartbeat(uint64_t ctx);
  // 回包到达时按 ctx 把这一票记到对应读请求上；攒够多数派就标记 done 并唤醒。
  // 调用前必须持有 mu_。
  void RecordReadAckLocked(uint64_t ctx, int server);
  // 推进 commitIndex：取所有 voter 的 matchIndex 中位数，并要求该下标条目
  // term == current_term_（Figure 8 守卫）。leader 侧【唯一】提交入口。
  // 调用前必须持有 mu_。
  //
  // 必须由三处调用：AE 回包回调（他人确认后）+ Start() +
  // ProposeConfChangeTo()（追加日志后）。只靠回包回调的话，当 quorum=1
  // （单 voter 集群，或其余节点全部不可达 / 已 removed）时没有任何回包到达
  // → commit_index_ 永不推进 → 写永不提交、ReadIndex 永久返回 -1
  // → 集群完全不可用。Raft 语义上 leader 自身持有即计入多数派。
  void AdvanceCommitIndexLocked(std::unique_lock<std::mutex>& lk);

  mutable std::mutex mu_;

  // =========================================================================
  // 域 1 · 构造期固定 —— peers_/persister_/me_ 与两个原子开关
  //   构造后不变（运行时只读，无需加锁）；killed_/cancel_rpcs_ 由 Kill()
  //   置位，是"停后台线程 + 让在途 RPC 立即失败"的广播闸。
  // =========================================================================
  std::vector<std::shared_ptr<labrpc::ClientEnd>> peers_;
  std::shared_ptr<Persister> persister_;
  int me_ = 0;

  std::atomic<bool> killed_{false};
  // Kill() 时置 true，让所有在途的 RPC 立即返回失败，
  // 这样后台线程能迅速退出，不会卡在 7 秒的模拟延迟里。
  std::atomic<bool> cancel_rpcs_{false};

  // =========================================================================
  // 域 2 · 核心 volatile 状态 —— 论文里"所有 server 都有"的那一份
  //   全部受 mu_ 保护。state_/current_term_/voted_for_ 是选举安全三件套；
  //   vote_granted_/prevote_granted_ 记录"谁投的"而非只记票数，计票时才能
  //   剔除"回包时已被移除 / 降级"的作废票（与 QuorumSizeLocked 同口径）。
  //   leader_id_ 是唯一的非论文字段：KVServer 据此在 kWrongLeader 回包里
  //   填 leader 地址，省掉客户端一轮盲目轮询。
  // =========================================================================
  ServerState state_ = ServerState::kFollower;
  int64_t current_term_ = 0;
  // 事件驱动退位标志：ApplyLoop 检测到本节点已被移除/降级出 voter 时置 true，
  // 但【不立即退位】——继续当 leader，让 ReplicateLoop 把 leader_commit（含本移除
  // 配置）通过普通心跳带出去；宽限 kStepdownGraceMs 后由 ReplicateLoop 真正退位。
  // 这样剩余 voter 必能拿到新 commitIndex → apply 配置 → 按新 quorum 选新 leader，
  // 杜绝原版"leader 退位太早 → 旧 quorum 缺我这一票 → 永远选不出"的死锁。
  bool pending_stepdown_ = false;
  raftcpp::TimePoint stepdown_after_{};  // pending_stepdown_ 置位时的退位截止时刻
  int voted_for_ = -1;    // 本任期把票投给了谁，-1 = 还没投
  int num_votes_ = 0;     // 本轮选举收到的票数
  int num_prevotes_ = 0;  // 预投票
  // 本轮各节点对本节点的（预）投票态度：0=未投/未回, 1=授予, -1=拒绝。
  // 必须记录"谁投的"而非只记一个整数票数，才能在计票时剔掉
  // "回包时已被移除 / 降级成 learner"的票（与 QuorumSizeLocked 同口径）。
  std::vector<int> vote_granted_;
  std::vector<int> prevote_granted_;

  // 最近一次"听到合法 leader / 给别人投了票"的时刻。
  // 选举超时是相对它来算的。
  raftcpp::TimePoint last_heartbeat_;
  // 用来唤醒选举定时器（收到心跳 / 被 Kill 时 notify）
  std::condition_variable tick_cv_;

  // ---- 重定向用：本节点认知的 leader 编号 ----
  // 收到合法 AppendEntries（含心跳）时记为 args.leader_id；自己当选时记为 me_。
  // 初始 -1（尚未认知任何 leader）。受 mu_ 保护（只在 AppendEntries /
  // BecomeLeaderIfQuorumLocked / GetLeaderId 内读写）。
  int leader_id_ = -1;

  // =========================================================================
  // 域 3 · 日志与 apply —— logs_/commit_index_/last_applied_ + apply 通道
  //   logs_[0] 是哨兵（index == snapshot_index_）；commit_index_ 与
  //   last_applied_ 之间就是"已提交未 apply"队列，由 ApplyLoop 消费。
  //   apply 通道是【单发送者】契约：只有 ApplyLoop push（快照也走
  //   pending_snapshot_ 中转，见域 4）；apply_cv_ 是它的唤醒源。
  // =========================================================================
  std::vector<LogEntry> logs_;    // logs_[0] 是哨兵，index = snapshot_index_
  // 日志持久化后端（依赖反转：只认 LogStorage 接口，不认具体类）。
  // 由 CreateLogStorage 工厂按 file_backed 注入 SegmentLogStorage / MemoryLogStorage；
  // 内存模式下注入 MemoryLogStorage（null-object），读路径仍全走 logs_ 向量。
  std::unique_ptr<LogStorage> log_store_;
  int64_t commit_index_ = 0;
  int64_t last_applied_ = 0;
  std::shared_ptr<raftcpp::Chan<ApplyMsg>> apply_ch_;
  // 唤醒 applier：commit_index_ 推进 / pending_snapshot_ 挂载 / Kill 时 notify。
  std::condition_variable apply_cv_;

#ifdef RAFT_BENCH_METRICS
  // 【仅 bench 埋点】index → 该提案进入 Start() 的时刻（ns，单调时钟）。
  // Start() 追加日志时登记，ApplyLoop 收集到该 index 时注销并记录
  // "propose→apply" 延迟。两处访问都在 mu_ 下（调用方均已持锁）。
  // 当选 / 退位 / 快照时整体清空 —— 这些条目不会再被 apply，留着就是泄漏。
  std::unordered_map<int64_t, uint64_t> propose_ts_;
#endif

  // =========================================================================
  // 域 4 · 快照 —— 覆盖状态 / 分块传输 / 两阶段落盘 / applier 暂停门
  //   横跨四块：2D 覆盖状态（snapshot_index_~recv_snap_epoch_）、leader 侧
  //   分块传输（snap_epoch_/snap_in_flight_）、blob 两阶段落盘
  //   （saved_blob_index_/snap_io_mu_）、apply 顺序保护
  //   （pending_snapshot_/install_gate_depth_）。
  //   锁序铁律：snap_io_mu_ → mu_（全项目仅 SaveSnapshotBlob 一处按此序，
  //   无反向路径，不会死锁）。
  // =========================================================================
  // 快照覆盖到最后这条日志（逻辑 index）。logs_[0].index 始终 == snapshot_index_，
  // 因此"逻辑 index - snapshot_index_" = 在 logs_ 中的下标。
  int64_t snapshot_index_ = 0; // 快照覆盖到最后这条日志（逻辑 index）
  int64_t snapshot_term_ = 0;  // 快照覆盖到最后这条日志（逻辑 term）
  // 裸的状态机 blob（不含磁盘头）。恢复时由 persister 读出、剥头校验后填入。
  // P0-1/P0-2：用 shared_ptr 管理 blob，发送侧 O(1) 钉死这一波所用的 buffer，
  // 切片时按需拷 chunk（不整拷贝），且 Raft::Snapshot 走「原子替换」而非原地改，
  // 多帧发送间隙源被替换也不会让新旧混合（详见 ReplicateLoop 钉死逻辑）。
  std::shared_ptr<std::string> snapshot_data_ = std::make_shared<std::string>();
  // 当前 snapshot_data_ 的 CRC32（分块传输时随每条 chunk 带上，follower done 时校验）。
  int snap_crc_ = 0;
  // snap_crc_ 是相对哪个 snapshot_index_ 算出来的（-1 = 尚未计算）。
  // 【惰性 CRC】只有 leader 真正要发分块快照时才算，算完按此键缓存，
  // 一次快照最多算一次——避免每个节点每次生成快照都全量算一遍（见 raft.cpp 注释）。
  int64_t snap_crc_index_ = -1;
  std::string recv_snap_buf_;   // 正在重组的完整 blob
  int64_t recv_snap_last_term_ = -1;  // 正在重组的快照的 last_included_term（用于检测「新快照到来」重置缓冲）
  int64_t recv_snap_index_ = -1;    // 正在重组的快照 last_included_index
  int recv_snap_epoch_ = -1;    // 正在重组的传输代号（档2：旧残片靠它识别并丢弃）

  // 分块快照：leader 侧 per-follower 当前分块传输代号（每次开新传输 +1）。
  // 与 snap_in_flight_ 配合：snap_in_flight_ 挡「重复洪发」，epoch 让 follower 能区分
  // 「同一快照的迟到残片（丢弃）」与「同一快照的新一轮重试（reset 重来）」。
  // 仅由该 follower 对应的 ReplicateLoop 线程读写 [s]，无跨线程竞争。
  std::vector<int> snap_epoch_;
  // 分块快照：leader 侧「是否正在给某 follower 分块传输快照」标记（防重复洪发）。
  // 仅由该 follower 对应的 ReplicateLoop 线程读写 [s]，无跨线程竞争。
  std::vector<char> snap_in_flight_;

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
  int64_t saved_blob_index_ = 0;

  // 只用于串行化 blob 落盘 I/O。★ 关键：慢 I/O 发生在 mu_ 之外，
  // 期间不占 raft 主锁。锁序恒为 snap_io_mu_ → mu_，且全项目仅
  // SaveSnapshotBlob 一处按此序取锁，无反向路径，不会死锁。
  std::mutex snap_io_mu_;

  // applyCh 单发送者：InstallSnapshot 在锁内把 snapshot 挂到 pending_snapshot_，
  // 由 ApplyLoop 在锁内取出、锁外 push。绝不允许 InstallSnapshot 自己也
  // push，否则会和 ApplyLoop 派发的 log entries 互相穿插，KVServer 状态错乱。
  // 仿 Go 版 src/raft/raft.go:133 的 pendingSnapshot 字段。
  std::optional<ApplyMsg> pending_snapshot_;

  // InstallSnapshot 阶段 2 的「applier 暂停门」深度（持 mu_ 访问）。
  //
  // 阶段 2 在持锁完成「状态突变（snapshot_index_/logs_ 截断/last_applied_ 推进）
  // → 落盘 → 挂 pending_snapshot_」；file 模式下 PersistLocked 等落盘时会
  // 【释放 mu_】。若不暂停 applier，它会在窗口期内依据已突变的状态把
  // last_applied_+1（快照覆盖点之后的条目）当普通命令先行派发 → 状态机
  // last_cmd_index_ 超过 snapshot_index → 随后快照挂上来触发 KVServer 的
  // InstallSnapshot 不变式 FATAL（TestSnapshotUnreliableRecoverConcurrent-
  // PartitionLinearizable3B 600s 看门狗现场实测复现）。
  // 旧实现 fsync 全程持 mu_，该交错不可能发生；group commit 放大了窗口。
  //
  // 为什么是深度而不是 bool：两个 InstallSnapshot 临界区可能套叠——B 可在 A 的
  // 落盘等待期内拿到 mu_ 进入自己的临界区（leader 更换时新旧快照同时在途）。
  // 先完成者若把门彻底打开，applier 会在 B 挂 pending 之前派发条目 → 同样的
  // 不变式破坏。深度计数保证最后一个出门的才开门。
  int install_gate_depth_ = 0;

  // =========================================================================
  // 域 5 · leader 复制 —— next/match_index + 心跳序号广播 + 节流/流水线
  //   只有 leader 写这些。next_index_ 是【发送指针】（发新批前进），
  //   match_index_ 是【确认指针】（ACK 前进、单调），二者之差即流水线
  //   窗口占用（> kPipelineMaxInFlight 时暂停发新批）。
  //   replicator_cv_ 是"有活儿干"的唤醒源：Start() 追加日志 /
  //   heartbeat_seq_ 自增时 notify_all，n-1 个 ReplicateLoop 各自比对
  //   last_heartbeat_seq_ 后行动——序号广播 + 每线程自比，去中心化分发。
  // =========================================================================
  std::vector<int64_t> next_index_;   // 下一个要发给该 follower 的日志下标
  std::vector<int64_t> match_index_;  // 已经确认该 follower 复制到的最高下标

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

  // 流水线（滑动窗口）实现说明：
  //   在途（已发未确认）条目数 = next_index_[s] - 1 - match_index_[s]。
  //   next_index_ 是【发送指针】，仅"发送新批次"时前进；
  //   match_index_ 是【确认指针】，仅收到成功 ACK 时前进（单调）。
  //   二者之差即窗口占用；> kPipelineMaxInFlight 时暂停发新批。
  //   不再需要 bool 在途标记：超时重发只需把发送指针回退到 match_index_+1（幂等重发）。
  // inflight_log_time_：最近一次"发出带日志 AE"的时刻，用于超时重发判定。
  std::vector<raftcpp::TimePoint> inflight_log_time_;
  // Start() 追加日志、或 heartbeat_seq_ 变化时 notify_all() 唤醒所有复制线程
  std::condition_variable replicator_cv_;

  // =========================================================================
  // 域 6 · CheckQuorum 与 Leader Lease —— 失多数派退位 + 租约读前提
  //   last_ack_time_（回包到达时刻）与 cq_consec_fail_（连续 RPC 失败计数）
  //   是两路互补的存活信号：前者兜住"回包永远不到"的真断连，后者容忍
  //   "延迟但终会到达"的长重排。lease_expire_ 仅 kEnableLeaseRead 打开时
  //   用于跳过心跳确认（默认关，不信任时钟漂移）。
  // =========================================================================
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

  // =========================================================================
  // 域 7 · ReadIndex —— follower 线性一致读的多数派确认状态机
  //   next_read_ctx_ 全局发号（单调递增、永不复用）；active_read_ 是
  //   【单槽在飞轮】（读合并：未完成轮允许并发读者挂入，done 后不收人）；
  //   read_safe_commit_ 是线性一致的硬前提——本 term 的 no-op 必须先
  //   提交，否则读到的是"还没应用进状态机"的 stale 值。
  // =========================================================================
  // 唤醒"正在等多数派确认"的读线程
  std::condition_variable read_cv_;

    // 【ReadIndex 读确认：per-request ctx 设计】
  std::atomic<uint64_t> next_read_ctx_{1};         // 全局唯一 ctx 号（单调递增，永不复用）
  // 【ReadIndex 读合并 · 单槽 + 共享指针】任意时刻【最多一轮】在飞读轮:未完成的轮
  // (!done)允许并发读者挂入(合并心跳);done 后不再收人——安全性承重件:挂入者的调用
  // 时刻必须早于多数派 ack 完成时刻,领导权验证才不会过期(否则分区+新主已 ACK 的写
  // 会被挂入者漏读)。用 shared_ptr 而非值单例:waiter 挂入时持有自己的对象副本,
  // active_read_ 被新轮覆写后旧 waiter 仍能看到自己轮的 done(值单例下会被"搁浅"
  // 到超时);末位离开者把 active_read_ 撤下,超时死轮不会吸住重试读者造成羊群效应。
  // 空指针 = 无在飞读轮。所有解引用均在 mu_ 下。
  std::shared_ptr<ReadIndexCtx> active_read_;  // 当前在飞读轮(nullptr => 无)

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
  int64_t read_safe_commit_ = 0;
  // =========================================================================
  // 域 8 · 成员变更 —— 三态角色 + 单飞保护 + Q2 冻结点
  //   is_member_ 是论文"C_old 与 C_new 的多数派必有交集"的落地点；
  //   pending_conf_index_ 是单飞闸（apply 时清零，新 leader 上任扫描重建）；
  //   removed_at_index_ 是隐私冻结点（与 is_member_ 两源取或，见
  //   raft_util.h 的 IsRemovedFrozen）。
  // =========================================================================
  // 【is_member_】成员变更 / Learner 三态数组，is_member_[i] 表示节点 i 的角色：
  //   kVoter   : 正式投票成员，参与一切多数派判定
  //   kLearner : 在集群里只追数据、不投票、不计入多数派（扩容零可用性损失）
  //   kRemoved : 不在当前配置 C 里（已移除 / 尚未加入）→ 不收 RPC、不投票、不计数
  // 这个数组就是论文里"C_old 与 C_new 的多数派必有交集"那条论证的落地点。
  std::vector<MemberRole> is_member_;
  // 单飞保护：当前"已 propose 但还没 apply（commit）"的成员变更条目下标。
  // 0 表示没有在途变更。同一时刻只允许一个变更在飞行（文档 §2.4 运维纪律）。
  // 新 leader 上任时会扫描日志重建这个值，保证切主后不丢失"在途变更"状态。
  int64_t pending_conf_index_ = 0;
  // Q2 隐私加固配套：记录每个节点"被移除时"的配置条目下标（冻结点）。
  // removed_at_index_[i] >= 0 表示节点 i 已在下标 idx 处被移除，leader 向它复制的
  // 日志最多到 idx（含移除条目本身），之后彻底冻结、不再发任何新日志/快照/心跳。
  // -1 表示无冻结点（节点是 voter/learner，或刚重启尚未从 ApplyLoop 重建）。
  // 不持久化：重启后节点本就从日志重放拿到过移除条目，cap<0 → 直接冻结即可，无需补发。
  std::vector<int64_t> removed_at_index_;
  // 锁内版本：调用方必须已持有 mu_（Start()/ReadIndex() 内部用）。
  bool IsRemovedSelfLocked() const;

  // =========================================================================
  // 域 9 · 提案保护与磁盘水位 —— Start() 入口两道闸 + 状态序列化
  //   max_uncommitted_entries_：未提交条目数背压（防海量写把未提交日志
  //   堆爆内存）；max_raft_state_bytes_：raft state 落盘体积水位（超了
  //   逼上层调 Snapshot() 压缩）。raft_state_bytes_ 缓存让 RaftStateSize
  //   从 O(日志条数) 降为 O(1)。
  // =========================================================================
  // 由 SetMaxUncommittedEntries() 修改；默认 kMaxUncommittedEntriesDefault。
  // 受 mu_ 保护（只在 Start() 内读、由 SetMaxUncommittedEntries() 写）。
  int max_uncommitted_entries_ = kMaxUncommittedEntriesDefault;

  // ---- #2.2 磁盘水位保护：raft state 字节数上限 ----
  // 由 SetMaxRaftStateBytes() 修改；默认 0=不限制。受 mu_ 保护（只在 Start() 内读、由 SetMaxRaftStateBytes() 写）。
  size_t max_raft_state_bytes_ = 0;

  // #2.2 辅助：编码 raft state 为字节串（PersistLocked / RaftStateSize 共用），调用方须持 mu_。
  std::string EncodeRaftStateLocked() const;
  // 缓存「上一次 PersistLocked 编码出的字节数」，使 RaftStateSizeLocked() 从 O(日志条数) 降为 O(1)。
  // 由 PersistLocked() 编码后写入；ReadPersist() 末尾用一次 EncodeRaftStateLocked() 初始化（构造期仅一次）。
  // 读取方须持 mu_（Start() / RaftStateSize() / ProposeConfChangeTo 均持锁）。
  size_t raft_state_bytes_ = 0;
  // 实现见 raft.cpp：需在 LogStorage 完整定义之后，故不能在此内联。
  // 段文件模式下会把 raft.log.seg 大小补进 RaftStateSize，保证磁盘水位保护（#2.2）仍生效。
  size_t RaftStateSizeLocked() const;

  // =========================================================================
  // 域 10 · state 文件 group commit —— 单一写者 + 版本号（仅 file 模式）
  //   单一写者是安全件：并发 votedFor 落盘若被后到的旧 blob 覆写回退，
  //   就是同任期双票 → 选举安全性崩塌。版本号让"PersistLocked 返回即
  //   耐久"的契约在异步化后仍逐字成立。
  // =========================================================================
  // SaveRaftState = write+fsync+rename+fsync(dir)（persister.h:79-121），两次
  // 同步 fsync 全在 mu_ 内，是锁内最后的慢 I/O。机制（单一写者）：提案线程持
  // mu_ 只做「编码+记版本+标记脏」后等自己版本落盘；PersistFlushLoop 不持 mu_
  // 写最新 blob + fsync —— state blob 是全量快照，同一窗口多个提案的状态是嵌套
  // 关系，一次落盘释放全部等待者（N 提案 → 1 次 fsync）。「PersistLocked 返回
  // 即耐久」契约逐字保留（快照 blob→state→截断顺序、votedFor/term 应答前落盘
  // 原样成立）。
  // 为什么必须单一写者：并发 votedFor 落盘可能被后到的旧 blob 覆写回退 →
  // 同任期双票 → 选举安全性崩塌。
  // 仅 file 模式启用；内存模式只是存 string、无 fsync 可合并，走原同步路径。
  bool persist_async_ = false;
  bool persist_dirty_ = false;               // 有已编码未落盘的 blob（持 mu_）
  uint64_t persist_version_ = 0;             // 每次登记脏时递增（持 mu_）
  uint64_t persist_synced_ = 0;              // 已落盘到的版本（flusher 写，持 mu_）
  std::string pending_state_blob_;           // 最新待落盘 state blob（持 mu_）
  std::condition_variable persist_cv_;       // flusher 等活（Kill 时也靠它退出）
  std::condition_variable persist_done_cv_;  // 提案者等落盘
  void PersistFlushLoop();

  // =========================================================================
  // 域 11 · 横切观测计数器 —— C5 静默计数 + AE 字节上限统计
  //   均为锁外并发更新的 atomic：前者证明"被移除节点真的在静默"
  //   （没有它，删除 C5 检查后测试仍全绿 = 假通过，mutation 抓不出来）；
  //   后者观测单条 AE 双上限之字节上限的截断行为。
  // =========================================================================
  std::atomic<int> election_suppressed_{0};

  // §5.2 单条 AE 字节上限的观测计数器（SendAppendEntries 在锁外并发更新，故用 atomic）。
  std::atomic<long long> ae_byte_truncated_count_{0};  // 因字节上限截断的批次数
  std::atomic<long long> max_ae_payload_bytes_{0};     // 单条 AE 最大 payload 字节
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
  Service::Handler read_index =
      [rf](const std::string& args) -> std::string {
    ReadIndexArgs a;
    a.Deserialize(args);
    ReadIndexReply r;
    rf->ReadIndexRPC(a, r);
    return r.Serialize();
  };
  return std::make_shared<Service>(
      "Raft", std::unordered_map<std::string, Service::Handler>{
                  {"RequestVote", request_vote},
                  {"RequestPreVote", request_prevote},
                  {"AppendEntries", append_entries},
                  {"InstallSnapshot", install_snapshot},
                  {"ReadIndex", read_index},
              });
}

}  // namespace raft
