// raft.cpp —— 你的主战场
//
// 只改这个文件。搜索 "TODO" 就能找到所有需要你填的地方。
//
// 建议顺序：
//   1) 先只做 2A（RequestVote / AppendEntries 的心跳部分 / StartElection）
//      → 跑 ./raft_test 2A
//   2) 再做 2B（Start / AppendEntries 日志部分 / ReplicateLoop / ApplyLoop）
//      → 跑 ./raft_test 2B
//   3) 最后 2C（PersistLocked / ReadPersist + 在改持久化状态的地方调用）
//      → 跑 ./raft_test 2C
//
// 卡住的时候回看 raft.h 的接口注释 + 论文 Figure 2 的状态机，对照 config.h
// 的测试脚手架确认行为；本仓库的测试即你的验收标准（./raft_test 2A/2B/2C/2D）。

#include "raft.h"

#include <cstdio>

namespace raft {

static raftcpp::TimePoint g_t0 = raftcpp::Now();
static bool TraceOn() {
  // ⚠️ TSan 修复：原写法 `static int v = -1; if (v<0) v = ...` 在多线程首次
  // 并发调用时，函数体内的二次赋值会触发 TSan 报 write-vs-write race（选举线程
  // 并发进 StartElection→Trace）。C++11 magic-static 只保证【声明初始化】线程
  // 安全，所以函数体里那次赋值才是 race 源。改成语意上的「声明即终值」，之后
  // 全程只读，race 消失。
  static const int v = (std::getenv("RAFT_LOG") != nullptr) ? 1 : 0;
  return v == 1;
}
static void Trace(const char* fmt, ...) {
  if (!TraceOn()) return;
  va_list ap; va_start(ap, fmt);
  std::fprintf(stderr, "[t=%lld] ", (long long)raftcpp::MillisSince(g_t0));
  std::vfprintf(stderr, fmt, ap);
  std::fprintf(stderr, "\n");
  va_end(ap);
}
// ===========================================================================
// 第一部分：序列化（已实现，不用改）
// ===========================================================================
//
// 编码顺序和解码顺序必须严格一致。这就是 C++ 手写"反射"的代价。

std::string RequestVoteArgs::Serialize() const {
  labrpc::Encoder e;
  e.Int(term).Int(candidate_id).Int(last_log_index).Int(last_log_term);
  return e.Take();
}

bool RequestVoteArgs::Deserialize(const std::string& s) {
  labrpc::Decoder d(s);
  return d.Int(term) && d.Int(candidate_id) && d.Int(last_log_index) &&
         d.Int(last_log_term) && d.Ok();
}

std::string RequestVoteReply::Serialize() const {
  labrpc::Encoder e;
  e.Int(term).Bool(vote_granted);
  return e.Take();
}

bool RequestVoteReply::Deserialize(const std::string& s) {
  labrpc::Decoder d(s);
  return d.Int(term) && d.Bool(vote_granted) && d.Ok();
}

std::string RequestPreVoteArgs::Serialize() const {
  labrpc::Encoder e;
  e.Int(term).Int(candidate_id).Int(last_log_index).Int(last_log_term);
  return e.Take();
}

bool RequestPreVoteArgs::Deserialize(const std::string& s) {
  labrpc::Decoder d(s);
  return d.Int(term) && d.Int(candidate_id) && d.Int(last_log_index) &&
         d.Int(last_log_term) && d.Ok();
}

std::string RequestPreVoteReply::Serialize() const {
  labrpc::Encoder e;
  e.Int(term).Bool(vote_granted);
  return e.Take();
}

bool RequestPreVoteReply::Deserialize(const std::string& s) {
  labrpc::Decoder d(s);
  return d.Int(term) && d.Bool(vote_granted) && d.Ok();
}

std::string AppendEntriesArgs::Serialize() const {
  labrpc::Encoder e;
  e.Int(term)
      .Int(leader_id)
      .Int(prev_log_index)
      .Int(prev_log_term)
      .Int(leader_commit);
  e.Int(static_cast<int>(entries.size()));
  for (const LogEntry& en : entries) {
    e.Int(en.term).Int(en.index).Bytes(en.command);
  }
  return e.Take();
}

bool AppendEntriesArgs::Deserialize(const std::string& s) {
  labrpc::Decoder d(s);
  if (!(d.Int(term) && d.Int(leader_id) && d.Int(prev_log_index) &&
        d.Int(prev_log_term) && d.Int(leader_commit)))
    return false;
  int n = 0;
  if (!d.Int(n) || n < 0) return false;
  entries.clear();
  entries.reserve(static_cast<size_t>(n));
  for (int i = 0; i < n; i++) {
    LogEntry en;
    if (!(d.Int(en.term) && d.Int(en.index) && d.Bytes(en.command)))
      return false;
    entries.push_back(std::move(en));
  }
  return d.Ok();
}

std::string AppendEntriesReply::Serialize() const {
  labrpc::Encoder e;
  e.Int(term).Bool(success).Int(next_index);
  return e.Take();
}

bool AppendEntriesReply::Deserialize(const std::string& s) {
  labrpc::Decoder d(s);
  return d.Int(term) && d.Bool(success) && d.Int(next_index) && d.Ok();
}

std::string InstallSnapshotArgs::Serialize() const {
  labrpc::Encoder e;
  e.Int(term).Int(leader_id).Int(last_included_index).Int(last_included_term);
  e.Bytes(data);
  return e.Take();
}

bool InstallSnapshotArgs::Deserialize(const std::string& s) {
  labrpc::Decoder d(s);
  return d.Int(term) && d.Int(leader_id) && d.Int(last_included_index) &&
         d.Int(last_included_term) && d.Bytes(data) && d.Ok();
}

std::string InstallSnapshotReply::Serialize() const {
  labrpc::Encoder e;
  e.Int(term);
  return e.Take();
}

bool InstallSnapshotReply::Deserialize(const std::string& s) {
  labrpc::Decoder d(s);
  return d.Int(term) && d.Ok();
}

// ===========================================================================
// 第二部分：构造 / 析构 / 生命周期（已实现，不用改）
// ===========================================================================

Raft::Raft(std::vector<std::shared_ptr<labrpc::ClientEnd>> peers, int me,
           std::shared_ptr<Persister> persister,
           std::shared_ptr<raftcpp::Chan<ApplyMsg>> apply_ch)
    : peers_(std::move(peers)),
      persister_(std::move(persister)),
      me_(me),
      apply_ch_(std::move(apply_ch)) {
  // 初始状态：follower，term 0，还没投过票
  state_ = ServerState::kFollower;
  current_term_ = 0;
  voted_for_ = -1;
  last_heartbeat_ = raftcpp::Now(); // ?

  // 哨兵条目：占住下标 0，这样 logs_[k].index == k
  logs_.clear();   // 清空元素,size==0，但capacity保持不变
  logs_.push_back(LogEntry{0, 0, Command()});

  match_index_.assign(peers_.size(), 0);   // 记录已经到达的最新的日志索引，初始化为0
  // 注意：next_index_ 不在这里用固定值 1 初始化，而是放在 ReadPersist 之后
  // 根据真实日志长度计算（见下方），以对齐 Go 版 Make 的 len(rf.log)。
  last_heartbeat_seq_.assign(peers_.size(), 0);   // 记录每个follower上次心跳的序号，变了就发心跳

  // 下面三个 vector 必须按 peers_.size() 初始化，否则 ReplicateLoop 里
  // last_send_time_[s] / inflight_log_[s] / inflight_log_time_[s] 在空 vector
  // 上越界读 → 段错误（2A 一启动就崩）。
  last_send_time_.assign(peers_.size(), raftcpp::TimePoint{});   // 复制线程上次发包时刻，用于节流
  inflight_log_.assign(peers_.size(), false);                    // 是否有带日志的 RPC 在途
  inflight_log_time_.assign(peers_.size(), raftcpp::TimePoint{});// 在途 RPC 发出时刻，用于超时重发

  // TODO(2C)：重启后要从磁盘恢复状态，在这里调用 ReadPersist(...)
  ReadPersist(persister_->ReadRaftState());

  // 对齐 Go 版 Make：nextIndex 初始化为"当前最后一条日志下标 + 1"。
  // 放在 ReadPersist 之后，确保带持久化日志/快照重启后，nextIndex 反映真实日志长度，
  // 而不是固定 1（Go 原版用 len(rf.log)，也是读盘后的值）。
  // 实战中当选时会再被 become-leader 回调重置一次，这里只是更稳健的初值。
  next_index_.assign(peers_.size(), LastLogIndexLocked() + 1);

  // ⚠️ 关键修复（对齐 Go 版 Make 里的 lastApplied = lastIncludedIndex）：
  // ReadPersist() 已经把 commit_index_ / last_applied_ 设成 snapshot_index_。
  // 这里【绝不能】再清零——否则会出现 snapshot_index_=1031 但 last_applied_=0
  // 的不一致。后果：
  //   1) InstallSnapshot 的过期守卫（last_included_index <= snapshot_index_）
  //      本应拦下的「回退快照」被误接受，KV 状态被旧/空 blob 覆盖 → 丢数据；
  //   2) ApplyLoop 对已被快照覆盖的 idx 反复「视为已 apply」，重放逻辑错乱。
  // 新鲜启动（无持久化数据）时 ReadPersist 直接 return，commit/lastApplied
  // 保持成员初值 0，行为不变。

}

// ！！！！！入口函数：启动Raft协议
void Raft::Start() {
  // 起后台线程。全部走 ThreadTracker：detached 但可统计，
  // 测试收尾时会等它们退出，避免"对象没了线程还在跑"的野指针。
  //
  // 为什么线程在这里起，而不是在构造函数里？
  // make_shared<Raft>(...) 内部:
  //  ├─ 分配 控制块 + Raft 对象（此时 __weak_this 是空 weak_ptr）
  //  ├─ 执行 Raft 构造体
  //  └─ shared_ptr 构造函数收尾时，检测到"这对象继承了 enable_shared_from_this"
  //       → 调一个内部函数把 __weak_this(weak_this是一个空weak_ptr由enable_shared_from_this初始化) 关联到控制块
  //         (libstdc++ 里是 _M_weak_assign)
  // std::shared_ptr<Raft> self = shared_from_this();  这里会将自己升级为 shared_ptr<Raft>，拷贝时自己引用计数加一
  // 因为ReplicateLoop属于类raft方法，raft类生成对象调用ReplicateLoop函数可以捕获到this;

  // 因为 ReplicateLoop 要用 shared_from_this()，而构造函数执行期间
  // 对象还没被 shared_ptr 接管，调它会抛 std::bad_weak_ptr。
  auto& tracker = raftcpp::ThreadTracker::Instance();
  tracker.Spawn([this] { ElectionTimerLoop(); });
  tracker.Spawn([this] { ApplyLoop(); });
  for (size_t i = 0; i < peers_.size(); i++) {
    if (static_cast<int>(i) == me_) continue;
    tracker.Spawn([this, i] { ReplicateLoop(static_cast<int>(i)); });
  }
}

Raft::~Raft() { Kill(); }
// Kill() 只是"打信号让线程收工"，它不负责 delete，也不触发析构。rf 这个对象要等到最后一个 shared_ptr 引用消失时才会析构。
void Raft::Kill() {
  bool expected = false;
  // compare_exchange 保证 Kill() 只真正执行一次（crash1 和 cleanup 都会调）
  if (!killed_.compare_exchange_strong(expected, true)) return;

  cancel_rpcs_.store(true);  // 让所有在途 RPC 立刻返回失败

  tick_cv_.notify_all();
  replicator_cv_.notify_all();
  apply_cv_.notify_all();
  if (apply_ch_) apply_ch_->Close();
}

std::pair<int, bool> Raft::GetState() {
  std::lock_guard<std::mutex> lk(mu_);
  return {current_term_, state_ == ServerState::kLeader};
}

std::string Raft::LogStatus() {
  std::lock_guard<std::mutex> lk(mu_);
  char buf[256];
  std::snprintf(buf, sizeof(buf),
                "server=%d state=%s term=%d commit=%d applied=%d logLen=%zu "
                "lastIdx=%d lastTerm=%d",
                me_, StateName(state_), current_term_, commit_index_,
                last_applied_, logs_.size(), LastLogIndexLocked(),
                LastLogTermLocked());
  return std::string(buf);
}

// 任期变大了 → 老老实实退回 follower。
// 注意：这里【不要】刷新 last_heartbeat_。原因见 GetState 上方的注释，
// 也见 doc/02-2A-领导者选举.md 里"不要在 ConvertToFollower 里续命"那节。
void Raft::ConvertToFollowerLocked(int new_term) {
  state_ = ServerState::kFollower;
  current_term_ = new_term;
  voted_for_ = -1;
  num_prevotes_ = 0;
  // TODO(2C)：term 和 votedFor 变了，要持久化
  PersistLocked();
}

// ===========================================================================
// 第三部分：TODO(2A) 选举定时器 + 拉票
// ===========================================================================

void Raft::ElectionTimerLoop() {
  //
  // 这个循环身兼两职：
  //   * 我是 leader      → 每 kHeartbeatInterval 毫秒发一次心跳
  //   * 我不是 leader    → 随机等 150~300ms，没听到 leader 就发起选举
  //
  while (!killed_.load()) {
    ServerState st;
    {
      std::lock_guard<std::mutex> lk(mu_);
      st = state_;
    }

    if (st == ServerState::kLeader) {
      // ---- leader：睡满一个心跳周期，然后自增心跳计数 ----
      {
        std::unique_lock<std::mutex> lk(mu_);
        // std::chrono::steady_clock::time_point内部重载了 + 运算符，可以直接加时间间隔
        auto deadline =
            raftcpp::Now() + std::chrono::milliseconds(kHeartbeatInterval);
        // 如果当前时间还没到超时时间，wait_for会睡眠并释放锁，等deadline时间到了/notify_all再唤醒，重新拿起锁，while循环继续
        while (!killed_.load() && raftcpp::Now() < deadline) {
          // wait_until(abs_time) 内部要把"未来的某个时刻"翻译成 timespec，deadline 已经过期时 macOS 这版 libc++ 会算出 tv_sec = -1，被 POSIX 拒绝并抛 system_error；改用 wait_for(相对时长) 就没"过期"问题，再加一层 if (remain > 0) 人工 clamp 做双保险。
          // 就算算出负值也会被wait_for clamp为0
          auto remain = deadline - raftcpp::Now();
          if (remain > std::chrono::milliseconds(0)) tick_cv_.wait_for(lk, remain);
        }
        if (killed_.load()) return;
        heartbeat_seq_++;  // 复制线程看到计数变了就知道该发心跳了
      }
      replicator_cv_.notify_all();
      continue;
    }

    // ---- follower / candidate：随机选举超时 ----
    //
    // 用【节点编号 me_】而不是 OS 线程派生这条随机流：
    // 这样"N 号节点第 1/2/3 次超时各是多少毫秒"跨运行完全一致，
    // 选举时序可复现（不再受"哪个线程先跑起来"影响）。
    // 原理见 util.h 里 RandEngineFor / NextThreadStreamIndex 的注释。
    int timeout = raftcpp::RandIntFor(static_cast<uint64_t>(me_),
                                      kElectionTimeoutMin, kElectionTimeoutMax);
    bool should_start = false;
    {
      std::unique_lock<std::mutex> lk(mu_);
      //
      // 计时起点【必须】在等待开始之前取 —— 这是本 lab 最容易踩的坑之一。
      //
      // 反例：如果把起点取在"等待结束之后"（比如进了 StartElection 才取
      // Now()），那么"等待快结束时才姗姗来迟的心跳"或"我刚投出去的那一票"
      // 都会被判成无效，节点刚给别人投完票又立刻自提一个更高的任期，
      // 把刚选出来的 leader 挤掉 —— 集群每 200ms 改朝换代一次，
      // 任期疯狂上涨，任何命令都提交不了。
      // TestRejoin2B / TestCount2B 就是专门抓这个的。
      //
      auto start_time = raftcpp::Now();
      auto deadline = start_time + std::chrono::milliseconds(timeout);
      while (true) {
        if (killed_.load()) return;
        // 自己已经是 leader 了 → 立刻回外层去发心跳。
        // 少了这句，刚当选的节点要干等最多 300ms 才发第一个心跳。
        if (state_ == ServerState::kLeader) break;
        // 这个等待窗口内有人续过命 → 本次超时作废，重新抽签重新计时
        if (last_heartbeat_ >= start_time) break;
        if (raftcpp::Now() >= deadline) {
          should_start = true;  // 整个窗口都没动静，确实该选了
          break;
        }
        // ⚠️ macOS libc++ 兼容性：绝对 time_point 的 wait_until 在 deadline 已过期时
        // 会转出负 tv_sec → EINVAL 崩溃。改用相对 wait_for + 保护（逻辑等价）。
        auto remain = deadline - raftcpp::Now();
        if (remain > std::chrono::milliseconds(0)) tick_cv_.wait_for(lk, remain);
      }
    }
    if (killed_.load()) return;
    if (!should_start) continue;

    // TODO(2A)：超时了，该发起选举了。
    //
    //   StartElection() 内部会在锁内再做兜底确认，并完成
    //   "判定 + 自提"的原子操作。
    StartElection();
  }
}

void Raft::StartElection() {
  // ===================== 阶段 1:预投票 Pre-Vote =====================
  // 目的:在自增 term 之前先探测"我能不能拿到多数派支持",
  //       避免落后/分区的节点自增 term 去打断正常集群(治你见过的 term 暴涨)。
  // 铁律:本阶段【绝不】改 current_term_、【绝不】写 voted_for_、【绝不】持久化。
  RequestPreVoteArgs pargs;
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (killed_.load()) return;
    if (state_ == ServerState::kLeader) return;
    // 候选超时：本轮真实选举没拿到多数派（典型是 2 节点分区下两候选在旧 term
    // 各自投自己、永远 split vote）。不能原地重发【同 term】的 RequestVote——
    // 那样 term 永远不前进、对方永远因 voted_for_!=自己 拒投，活锁。
    // 正确做法：退回 follower（清 voted_for_/num_prevotes_/state_），让下面
    // "follower 预投票"流程重走一遍；预投票成功会经 StartRealElection 自增 term，
    // 于是 term 前进一格，对方收到更高 term 的 RequestVote 会退位让票，集群收敛。
    // ConvertToFollowerLocked 是 Locked 函数，必须在持锁时调用（内部不自加锁）。
    if (state_ == ServerState::kCandidate) {
      ConvertToFollowerLocked(current_term_);
    }
    if (!(last_heartbeat_ < raftcpp::Now())) return;  // 兜底二次确认,沿用原逻辑

    pargs.term = current_term_;            // 关键:用【当前】term,不 ++
    pargs.candidate_id = me_;
    pargs.last_log_index = LastLogIndexLocked();
    pargs.last_log_term = LastLogTermLocked();
    num_prevotes_ = 1;                     // 自己给自己一票（退位清 0 后重新发起预投票）
  }

  std::shared_ptr<Raft> self = shared_from_this();
  for (size_t i = 0; i < peers_.size(); i++) {
    if (static_cast<int>(i) == me_) continue;
    peers_[i]->CallAsyncTyped<RequestPreVoteArgs, RequestPreVoteReply>(
        "Raft.RequestPreVote", pargs,
        [self, pargs](bool ok, const RequestPreVoteReply& reply) {
          if (!ok) return;  // 丢包 / 对方挂了

          bool start_real = false;
          {
            std::lock_guard<std::mutex> lk(self->mu_);
            if (self->killed_.load()) return;

            // 预投票期间发现更高 term → 退位(学到新 term,停止捣乱)
            if (reply.term > self->current_term_) {
              self->ConvertToFollowerLocked(reply.term);
              self->last_heartbeat_ = raftcpp::Now();
              self->tick_cv_.notify_all();
              return;
            }

            // 只有仍是 follower(还没进入真实选举)才累计,避免重复触发
            // + pargs.term 校验:丢弃本轮之前的旧轮次预投票回复(逻辑重置)
            if (pargs.term == self->current_term_ &&
                self->state_ == ServerState::kFollower && reply.vote_granted) {
              self->num_prevotes_++;
              if (self->num_prevotes_ > static_cast<int>(self->peers_.size()) / 2) {
                start_real = true;
              }
            }
          }  // 锁在此释放,避免调用 StartRealElection 时重复加锁死锁
          if (start_real) self->StartRealElection();
        },
        &cancel_rpcs_);
  }
}

void Raft::StartRealElection() {
  // ===================== 阶段 2:真实投票 Real Vote =====================
  // 走到这里说明已拿到多数派预投票。此刻才【真正自增 term + 写 votedFor + 持久化】。
  RequestVoteArgs args;
  {
    std::lock_guard<std::mutex> lk(mu_);
    num_prevotes_ = 0;  // 预投票使命结束,清理(与 num_votes_ 无关)

    if (killed_.load()) return;
    if (state_ == ServerState::kLeader) return;
    if (!(last_heartbeat_ < raftcpp::Now())) return;

    Trace("S%d 自提: term %d -> %d (state=%s)", me_, current_term_, current_term_ + 1, StateName(state_));
    state_ = ServerState::kCandidate;
    current_term_++;
    voted_for_ = me_;
    num_votes_ = 1;  // 自己投自己
    last_heartbeat_ = raftcpp::Now();

    args.term = current_term_;
    args.candidate_id = me_;
    args.last_log_index = LastLogIndexLocked();
    args.last_log_term = LastLogTermLocked();

    PersistLocked();
  }

  SendRequestVoteRPCs();
}

// ===================== 真实投票 RPC 发送 =====================
// 被 StartRealElection(刚成为 candidate)与 candidate 超时重发共用。
// 只发送 RequestVote RPC,【不】修改 state_/current_term_/num_votes_(否则会重复自增 term)。
void Raft::SendRequestVoteRPCs() {
  RequestVoteArgs args;
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (killed_.load()) return;
    if (state_ != ServerState::kCandidate) return;
    args.term = current_term_;
    args.candidate_id = me_;
    args.last_log_index = LastLogIndexLocked();
    args.last_log_term = LastLogTermLocked();
  }

  std::shared_ptr<Raft> self = shared_from_this();
  for (size_t i = 0; i < peers_.size(); i++) {
    if (static_cast<int>(i) == me_) continue;
    peers_[i]->CallAsyncTyped<RequestVoteArgs, RequestVoteReply>(
        "Raft.RequestVote", args,
        [self, args](bool ok, const RequestVoteReply& reply) {
          if (!ok) return;

          std::lock_guard<std::mutex> lk(self->mu_);
          if (self->killed_.load()) return;

          if (reply.term > self->current_term_) {
            self->ConvertToFollowerLocked(reply.term);
            self->last_heartbeat_ = raftcpp::Now();
            self->tick_cv_.notify_all();
            return;
          }

          if (self->state_ == ServerState::kCandidate &&
              args.term == self->current_term_ && reply.vote_granted) {
            self->num_votes_++;
            if (self->num_votes_ > static_cast<int>(self->peers_.size()) / 2) {
              Trace("S%d 当选 leader term=%d lastIdx=%d", self->me_, self->current_term_, self->LastLogIndexLocked());
              self->state_ = ServerState::kLeader;
              // 条件补 no-op(沿用原 415-423 逻辑,满足 Figure 8)
              if (self->LastLogIndexLocked() > self->commit_index_) {
                Trace("S%d 当选补 no-op: term=%d idx=%d (commitIndex=%d, 有未提交旧尾巴)",
                      self->me_, self->current_term_,
                      self->LastLogIndexLocked() + 1, self->commit_index_);
                self->logs_.push_back(
                    LogEntry{self->current_term_,
                             self->LastLogIndexLocked() + 1, Command("")});
                self->PersistLocked();
              }
              int last = self->LastLogIndexLocked();
              for (size_t k = 0; k < self->peers_.size(); k++) {
                self->next_index_[k] = last + 1;
                self->match_index_[k] = 0;
              }
              self->heartbeat_seq_++;
              self->last_heartbeat_ = raftcpp::Now();
              self->tick_cv_.notify_all();
              self->replicator_cv_.notify_all();
            }
          }
        },
        &cancel_rpcs_);
  }
}

// ===========================================================================
// 第四部分：TODO(2A/2B) RPC handler
// ===========================================================================

void Raft::RequestVote(const RequestVoteArgs& args, RequestVoteReply& reply) {
  std::lock_guard<std::mutex> lk(mu_);

  reply.term = current_term_;
  reply.vote_granted = false;
  if (killed_.load()) return;

  // 规则 1：请求任期比我小 → 直接拒绝
  if (args.term < current_term_) return;

  // 规则 2：请求任期比我大 → 先退位
  if (args.term > current_term_) {
    ConvertToFollowerLocked(args.term);
    reply.term = current_term_;
  }

  // 规则 3：同一任期只能投一票（但可以重复投给同一个候选人）
  if (voted_for_ != -1 && voted_for_ != args.candidate_id) return;

  // 规则 4：候选人的日志必须至少不比我旧
  int my_last_term = LastLogTermLocked();
  int my_last_index = LastLogIndexLocked();
  bool up_to_date = (args.last_log_term > my_last_term) ||
                    (args.last_log_term == my_last_term &&
                     args.last_log_index >= my_last_index);
  if (!up_to_date) return;

  // 投票
  Trace("S%d 投票给 S%d (term %d)", me_, args.candidate_id, current_term_);
  voted_for_ = args.candidate_id;
  reply.vote_granted = true;
  // 给别人投票 == 承认这个任期里我听到过别人，所以刷新选举定时器
  last_heartbeat_ = raftcpp::Now();
  tick_cv_.notify_all();
  PersistLocked();
}

void Raft::RequestPreVote(const RequestPreVoteArgs& args,
                          RequestPreVoteReply& reply) {
  std::lock_guard<std::mutex> lk(mu_);

  reply.term = current_term_;
  reply.vote_granted = false;
  if (killed_.load()) return;

  // ===== 与 RequestVote 的唯一区别:本函数【绝不】改变任何持久/任期状态 =====
  // - 即使 args.term < current_term_ 也不退位、不拒绝(探测是 probe,放行才能夺回领导)
  // - 绝不写 voted_for_、绝不 PersistLocked()、绝不 current_term_ = args.term
  // 只做一件事:比较日志新旧(沿用 RequestVote 的 up_to_date 规则)。
  int my_last_term = LastLogTermLocked();
  int my_last_index = LastLogIndexLocked();
  bool up_to_date = (args.last_log_term > my_last_term) ||
                    (args.last_log_term == my_last_term &&
                     args.last_log_index >= my_last_index);
  if (!up_to_date) return;   // 对方日志比我还旧 → 不给预投票(否则可能产生落后 leader)

  // 授予预投票,但不改任何状态、不刷新 last_heartbeat_(避免无限推迟自己的选举)
  reply.vote_granted = true;

}

void Raft::AppendEntries(const AppendEntriesArgs& args,
                         AppendEntriesReply& reply) {
  std::lock_guard<std::mutex> lk(mu_);

  reply.term = current_term_;
  reply.success = false;
  reply.next_index = 0;
  if (killed_.load()) return;

  // 规则 1：任期比我小 → 拒绝
  if (args.term < current_term_) return;

  // 收到合法 leader 的消息 → 续命
  last_heartbeat_ = raftcpp::Now();
  tick_cv_.notify_all();

  // 任期更大（或我是 candidate 但对方同任期）→ 退位
  if (args.term > current_term_ || state_ != ServerState::kFollower) {
    ConvertToFollowerLocked(args.term);
    reply.term = current_term_;
  }

  int last_index = LastLogIndexLocked();

  // ---- 日志一致性检查 ----
  if (args.prev_log_index > last_index) {
    // 我的日志太短，接不上
    reply.next_index = last_index + 1;
    return;
  }
  if (args.prev_log_index >= snapshot_index_) {
    size_t pos = static_cast<size_t>(args.prev_log_index - snapshot_index_);
    if (logs_[pos].term != args.prev_log_term) {
      // 位置上有东西但任期对不上（冲突）→ 快速回退：
      // 一次性跳过整个冲突任期，而不是让 leader 一格一格退。
      int conflict_term = logs_[pos].term;
      int f = args.prev_log_index;
      while (f > snapshot_index_ + 1 &&
             logs_[static_cast<size_t>(f - 1 - snapshot_index_)].term ==
                 conflict_term) {
        f--;
      }
      reply.next_index = std::max(f, snapshot_index_ + 1);
      return;
    }
  }
  // else：prev_log_index 落在快照区间内，已被快照覆盖，视为匹配

  // ---- 追加 / 覆盖日志 ----
  bool log_changed = false;
  for (const LogEntry& e : args.entries) {
    if (e.index <= 0) continue;
    if (e.index <= snapshot_index_) continue;   // 已在快照中，跳过
    size_t pos = static_cast<size_t>(e.index - snapshot_index_);
    if (e.index <= LastLogIndexLocked()) {
      if (logs_[pos].term != e.term) {
        // 任期冲突：砍掉从 e.index 开始的所有旧条目，再接上新的
        logs_.resize(pos);
        logs_.push_back(e);
        log_changed = true;
      }
      // 任期一样 = 已经有了，什么都不做（保证幂等）
    } else {
      logs_.push_back(e);
      log_changed = true;
    }
  }
  if (log_changed) PersistLocked();

  // ---- 推进 commitIndex ----
  //
  // 论文原文："If leaderCommit > commitIndex, set commitIndex =
  //           min(leaderCommit, index of last new entry)"
  //
  // "index of last new entry" = prevLogIndex + len(entries)，
  // 也就是【leader 这一趟真正确认匹配到的位置】。
  //
  // ⚠️ 极容易写错成 min(leaderCommit, 我自己的日志长度)。后果很严重：
  // 本节点日志里若有一条"自己当年当 leader 时追加、但从未提交"的脏条目，
  // 而 leader 恰好发来一个空心跳（entries 为空），且 prevLogIndex 落在
  // 这条脏条目之前 —— 一致性检查会通过，follower 就把它自己的脏条目当成
  // "已提交"应用给上层状态机了。
  // TestRejoin2B 正是构造了这个场景（分区 leader 带着未提交日志回归）。
  if (args.leader_commit > commit_index_) {
    int last_new = args.prev_log_index + static_cast<int>(args.entries.size());
    int new_commit = std::min(args.leader_commit, last_new);
    if (new_commit > commit_index_) {
      commit_index_ = new_commit;
      apply_cv_.notify_all();
    }
  }

  reply.success = true;
}

// ===========================================================================
// 第五部分：TODO(2B) Start / 日志复制 / 提交
// ===========================================================================

StartResult Raft::Start(const Command& command) {
  std::lock_guard<std::mutex> lk(mu_);

  StartResult result;
  result.term = current_term_;
  if (killed_.load() || state_ != ServerState::kLeader) return result;

  int index = LastLogIndexLocked() + 1;
  logs_.push_back(LogEntry{current_term_, index, command});
  PersistLocked();

  result.is_leader = true;
  result.index = index;
  result.term = current_term_;

  // 立刻唤醒复制线程，别等下一个心跳。
  // 注意：这里只能 notify，绝不能直接调发 RPC 的函数 —— 正持着 mu_，
  // 而 RPC 回包处理也要 mu_，直接调就是死锁。
  replicator_cv_.notify_all();
  return result;
}

void Raft::ReplicateLoop(int server) {
  const size_t s = static_cast<size_t>(server);
  std::shared_ptr<Raft> self = shared_from_this();

  while (!killed_.load()) {
    AppendEntriesArgs args;
    InstallSnapshotArgs snap_args;
    bool send_now = false;
    bool send_snap = false;
    {
      std::unique_lock<std::mutex> lk(mu_);

      //
      // 等到"该发点什么了"：
      //   (1) 被 Kill
      //   (2) 距上次发送已超过 kMinSendIntervalMs（节流，防止空转打爆 RPC）
      //       且下面两条命中其一：
      //         · 心跳计数变了                    → 发心跳
      //         · 有新日志、且当前没有带日志的 RPC 在途（或已在途超过
      //           kLogRetryMs，说明回信丢了，重发）→ 带上日志发
      //
      // ⚠️ 踩过的坑：条件千万别写成 "state_ == kLeader"。
      // condition_variable::wait(lk, pred) 的语义是 while(!pred()) wait()，
      // 谓词一进来就成立时它会【立即返回、一毫秒都不等】→ 空转，
      // 一秒钟几万次 RPC，TestCount2B 直接挂。
      //
      // wait_until 的截止时间取"节流到期时刻"和"现在 +5ms"的较大值：
      // 保证不会忙等，又能在节流到期后尽快重试。
      auto earliest = last_send_time_[s] +
                      std::chrono::milliseconds(kMinSendIntervalMs);
      auto wake = std::max(earliest, raftcpp::Now() +
                                         std::chrono::milliseconds(5));
      // macOS libc++ 兼容性：绝对 time_point 的 wait_until 在 wake 已过期时
      // 会转出负 tv_sec → EINVAL 崩溃。改用相对 wait_for（过期时 remain 夹到 0，
      // wait_for(lk, 0, pred) 立即检查一次谓词后返回，等价 wait_until(过期 tp)）。
      auto remain = wake - raftcpp::Now();
      if (remain < std::chrono::milliseconds(0)) remain = std::chrono::milliseconds(0);
      replicator_cv_.wait_for(lk, remain, [this, s] {
        if (killed_.load()) return true;
        if (state_ != ServerState::kLeader) return false;
        if (raftcpp::Now() < last_send_time_[s] +
                                 std::chrono::milliseconds(kMinSendIntervalMs)) {
          return false;  // 节流中
        }
        if (heartbeat_seq_ != last_heartbeat_seq_[s]) return true;
        // 有新日志要发？
        if (next_index_[s] > LastLogIndexLocked()) return false;
        if (!inflight_log_[s]) return true;
        return raftcpp::Now() - inflight_log_time_[s] >
               std::chrono::milliseconds(kLogRetryMs);
      });

      if (killed_.load()) return;
      if (state_ != ServerState::kLeader) continue;

      bool has_more = next_index_[s] <= LastLogIndexLocked();
      bool may_send_log = has_more &&
                          (!inflight_log_[s] ||
                           raftcpp::Now() - inflight_log_time_[s] >
                               std::chrono::milliseconds(kLogRetryMs));
      bool heartbeat_due = heartbeat_seq_ != last_heartbeat_seq_[s];
      if (!may_send_log && !heartbeat_due) continue;

      last_heartbeat_seq_[s] = heartbeat_seq_;
      last_send_time_[s] = raftcpp::Now();

      int next = next_index_[s];
      if (next <= 0) next = 1;

      // 该 follower 落后到快照点之前 → 改发 InstallSnapshot 而非 AppendEntries。
      // 必须放在计算 prev_log_index/term 之前：否则 next-1 可能落在快照区间，
      // logs_[next-1] 会越界（截断后 logs_ 里没有那些下标）。
      if (next <= snapshot_index_) {
        snap_args.term = current_term_;
        snap_args.leader_id = me_;
        snap_args.last_included_index = snapshot_index_;
        snap_args.last_included_term = snapshot_term_;
        snap_args.data = snapshot_data_;
        send_snap = true;
      } else {
        args.term = current_term_;
        args.leader_id = me_;
        args.leader_commit = commit_index_;
        args.prev_log_index = next - 1;
        args.prev_log_term =
            logs_[static_cast<size_t>(next) - 1 - snapshot_index_].term;
        args.entries.clear();

        if (may_send_log) {
          // 【必须拷贝，不能持有 logs_ 里元素的引用】
          // 出了锁，别的线程（AppendEntries handler / Start）会改 logs_，
          // 拿着引用去序列化就是数据竞争 —— TSAN 一定会报。
          int last = LastLogIndexLocked();
          for (int i = next; i <= last; i++) {
            args.entries.push_back(
                logs_[static_cast<size_t>(i) - snapshot_index_]);
          }
          inflight_log_[s] = true;
          inflight_log_time_[s] = raftcpp::Now();
        }
        // 否则就是纯心跳：只带 prevLog 做一致性检查，不带任何日志
        send_now = true;
      }
    }
    if (send_snap) {
      peers_[s]->CallAsyncTyped<InstallSnapshotArgs, InstallSnapshotReply>(
          "Raft.InstallSnapshot", snap_args,
          [self, server, snap_args](bool ok, const InstallSnapshotReply& reply) {
            const size_t s = static_cast<size_t>(server);
            if (!ok) return;  // 丢包 / 对方挂了，下一轮重发
            std::lock_guard<std::mutex> lk(self->mu_);
            if (self->killed_.load()) return;
            if (reply.term > self->current_term_) {
              self->ConvertToFollowerLocked(reply.term);
              self->last_heartbeat_ = raftcpp::Now();
              self->tick_cv_.notify_all();
              self->replicator_cv_.notify_all();
              return;
            }
            // 快照已装好：把该 follower 的复制进度推到快照点之后，
            // 之后走普通 AppendEntries。
            self->match_index_[s] =
                std::max(self->match_index_[s], snap_args.last_included_index);
            self->next_index_[s] = snap_args.last_included_index + 1;
            self->replicator_cv_.notify_all();
          },
          &cancel_rpcs_);
      continue;
    }
    if (!send_now) continue;

    // 发射后不管：RPC 在路上时本线程继续跑，不会被断连节点的
    // 0~7000ms 模拟延迟拖死。
    peers_[s]->CallAsyncTyped<AppendEntriesArgs, AppendEntriesReply>(
        "Raft.AppendEntries", args,
        [self, server, args](bool ok, const AppendEntriesReply& reply) {
          const size_t s = static_cast<size_t>(server);          
          if (!ok) {                              // 丢包 / 对方挂了
            std::lock_guard<std::mutex> lk(self->mu_);
            if (!self->killed_.load() && !args.entries.empty())
              self->inflight_log_[s] = false;    // 立刻清标记
            return;
          }

          std::lock_guard<std::mutex> lk(self->mu_);
          if (self->killed_.load()) return;

          // 这一批在途日志已经有了结论（无论成败），可以再发下一批了
          if (!args.entries.empty()) self->inflight_log_[s] = false;

          if (reply.term > self->current_term_) {
            self->ConvertToFollowerLocked(reply.term);
            self->last_heartbeat_ = raftcpp::Now();
            self->tick_cv_.notify_all();
            self->replicator_cv_.notify_all();
            return;
          }

          // 二次确认：迟到的回包作废
          if (args.term != self->current_term_ ||
              self->state_ != ServerState::kLeader) {
            return;
          }

          if (reply.success) {
            int new_match =
                args.prev_log_index + static_cast<int>(args.entries.size());
            if (new_match > self->match_index_[s]) {
              self->match_index_[s] = new_match;
            }
            self->next_index_[s] = self->match_index_[s] + 1;

            // ---- 推进 commitIndex：取所有 matchIndex 的中位数 ----
            // 论文 §5.4 + Figure 8 的坑：只有【当前任期】的日志能靠多数派提交；
            // 旧任期的日志必须等当前任期的某条日志被提交后"顺带"提交。
            // 少了 `logs_[N].term == current_term_` 这个判断，
            // TestFigure82C 必挂。
            std::vector<int> matched;
            matched.reserve(self->peers_.size());
            for (size_t i = 0; i < self->peers_.size(); i++) {
              if (static_cast<int>(i) == self->me_) {
                matched.push_back(self->LastLogIndexLocked());
              } else {
                matched.push_back(self->match_index_[i]);
              }
            }
            std::sort(matched.begin(), matched.end());
            int n = matched[matched.size() / 2];
            if (n > self->commit_index_ && n >= 0) {
              size_t pos = static_cast<size_t>(n) - self->snapshot_index_;
              bool term_ok = (pos < self->logs_.size() &&
                              self->logs_[pos].term == self->current_term_);
              if (term_ok) {
                self->commit_index_ = n;
                self->apply_cv_.notify_all();
                self->heartbeat_seq_++;
                self->replicator_cv_.notify_all();
              }
            }
            if (self->next_index_[s] <= self->LastLogIndexLocked()) {
              self->replicator_cv_.notify_all();  // 还有积压，继续发
            }
          } else {
            // 回退 nextIndex，下次少发一点。
            // 下界取 matchIndex+1：防止迟到的旧回包把进度往回拽。
            int hint = reply.next_index;
            if (hint > 0) {
              self->next_index_[s] = std::max(hint, self->match_index_[s] + 1);
            } else {
              self->next_index_[s] =
                  std::max(1, self->next_index_[s] - 1);
            }
            self->replicator_cv_.notify_all();  // 立刻再试，别等心跳
          }
        },
        &cancel_rpcs_);
  }
}

void Raft::ApplyLoop() {
  // apply_ch_ 有且只能有一个发送者（本函数）。
  // InstallSnapshot 只把 snapMsg 挂到 pending_snapshot_，由本函数取出派发，
  // 避免 InstallSnapshot 直接 push 与本函数的 log entries 派发互相穿插，
  // 导致 snapshot 应用顺序错乱 → replicas 发散。
  while (!killed_.load()) {
    ApplyMsg msg;
    bool has_msg = false;
    {
      std::unique_lock<std::mutex> lk(mu_);
      apply_cv_.wait_for(lk, std::chrono::milliseconds(10), [this] {
        return killed_.load() || pending_snapshot_.has_value() ||
               (commit_index_ > last_applied_);
      });
      if (killed_.load()) return;

      // 1) 优先派发快照（仿 Go 版 src/raft/raft.go:853-859）
      if (pending_snapshot_.has_value()) {
        Trace("S%d ApplyLoop派发snap idx=%d (commit=%d applied=%d)", me_,
              pending_snapshot_->snapshot_index, commit_index_, last_applied_);
        msg = std::move(*pending_snapshot_);
        pending_snapshot_.reset();
        has_msg = true;
      } else if (commit_index_ > last_applied_) {
        // 2) 派发已提交的 log entries
        int idx = last_applied_ + 1;
        if (idx <= snapshot_index_) {
          last_applied_ = idx;  // 已被快照覆盖，视为已 apply
        } else {
          size_t pos = static_cast<size_t>(idx - snapshot_index_);
          if (pos < logs_.size()) {
            last_applied_ = idx;
            msg.command_valid = true;
            msg.command = logs_[pos].command;
            msg.command_index = idx;
            has_msg = true;
          }
          // else：该 idx 尚未复制到本节点，等下一轮
        }
      }
    }
    if (has_msg) {
      apply_ch_->Push(std::move(msg));  // 在锁外发送
    }
  }
}

// ===========================================================================
// 第六部分补充：快照（Lab 3 日志压缩）
// ===========================================================================

// 1.必须已 apply：快照本质是「状态机在 index=X 的物理快照」，所以 snap_idx = rafts_[i]->LastApplied()（config.cpp:211）
// 2. lastApplied ≤ commitIndex 不变量：由 ApplyLoop（raft.cpp:782-797）逐条推进维护
// 3. Raft::Snapshot 兜底拒绝 index > commit_index_（raft.cpp:821）——双保险
void Raft::Snapshot(int index, const std::string& snapshot) {
  std::lock_guard<std::mutex> lk(mu_);

  // 只能快照已提交的，否则未提交数据会被当已持久化。
  if (index > commit_index_) {
    Trace("S%d Snapshot 拒绝：index=%d > commit_index_=%d", me_, index,
          commit_index_);
    return;
  }
  // 已经覆盖到更靠后的快照了，无需重复截断。
  if (index <= snapshot_index_) return;

  // 在旧 logs_ 里定位 index 对应的下标：旧哨兵 index == 旧 snapshot_index_，
  // 所以逻辑 index 在旧 logs_ 中的下标 = index - 旧 snapshot_index_。
  size_t pos = static_cast<size_t>(index - snapshot_index_);
  if (pos >= logs_.size()) return;  // 防御：index 不合法
  int snap_term = logs_[pos].term;

  // 截断 logs_：保留哨兵(index 对齐到快照点) + 快照点之后的条目。
  std::vector<LogEntry> new_logs;
  LogEntry sentinel;
  sentinel.index = index;
  sentinel.term = snap_term;
  new_logs.push_back(sentinel);
  for (size_t i = pos + 1; i < logs_.size(); ++i) {
    new_logs.push_back(std::move(logs_[i]));
  }
  logs_.swap(new_logs);

  snapshot_index_ = index;
  snapshot_term_ = snap_term;
  snapshot_data_ = snapshot;
  snapshot_dirty_ = true;

  PersistLocked();
  Trace("S%d 生成快照 index=%d term=%d 日志截断到 %zu 条", me_, snapshot_index_,
        snapshot_term_, logs_.size());
}

void Raft::InstallSnapshot(const InstallSnapshotArgs& args,
                           InstallSnapshotReply& reply) {
  // ⚠️ 单相设计（仿 Go 版 src/raft/raft.go:882-924）
  //   立即在 raft 锁内做完所有 raft state 变更：
  //     1) 更新 snapshot_index_ / snapshot_term_ / snapshot_data_
  //     2) 截断 logs_（重建哨兵）
  //     3) 推进 commit_index_ / last_applied_ 到 args.last_included_index
  //     4) PersistLocked() 一次性落盘 raft state + snapshot blob
  //   然后才把 snapMsg 挂 pending_snapshot_，由 ApplyLoop 统一派发给 KVServer。
  //
  // ⚠️ 修复 · applyCh 单发送者：与 v3 "minimal fix" 相同的处置——本函数【不再
  // 直接 push 到 apply_ch_】，只挂 pending_snapshot_，由 ApplyLoop 统一派发。
  //
  // 为什么不能再保留两段式（handler 只挂 + CondInstallSnapshot 完成截断）？
  //   v3 "minimal fix" 的实测表明，那设计有一个**必现**的窗口：
  //     t0  InstallSnapshot 收到 args.last_included_index=888，挂 pending_snapshot_，返回。
  //     t1  ApplyLoop 派发 snapMsg 给 KVServer。
  //     t1' 与此同时，leader 又发来 AppendEntries(prev=869, entries=[870..904])
  //          ——因为 snapshot_index_ 仍 = 869、logs_ 仍含 [869..904] 的旧条目，
  //          AppendEntries 不会拒，这些 entries 全部进 logs_，并被 commit+派发出去。
  //     t2  KVServer 终于处理 snapMsg、调用 CondInstallSnapshot(888) 截断 logs_。
  //   而 idx=870..888 这段"先于 snap 应用"的旧条目，可能跟 idx>=889 的"新"
  //   条目拼接起来形成前缀冲突的 logs_，且 KV state 已经被旧 entries 改过——
  //   当 CondInstallSnapshot 完成重置 KV state 到 snap 时刻，idx=870..888 的旧
  //   entries 不会"回滚"再 apply（已被 snap 取代），但 idx=889..904 的新 entries
  //   已经 modify 过 KV state → 客户端发过来的 op 在 leader 上看到的"线性读"=
  //   snap 之后状态，但 follower 此刻的 KV state 还残留 idx=870..888 的旧值。
  //   TestSnapshotUnreliableRecover3B 的 "server[0] vs server[1] key=X" 就这样发散。
  //
  //   单相设计让这个窗口期归零：handler 返回前所有 raft state 已一致，
  //   ApplyLoop 不会再派发 idx ≤ snap_index 的旧条目。
  ApplyMsg msg;
  {
    std::lock_guard<std::mutex> lk(mu_);
    reply.term = current_term_;

    if (args.term < current_term_) return;  // 旧 leader 的快照，拒绝

    if (args.term > current_term_) {
      ConvertToFollowerLocked(args.term);
      reply.term = current_term_;
    }
    last_heartbeat_ = raftcpp::Now();
    tick_cv_.notify_all();

    // 已经包含这个快照（或更新的），无需重复安装（idempotent）。
    // ⚠️ 改用 last_applied_ 判旧（对齐 Go 版 raft.go:904）：
    // 基准必须是「状态机已应用位置」，而不是 snapshot_index_（日志截断点）。
    // 二者恒有 last_applied_ >= snapshot_index_；若只比 snapshot_index_，存在窗口
    // snapshot_index_ < args.index <= last_applied_：follower 已应用到 100、本地
    // 快照截断到 90，leader 迟到发快照 95 会被误接受 → 日志截断 + lastIncludedIndex
    // 推进到 95，若 lastApplied 再被回退，96~100 的 Put/Append 会被重放 → 重复 append
    // + 丢新日志（回归 bug：get wrong value / got []）。
    if (args.last_included_index <= last_applied_) return;

    int old_snap_index = snapshot_index_;
    snapshot_index_ = args.last_included_index;
    snapshot_term_ = args.last_included_term;
    snapshot_data_ = args.data;
    snapshot_dirty_ = true;

    // 截断 logs_：哨兵重建（index 对齐到快照点）；args.last_included_index+1 之后
    // 的旧条目保留，等 leader 重发覆盖即可（即使保留也可能错位，AppendEntries 的
    // 一致性检查会基于 prev_log_index 砍掉冲突段）。
    //
    // 注意：logs_[0] 必须同步改写——老哨兵的 index/term 已不再对应新 snap_index_，
    // 否则 AppendEntries 的 pos = e.index - snapshot_index_ 会算错。
    size_t pos = static_cast<size_t>(args.last_included_index - old_snap_index);
    if (pos < logs_.size()) {
      std::vector<LogEntry> new_logs;
      new_logs.reserve(logs_.size() - pos);
      for (size_t i = pos; i < logs_.size(); ++i) {
        new_logs.push_back(std::move(logs_[i]));
      }
      new_logs[0] = LogEntry{args.last_included_term, args.last_included_index, ""};
      logs_.swap(new_logs);
    } else {
      // 极落后 follower：logs_ 整段被快照覆盖，重建只剩新哨兵。
      logs_.clear();
      logs_.push_back(LogEntry{args.last_included_term, args.last_included_index, ""});
    }

    // 推进 commit/lastApplied：snap 覆盖的条目都已被 leader 提交，ApplyLoop
    // 不必再重放它们。
    if (commit_index_ < args.last_included_index) {
      commit_index_ = args.last_included_index;
    }
    if (last_applied_ < args.last_included_index) {
      last_applied_ = args.last_included_index;
    }
    apply_cv_.notify_all();

    // 一次性落盘：raft state（含截断后的 logs_、新 snapshot 元数据）+ snap blob
    PersistLocked();

    msg.snapshot_valid = true;
    msg.snapshot = args.data;  // 锁内拷一份
    msg.snapshot_index = args.last_included_index;
    msg.snapshot_term = args.last_included_term;

    // 挂到 pending_snapshot_，由 ApplyLoop 统一派发。
    // 若已有待投递快照，只保留更新的那个（旧的已被新的完全覆盖，投它没有意义）。
    if (!pending_snapshot_.has_value() ||
        pending_snapshot_->snapshot_index < msg.snapshot_index) {
      Trace("S%d InstallSnapshot单相挂pending idx=%d term=%d (已挂=%s)", me_,
            msg.snapshot_index, msg.snapshot_term,
            pending_snapshot_.has_value() ? "是" : "否");
      pending_snapshot_ = std::move(msg);
      apply_cv_.notify_all();  // 唤醒 ApplyLoop
    }
    return;
  }
}

bool Raft::CondInstallSnapshot(int index, int term,
                               const std::string& snapshot) {
  (void)snapshot;
  std::lock_guard<std::mutex> lk(mu_);
  // 单相设计后，本函数【只做"是否已接受过这个 snap"的查询】——所有 raft state
  // 修改已经在 InstallSnapshot 内完成（截断 + 推进 + 持久化）。
  //
  // 返回 true：raft 当前快照元数据正好匹配 (index, term)，
  //           KVServer 可以安全 ApplySnapshotLocked 重置 KV state。
  // 返回 false：边界场景——
  //   1) stale snap（已被更新覆盖）→ KV state 已是更新的，不要回退
  //   2) 极小概率：InstallSnapshot 单相修改后 last_applied_ > index，意味着
  //      leader 在装 snap 后又发了一批新 entries 已经 apply；KVServer 应让
  //      ApplyLoop 派的 logs 来追平，不能用更旧的 snap 覆盖更新的 KV state。
  if (index != snapshot_index_ || term != snapshot_term_) {
    Trace("S%d CondInstallSnapshot idx=%d term=%d → false (raft snap_idx=%d snap_term=%d last_applied=%d)",
          me_, index, term, snapshot_index_, snapshot_term_, last_applied_);
    return false;
  }
  if (last_applied_ > index) {
    Trace("S%d CondInstallSnapshot idx=%d → false (stale: last_applied=%d)", me_, index, last_applied_);
    return false;
  }
  Trace("S%d CondInstallSnapshot idx=%d term=%d → true", me_, index, term);
  return true;
}

// ===========================================================================
// 第六部分：TODO(2C) 持久化
// ===========================================================================

void Raft::PersistLocked() {
  labrpc::Encoder e;
  e.Int(current_term_).Int(voted_for_);
  e.Int(snapshot_index_).Int(snapshot_term_);  // 快照元数据也要随状态恢复
  e.Int(static_cast<int>(logs_.size()));
  for (const LogEntry& en : logs_) {
    e.Int(en.term).Int(en.index).Bytes(en.command);
  }

  if (snapshot_dirty_) {
    persister_->SaveStateAndSnapshot(e.Take(), snapshot_data_);  // 冷路径：都写
    snapshot_dirty_ = false;
  } else {
    persister_->SaveRaftState(e.Take());   // 热路径：只写状态，快照文件原样留着
  }
}

void Raft::ReadPersist(const std::string& data) {
  if (data.empty()) return;

  labrpc::Decoder d(data);
  int term = 0, voted = -1, snap_idx = 0, snap_term = 0, n = 0;
  if (!(d.Int(term) && d.Int(voted) && d.Int(snap_idx) && d.Int(snap_term) &&
        d.Int(n)))
    return;
  if (n < 0 || n > (1 << 22)) return;

  std::vector<LogEntry> logs;
  logs.reserve(static_cast<size_t>(n));
  for (int i = 0; i < n; i++) {
    LogEntry en;
    if (!(d.Int(en.term) && d.Int(en.index) && d.Bytes(en.command))) return;
    logs.push_back(std::move(en));
  }

  current_term_ = term;
  voted_for_ = voted;
  snapshot_index_ = snap_idx;
  // 重启一致性：快照覆盖的条目都已被提交且应用（对齐 Go 版 Make 里
  // lastApplied = lastIncludedIndex）。不设这个，会出现 snapshot_index_ 很大
  // 但 last_applied_=0 的错位 → InstallSnapshot 的守卫失效、陈旧快照被接受 → 丢数据。
  commit_index_ = snap_idx;
  last_applied_ = snap_idx;
  snapshot_term_ = snap_term;
  snapshot_data_ = persister_->ReadSnapshot();  // 不透明 blob，直接恢复
  logs_ = std::move(logs);

  // 现在才是真正的"消费完整字节流"边界
  // 注：原代码在上方赋值后又用 snapshot_index_ 重复赋了一次 commit_index_/last_applied_，
  // 二者等价（snapshot_index_ == snap_idx），属冗余，已删除。
  if (!d.Ok()) return;
}

}  // namespace raft
