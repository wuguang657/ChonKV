// raft.cpp —— Raft 核心实现（2A / 2B / 2C / 2D 均已完整实现）

#include "raft.h"
#include <cstdio>
#include <cstdlib>  // std::abort（P1-1：坏 snapshot blob 的 fail-stop）

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
// §2.1 快照分块：CRC32（IEEE 802.3 多项式）工具 —— 用于分块传输完整性校验。
// 完整快照 blob 在 leader 侧算一次 CRC32，随每条 chunk 带上；follower 收到 done
// 重组出完整 blob 后重算并比对，不一致（半截写 / 分块丢失 / 损坏）则拒绝安装。
// 截断成 int 仅用于「相等比较」，双射不丢区分度。
// ===========================================================================
// ⚠️ TSan 性能护栏：Crc32 是纯函数（只读入参、不碰任何共享状态），本来就不该被
// TSan 逐访存插桩。实测记载（见 Raft::Snapshot 处注释）：每次快照都算一遍 CRC32
// 会把 KV 的 apply/快照循环整体拖慢 → 日志来不及截断 → "logs were not trimmed"
// （RELEASE+TSAN 50 轮 3/50）。P1-1 要求每次落盘都算 CRC，若不关插桩等于把这笔
// 开销原样加回来。故这里显式关闭本函数的 TSan 插桩。
#if defined(__clang__)
#define RAFT_NO_SANITIZE_THREAD __attribute__((no_sanitize("thread")))
#else
#define RAFT_NO_SANITIZE_THREAD
#endif

// 原始循环（不做「末态取反」），供增量续算复用。
RAFT_NO_SANITIZE_THREAD
static uint32_t Crc32Raw(uint32_t crc, const char* data, size_t n) {
  // C++11 magic statics：lambda 内 static 表 + 外层 static 指针，
  // 首次调用由运行时保证线程安全的单次初始化，消除 TSan data race。
  // 不再用手写 if(!inited) 双检查锁（那正是竞态根源）。
  static const uint32_t* table = []() {
    static uint32_t t[256];
    for (uint32_t i = 0; i < 256; ++i) {
      uint32_t c = i;
      for (int k = 0; k < 8; ++k)
        c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
      t[i] = c;
    }
    return t;
  }();
  for (size_t i = 0; i < n; i++)
    crc = table[(crc ^ static_cast<uint8_t>(data[i])) & 0xff] ^ (crc >> 8);
  return crc;
}

uint32_t Crc32Update(uint32_t crc, const char* data, size_t n) {
  // 入参 crc 是「已取反的终值」：先还原成运行值再续算，末尾再取反。
  // 这样 Crc32Update(a‖b) == Crc32Update(Crc32Update(0, a), b)，可分段流式计算。
  return Crc32Raw(crc ^ 0xFFFFFFFFu, data, n) ^ 0xFFFFFFFFu;
}

int Crc32(const char* data, size_t n) {
  // 空串的 CRC32 == 0，故以 0 起算即为标准 CRC32。
  return static_cast<int>(Crc32Update(0, data, n));
}

// ===========================================================================
// 第一部分：序列化（已实现，不用改）
// ===========================================================================
//

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
      .Int(leader_commit)
      .Int(read_ctx);
  e.Int(static_cast<int>(entries.size()));
  for (const LogEntry& en : entries) {
    e.Int(en.term).Int(en.index).Bytes(en.command)
    .Int(static_cast<int>(en.type)).Int(en.conf_server).Int(en.conf_role);
  }
  return e.Take();
}

bool AppendEntriesArgs::Deserialize(const std::string& s) {
  labrpc::Decoder d(s);
  if (!(d.Int(term) && d.Int(leader_id) && d.Int(prev_log_index) &&
        d.Int(prev_log_term) && d.Int(leader_commit) && d.Int(read_ctx)))
    return false;
  int n = 0;
  if (!d.Int(n) || n < 0) return false;
  entries.clear();
  entries.reserve(static_cast<size_t>(n));
  for (int i = 0; i < n; i++) {
    LogEntry en;
    int et = 0;
    if (!(d.Int(en.term) && d.Int(en.index) && d.Bytes(en.command) &&
          d.Int(et) && d.Int(en.conf_server) && d.Int(en.conf_role)))
      return false;
    en.type = static_cast<EntryType>(et);
    entries.push_back(std::move(en));
  }
  return d.Ok();
}

std::string AppendEntriesReply::Serialize() const {
  labrpc::Encoder e;
  e.Int(term).Bool(success).Int(next_index).Int(read_ctx);
  return e.Take();
}

bool AppendEntriesReply::Deserialize(const std::string& s) {
  labrpc::Decoder d(s);
  return d.Int(term) && d.Bool(success) && d.Int(next_index) && d.Int(read_ctx) && d.Ok();
}

std::string InstallSnapshotArgs::Serialize() const {
  labrpc::Encoder e;
  e.Int(term).Int(leader_id).Int(last_included_index).Int(last_included_term);
  e.Int(offset).Int(chunk_size).Int(done ? 1 : 0).Int(crc).Int(epoch);
  e.Bytes(data);
  e.Int(static_cast<int>(members.size()));
  for (int m : members) e.Int(m);
  return e.Take();
}

bool InstallSnapshotArgs::Deserialize(const std::string& s) {
  labrpc::Decoder d(s);
  int done_flag = 0;
  if (!(d.Int(term) && d.Int(leader_id) && d.Int(last_included_index) &&
        d.Int(last_included_term) && d.Int(offset) && d.Int(chunk_size) &&
        d.Int(done_flag) && d.Int(crc) && d.Int(epoch) && d.Bytes(data)))
    return false;
  done = (done_flag != 0);
  int nm = 0;
  if (!d.Int(nm) || nm < 0) return false;
  members.clear();
  members.reserve(static_cast<size_t>(nm));
  for (int i = 0; i < nm; i++) {
    int m = 0;
    if (!d.Int(m)) return false;
    members.push_back(m);
  }
  return d.Ok();
}

std::string InstallSnapshotReply::Serialize() const {
  labrpc::Encoder e;
  e.Int(term).Int(ok_install ? 1 : 0);
  return e.Take();
}

bool InstallSnapshotReply::Deserialize(const std::string& s) {
  labrpc::Decoder d(s);
  int ok = 0;
  bool ok_parse = d.Int(term) && d.Int(ok) && d.Ok();
  if (ok_parse) ok_install = (ok != 0);
  return ok_parse;
}

std::string ReadIndexArgs::Serialize() const {
  labrpc::Encoder e;
  e.Int(term);
  return e.Take();
}

bool ReadIndexArgs::Deserialize(const std::string& s) {
  labrpc::Decoder d(s);
  return d.Int(term) && d.Ok();
}

std::string ReadIndexReply::Serialize() const {
  labrpc::Encoder e;
  e.Int(term).Int(read_index);
  return e.Take();
}

bool ReadIndexReply::Deserialize(const std::string& s) {
  labrpc::Decoder d(s);
  return d.Int(term) && d.Int(read_index) && d.Ok();
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
  last_ack_time_.assign(peers_.size(), raftcpp::TimePoint{});    // CheckQuorum/Lease：follower 最近一次成功回包时刻
  cq_consec_fail_.assign(peers_.size(), 0);   // CheckQuorum：每 follower 连续 RPC 失败计数，初始化为 0（健康）

  is_member_.assign(peers_.size(), MemberRole::kVoter);  // 初始化所有节点都是正式投票成员
  removed_at_index_.assign(peers_.size(), -1);  // Q2：冻结点初始化为"无上限"
  last_quorum_check_ = raftcpp::Now();
  // 下面三个 vector 必须按 peers_.size() 初始化，否则 ReplicateLoop 里
  // last_send_time_[s] / inflight_log_[s] / inflight_log_time_[s] 在空 vector
  // 上越界读 → 段错误（2A 一启动就崩）。
  last_send_time_.assign(peers_.size(), raftcpp::TimePoint{});   // 复制线程上次发包时刻，用于节流
  inflight_log_time_.assign(peers_.size(), raftcpp::TimePoint{});// 在途 RPC 发出时刻，用于超时重发

  // 重启后从磁盘恢复 raft state（含快照边界，见 ReadPersist）。
  ReadPersist(persister_->ReadRaftState());

  // 对齐 Go 版 Make：nextIndex 初始化为"当前最后一条日志下标 + 1"。
  // 放在 ReadPersist 之后，确保带持久化日志/快照重启后，nextIndex 反映真实日志长度，
  // 而不是固定 1（Go 原版用 len(rf.log)，也是读盘后的值）。
  // 实战中当选时会再被 become-leader 回调重置一次，这里只是更稳健的初值。
  next_index_.assign(peers_.size(), LastLogIndexLocked() + 1);
  snap_in_flight_.assign(peers_.size(), 0);  // §2.1：每 follower 分块快照在途标记
  snap_epoch_.assign(peers_.size(), 0);       // §2.1：每 follower 当前传输代号（档2）

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

int Raft::GetLeaderId() const {
  std::lock_guard<std::mutex> lk(mu_);
  return leader_id_;
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
  pending_stepdown_ = false;  // 已退位，清理事件驱动退位标志
  // 退位即作废旧 leader 租约：避免 CheckQuorum 退位(不增 term)后残留的
  // lease_expire_ 停在“未来时间”，使该节点重当选瞬间误走快路径
  // (raft.cpp:2111) 返回 stale 的 commit_index_。租约是绝对时间戳，
  // 必须随退位清零(RAII)。详见 lease read 审计结论。
  lease_expire_ = raftcpp::TimePoint{};
  // term / votedFor 变了，必须在应答 RPC 【之前】落盘：丢了它们就可能
  // 在同一 term 投两次票 → 安全性崩塌。所以这里不能挪到锁外异步做。
  PersistLocked();
  // 退位即唤醒所有正在 wait_for 的 ReadIndex 读线程：被更高 term / 事件驱动退位的
  // 各路径(563/669/1264/1304/2163 等)都经此函数。它们会因 term 变化或
  // state_!=kLeader 立刻退出返回 -1，无需干等满 deadline。统一在此发，避免各
  // 退位调用点遗漏(原 AppendEntries 回调:2163 缺此唤醒。
  read_cv_.notify_all();
}

// ===========================================================================
// 第三部分：选举定时器 + 拉票
// ===========================================================================

void Raft::ElectionTimerLoop() {
  //
  // 这个循环身兼两职：
  //   * 我是 leader      → 每 kHeartbeatInterval 毫秒发一次心跳
  //   * 我不是 leader    → 随机等 150~300ms，没听到 leader 就发起选举
  //
  // CheckQuorum 的自检节流：不需要每 50ms 都查一次，每 kCheckQuorumIntervalMs
  // 查一次就够（150ms，正好是一个最短选举超时）。
  while (!killed_.load()) {
    // 持锁决定分支 + 贯穿整个迭代：用实时 state_ 取代原 st 快照（原快照取完即释放锁，
    // 再用它分支属于易误导的写法）。两条分支复用同一把锁，避免重复加锁。
    std::unique_lock<std::mutex> lk(mu_);
    if (killed_.load()) return;

    if (state_ == ServerState::kLeader) {
      // ---- leader：睡满一个心跳周期，然后自增心跳计数 ----
      // （锁 lk 已在 while 体顶部持有，此处不再单独 unique_lock）
      // std::chrono::steady_clock::time_point内部重载了 + 运算符，可以直接加时间间隔
      // 这里不加voter判断，宽限期内 state_ 仍是 kLeader（ApplyLoop 没立刻转 follower），这段时间里它必须继续跑 leader 分支发心跳，整个「退位宽限期」设计才成立。
      auto deadline =
          raftcpp::Now() + std::chrono::milliseconds(kHeartbeatInterval);
        // 如果当前时间还没到超时时间，wait_for会睡眠并释放锁，等deadline时间到了/notify_all再唤醒，重新拿起锁，while循环继续
        while (!killed_.load() && state_ == ServerState::kLeader && raftcpp::Now() < deadline) {
          // wait_until(abs_time) 内部要把"未来的某个时刻"翻译成 timespec，deadline 已经过期时 macOS 这版 libc++ 会算出 tv_sec = -1，被 POSIX 拒绝并抛 system_error；改用 wait_for(相对时长) 就没"过期"问题，再加一层 if (remain > 0) 人工 clamp 做双保险。
          // 就算算出负值也会被wait_for clamp为0
          auto remain = deadline - raftcpp::Now();
          if (remain > std::chrono::milliseconds(0)) tick_cv_.wait_for(lk, remain);
        }
        if (killed_.load()) return;
        // ★ 核心修复（bug ①）：wait_for 会释放锁，期间可能被更高 term 的 AppendEntries 退位。
        //   若已不是 leader，绝不跑下方 CheckQuorum 等 leader 专属动作，直接回外层循环走
        //   follower 分支，避免 ConvertToFollowerLocked 误清 voted_for_ 导致同 term 双投票。
        if (state_ != ServerState::kLeader) { lk.unlock(); continue; }

        // ===================================================================
        // CheckQuorum（生产级扩展 ①）：leader 定期自查
        // "最近一个选举超时窗口内，我还能联系上多数派吗？"
        // ===================================================================
        // 为什么必须自查？
        //   网络分区把 leader 隔离到少数派之后，它发的心跳收不到多数派回包，
        //   却仍然以为自己是 leader。后果有两个：
        //     (1) 它继续以 leader 身份应答客户端 → 读到早就被新 leader 改过的
        //         旧值（脏读 / stale read）；
        //     (2) 它的 term 还挂在自己身上，会干扰新 leader 的选举。
        //   有了 CheckQuorum，被隔离的 leader 会【主动退位】成 follower，
        //   客户端随即被重定向到真正的新 leader 上。
        //
        // 为什么退位【不增 term】？
        //   这里只是"放弃 leader 身份"，term 保持不变 —— 和预投票
        //   "不随便涨 term"是同一个道理：涨 term 会打断正常集群的选举。
        // 脏窗口 = 一个 CheckQuorum 间隔（~150ms）​。这 150ms 里如果有个客户端连着旧 leader 发读请求，拿到的值可能已经被新 leader 改过了 → 脏读。
        // CheckQuorum：leader 每 ~150ms 自查一次（闸门 now_q - last_quorum_check_ >= 150ms），回顾最近 150ms 内自己是否收到了多数派（含自己）的回包；若某个 follower 的回包时间戳超过 150ms 没刷新（即收不到它的回复），且凑不齐 majority → 退位。而 leader 每 50ms 固定发一次 AppendEntries（空/带日志），落后太多时另发 InstallSnapshot——发是 leader 主动的，CheckQuorum 查的是"回包收没收到"​。
        auto now_q = raftcpp::Now();
        if (now_q - last_quorum_check_ >=
            std::chrono::milliseconds(kCheckQuorumIntervalMs)) {
          last_quorum_check_ = now_q;
          const int majority = MemberCountLocked() / 2 + 1;
          // 存活判定用 cq_consec_fail_（最近 RPC 成败），而不是 last_ack_time_（回包到达时刻）：
          // 长重排下回包会延迟 200~2200ms 才到，last_ack_time_ 因此长期"陈旧"，
          // 若按它数"窗口内联系上几个"会把【健康但因延迟暂时没收到回包】的 follower
          // 判成失联 → leader 被反复误杀 → 集群选不出稳定 leader（TestFigure8Unreliable2C 挂死）。
          // cq_consec_fail_ 在回包 ok=true（哪怕迟到）时清零、ok=false 时累加，
          // 自然区分"延迟"与"真断连"；容忍连续 kCQMaxConsecFail 次失败以扛住 10% 丢包抖动。
          // ---- C3：leader 自己那一票也要校验身份 ----
          // 原写法 `int live = 1;` 无条件给自己记一票。但「退位宽限期」内本节点可能
          // 已被降级成 learner / 移除（is_member_[me_] 不再是 kVoter），此时它不该再
          // 用「自己这一票」去凑 quorum —— 否则会把「一个 voter 都没联系上」误判成
          // 「还联系上了 1 个」，让已经出局的 leader 在宽限期内看起来仍有 quorum，
          // 从而延长它继续以 leader 身份应答客户端的窗口（= 脏读窗口）。
          // 正常场景零影响：只要自己仍是 voter，live 照样从 1 起算。
          // 极端情形（0 voter 配置）下 majority = 1 而 live = 0 → 立即退位，正是期望行为。
          int live = (me_ < static_cast<int>(is_member_.size()) &&
                      IsVoter(is_member_[me_]))
                         ? 1
                         : 0;  // 自己仍算一票（不会给自己发心跳）——但必须是 voter
          for (size_t i = 0; i < cq_consec_fail_.size(); i++) {
            if (i >= is_member_.size() || !IsVoter(is_member_[i])) continue;
            if (static_cast<int>(i) == me_) continue;
            // 两路存活信号取"或"：
            //  (a) cq_consec_fail_[i] < kCQMaxConsecFail —— 最近收到过 ok=true
            //      （哪怕迟到，长重排下回包会延迟 200~2200ms 但终会到达，容忍它）；
            //  (b) now_q - last_ack_time_[i] < kCQStaleWindowMs —— 最近一个窗口内
            //      收到过 ok=true。真断连时 ok=true 永远到不了、last_ack_time_ 被冻住
            //      → 越过窗口即判死（不依赖回调计数的权威可达性判断，与 cq 互补）。
            bool alive_by_recent =
                cq_consec_fail_[i] < kCQMaxConsecFail;
            bool alive_by_ack =
                (last_ack_time_[i].time_since_epoch().count() != 0) &&
                (now_q - last_ack_time_[i] <
                 std::chrono::milliseconds(kCQStaleWindowMs));
            if (alive_by_recent || alive_by_ack) live++;
          }
          if (live < majority) {
            Trace("S%d CheckQuorum 失败: 只联系上 %d/%d 个, 主动退位 term=%d",
                  me_, live, majority, current_term_);
            // term 不变，只放弃 leader 身份 + 清空投票 + 落盘
            ConvertToFollowerLocked(current_term_);
            // 唤醒：选举循环（去等选举超时）、复制线程（停发心跳）、
            // 以及正在等 ReadIndex 的读线程（让它赶紧失败返回 -1）
            tick_cv_.notify_all();
            replicator_cv_.notify_all();
            read_cv_.notify_all();
            lk.unlock();
            continue;  // 下一轮它会走 follower 分支，乖乖等新 leader 的心跳
          }
        }

        heartbeat_seq_++;  // 复制线程看到计数变了就知道该发心跳了
      lk.unlock();        // 释放锁后再 notify，避免被唤醒线程立刻阻塞在 mu_ 上
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
        // 这里不用加isvoter判断，后面startelection会判断
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
    if (!should_start) { lk.unlock(); continue; }

    // ---- C5：自己已被配置移除 → 主动静默，不发起选举 ----
    // StartElection() 内部本来就有 !IsVoter 的兜底 return（raft.cpp:460），
    // 但那是"进了门才被拦"。这里在【入口】拦掉并计数，意义有三：
    //   1) 不自增 term 去打断正常集群（防旧节点捣乱多数派，etcd 同款）；
    //   2) 不白跑一趟 Pre-Vote 广播（省掉无用 RPC 与 CPU）；
    //   3) 计数可被测试断言 —— 证明静默真的生效，而非"碰巧没超时"。
    // 用 continue 而非 break/return：保留定时器循环，若后续配置把本节点加回
    // （kLearner/kVoter），IsRemovedSelfLocked() 转假后能立刻恢复参选能力。
    if (IsRemovedSelfLocked()) {
      election_suppressed_.fetch_add(1);
      lk.unlock();
      continue;
    }

    // TODO(2A)：超时了，该发起选举了。
    //
    //   StartElection() 内部会在锁内再做兜底确认，并完成
    //   "判定 + 自提"的原子操作。
    lk.unlock();
    StartElection();
  }
}

void Raft::StartElection() {
  // ===================== 阶段 1:预投票 Pre-Vote =====================
  // 目的:在自增 term 之前先探测"我能不能拿到多数派支持",
  //       避免落后/分区的节点自增 term 去打断正常集群(治你见过的 term 暴涨)。
  // 铁律:本阶段【绝不】改 current_term_、【绝不】写 voted_for_、【绝不】持久化。
  RequestPreVoteArgs pargs;
  // 锁外决定收件人会有数据竞争（is_member_ 是 vector，配置变更时可能 reallocate）。
  // 改为：在下面锁内把 voter 收件人下标收集到快照，锁外遍历快照发送。
  std::vector<int> prevote_targets;
  // 单 voter 快路径标志：锁内判定、锁外动作。
  // ⚠️ StartRealElection() 内部会自己加 mu_（非递归锁），必须在锁【外】调用，
  //    所以这里只能带标志出去，不能在锁区内直接调。
  bool self_only = false;
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (killed_.load()) return;
    if (state_ == ServerState::kLeader) return;
    // 非 voter（被移除 / learner）绝不出任 candidate：既不发预投票、也不自提。
    // 否则被移除的节点仍跑选举定时器，可能在剩余 voter 选出新 leader 之前抢先自提、
    // 并靠“日志最新”拿到多数票，变成幽灵 leader（TestRemoveLeaderSelf 抓的就是这个）。
    if (!IsVoter(is_member_[me_])) return;
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
    // ③ 守卫修正：原式 !(last_heartbeat_ < Now()) 恒假（last_heartbeat_ 是过去时间戳、
    //    恒 < Now()），return 从不触发 → 死守卫。改为"最近一个最小选举超时内听过 leader
    //    心跳就放弃本轮预投票"，真正拦住无谓自提把健康 leader 顶掉。
    if (raftcpp::Now() - last_heartbeat_ < std::chrono::milliseconds(kElectionTimeoutMin)) return;

    pargs.term = current_term_;            // 关键:用【当前】term,不 ++
    pargs.candidate_id = me_;
    pargs.last_log_index = LastLogIndexLocked();
    pargs.last_log_term = LastLogTermLocked();
    num_prevotes_ = IsVoter(is_member_[me_]) ? 1 : 0;  // 自己是 voter 才算自己那票（被移除则 0）
    // 记录"谁投的"而非只记票数：本轮预投票态度清零，自己那票按当前身份记。
    prevote_granted_.assign(peers_.size(), 0);
    if (IsVoter(is_member_[me_])) prevote_granted_[me_] = 1;
    // 锁内快照收件人：避免锁外读 is_member_ 触发数据竞争 / stale 视图。
    for (size_t ti = 0; ti < peers_.size(); ti++) {
      if (static_cast<int>(ti) == me_) continue;
      if (ti >= is_member_.size() || !IsVoter(is_member_[ti])) continue;
      prevote_targets.push_back(static_cast<int>(ti));
    }
    // 【单 voter 快路径】自己之外无 voter peer 时，上面收集不到任何收件人，
    // 也就不会发出任何预投票 RPC → 异步回包计数永不触发 → 卡死在预投票、
    // 永远选不出 leader（实测：去掉这段 TestStartRejectedForNonVoter 5/5 失败，
    // "expected one leader, got none"）。
    // 此时 quorum=1，自己那一票（上面 prevote_granted_[me_]=1）本就够了，
    // 故在此（已在锁内，is_member_/prevote_granted_ 均为最新）同步判一次；
    // 动作放锁外。与真实选举侧 StartRealElection"发票前主动判一次"成对。
    if (state_ == ServerState::kFollower &&
        CountGrantedPrevotesLocked() >= QuorumSizeLocked()) {
      self_only = true;
    }
  }
  // 锁外可能会stale，但是StartRealElection()再次判断
  if (self_only) {
    StartRealElection();
    return;
  }

  // 如果在锁释放期间增减 voter:
  // 减 voter：QuorumSizeLocked() 分母缩小，被减节点的 stale 票被过滤不进分子 → 回包侧能正确识别"现在我自己就够 quorum"。
  // 增 voter：分母变大，但新节点没收到过本轮 prevote（收件人是 L476-480 锁内按旧配置快照的），分子不增 → 不会误触发。
  std::shared_ptr<Raft> self = shared_from_this();
  for (int i : prevote_targets) {
    peers_[i]->CallAsyncTyped<RequestPreVoteArgs, RequestPreVoteReply>(
        "Raft.RequestPreVote", pargs,
        [self, pargs, i](bool ok, const RequestPreVoteReply& reply) {
          if (!ok) return;  // 丢包 / 对方挂了
          // 假如他在回包后，自己被移除了，便进入不了真实选举;
          // 假如follower被移除了，有self->CountGrantedPrevotesLocked() >= self->QuorumSizeLocked()兜底
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
              self->num_prevotes_++;  // 仅作调试计数；判定以下面同口径计票为准
              // 记下这一票是谁投的，计票时按【当前】配置剔掉已移除 / learner 的票。
              self->prevote_granted_[i] = 1;
              if (self->CountGrantedPrevotesLocked() >= self->QuorumSizeLocked()) {
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
  {
    std::lock_guard<std::mutex> lk(mu_);
    num_prevotes_ = 0;  // 预投票使命结束,清理(与 num_votes_ 无关)

    if (killed_.load()) return;
    if (state_ != ServerState::kFollower) return;
    // 双保险：走到“自提”这一步说明要当 candidate，非 voter 绝不允许（被移除 / learner）。
    if (!IsVoter(is_member_[me_])) return;
    // ③ 守卫修正：同 StartElection 的死守卫，改为"近期听过 leader 心跳就放弃自提"。
    if (raftcpp::Now() - last_heartbeat_ < std::chrono::milliseconds(kElectionTimeoutMin)) return;

    Trace("S%d 自提: term %d -> %d (state=%s)", me_, current_term_, current_term_ + 1, StateName(state_));
    state_ = ServerState::kCandidate;
    current_term_++;
    voted_for_ = me_;
    num_votes_ = IsVoter(is_member_[me_]) ? 1 : 0;  // 自己是 voter 才算自己那票（被移除则 0）
    // 记录"谁投的"而非只记票数：本轮投票态度清零，自己那票按当前身份记。
    vote_granted_.assign(peers_.size(), 0);
    if (IsVoter(is_member_[me_])) vote_granted_[me_] = 1;
    last_heartbeat_ = raftcpp::Now();

    PersistLocked();
    // 【修复】集群只剩 1 个 voter（quorum=1,自己那票就够）时,SendRequestVoteRPCs
    // 不会发出任何 RPC,回包回调永远不来,就等不到判定触发,leader 永远选不出。
    // 这里发票前主动判一次(与 BecomeLeaderIfQuorumLocked 注释 (b) 的意图一致)。
    if (CountGrantedVotesLocked() >= QuorumSizeLocked()) {
      BecomeLeaderIfQuorumLocked();
    }
  }

  SendRequestVoteRPCs();
}

// ===================== 真实投票 RPC 发送 =====================
// 被 StartRealElection(刚成为 candidate)与 candidate 超时重发共用。
// 只发送 RequestVote RPC,【不】修改 state_/current_term_/num_votes_(否则会重复自增 term)。
void Raft::SendRequestVoteRPCs() {
  RequestVoteArgs args;
  // 锁外决定收件人会有数据竞争（is_member_ 是 vector，配置变更时可能 reallocate）。
  // 改为：在下面锁内把 voter 收件人下标收集到快照，锁外遍历快照发送。
  std::vector<int> vote_targets;
  {
    std::lock_guard<std::mutex> lk(mu_);
    // 这里不用再判断单voter的问题，即使在上面放锁期间又变为单voter，会再发一轮StartElection
    if (killed_.load()) return;
    if (state_ != ServerState::kCandidate) return;
    // Q3 双保险：发真实投票 RPC 前再确认自己仍是 voter。
    // StartRealElection 已在校验过一次（且 state_==kCandidate 本身就意味着刚通过该校验），
    // 但二者不在同一把锁内：从 StartRealElection 解锁到本函数重新加锁之间，可能有成员变更
    // 把自己降成非 voter。此时若仍发出 RequestVote，对端 HandleRequestVote 会据规则 2.5
    // 拒票（不会出错），这里只是提前短路、让自身语义更清晰。
    // 加 is_member_ 就绪守卫：避免重启瞬间 is_member_ 尚未恢复时被误判成非 voter 而发不出
    // 票、选不出 leader（非 voter 本就不该候选，但守卫确保不会因“未就绪”而非预期返回）。
    if (!is_member_.empty() && me_ < static_cast<int>(is_member_.size()) &&
        !IsVoter(is_member_[me_])) {
      return;
    }
    // 锁内快照收件人：避免锁外读 is_member_ 触发数据竞争 / stale 视图。
    for (size_t ti = 0; ti < peers_.size(); ti++) {
      if (static_cast<int>(ti) == me_) continue;
      if (ti >= is_member_.size() || !IsVoter(is_member_[ti])) continue;
      vote_targets.push_back(static_cast<int>(ti));
    }
    args.term = current_term_;
    args.candidate_id = me_;
    args.last_log_index = LastLogIndexLocked();
    args.last_log_term = LastLogTermLocked();
  }

  std::shared_ptr<Raft> self = shared_from_this();
  for (int i : vote_targets) {
    peers_[i]->CallAsyncTyped<RequestVoteArgs, RequestVoteReply>(
        "Raft.RequestVote", args,
        [self, args, i](bool ok, const RequestVoteReply& reply) {
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
              args.term == self->current_term_ && reply.vote_granted &&
    IsVoter(self->is_member_[self->me_]) ) {
            self->num_votes_++;  // 仅作调试计数；判定以下面同口径计票为准
            // 记下这一票是谁投的；计票时按【当前】配置剔掉已移除 / learner 的票，
            // 保证分子（赞成票）与分母（QuorumSizeLocked 只数 voter）同口径。
            // 不用担心在发rpc包之前清零该数组，延迟rpc包误改全局状态，有term保护。
            self->vote_granted_[i] = 1;
            self->BecomeLeaderIfQuorumLocked();
          }
        },
        &cancel_rpcs_);
  }
}

// ===========================================================================
// 当选判定 + 上任初始化（调用前必须持有 mu_）
// ===========================================================================
// 从 RequestVote 回包回调里抽出来，供两处复用：
//   (a) 收到投票回包时；
//   (b) StartRealElection 发完票之后【主动判一次】——
//       否则当集群只剩 1 个 voter（quorum=1，自己那一票就够）时，永远等不到
//       任何“授予”回包来触发判定，leader 永远选不出来。
//       实证：⑰ TestStartRejectedForNonVoter 稳定 50% 失败
//       （expected one leader, got none）：learner/removed 按正确语义拒投后，
//        唯一 voter 自己那票再也等不到触发判定的回包。
void Raft::BecomeLeaderIfQuorumLocked() {
  if (state_ != ServerState::kCandidate) return;
  // 与回包处一致：自己必须仍是 voter 才能上任（被移除 / learner 不得当 leader）
  if (!IsVoter(is_member_[me_])) return;
  if (CountGrantedVotesLocked() < QuorumSizeLocked()) return;
    Trace("S%d 当选 leader term=%d lastIdx=%d", me_, current_term_, LastLogIndexLocked());
    // 【CheckQuorum 硬化】新当选 leader 给足一个自查周期的宽限：
    // 重置自查时钟，否则 last_quorum_check 还是很久前(当 follower/
    // candidate 时)的旧值，下一轮 CheckQuorum 零宽限、follower 还没
    // 回心跳 ack 就可能被误退位(不可靠网络/高负载下尤其 flaky)。
    last_quorum_check_ = raftcpp::Now();
    // 条件补 no-op(沿用原 415-423 逻辑,满足 Figure 8)；
    // 同时定下"本 term 读安全下标"read_safe_commit_：
    //   - 有未提交旧尾巴 → 补 no-op 在 LastLogIndex+1，读安全点就是这条 no-op；
    //     必须等它提交、commit_index_ 抬到覆盖上任前已提交 entry，ReadIndex 才安全。
    //   - 无未提交尾巴 → commit_index_ 已是准确值（选举约束保证含全部已提交
    //     entry），立即可安全读。
    if (LastLogIndexLocked() > commit_index_) {
      Trace("S%d 当选补 no-op: term=%d idx=%d (commitIndex=%d, 有未提交旧尾巴)",
            me_, current_term_,
            LastLogIndexLocked() + 1, commit_index_);
      logs_.push_back(
          LogEntry{current_term_,
                   LastLogIndexLocked() + 1, Command(""),
                   EntryType::kNoOp});
      PersistLocked();
      // 读安全点 = 这条 no-op 自身的下标（push 之后 LastLogIndexLocked()
      // 就是它），不是它再 +1 —— 否则 no-op 提交后 commit_index_ 永远差 1，
      // ReadIndex 会永久返回 -1，所有读都 WrongLeader → 60s 挂死。
      read_safe_commit_ = LastLogIndexLocked();
    } else {
      read_safe_commit_ = commit_index_;
    }
    int last = LastLogIndexLocked();
    for (size_t k = 0; k < peers_.size(); k++) {
      next_index_[k] = last + 1;
      match_index_[k] = 0;
    }
    heartbeat_seq_++;
    last_heartbeat_ = raftcpp::Now();
    tick_cv_.notify_all();
    replicator_cv_.notify_all();
    // ⚠️ 最后再置 leader：read_safe_commit_ 已先定好，避免 ReadIndex 在
    // 读安全点设好前误判 commit_index_ >= read_safe_commit_(旧值0) 而提前放行读。
    // （整段都在 mu_ 锁内，ReadIndex 也取锁，所以不存在真正并发；此处仅为语义清晰。）
    state_ = ServerState::kLeader;
    leader_id_ = me_;  // 自己就是 leader，重定向回填直接用自己
    // 新当选：作废任何残留的"待退位"意图——它属于上一任的身份结论，带过来会让
    // 一个刚当选的合法 leader 拒收写/拒接读、并在宽限到点后无故自我退位。
    // 当前所有调用路径上都已由 ConvertToFollowerLocked 清过，此处属防御性兜底。
    pending_stepdown_ = false;
    // 新身份 / 新任期，重新数起：别把上一任的"连续失败记忆"带到新 leader 上
    // （否则旧任期里断过的 follower 在新任期头一轮就被误判死，要等它首个 ok=true 才恢复）。
    cq_consec_fail_.assign(peers_.size(), 0);
    // 上任扫描：重建"在途成员变更"状态（崩溃/切主恢复后保持一致）。
    // 一次只允许一个变更在飞行，取最后一个未提交（index > commit_index_）的 conf 条目。
    pending_conf_index_ = 0;
    for (int ci = commit_index_ + 1;
         ci <= LastLogIndexLocked(); ci++) {
      size_t cpos = static_cast<size_t>(ci - snapshot_index_);
      if (cpos < logs_.size() && logs_[cpos].type == EntryType::kConfChange) {
        pending_conf_index_ = ci;
      }
    }
    // 【Q2 冻结点重建】removed_at_index_ 是每个节点各自的本地状态
    // （默认 -1），不会随日志/选举传递。若不重建，本节点刚当选 leader
    // 时它对"谁已被移除、冻结在哪一条"一无所知：在 ApplyLoop 走到那条
    // conf 条目之前 is_member_[sv] 仍是 kVoter，ReplicateLoop 就会继续
    // 给已移除节点复制新日志 → Q2 冻结失效。
    // 实证：㉓ TestRemovedNodeStopsReceivingReplication 偶发失败
    // （removed 节点日志 11 -> 12/13），换主越频繁命中率越高。
    // 上任时扫一遍日志里的 conf 条目即可恢复正确冻结下标。
    removed_at_index_.assign(peers_.size(), -1);
    for (const LogEntry& ce : logs_) {
      if (ce.type != EntryType::kConfChange) continue;
      int sv = ce.conf_server;
      if (sv < 0 ||
          sv >= static_cast<int>(removed_at_index_.size()))
        continue;
      MemberRole role = static_cast<MemberRole>(ce.conf_role);
      // 取最后一条针对该节点的配置决定：移除→记下冻结下标，
      // 加回 / 降级 learner→清除冻结（它重新参与复制）。
      removed_at_index_[sv] =
          (role == MemberRole::kRemoved) ? ce.index : -1;
    }
}

// ===========================================================================
// 第四部分：RPC handler
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

  // 规则 2.5：候选人已不在当前 voter 集合（被成员变更移除）→ 拒绝投票。
  // 否则被移除节点仍能靠"自己一票 + 别人一票"凑出多数派当上 leader，破坏安全性。
  if (args.candidate_id < 0 ||
      args.candidate_id >= static_cast<int>(is_member_.size()) ||
      !IsVoter(is_member_[args.candidate_id])) {
    return;
  }

  // 规则 2.6：【我自己】已被移除（kRemoved）→ 不投票。
  // 少了这条，被移除节点会持续投出幽灵票：例如 3 节点移除一个、再断开
  // 另一个真实 voter，在线真实 voter 只剩 1 个 < quorum(2)，却仍能靠被
  // 移除节点那张票凑够 quorum 选出 leader（RemovedNodeExcludedFromQuorum）。
  //
  // ⚠️ 这里【只】禁 removed，不禁 learner：
  //   · learner 仍在配置内、仍需复制日志；它那张票会被计票方
  //     CountGrantedVotes（只数 voter）自然过滤掉，不会成幽灵票；
  //   · 若连 learner 也禁，则「把唯一 follower 降级成 learner」后集群
  //     只剩 1 个 voter 且再也收不到任何授予回包，leader 永远选不出来
  //     （⑰ TestStartRejectedForNonVoter：expected one leader, got none）。
  // 少了这条，被移除节点会持续投出"幽灵票"：例如 3 节点移除一个、再断开另一个
  // 真实 voter，在线真实 voter 只剩 1 个 < quorum(2)，却仍能靠被移除节点那张票
  // 凑够 quorum 选出 leader —— TestRemovedNodeExcludedFromQuorum 要防的正是这个。
  if (is_member_.empty() || me_ >= static_cast<int>(is_member_.size()) ||
      is_member_[me_] == MemberRole::kRemoved) {
    return;
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

  // 规则 2.5：候选人已不在当前 voter 集合（被成员变更移除）→ 拒绝投票。
  // 否则被移除节点仍能靠"自己一票 + 别人一票"凑出多数派当上 leader，破坏安全性。
  // 回包时 follower 不再是 voter，这票照样算数，因为：(1) 票在授予时刻就合法且占死；(2) 它只占选举、不占提交；(3) 回包复查反而致死锁
  if (args.candidate_id < 0 ||
      args.candidate_id >= static_cast<int>(is_member_.size()) ||
      !IsVoter(is_member_[args.candidate_id])) {
    return;
  }

  // 规则 2.6（与 RequestVote 对称）：【我自己】已被移除 → 连预投票都不给。
  // 同样只禁 removed、不禁 learner（理由同 RequestVote）。
  // 否则被移除节点会投出幽灵票，在预投票阶段就把少数派选民"抬"过 quorum 门槛，
  // 进而触发本不该发生的真实选举。
  if (is_member_.empty() || me_ >= static_cast<int>(is_member_.size()) ||
      is_member_[me_] == MemberRole::kRemoved) {
    return;
  }

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
  reply.read_ctx = args.read_ctx;
  if (killed_.load()) return;

  // 规则 1：任期比我小 → 拒绝
  if (args.term < current_term_) return;

  // 走到这里说明发来的是【合法（同任期或更新任期）的 leader】→ 记下它的编号，
  // 供上层做 WrongLeader 重定向（kvraft 的 Get/PutAppend reply 回填 leader_id）。
  // 即便随后因为任期更大而退位，这条消息的发送方也确实是当前/更新任期的合法
  // leader，记它没错；下一任 leader 的心跳会把 leader_id_ 覆盖成正确值。
  leader_id_ = args.leader_id;

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
// 第五部分：Start / 日志复制 / 提交
// ===========================================================================

// ---------------------------------------------------------------------------
// 推进 commitIndex（leader 侧【唯一】入口）
// ---------------------------------------------------------------------------
// 取所有 voter 的 matchIndex 中位数，且要求该下标的条目 term == current_term_
// （Figure 8 守卫）。调用前必须持有 mu_。
//
// ⚠️ 为什么必须抽成函数、而不是只内联在 AE 回包回调里：
//   leader 自己那一票（LastLogIndexLocked）本来就计入多数派，所以当 quorum=1
//   —— 单 voter 集群，或缩容到 1 voter 且其余节点 removed/断连 —— 时，
//   【根本不需要任何回包就能提交】。若只靠 AE 回包触发重算，一旦没有任何回包
//   到达（其余节点全部不可达），commit_index_ 就永不推进 → 写永不提交、
//   ReadIndex 因 commit_index_ < read_safe_commit_ 永久返回 -1 → 集群不可用。
//   因此 Start() / ProposeConfChangeTo() 在追加日志后也必须各调一次：
//   Raft 语义上 leader 自身持有即计入多数派。
// 安全性不变：判定逻辑与原内联版逐行等价（中位数下标 = size - quorum、
// Figure 8 term 守卫、只数 IsVoter），多调一次只是让「该提交时能提交」。
void Raft::AdvanceCommitIndexLocked() {
  // ⚠️ 只有 leader 能提交。三处调用点（AE 回包回调 / Start / ProposeConfChangeTo）
  // 目前都在 leader 路径上，所以这行是纯防御；但一旦将来被误接到非 leader 路径，
  // quorum=1 时会把「自己刚追加、还没复制给任何人」的日志判成已提交 → 安全性直接崩塌
  // （客户端拿到 ack 的条目可能随本节点崩溃而永久丢失）。宁可早退，不可误提交。
  if (state_ != ServerState::kLeader) return;
  std::vector<int> matched;
  matched.reserve(MemberCountLocked());
  for (size_t i = 0; i < peers_.size(); i++) {
    if (i >= is_member_.size() || !IsVoter(is_member_[i])) {
      continue;  // 非投票成员不参与提交计票
    }
    if (static_cast<int>(i) == me_) {
      matched.push_back(LastLogIndexLocked());
    } else {
      matched.push_back(match_index_[i]);
    }
  }
  std::sort(matched.begin(), matched.end());
  const size_t quorum = static_cast<size_t>(QuorumSizeLocked());
  // ⚠️ 下溢防御：matched.size() < quorum 时下面 matched[size - quorum] 会
  // size_t 下溢 → 越界读。可能发生于「is_member_ 里的 voter 数 >
  // peers_ 覆盖到的 voter 数」（例如成员已加入但 peers_ 还没建好）。
  if (matched.empty() || matched.size() < quorum) return;
  // ⚠️ 升序排序后，要让「第 k 小」恰好有 quorum 个元素 ≥ 它，下标必须是
  // size - quorum（不是 size/2）：voter 数为偶数时 size/2 会悄悄少算一票
  // （4 节点退化成「2 票即可提交」），安全性直接破防。
  int n = matched[matched.size() - quorum];
  if (n > commit_index_ && n >= 0) {
    size_t pos = static_cast<size_t>(n) - snapshot_index_;
    // 论文 §5.4 + Figure 8：只有【当前任期】的日志能靠多数派提交。
    bool term_ok = (pos < logs_.size() && logs_[pos].term == current_term_);
    if (term_ok) {
      commit_index_ = n;
      apply_cv_.notify_all();
      heartbeat_seq_++;
      replicator_cv_.notify_all();
    }
  }
}

StartResult Raft::Start(const Command& command) {
  std::lock_guard<std::mutex> lk(mu_);

  StartResult result;
  result.term = current_term_;
  // 事件驱动退位宽限期内，本节点虽仍是 leader 但已被移除/降级出 voter，
  // 不得再接受客户端写（否则会把后置数据写进自己日志，破坏 Q2 冻结/静默）。
  if (killed_.load() || state_ != ServerState::kLeader || pending_stepdown_)
    return result;

  // ---- C5 纵深防御：已被配置移除的节点不得再接受客户端写 ----
  // 正常路径上 pending_stepdown_ / !IsVoter 已经拦过，这里再拦一次是防止
  // 那些守卫被后续改动绕过（被移除的节点不该再往日志里塞任何东西）。
  if (IsRemovedSelfLocked()) return result;

  // ---- C1 背压：未提交 entry 达上限 → 拒绝新提案 ----
  // 必须在 push_back【之前】判定，否则超限那一刻还会多塞一条进去。
  // 语义同 etcd MaxUncommittedEntries：我确实是 leader（所以 is_leader 语义
  // 上仍为真），但出于内存保护拒绝本次提案，调用方看到 backpressure=true
  // 应当"稍后重试"，而不是"换节点重试"。
  // 注意：result.is_leader 保持 false —— kvraft/server.cpp:259 据此返回
  // Err::kWrongLeader，Clerk 会重试；默认上限 10000 在 lab 规模永不触发，
  // 因此现有用例零行为变更。
  if (max_uncommitted_entries_ > 0 &&
      (LastLogIndexLocked() - commit_index_) >= max_uncommitted_entries_) {
    result.backpressure = true;
    return result;
  }

  // ---- #2.2 磁盘水位保护：raft state 过大 → 拒绝新写入，逼上层快照 ----
  // 必须在 push_back【之前】判定（同 C1 背压）。默认 max_raft_state_bytes_==0 不限制，
  // 与现有行为完全一致。语义上本节点仍是 leader（is_leader 保持 false，让上层当
  // WrongLeader 重试），但本次提案被拒；上层应调 Snapshot() 压缩、待 raft state
  // 缩小后再重试。这样即使上层忘了触发快照，Raft 层也不会无限涨盘拖垮节点。
  if (max_raft_state_bytes_ > 0 &&
      RaftStateSizeLocked() >= max_raft_state_bytes_) {
    result.disk_full = true;
    return result;
  }

  int index = LastLogIndexLocked() + 1;
  logs_.push_back(LogEntry{current_term_, index, command});
  PersistLocked();

  result.is_leader = true;
  result.index = index;
  result.term = current_term_;

  // 立刻唤醒复制线程，别等下一个心跳。
  // 注意：这里只能 notify，绝不能直接调发 RPC 的函数 —— 正持着 mu_，
  // 而 RPC 回包处理也要 mu_，直接调就是死锁。
  AdvanceCommitIndexLocked();  // 单 voter / 无回包可达时也能提交
  replicator_cv_.notify_all();
  return result;
}

// 退位宽限（ms）：ApplyLoop 检测到本节点被移除/降级出 voter 时只置 pending_stepdown_
// 标志并继续当 leader；ReplicateLoop 在多发 kStepdownGraceMs 心跳（把 leader_commit
// 含移除配置带出去）之后才真正退位。kHeartbeatInterval=50ms，故 200ms ≈ 4 轮，足以让
// 所有 voter 收到新 commitIndex 并 apply 配置、按新 quorum 选出新 leader。
// 事件驱动、零阻塞、无额外 RPC、无人工预算 —— 取代原版"同步广播 + 锁外等 ack"的脆性设计。
static constexpr int kStepdownGraceMs = 200;

void Raft::ReplicateLoop(int server) {
  const size_t s = static_cast<size_t>(server);
  std::shared_ptr<Raft> self = shared_from_this();

  while (!killed_.load()) {
    AppendEntriesArgs args;
    InstallSnapshotArgs snap_args;
    bool send_now = false;
    bool send_snap = false;
    // P0-1/P0-2：钉死这一波发送所用的快照 buffer（shared_ptr O(1) 拷贝，不整拷贝 blob）。
    // 多帧发送期间若 Raft::Snapshot 原子替换 snapshot_data_，src_snap 仍指向旧 buffer，
    // 切片不会拿到新旧混合数据。
    std::shared_ptr<std::string> src_snap;
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
        // 只冻结【已移除】节点（Q2 隐私加固），不冻结 learner：
        // learner 仍在集群内、必须及时收到日志才能追上，不能与 removed 混同抑制。
        // 判据用 IsRemovedFrozen 而非 !IsVoter —— 后者会把 learner（该收日志）
        // 和 removed（不该收新数据）混为一谈；前者还额外覆盖【提案移除 -> apply】
        // 窗口（此间 is_member_[s] 仍是 kVoter，但 removed_at_index_ 已置位）。
        if (IsRemovedFrozen(is_member_, removed_at_index_,
                            static_cast<int>(s))) {
          return false;
        }
        if (raftcpp::Now() < last_send_time_[s] +
                                 std::chrono::milliseconds(kMinSendIntervalMs)) {
          return false;  // 节流中
        }
        if (heartbeat_seq_ != last_heartbeat_seq_[s]) return true;
        // 流水线窗口判断：在途条目数 = next_index_ - 1 - match_index_
        bool has_new = next_index_[s] <= LastLogIndexLocked();
        int64_t inflight =
            static_cast<int64_t>(next_index_[s]) - 1 - match_index_[s];
        bool timed_out =
            inflight > 0 &&
            (raftcpp::Now() - inflight_log_time_[s] >
             std::chrono::milliseconds(kLogRetryMs));
        // 窗口已满且非超时重发 → 等 ACK（释放窗口）
        if (inflight >= kPipelineMaxInFlight && !timed_out) return false;
        // 既无新日志、又无需超时重发 → 等待
        if (!has_new && !timed_out) return false;
        return true;
      });

      if (killed_.load()) return;
      if (state_ != ServerState::kLeader) continue;

      // 事件驱动退位：ApplyLoop 已置 pending_stepdown_，且已过宽限期
      // （期间普通心跳已将 leader_commit 含移除配置带出去）→ 真正退位。
      // 多个 ReplicateLoop 线程可能同时进入此分支，故用 state_==kLeader 守卫，
      // 只有第一个真正退位，其余看到已非 leader 自动跳过。
      if (pending_stepdown_ && state_ == ServerState::kLeader &&
          raftcpp::Now() >= stepdown_after_) {
        // 退位前【复查身份】：宽限期内可能又 apply 了"把本节点加回 voter"的配置
        // 条目（连续两次变更：先降级、再加回）。此时它已是合法 voter leader，不该
        // 再退位——否则白白丢掉领导权、平白多一次选举。反之则撤销退位意图。
        if (!IsVoter(is_member_[me_])) {
          ConvertToFollowerLocked(current_term_);
        } else {
          pending_stepdown_ = false;
        }
        continue;
      }

      // ===== Q2 隐私加固：removed 节点只接收"移除它自己的那条配置条目"为止 =====
      // 必须先让 removed 节点收到自己的移除条目，它才知道自己被移除；否则会卡在
      // kVoter 永远收不到移除 → 集群成员视图分叉。removed_at_index_[s] 即该"冻结点"
      // 下标（提案时 ProposeConfChangeTo 写入，apply 时 ApplyLoop 再写一次）。
      // cap < 0（如刚重启尚未重建）则日志/快照全部不发（removed 节点本就从日志
      // 重放拿到过移除条目，不依赖 leader 补发）。
      //
      // 冻结判据必须【两源取或】，不能只用一个：
      //   (a) removed_at_index_[s] >= 0（冻结点）：覆盖【提案 → apply】这段窗口，
      //       此间 is_member_[s] 仍是 kVoter。只用 is_member_ 判 → cap 形同虚设，
      //       leader 会继续把 conf 之后的新条目（如 index 12/13）复制给待移除节点。
      //       实证：㉓ TestRemovedNodeStopsReceivingReplication 偶发失败（11 -> 12/13），
      //       换主/提案越频繁命中率越高（golden 实测 ~40%，换此法后降到 0）。
      //   (b) is_member_[s] == kRemoved（配置视图）：覆盖【经 InstallSnapshot 恢复配置】
      //       的场景——快照会把 conf 条目一并截断，新 leader 上任扫描 logs_ 扫不到那条
      //       conf，removed_at_index_ 重建不出来（仍是 -1）。若只看 (a)，本节点明明已
      //       从快照里读到 is_member_[s]==kRemoved，却仍继续给它发新数据 → 隐私泄漏。
      // 两者任一为真即冻结，见 raft.h 的 IsRemovedFrozen。
      bool removed = IsRemovedFrozen(is_member_, removed_at_index_,
                                     static_cast<int>(s));
      int removed_cap =
          (s < removed_at_index_.size()) ? removed_at_index_[s] : -1;

      bool has_more = next_index_[s] <= LastLogIndexLocked();
      int64_t inflight =
          static_cast<int64_t>(next_index_[s]) - 1 - match_index_[s];
      bool window_ok = inflight < kPipelineMaxInFlight;
      bool timed_out =
          inflight > 0 &&
          (raftcpp::Now() - inflight_log_time_[s] >
           std::chrono::milliseconds(kLogRetryMs));
      bool may_send_log = (has_more && window_ok) || timed_out;
      if (removed) {
        // 超过冻结点（含移除条目本身）的日志一律不发；cap<0 则连移除条目都不补发（冻结）
        if (removed_cap < 0 || next_index_[s] > removed_cap) may_send_log = false;
        if (next_index_[s] <= snapshot_index_) {
          next_index_[s] = snapshot_index_ + 1;
          if (next_index_[s] > removed_cap) may_send_log = false;
        }
      }
      bool heartbeat_due = heartbeat_seq_ != last_heartbeat_seq_[s];
      // 注意：removed 节点【允许心跳】。心跳只带 leader_commit（一个下标，不含任何
      // 客户端数据），不泄漏隐私；但 removed 节点需要靠心跳推进自己的 commit_index_，
      // 才能 apply 到"把它自己移除"的那条配置条目。日志/快照仍受上方 cap 与 !removed
      // 闸门严格限制，新客户端数据绝不会发给它。
      if (!may_send_log && !heartbeat_due) continue;

      last_heartbeat_seq_[s] = heartbeat_seq_;
      last_send_time_[s] = raftcpp::Now();

      // 超时重发：把发送指针回退到 match_index_+1（重发未确认区间，幂等）
      int send_from = next_index_[s];
      if (timed_out) send_from = match_index_[s] + 1;
      int next = send_from;
      if (next <= 0) next = 1;
      // removed 节点补不齐快照点之前的区间（已被压缩），timed_out 重发也不许回退到
      // 快照区间——否则条目循环 logs_[i - snapshot_index_] 又是负下标。
      if (removed && next <= snapshot_index_ && may_send_log) next = snapshot_index_ + 1;

      // next_index_ 推进到 snapshot_index_+1 之后，再正常发 AppendEntries。
      bool lagging_for_snapshot = (next <= snapshot_index_ && !removed);
      if (lagging_for_snapshot && !snap_in_flight_[s]) {
        snap_in_flight_[s] = 1;  // 先占坑（持锁），回调在最后一块完成时才清零
        snap_args.epoch = ++snap_epoch_[s];  // 每开一帧新分块传输代号 +1（档2：旧残片靠它识别）
        snap_args.term = current_term_;
        snap_args.leader_id = me_;
        snap_args.last_included_index = snapshot_index_;
        snap_args.last_included_term = snapshot_term_;
        // P0-1：不再整 blob 深拷贝到 snap_args.data（原 L1330 锁内 O(blob) 拷贝）。
        // 改为钉死 shared_ptr，锁外切片时按需取 chunk（见下方发送循环）。
        src_snap = snapshot_data_;
        // 【惰性 CRC】：只有真要发分块快照时才算，并按 snapshot_index_ 缓存，
        // 一次快照最多算一次（详见 Raft::Snapshot 里作废缓存处的注释）。
        if (snap_crc_index_ != snapshot_index_) {
          snap_crc_ = Crc32(snapshot_data_->data(), snapshot_data_->size());
          snap_crc_index_ = snapshot_index_;
        }
        snap_args.crc = snap_crc_;
        // 把当前成员配置随快照一起发给落后 follower（见 InstallSnapshot 处重建）。
        snap_args.members.clear();
        for (MemberRole r : is_member_) snap_args.members.push_back(static_cast<int>(r));
        send_snap = true;
      } else {
        // 快照在途（lagging_for_snapshot 且 snap_in_flight_ 已置位）：本轮只发心跳，
        // 不带头皮日志条目——理由见上方 lagging_for_snapshot 注释。may_send_log 置否后
        // 下方 entries 构建被跳过，但 send_now 仍为 true（纯心跳仍发出，用于推进
        // follower 的 commitIndex）。
        if (lagging_for_snapshot) may_send_log = false;
        args.term = current_term_;
        args.leader_id = me_;
        args.leader_commit = commit_index_;
        args.prev_log_index = next - 1;
        // prev_log_term 安全计算：removed 节点可能落后到快照点之前（leader 已压缩
        // 过它的冻结点之前的下标），直接按下标取会越界 → 用快照 term 兜底。
        if (next - 1 < snapshot_index_) {
          args.prev_log_term = snapshot_term_;
        } else {
          size_t pos = static_cast<size_t>(next) - 1 - snapshot_index_;
          args.prev_log_term = (pos < logs_.size())
                                   ? logs_[pos].term
                                   : snapshot_term_;
        }
        args.entries.clear();

        if (may_send_log) {
          // 【必须拷贝，不能持有 logs_ 里元素的引用】
          // 出了锁，别的线程（AppendEntries handler / Start）会改 logs_，
          // 拿着引用去序列化就是数据竞争 —— TSAN 一定会报。
          int last = LastLogIndexLocked();
          // Q2：removed 节点最多发到冻结点（含移除条目），绝不越过泄漏新客户端数据。
          if (removed && removed_cap >= 0 && last > removed_cap)
            last = removed_cap;
          // 单条 RPC 条目上限：截断到最多 kMaxEntriesPerRpc 条，
          // 既限制单包大小，也定义"一批"的粒度（窗口以条目计，等价于若干批在途）。
          if (last - next + 1 > kMaxEntriesPerRpc)
            last = next + kMaxEntriesPerRpc - 1;
          // §5.2 双上限：条目数上限已在上面把 last 截到 kMaxEntriesPerRpc；
          // 这里再加字节上限——累加每条 entry 的 command 字节，超过 kMaxBytesPerRpc
          // 且已至少发出一条时截断（单条超大 value 仍至少发一条，避免永远发不出去卡死）。
          size_t entry_bytes = 0;
          for (int i = next; i <= last; i++) {
            // §2.1 防御性钳制：next 一旦落在快照区间内（理论上不该发生——落后到
            // 快照点之前应改走 InstallSnapshot；但 snap_in_flight_ 在途窗口等边界下
            // ReplicateLoop 可能回退到 AE 分支），绝不能用 i-snapshot_index_ 去读
            // logs_（会 size_t 下溢 → 越界读内存垃圾 → 发出乱码 entry 污染 follower）。
            // 快照区间内这些条目 follower 本就该从快照里拿，直接跳过即可。
            if (i <= static_cast<int>(snapshot_index_)) continue;
            size_t pos = static_cast<size_t>(i) - snapshot_index_;
            if (i > next && entry_bytes + logs_[pos].command.size() > kMaxBytesPerRpc) {
              ae_byte_truncated_count_.fetch_add(1, std::memory_order_relaxed);
              break;
            }
            args.entries.push_back(logs_[pos]);
            entry_bytes += logs_[pos].command.size();
          }
          last = next + static_cast<int>(args.entries.size()) - 1; 
          // §5.2 记录本批 payload 字节（观测用：断言单条 AE 不超 kMaxBytesPerRpc）。
          // 各 follower 的发送循环并发跑在锁外，用 relaxed store 即可——
          // 每个线程只写自己这批的 payload（必然 <= 上限），最终值不会超过上限。
          {
            long long b = static_cast<long long>(entry_bytes);
            long long cur = max_ae_payload_bytes_.load(std::memory_order_relaxed);
            if (b > cur) max_ae_payload_bytes_.store(b, std::memory_order_relaxed);
          }
          // 流水线核心：发送指针只在"真正发出新批"时前进，绝不因 ACK 回退。
          // 窗口占用由 (next_index_ - 1 - match_index_) 自动释放，无需显式清 bool。
          next_index_[s] = last + 1;
          inflight_log_time_[s] = raftcpp::Now();
        }
        // 否则就是纯心跳：只带 prevLog 做一致性检查，不带任何日志
        send_now = true;
      }
    }
    if (send_snap) {
      // §2.1 分块快照：把完整快照切片成 kSnapshotChunkSize 的 chunk，逐条 RPC 发出。
      // 每条 chunk 携带 offset / done / crc；follower 按 offset 重组、done 时校验 CRC
      // 后安装。可靠网络下一次性全发（in-order 投递，无空洞）；不可靠丢失时 follower
      // 收不全 → ok_install=false → 下一轮心跳重发整块（正确，只是略慢）。
      // P0-1：total 取钉死的 src_snap（不再依赖 snap_args.data 整拷贝）。
      const size_t total = src_snap->size();
      size_t off = 0;
      int last_idx = snap_args.last_included_index;
      int snap_term = snap_args.term;  // 发送时任期，用于回调二次确认（防旧 term 迟到回包）
      do {
        size_t remain = total - off;
        size_t len = (remain < kSnapshotChunkSize) ? remain : kSnapshotChunkSize;
        bool is_last = (off + len >= total);
        InstallSnapshotArgs chunk_args;  // 不复用 snap_args（避免整体拷贝 1GB blob）
        chunk_args.term = snap_args.term;
        chunk_args.leader_id = snap_args.leader_id;
        chunk_args.last_included_index = snap_args.last_included_index;
        chunk_args.last_included_term = snap_args.last_included_term;
        chunk_args.offset = static_cast<int>(off);
        chunk_args.chunk_size = static_cast<int>(len);
        chunk_args.done = is_last;
        chunk_args.crc = snap_args.crc;
        chunk_args.epoch = snap_args.epoch;
        // P0-1：从钉死的 src_snap 切片（每次只拷一个 chunk 的 len 字节，非整 blob）。
        chunk_args.data = src_snap->substr(off, len);
        chunk_args.members = snap_args.members;
        peers_[s]->CallAsyncTyped<InstallSnapshotArgs, InstallSnapshotReply>(
            "Raft.InstallSnapshot", chunk_args,
            [self, server, is_last, last_idx, snap_term, send_epoch = snap_args.epoch](bool ok, const InstallSnapshotReply& reply) {
              const size_t s = static_cast<size_t>(server);
              std::lock_guard<std::mutex> lk(self->mu_);
              if (self->killed_.load()) return;
              // CheckQuorum liveness：回包 ok=true（哪怕迟到）→ 清零连续失败计数；
              // ok=false（丢包 / 真断连）→ 累加。据此区分"回包延迟"与"真失联"。
              if (ok) self->cq_consec_fail_[s] = 0;
              else { if (self->cq_consec_fail_[s] < kCQMaxConsecFail) self->cq_consec_fail_[s]++; }
              // OK 为false时, reply.term默认为0
              if (reply.term > self->current_term_) {
                self->ConvertToFollowerLocked(reply.term);
                self->last_heartbeat_ = raftcpp::Now();
                self->tick_cv_.notify_all();
                self->replicator_cv_.notify_all();
                self->snap_in_flight_[s] = 0;
                return;
              }
              // 二次确认：迟到的快照回包作废（与 AppendEntries 回调的对称守卫，
              // 防旧 term / 退位后的迟到回包误推进 match_index_）。
              if (snap_term != self->current_term_ || self->state_ != ServerState::kLeader) {
                if (send_epoch == self->snap_epoch_[s]) self->snap_in_flight_[s] = 0;
                return;
              }
              // 最后一块回包：仅当 RPC 送达(ok) 且 follower 真正安装成功(ok_install) 才推进
              // 复制进度。follower 在「last_included <= last_applied（早就有这份快照）」时幂等
              // 返回 ok_install=true（不落盘但视为已安装），故不会卡死（TestSnapshotRPC3B
              // key=c 正常）；不可靠网下中间块丢失致 CRC 失败时 ok_install=false，leader 不
              // 推进 → 下一轮心跳重发整块自愈，消除「虚报已追平 → follower 永久饿死」的活性边角。
              if (is_last) {
                if (ok && reply.ok_install) {
                  self->match_index_[s] = std::max(self->match_index_[s], last_idx);
                  self->next_index_[s] = last_idx + 1;
                }
                self->snap_in_flight_[s] = 0;
                self->replicator_cv_.notify_all();
              }
            },
            &cancel_rpcs_);
        off += len;
      } while (off < total);
      // 注意：snap_in_flight_[s]=1 已在上面的持锁决策块里置位；
      // 最后一块的回包回调（持锁）会把它清零。不要在这里再置 1，否则会与回调竞争。
      continue;
    }
    if (!send_now) continue;

    // 发射后不管：RPC 在路上时本线程继续跑，不会被断连节点的
    // 0~7000ms 模拟延迟拖死。
    peers_[s]->CallAsyncTyped<AppendEntriesArgs, AppendEntriesReply>(
        "Raft.AppendEntries", args,
        [self, server, args](bool ok, const AppendEntriesReply& reply) {
          const size_t s = static_cast<size_t>(server);
          std::lock_guard<std::mutex> lk(self->mu_);
          if (self->killed_.load()) return;
          // CheckQuorum liveness：回包 ok=true（哪怕迟到）→ 清零连续失败计数；
          // ok=false（丢包 / 真断连）→ 累加。据此区分"回包延迟"与"真失联"。
          if (ok) self->cq_consec_fail_[s] = 0;
          else {
            if (self->cq_consec_fail_[s] < kCQMaxConsecFail) self->cq_consec_fail_[s]++;
            return;  // 丢包 / 对方挂了，下一轮重发（封顶防无界增长）
          }

          // 这一批在途日志已经有了结论（无论成败），可以再发下一批了
          // （流水线：窗口占用由 next_index_ - match_index_ 自动释放，无需显式清 bool）

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

          // ---- CheckQuorum / Leader Lease / ReadIndex：记下这次"联系" ----
          // 位置很有讲究：刻意放在 if (reply.success) 【之前】。
          // 因为 success=false 只表示"日志对不上、需要回退"，但这个 follower
          // 确实收到了我的 AppendEntries 并且回了包 —— 这本身就证明它承认
          // 我是这个 term 的 leader。只有 ok==false（丢包 / 超时）才真的算
          // "联系不上它"。
          // 如果只在 success 时才记，那么在日志冲突频繁的 Figure8Unreliable
          // 场景里，leader 会误判自己"联系不上多数派"而反复退位，
          // 集群永远提交不了任何东西。
          self->last_ack_time_[s] = raftcpp::Now();

          if (reply.success) {
            int new_match =
                args.prev_log_index + static_cast<int>(args.entries.size());
            if (new_match > self->match_index_[s]) {
              self->match_index_[s] = new_match;
            }
            // 流水线：不再把发送指针拽回 match+1（否则已提前的发送指针被回退
            // → 重发已发批次 → 流水线名存实亡）。next_index_ 只在"发送新批次"时前进
            // （见发送分支）；此处只推进确认指针 match_index_。
            // self->next_index_[s] = self->match_index_[s] + 1;  // 删除
            // Leader Lease：只要多数派在最近一个选举超时窗口内确认过我，
            // 就把租约往后推。租约 = "我确信这段时间里没人能选出新 leader"。
            // 续租依赖近实时 ack：CountRecentAcksLocked 数的是 last_ack_time_ 在 kElectionTimeoutMin 内刷新的 voter；不可靠网回包延迟会让它过于保守→租约建不起来→退化慢路径（安全取舍，非 bug）。
            if (self->CountRecentAcksLocked(kElectionTimeoutMin) >
                self->MemberCountLocked() / 2) {
              self->lease_expire_ =
                  raftcpp::Now() + std::chrono::milliseconds(kLeaderLeaseMs);
            }
            self->AdvanceCommitIndexLocked();
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
            if (logs_[pos].type == EntryType::kConfChange) {
              // ---- 成员变更条目：apply 时才切换本地配置（不是收到就切）----
              int sv = logs_[pos].conf_server;
              // 目标角色三态：kRemoved / kLearner / kVoter（原来是 bool，塞不下 learner）
              MemberRole target = static_cast<MemberRole>(logs_[pos].conf_role);
              if (sv >= 0 && sv < static_cast<int>(is_member_.size())) {
                is_member_[sv] = target;
                // Q2 隐私加固：记下"节点 sv 被移除时的配置条目下标"作为复制冻结点。
                // 之后 leader 向 sv 复制日志最多到 idx（含本条移除条目），保证 sv 先
                // 收到自己的移除条目、知道被移除，再彻底冻结（不再收任何新数据）。
                // 非 kRemoved（加回 voter / 降级 learner）→ 清除冻结，恢复向它复制。
                removed_at_index_[sv] =
                    (target == MemberRole::kRemoved) ? idx : -1;
              }
              // 本变更已 apply → 清除在途标记（允许下一个变更）
              if (logs_[pos].index == pending_conf_index_) {
                pending_conf_index_ = 0;
              }
              if (pending_conf_index_ == 0) {
                int voters = static_cast<int>(MemberCountLocked());
                if (voters > 0 && voters % 2 == 0) {
                  std::fprintf(stderr,
                      "[raft] WARN: 当前集群 voter 数为偶数(%d)。偶数集群容错能力与奇数"
                      "相同却多耗费一台节点，且存在精确 50/50 网络分区导致整体不可用的风险；"
                      "建议保持奇数个 voter。\n",
                      voters);
                }
              }
              PersistLocked();  // 配置变更也要落盘，重启能恢复
              // 本节点已不是 voter（被移除、或降级成 learner）且是 leader → 主动退位。
              // learner 不投票也不该当 leader，所以判据用 !IsVoter 而不是只判 kRemoved。
              if (!IsVoter(is_member_[me_]) &&
                  state_ == ServerState::kLeader) {
                // 事件驱动退位：不在 applier 线程里阻塞等 ack。置标志后继续当
                // leader，让 ReplicateLoop 把 leader_commit（含本移除配置）通过普通
                // 心跳带出去；宽限 kStepdownGraceMs 后由 ReplicateLoop 真正退位。
                // 这样剩余 voter 必能拿到新 commitIndex → apply 配置 → 按新 quorum
                // 选新 leader，根除"旧 quorum 缺我这一票"的死锁，且无阻塞/无额外 RPC。
                pending_stepdown_ = true;
                stepdown_after_ =
                    raftcpp::Now() + std::chrono::milliseconds(kStepdownGraceMs);
              }
              tick_cv_.notify_all();
              replicator_cv_.notify_all();
              // 配置条目【也要】作为一条命令派发给状态机/harness。
              // 原因：harness 的 apply 顺序检查要求每个 index 都被记录；
              // 若 has_msg=false 直接丢弃，conf 占据的 index 会留下空洞，
              // 下一条真实命令会被误报 "apply out of order"。
              // 载荷用哨兵字符串，应用层（KV）按需忽略即可；配置条目本身
              // 已由上面这段逻辑在 raft 层内部消费（切换 is_member_），
              // 不会被当成客户端写命令执行。
              msg.command_valid = true;
              msg.command = "CONF:" + std::to_string(sv) + ":" +
                            (target == MemberRole::kVoter     ? "V"
                             : target == MemberRole::kLearner ? "L"
                                                              : "R");
              msg.command_index = idx;
              msg.entry_type = EntryType::kConfChange;
              has_msg = true;
            } else if (logs_[pos].type == EntryType::kNoOp) {
              // no-op：leader 当选补的空日志，只为推进 commitIndex / 读安全点。
              msg.command_valid = true;
              msg.command = logs_[pos].command;  // 空串
              msg.command_index = idx;
              msg.entry_type = EntryType::kNoOp;
              has_msg = true;
            } else {
              // kNormal：真实客户端命令
              msg.command_valid = true;
              msg.command = logs_[pos].command;
              msg.command_index = idx;
              msg.entry_type = EntryType::kNormal;
              has_msg = true;
            }
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

// ---------------------------------------------------------------------------
// 磁盘上 snapshot blob 的格式：
//   V1（老）[magic(8B)][index(4B)][term(4B)][len(4B)][raw...]
//   V2（现）[magic(8B)][index(4B)][term(4B)][len(4B)][crc(4B)][raw...]
//
// 为什么 blob 需要「自描述」的头？
//   blob 与 raft state 现在是【两次独立落盘】（阶段1 落 blob / 阶段2 提交
//   state），必须能判断"盘上这个 blob 到底对应哪个 index"：
//     · 恢复时 blob.index == state.snapshot_index_  → 已提交，采用；
//     · 恢复时 blob.index != state.snapshot_index_  → 崩在两阶段之间，
//       blob 属于"写了一半"的未提交快照 → 丢弃，从完整的日志重放。
//   没有这个头就无法区分，只能靠"猜测"，那正是丢数据的来源。
//
// 为什么 V2 再加一个 crc？（P1-1）
//   头里写了 len，读出来长度对得上，但【内容烂没烂完全不知道】：磁盘 bit rot
//   （放着不动翻了个比特）、写一半掉电，都会让 payload 烂掉而头看起来正常。
//   而日志早已被截断到 index=X —— 0..X 的状态【只存在这一份 blob 里】，读坏了
//   就永远重放不出来（state diverged）。所以必须能自校验，且损坏要被【发现】。
// ---------------------------------------------------------------------------
namespace {

constexpr char kSnapMagicV1[] = "RAFTSNP1";  // V1：无 CRC，只能兼容读（向前兼容）
constexpr char kSnapMagic[] = "RAFTSNP2";    // V2：头里带 CRC，当前写入格式

void PutInt32(std::string& s, int32_t v) {
  uint32_t u = static_cast<uint32_t>(v);
  for (int i = 0; i < 4; ++i) {
    s.push_back(static_cast<char>((u >> (i * 8)) & 0xFFu));
  }
}

int32_t GetInt32(const std::string& s, size_t off) {
  uint32_t u = 0;
  for (int i = 3; i >= 0; --i) {
    u = (u << 8) | static_cast<unsigned char>(s[off + static_cast<size_t>(i)]);
  }
  return static_cast<int32_t>(u);
}

}  // namespace

// P1-1：落盘 blob 的自校验编码。CRC 覆盖【头的前 20 字节 + payload】——
// 不只保护数据区，index/term/len 被翻改也能发现（否则攻击者/位翻转改一下 len
// 就能让上层拿到一份"长度自洽但内容错位"的状态）。
std::string EncodeSnapshotBlob(int index, int term, const std::string& raw) {
  std::string out;
  out.reserve(kSnapHeaderSize + raw.size());
  out.append(kSnapMagic, 8);
  PutInt32(out, index);
  PutInt32(out, term);
  PutInt32(out, static_cast<int32_t>(raw.size()));
  // 两段流式计算：先头 20B，再续算 payload —— 零额外拷贝（不拼临时串）。
  uint32_t crc = Crc32Update(0, out.data(), kSnapHeaderSizeV1);
  crc = Crc32Update(crc, raw.data(), raw.size());
  PutInt32(out, static_cast<int32_t>(crc));
  out.append(raw);
  return out;
}

// P1-1：落盘 blob 的自校验解码。
// 三态返回（配合 corrupt 出参）是核心设计：
//   ① 解码成功                       → return true
//   ② 空 / 非本格式（含老数据/垃圾）  → return false, corrupt=false（= 无快照，首次启动）
//   ③ 本格式但 CRC 或长度对不上       → return false, corrupt=true（= 真损坏，须 fail-stop）
// ②③ 必须分开：把 ③ 当成 ② 会让节点"丢掉唯一的状态副本还继续跑"（见 ReadPersist）。
bool DecodeSnapshotBlob(const std::string& blob, int* index, int* term,
                        std::string* raw, bool* corrupt) {
  if (corrupt) *corrupt = false;
  if (blob.size() < kSnapHeaderSizeV1) return false;  // 空/过短 → 无快照
  const bool v2 = (blob.compare(0, 8, kSnapMagic, 8) == 0);
  const bool v1 = !v2 && (blob.compare(0, 8, kSnapMagicV1, 8) == 0);
  if (!v1 && !v2) return false;  // 非本格式 → 无快照（不是"损坏"）
  const size_t hdr = v2 ? kSnapHeaderSize : kSnapHeaderSizeV1;
  // 走到这里说明 magic 是本格式的：后面任何对不上都只能是【真损坏】（半截写/位翻转）。
  if (blob.size() < hdr) {
    if (corrupt) *corrupt = true;
    return false;
  }
  const int32_t idx = GetInt32(blob, 8);
  const int32_t t = GetInt32(blob, 12);
  const int32_t len = GetInt32(blob, 16);
  if (len < 0) {
    if (corrupt) *corrupt = true;
    return false;
  }
  if (blob.size() != hdr + static_cast<size_t>(len)) {
    if (corrupt) *corrupt = true;  // 长度自相矛盾 = 被截断或被追加
    return false;
  }
  if (v2) {
    const uint32_t want = static_cast<uint32_t>(GetInt32(blob, kSnapHeaderSizeV1));
    uint32_t got = Crc32Update(0, blob.data(), kSnapHeaderSizeV1);
    got = Crc32Update(got, blob.data() + hdr, static_cast<size_t>(len));
    if (got != want) {
      if (corrupt) *corrupt = true;  // bit rot / 半截写
      return false;
    }
  }
  // V1 老格式无 CRC 可校验：长度自洽即认为可信（向前兼容，老盘能读）。
  if (index) *index = idx;
  if (term) *term = t;
  if (raw) raw->assign(blob, hdr, static_cast<size_t>(len));
  return true;
}

// Raft::SaveSnapshotBlob —— 见 raft.h 声明处的完整说明。
//
// 复述最关键的一点：真正慢的 I/O（真实磁盘上的 write + fsync）发生在
// 【raft 主锁 mu_ 之外】。mu_ 只在两处被短暂持有，且都只做 O(1) 的整数
// 比较/赋值，绝不会卡住心跳、读心跳或选举。
bool Raft::SaveSnapshotBlob(int index, int term, const std::string& raw_blob) {
  // snap_io_mu_ 串行化所有 blob 写：两个并发的快照（比如本地 Snapshot(100)
  // 和 InstallSnapshot(150)）不会互相覆盖成更旧的那个。
  std::lock_guard<std::mutex> io(snap_io_mu_);
  {
    std::lock_guard<std::mutex> lk(mu_);
    // 单调保护：盘上已有 index >= 本次 index 的 blob，再写就是回退覆盖。
    // 回退覆盖是致命的——若 state 已截断到更高的 index，恢复时这个更旧的
    // blob 会被判"未提交"丢弃，而日志也被截断了，状态就重放不出来了。
    if (index <= saved_blob_index_) return false;
  }

  // ★★★ 慢 I/O：这里是将来的 write() + fsync()，此刻不在 mu_ 内 ★★★
  persister_->SaveSnapshotOnly(EncodeSnapshotBlob(index, term, raw_blob));

  {
    std::lock_guard<std::mutex> lk(mu_);
    if (index > saved_blob_index_) saved_blob_index_ = index;
  }
  return true;
}

// 1.必须已 apply：快照本质是「状态机在 index=X 的物理快照」，所以 snap_idx = rafts_[i]->LastApplied()（config.cpp:211）
// 2. lastApplied ≤ commitIndex 不变量：由 ApplyLoop（raft.cpp:782-797）逐条推进维护
// 3. Raft::Snapshot 兜底拒绝 index > commit_index_ —— 双保险
void Raft::Snapshot(int index, const std::string& snapshot) {
  // ========== 阶段 0：快检查（持 mu_，只读，不做 I/O）==========
  // 目的：在动手落盘之前先确认这个快照值得做，并把 term 取出来
  //      （term 只能在锁内从 logs_ 读）。
  int snap_term = 0;
  {
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
    snap_term = logs_[pos].term;
  }

  // ========== 阶段 1：blob 落盘（★ 慢 I/O，在 raft 主锁 mu_ 之外）==========
  // 真实磁盘上这里是几百 MB 的 write + fsync（几百 ms～几秒）。放在锁内
  // 会把心跳/读心跳/选举全饿死。详见 SaveSnapshotBlob 的说明。
  SaveSnapshotBlob(index, snap_term, snapshot);

  // ========== 阶段 2：持锁提交 raft state（快：只有元数据 + 日志截断）==========
  std::lock_guard<std::mutex> lk(mu_);

  // double-check：阶段 1 不占锁，期间状态可能已变（例如被装上了更新的快照），
  // 三个条件必须重判，否则会拿一个已过期的 index 去做日志截断。
  if (index > commit_index_) return;
  if (index <= snapshot_index_) return;
  size_t pos = static_cast<size_t>(index - snapshot_index_);
  if (pos >= logs_.size()) return;  // 防御：index 不合法

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
  // P0-2：原子替换（shared_ptr 赋值），不原地改。正在发送的旧 buffer 由
  // ReplicateLoop 持有的 shared_ptr 钉死，不会被这次替换影响。
  snapshot_data_ = std::make_shared<std::string>(std::move(snapshot));
  // 【惰性 CRC】这里【不】计算 CRC，只作废缓存。
  // 本函数由 applier 高频调用（实测 20s 内每节点数千次快照）。若每次都对整个 blob
  // （本测试后期 ~73KB）算一遍 CRC32，在 TSan 下（每次访存都插桩）会把 KV 的
  // apply/快照循环整体拖慢，落后几十次快照才轮得上一次裁剪 → 日志来不及截 →
  // "logs were not trimmed"（RELEASE+TSAN 50 轮实测 3/50，base 无此开销为 0/50）。
  // CRC 只有 leader 真要发分块快照时才用得上，故推迟到 ReplicateLoop 那一刻计算。
  snap_crc_ = 0;
  snap_crc_index_ = -1;
  // 注意：blob 已在阶段 1 落盘，这里【只】持久化 raft state（PersistLocked
  // 不再碰 blob）。落盘顺序恒为「先 blob 后 state」，靠 blob 头里的 index
  // 做崩溃校验，见 DecodeSnapshotBlob / ReadPersist。

  PersistLocked();
  Trace("S%d 生成快照 index=%d term=%d 日志截断到 %zu 条", me_, snapshot_index_,
        snapshot_term_, logs_.size());
}

void Raft::InstallSnapshot(const InstallSnapshotArgs& args,
                           InstallSnapshotReply& reply) {
  // §2.1 分块快照接收：先按 offset 重组完整 blob，done 且 CRC 校验通过后才安装。
  // 中间 chunk 只落本地重组缓冲，不碰 raft state / 不落盘（幂等、可重传）。
  std::string assembled;  // done+校验通过后承载完整 blob，供阶段 1/2 使用

  // ========== 阶段 0：快判 + 分块重组（持 mu_）==========
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

    // 合法（同任期或更新任期）的 leader → 记下编号，供上层 WrongLeader 重定向。
    leader_id_ = args.leader_id;

    // 已经包含这个快照（或更新的）→ 幂等视为已安装：不落盘、不动分块缓冲，
    // 但回 ok_install=true，让 leader 据此推进 match_index_（避免「早就有却因
    // ok_install=false 永不推进」的死循环，见 TestSnapshotRPC3B key=c）。
    if (args.last_included_index <= last_applied_) {
      reply.ok_install = true;
      return;
    }

    // 分块重组（档2 epoch 加固）：
    // - 不同身份的快照（last_included_term/index 变化）：比当前在途的【更新】才采纳并 reset；
    //   更旧的迟到残片直接忽略（不 reset、不写），避免冲掉新快照在途缓冲。
    // - 同一身份的快照：offset==0 表示 leader 开启新一轮传输，直接 reset 重组；
    //   offset>0 的迟到残片才靠 epoch 去重（比当前更小 → 忽略）。这样 leader 重启后
    //   首帧（epoch 从 1 起、offset==0）直接被采纳，不会卡在「旧残片忽略」的活性退化。
    bool new_snapshot =
        (recv_snap_index_ != args.last_included_index) || (recv_snap_last_term_ != args.last_included_term);
    if (new_snapshot) {
      bool newer = (recv_snap_index_ < 0) ||  // 尚无在途缓冲
                   (args.last_included_index > recv_snap_index_) ||
                   (args.last_included_index == recv_snap_index_ &&
                    args.last_included_term > recv_snap_last_term_);
      if (!newer) {
        // 更旧快照的迟到残片 → 忽略（不 reset、不写，避免冲掉新快照在途缓冲）。
        reply.ok_install = false;
        return;
      }
      recv_snap_buf_.clear();
      recv_snap_index_ = args.last_included_index;
      recv_snap_last_term_ = args.last_included_term;
      recv_snap_epoch_ = args.epoch;
    } else {
      // 同一快照：offset==0 表示 leader 开启了新一轮传输（每次发送/重发都从 0 起），
      // 直接清空缓冲重组；offset>0 的迟到残片才用 epoch 去重（比当前更小则忽略）。
      // 这样 leader 重启后首帧（epoch 从 1 起、offset==0）直接被采纳，不会因 epoch
      // 比 follower 残留值小而卡在「旧残片忽略」（自愈但慢）的活性退化里。
      if (args.offset == 0) {
        recv_snap_buf_.clear();
        recv_snap_epoch_ = args.epoch;
      } else if (args.epoch != recv_snap_epoch_) {
        if (args.epoch > recv_snap_epoch_) {
          recv_snap_buf_.clear();
          recv_snap_epoch_ = args.epoch;
        } else {
          reply.ok_install = false;
          return;
        }
      }
    }
    // 把本 chunk 写入缓冲的 offset 处（按需扩容，未到的区域填 0）。
    size_t off = static_cast<size_t>(args.offset);
    // §2.1 防御（生产加固，P0-3）：防伪造 RPC 撑爆内存（OOM DoS）。
    // 正常分块每块 data ≤ kSnapshotChunkSize；offset 非法（<0）或
    // offset+data 之和超过 kMaxRecvSnapBuf 即视为恶意/异常，拒绝并复位在途缓冲，
    // 等 leader 下一轮合法传输重发（与 done 时 CRC 失败走同一复位路径）。
    if (args.offset < 0 ||                                 // offset 非法
        args.data.size() > kSnapshotChunkSize ||           // 单块过大
        off + args.data.size() > kMaxRecvSnapBuf) {        // 总缓冲超限
      recv_snap_buf_.clear();
      recv_snap_index_ = -1;
      recv_snap_last_term_ = -1;
      recv_snap_epoch_ = -1;
      reply.ok_install = false;
      return;
    }
    if (off + args.data.size() > recv_snap_buf_.size())
      recv_snap_buf_.resize(off + args.data.size());
    if (!args.data.empty())
      recv_snap_buf_.replace(off, args.data.size(), args.data);

    if (!args.done) {
      // 中间 chunk：仅重组，回包（ok_install=false 表示尚未安装）。
      reply.ok_install = false;
      return;
    }

    // done chunk：先判定完整性 + CRC 校验。
    size_t expect_total = off + args.data.size();
    bool complete = (recv_snap_buf_.size() == expect_total);
    bool crc_ok = complete &&
                  (Crc32(recv_snap_buf_.data(), recv_snap_buf_.size()) == args.crc);
    // 不完整（分块丢失 / 乱序）或 CRC 不符（半截写 / 损坏）→ 拒绝安装，清缓冲，
    // 等 leader 下一轮心跳重发整块（正确，只是略慢）。
    if (!crc_ok) {
      recv_snap_buf_.clear();
      recv_snap_index_ = -1;
      recv_snap_last_term_ = -1;
      recv_snap_epoch_ = -1;
      reply.ok_install = false;
      return;
    }
    // 校验通过：把完整 blob 挪出（锁外 I/O 用），并复位接收状态。
    assembled = std::move(recv_snap_buf_);
    recv_snap_index_ = -1;
    recv_snap_last_term_ = -1;
    recv_snap_epoch_ = -1;
  }

  // ========== 阶段 1：blob 落盘（★ 慢 I/O，在 raft 主锁 mu_ 之外）==========
  // 这里是 follower 侧真正的重活：几百 MB 的快照要 write + fsync。
  // 旧实现在 mu_ 内做，会堵死本节点的 AppendEntries / 选举定时器；
  // 若多数派 follower 同时装快照，leader 收不到 ack → 被 CheckQuorum 误退位。
  SaveSnapshotBlob(args.last_included_index, args.last_included_term, assembled);

  // ========== 阶段 2：持锁完成 raft state 变更（快）==========
  // msg 在【锁外】构造：锁内不再做大 blob 的拷贝。
  ApplyMsg msg;
  msg.snapshot_valid = true;
  msg.snapshot = assembled;
  msg.snapshot_index = args.last_included_index;
  msg.snapshot_term = args.last_included_term;
  {
    std::lock_guard<std::mutex> lk(mu_);
    reply.term = current_term_;

    // double-check：阶段 1 未持锁，期间状态可能已变。
    if (args.term < current_term_) return;  // 旧 leader 的快照，拒绝

    // 已经包含这个快照（或更新的），无需重复安装（idempotent）。
    // ⚠️ 改用 last_applied_ 判旧（对齐 Go 版 raft.go:904）：
    // 基准必须是「状态机已应用位置」，而不是 snapshot_index_（日志截断点）。
    if (args.last_included_index <= last_applied_) return;

    int old_snap_index = snapshot_index_;
    snapshot_index_ = args.last_included_index;
    snapshot_term_ = args.last_included_term;
    snapshot_data_ = std::make_shared<std::string>(std::move(assembled));
    // 惰性 CRC：blob 变了就作废缓存，等本节点真要发快照时再算（见 Raft::Snapshot）。
    snap_crc_ = 0;
    snap_crc_index_ = -1;
    // 成员配置随快照一起恢复：落后 follower 装快照时本地 logs_ 可能已不含覆盖
    // 区间内的 conf 条目（被截断），必须用 leader 在快照点处的角色数组重建，
    // 否则 is_member_ 与 leader 不一致 → 多数派判定错位 → 安全性崩塌。
    if (static_cast<int>(args.members.size()) == static_cast<int>(peers_.size())) {
      is_member_.clear();     // 清空元素，不会释放分配的capacity的内存区
      is_member_.reserve(args.members.size());   // 如果size小于上条的capacity，不会缩容
      for (int m : args.members) {
        is_member_.push_back(static_cast<MemberRole>(m));
      }
      if (!IsVoter(is_member_[me_]) && state_ == ServerState::kLeader) {
        ConvertToFollowerLocked(current_term_);
      }
    }

    // 截断 logs_：哨兵重建（index 对齐到快照点）；args.last_included_index+1 之后
    // 的旧条目保留，等 leader 重发覆盖即可（即使保留也可能错位，AppendEntries 的
    // 一致性检查会基于 prev_log_index 砍掉冲突段）。
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

    // 落盘 raft state（含截断后的 logs_、新 snapshot 元数据）。
    PersistLocked();

    // 挂到 pending_snapshot_，由 ApplyLoop 统一派发。
    if (!pending_snapshot_.has_value() ||
        pending_snapshot_->snapshot_index < msg.snapshot_index) {
      Trace("S%d InstallSnapshot单相挂pending idx=%d term=%d (已挂=%s)", me_,
            msg.snapshot_index, msg.snapshot_term,
            pending_snapshot_.has_value() ? "是" : "否");
      pending_snapshot_ = std::move(msg);
      apply_cv_.notify_all();  // 唤醒 ApplyLoop
    }
    reply.ok_install = true;  // 仅当真正安装成功时置 true（leader 据此推进 match）
    return;
  }
}

bool Raft::CondInstallSnapshot(int index, int term,
                               const std::string& snapshot) {
  (void)snapshot;
  std::lock_guard<std::mutex> lk(mu_);
  // 单相设计后，本函数【只做"这个快照还能不能装"的查询】——所有 raft state
  // 修改已经在 InstallSnapshot 内完成（截断 + 推进 + 持久化）。
  //
  // 唯一判据：(index, term) 必须等于 raft【当前】的快照元数据。
  //   · 相等 → 这就是本节点刚装的那个快照，状态机应当安装它；
  //   · 不等 → raft 已经装了别的（更新的）快照，这条消息已过时，装它会回退状态机。
  //
  // ⚠️ 这里【不要】用 last_applied_ 判旧（历史上曾有 `last_applied_ > index → false`）。
  // 原因：本实现里 last_applied_ 的语义是「raft 已派发到 apply_ch_ 的位置」，而不是
  // 「状态机已经应用到哪」——ApplyLoop 派发快照消息后会【立刻继续】派发 N+1、N+2…，
  // 而状态机在另一个线程【异步】消费。于是正常时序下就会出现
  //     last_applied_ = N+k  >  index = N
  // 该判据被误命中 → 状态机拒绝安装快照 → 快照覆盖区间 [1..N] 在状态机里留下空洞
  // → 下一条命令 apply N+k+1 时前一条不在 → harness 报 "apply out of order"
  //   （偶发失败 TestReadIndexSnapshotUnreliable 的根因，自然失败率 <1/40）。
  //
  // 而它想防的"用旧快照覆盖更新的状态机"其实【不会发生】：apply_ch_ 是 FIFO，
  // 快照消息恒排在它之后的 entries 之前被状态机处理；且 pending_snapshot_ 只保留
  // 最新快照（InstallSnapshot 内比较 snapshot_index 后覆盖），不会派发更旧的快照。
  // 所以这条判据既会误伤、又是冗余的 —— 已删除。
  if (index != snapshot_index_ || term != snapshot_term_) {
    Trace("S%d CondInstallSnapshot idx=%d term=%d → false (raft snap_idx=%d snap_term=%d last_applied=%d)",
          me_, index, term, snapshot_index_, snapshot_term_, last_applied_);
    return false;
  }
  Trace("S%d CondInstallSnapshot idx=%d term=%d → true", me_, index, term);
  return true;
}

std::vector<int> Raft::MembershipView() const {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<int> v;
  v.reserve(is_member_.size());
  for (MemberRole r : is_member_) v.push_back(static_cast<int>(r));
  return v;
}

int Raft::QuorumSize() const {
  std::lock_guard<std::mutex> lk(mu_);
  return QuorumSizeLocked();
}

int Raft::LastLogIndex() const {
  std::lock_guard<std::mutex> lk(mu_);
  return LastLogIndexLocked();
}

// ===========================================================================
// C1 未提交 entry 上限（背压）访问器
// ===========================================================================
void Raft::SetMaxUncommittedEntries(int n) {
  std::lock_guard<std::mutex> lk(mu_);
  max_uncommitted_entries_ = n;
}

int Raft::MaxUncommittedEntries() const {
  std::lock_guard<std::mutex> lk(mu_);
  return max_uncommitted_entries_;
}

int Raft::UncommittedCount() const {
  std::lock_guard<std::mutex> lk(mu_);
  return LastLogIndexLocked() - commit_index_;
}

// ===========================================================================
// C5 被移除节点主动退场
// ===========================================================================
// 【设计选择】从 is_member_ 派生，不新增持久化字段：
//   · 省掉 PersistLocked/ReadPersist 的字节流兼容负担（见 raft-cpp-port §3）；
//   · 天然与"配置变更在 apply 时切换本地配置"保持同一时刻同步；
//   · 节点被加回（kLearner/kVoter）时自动转假，无需额外清理逻辑。
bool Raft::IsRemovedSelfLocked() const {
  if (me_ < 0 || me_ >= static_cast<int>(is_member_.size())) return false;
  return is_member_[me_] == MemberRole::kRemoved;
}

bool Raft::IsRemovedSelf() {
  std::lock_guard<std::mutex> lk(mu_);
  return IsRemovedSelfLocked();
}

int Raft::ElectionSuppressedCount() const {
  return election_suppressed_.load();
}

// 两态便捷入口：add=true → kVoter；add=false → kRemoved。
// 想设成 kLearner 请用下面的 ProposeConfChangeTo(server, MemberRole::kLearner)。
StartResult Raft::ProposeConfChange(int server, bool add) {
  return ProposeConfChangeTo(server, add ? MemberRole::kVoter
                                         : MemberRole::kRemoved);
}

// 三态入口：把 server 设为指定角色（kRemoved / kLearner / kVoter）。
// 内部追加一条 conf 配置条目，与普通命令走同一条"复制 → 提交 → apply"路径；
// 节点在【apply 到该条目时】才真正切换本地 is_member_（不是收到就切），
// 保证所有节点按相同顺序、在同一逻辑时刻切换。
StartResult Raft::ProposeConfChangeTo(int server, MemberRole target) {
  std::lock_guard<std::mutex> lk(mu_);
  StartResult res;
  res.term = current_term_;
  res.is_leader = (state_ == ServerState::kLeader);
  if (!res.is_leader) return res;
  // 与 Start() 对称：事件驱动退位宽限期内，本节点虽仍是 leader 但已被移除/降级出
  // voter，不得再发起新的成员变更。否则会由"非 voter 的 leader"驱动配置变更——
  // 由于 AppendEntries 只校验 term、不校验 sender 是否 voter，该变更仍可能被多数派
  // 接受并提交，等于让无权节点改写配置。
  // 注意：res.is_leader 上面已按 state_ 置为 true，这里必须显式改回 false，
  // 让调用方能像 Start() 那样用 is_leader 判定"是否被接受"，而不是去猜 index==0。
  if (pending_stepdown_) {
    res.is_leader = false;
    return res;
  }
  // 单飞保护：已有未提交的成员变更在途，拒绝新的（文档 §2.4 运维纪律）。
  if (pending_conf_index_ != 0) return res;
  if (server < 0 || server >= static_cast<int>(is_member_.size())) return res;
  // 角色合法性：只接受三态之一，防止 conf_role 被塞进野值。
  if (target != MemberRole::kRemoved && target != MemberRole::kLearner &&
      target != MemberRole::kVoter) {
    return res;
  }
  // 安全阀：变更后的配置必须至少保留一个 voter，否则集群再也无法选出 leader（quorum=0）。
  // 危险情形：server 当前是 kVoter，却要被设为 kRemoved / kLearner（移除或降级最后一个 voter），
  // 且当前只剩这一个 voter —— 必须拒绝。is_member_ 此刻是【已提交】配置（变更只在 apply 时切换），
  // MemberCountLocked() 取的是同一把锁内快照，与真正 apply 后的计数口径一致。
  if (IsVoter(is_member_[server]) && !IsVoter(target) && MemberCountLocked() <= 1) {
    return res;  // 会变成 0 voter 配置，拒绝（res.index 保持 -1 = StartResult 默认，未接受）
  }
  // 追加一条 conf 配置条目（与普通命令走同一复制/提交/apply 路径）。
  LogEntry e;
  e.term = current_term_;
  e.index = LastLogIndexLocked() + 1;
  e.type = EntryType::kConfChange;              // 标记：这是"成员变更条目"
  e.conf_server = server;                  // 改谁（比如节点2）
  e.conf_role = static_cast<int>(target);  // 改成什么角色（三态）
  // #2.2 磁盘水位保护：与 Start() 对称，配置变更也会 append 一条日志，同样受配额约束。
  // 必须在【任何副作用（含下方 removed_at_index_ 冻结点设置）之前】判定，否则 disk_full
  // 早退会留下"被污染的下标却没有对应日志条目"的错位状态。默认 max_raft_state_bytes_==0
  // 不限制，与现有行为完全一致。语义上本节点仍是 leader，但本次提案被拒；与 Start() 对齐，
  // 下方 disk_full 早退处显式将 is_leader 置 false，调用方应以 disk_full 字段判定（而非
  // is_leader）、稍后调 Snapshot() 压缩再重试。
  if (max_raft_state_bytes_ > 0 &&
      RaftStateSizeLocked() >= max_raft_state_bytes_) {
    res.disk_full = true;
    res.is_leader = false;  // 对齐 Start()：disk_full 时 is_leader 显式 false，避免上层误判"被接受"
    return res;
  }
  // Q2 隐私加固：在【提案此刻】就记下"server 被移除时的配置条目下标"作为复制冻结点。
  // 必须早于 conf 提交/apply：否则 ReplicateLoop 可能在 cap 设置前就把 conf 之后的
  // 新客户端条目（index > e.index）拷进发送缓冲并发给 server，造成"removed 节点多收
  // 数据"的竞态。提案时下标 e.index 已知，且本函数持 mu_，ReplicateLoop 持锁拷贝
  // 条目时必能看到该 cap。ApplyLoop apply 时还会再设一次（覆盖 follower / 重启重放）。
  // 非 kRemoved（加回 voter / 降级 learner）→ 清除冻结，否则加回后仍被误冻结。
  removed_at_index_[server] = (target == MemberRole::kRemoved) ? e.index : -1;
  logs_.push_back(std::move(e));
  pending_conf_index_ = e.index;           // 记下"这条在飞行"，单飞保护
  PersistLocked();
  AdvanceCommitIndexLocked();  // 同上：单 voter 时 conf 条目也要能提交
  replicator_cv_.notify_all();
  res.index = pending_conf_index_;
  return res;
}

// ===========================================================================
// 第六部分：持久化
// ===========================================================================

// #2.2 辅助：把当前 raft state 编码为字节串（term/votedFor/snapshot 元数据/log/成员配置），
// 不含快照 blob（blob 单独由 SaveSnapshotBlob 落盘）。PersistLocked 与 RaftStateSize 共用，
// 保证「写入磁盘的字节数」与「对外暴露的 RaftStateSize」严格一致。调用方须持 mu_。
std::string Raft::EncodeRaftStateLocked() const {
  labrpc::Encoder e;
  e.Int(current_term_).Int(voted_for_);
  e.Int(snapshot_index_).Int(snapshot_term_);  // 快照元数据也要随状态恢复
  e.Int(static_cast<int>(logs_.size()));
  for (const LogEntry& en : logs_) {
    e.Int(en.term).Int(en.index).Bytes(en.command)
    .Int(static_cast<int>(en.type)).Int(en.conf_server).Int(en.conf_role);
  }
  // 持久化当前成员配置（与日志一起落盘，重启能恢复）。
  e.Int(static_cast<int>(is_member_.size()));
  for (MemberRole r : is_member_) e.Int(static_cast<int>(r));
  e.Int(pending_conf_index_);
  return e.Take();
}

void Raft::PersistLocked() {
  // ⚠️ 这里【只写 raft state】，不再写 snapshot blob。
  // blob 已由 SaveSnapshotBlob() 在锁外单独落盘（阶段 1）。
  // 落盘顺序恒为「先 blob（带 index 头）→ 后 state」，崩溃一致性靠
  // DecodeSnapshotBlob() 比对 blob.index 与 snapshot_index_ 来保证。
  // #2.2 优化：编码一次，既用于落盘、又缓存字节数供 RaftStateSizeLocked() O(1) 读取，
  // 避免 Start()/ProposeConfChangeTo 每次提案都重编一遍（启用配额时从 O(N) 降到 O(1)）。
  // 调用方均已持 mu_，写入 raft_state_bytes_ 安全。
  std::string blob = EncodeRaftStateLocked();
  raft_state_bytes_ = blob.size();
  persister_->SaveRaftState(std::move(blob));
}

size_t Raft::RaftStateSize() const {
  std::lock_guard<std::mutex> lk(mu_);
  return RaftStateSizeLocked();
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
    int et = 0;
    if (!(d.Int(en.term) && d.Int(en.index) && d.Bytes(en.command) &&
          d.Int(et) && d.Int(en.conf_server) && d.Int(en.conf_role)))
      return;
    en.type = static_cast<EntryType>(et);
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
  // ---- 恢复 snapshot blob：剥磁盘头 + 与 raft state 对齐 ----
  //
  // blob 与 state 是两次独立落盘，因此 blob 可能【领先】于 state：
  // 崩溃若发生在「阶段1 落盘 blob」之后、「阶段2 提交 state」之前，盘上
  // 就是 blob(index=X) + state(snapshot_index_ < X)。
  //
  // 这种"未提交"的 blob 必须【追认采用】，绝不能丢弃：
  //   本节点的日志早被更早的快照截断过（logs 只保留上次快照点之后的条目），
  //   0..X 的状态【只存在于 blob 里】——丢掉就永远重放不出来，状态机凭空
  //   缺一大块 → "replicas did not converge / state diverged"。
  //   （实测：TestSnapshotRecoverManyClients3B 就是这么发散的。）
  //
  // 安全性由「写 blob 时 last_applied_ 必然 >= index」保证：
  //   · Snapshot() 由 applier 在 apply 之后触发（index <= 已 apply 位置）；
  //   · InstallSnapshot() 在阶段2 就推进了 last_applied_ 到 index。
  //   所以 blob 是自包含的真实状态，把边界抬到它不会"超前于实际"。
  // （etcd 也是这个语义：恢复时以磁盘上最新的 snapshot 文件为准。）
  {
    std::string disk_blob = persister_->ReadSnapshot();
    int b_idx = 0, b_term = 0;
    std::string b_raw;
    bool corrupt = false;
    if (DecodeSnapshotBlob(disk_blob, &b_idx, &b_term, &b_raw, &corrupt)) {
      if (b_idx > snapshot_index_) {
        Trace("S%d 追认未提交的 snapshot blob：blob.index=%d > snapshot_index_=%d",
              me_, b_idx, snapshot_index_);
        // 同步把日志截断到 b_idx（哨兵对齐）——否则后续
        // "index - snapshot_index_" 的定位会整体错位。
        size_t pos = static_cast<size_t>(b_idx - snapshot_index_);
        if (pos < logs.size()) {
          std::vector<LogEntry> new_logs;
          new_logs.reserve(logs.size() - pos);
          for (size_t i = pos; i < logs.size(); ++i) {
            new_logs.push_back(std::move(logs[i]));
          }
          new_logs[0] = LogEntry{b_term, b_idx, ""};
          logs.swap(new_logs);
        } else {
          logs.clear();
          logs.push_back(LogEntry{b_term, b_idx, ""});
        }
        snapshot_index_ = b_idx;
        snapshot_term_ = b_term;
        if (commit_index_ < b_idx) commit_index_ = b_idx;
        if (last_applied_ < b_idx) last_applied_ = b_idx;
      }
      if (b_idx == snapshot_index_) {
        snap_crc_ = Crc32(b_raw.data(), b_raw.size());
        snap_crc_index_ = snapshot_index_;  // 已算好，记为对该 index 的缓存
        snapshot_data_ = std::make_shared<std::string>(std::move(b_raw));  // 裸状态机数据
      }
      // 盘上 blob 的 index 恒 >= 已提交边界，之后只允许写更新的。
      saved_blob_index_ = snapshot_index_;
    } else if (corrupt) {
      // ★ P1-1：blob 是本格式（magic 对得上）但 CRC / 长度对不上 = 真损坏。
      // 这里【绝不能】像下面那样"视为无快照"继续跑：日志早已被截断到
      // snapshot_index_=X，0..X 的状态只活在这一个 blob 里，一旦悄悄跳过，
      // 状态机就永久缺一大块 → replicas did not converge / state diverged，
      // 且不可自愈（重启还是同一个坏文件）。
      // 所以 fail-stop：宁可当街崩，也不带着一份错状态继续对外服务 ——
      // 与 persister 落盘失败即 abort 是同一条铁律（持久化不可信就不许继续）。
      std::fprintf(stderr,
                   "S%d FATAL: 磁盘 snapshot blob 校验失败（CRC/长度不符），"
                   "疑似 bit rot 或半截写：blob_size=%zu snapshot_index_=%d\n",
                   me_, disk_blob.size(), snapshot_index_);
      std::abort();
    } else {
      // 空 / 老格式 / 非本格式：视为无快照（首次启动就是这种情况）。
      saved_blob_index_ = snapshot_index_;
    }
  }
  logs_ = std::move(logs);
  // 恢复成员配置（与 state 一起落盘）。peer 数与磁盘不符时退化为全 voter（安全默认）。
  {
    int nm = 0;
    if (!d.Int(nm)) return;
    std::vector<MemberRole> restored;
    restored.reserve(nm > 0 ? static_cast<size_t>(nm) : 0);
    bool ok = (nm == static_cast<int>(peers_.size()));
    for (int i = 0; i < nm; i++) {
      int rv = 0;
      if (!d.Int(rv)) return;
      if (ok) restored.push_back(static_cast<MemberRole>(rv));
    }
    if (ok) {
      is_member_ = std::move(restored);
    } else {
      is_member_.assign(peers_.size(), MemberRole::kVoter);
    }
    int pc = 0;
    if (!d.Int(pc)) return;
    pending_conf_index_ = pc;
  }
  // 现在才是真正的"消费完整字节流"边界
  // 注：原代码在上方赋值后又用 snapshot_index_ 重复赋了一次 commit_index_/last_applied_，
  // 二者等价（snapshot_index_ == snap_idx），属冗余，已删除。
  // #2.2 初始化缓存的持久化字节数（构造期单线程、ctor 调 ReadPersist 时无并发；
  // 直接重编一次即可，代价仅一次）。之后 RaftStateSizeLocked() 全程 O(1)。
  raft_state_bytes_ = EncodeRaftStateLocked().size();
  if (!d.Ok()) return;
}

int Raft::MemberCountLocked() const {
  int c = 0;
  for (MemberRole m : is_member_) {
    if (IsVoter(m)) c++;
  }
  return c;
}

// ===========================================================================
// 选举计票（纯函数）
// ===========================================================================
// 只数"当前仍是 voter"的赞成票，与 QuorumSizeLocked()（只数 IsVoter）
// 保持【分子分母同口径】。
//
// 为什么必须过滤：若把"回包时已被移除 / 降级成 learner"的票也算进分子，
// 却拿"当前 voter 多数"当门槛，候选者就能靠一张已作废的票凑够 quorum
// 当选 —— leader 并非由当前配置多数选出，选举合法性被破坏。
// （典型场景：候选者发出投票请求时对方还是 voter，回包前配置变更把对方
//  移除，同时 quorum 随配置缩小而下降，那张票正好补上缺口。）
int CountGrantedVotes(const std::vector<MemberRole>& is_member,
                      const std::vector<int>& votes) {
  int n = 0;
  for (size_t i = 0; i < is_member.size() && i < votes.size(); i++) {
    if (!IsVoter(is_member[i])) continue;  // 已移除 / learner：票作废
    if (votes[i] == 1) n++;
  }
  return n;
}

int Raft::CountGrantedVotesLocked() const {
  return CountGrantedVotes(is_member_, vote_granted_);
}

int Raft::CountGrantedPrevotesLocked() const {
  return CountGrantedVotes(is_member_, prevote_granted_);
}

// leader 在最近 150ms 里有没有收到过 i 的回包
int Raft::CountRecentAcksLocked(int window_ms) const {
  const auto cutoff = raftcpp::Now() - std::chrono::milliseconds(window_ms);
  int count = 0;
  for (size_t i = 0; i < last_ack_time_.size(); i++) {
    // 已被移除 / 尚未正式加入的节点直接跳过：它连成员都不是，
    // 它的"确认"对多数派判定没有任何意义。
    // （少了这层过滤，被移除的节点凭一己之力就能凑出多数派 → 安全性崩塌。）
    if (i >= is_member_.size() || !IsVoter(is_member_[i])) continue;
    // leader 自己永远算一票：它不会给自己发 AppendEntries，
    // last_ack_time_[me_] 不会被心跳回包刷新，必须无条件计入，
    // 否则刚当选的 leader 会把自己判成"联系不上多数派"而立刻退位。
    if (static_cast<int>(i) == me_) {
      count++;
      continue;
    }
    if (last_ack_time_[i] > cutoff) count++;
  }
  return count;
}

void Raft::BroadcastReadHeartbeat(int ctx) {
  // 给每个 follower 发一条"带 read_ctx 的空心跳"。follower 在 AppendEntriesReply
  // 里原样回显 read_ctx，leader 的回包处理据此调 RecordReadAckLocked 把票记到
  // 对应的读请求上。
  for (size_t s = 0; s < peers_.size(); s++) {
    if (s == static_cast<size_t>(me_)) continue;
    AppendEntriesArgs args;
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (state_ != ServerState::kLeader) return;   // 已退位，发也没意义
      if (s >= is_member_.size() || !IsVoter(is_member_[s])) continue;  // 只读 voter
      args.term = current_term_;
      args.leader_id = me_;
      args.leader_commit = commit_index_;
      args.prev_log_index = LastLogIndexLocked();
      // prev_log_term 安全计算：与 ReplicateLoop（raft.cpp:1301-1307）【完全同口径】
      // —— 先算偏移再判 `pos < logs_.size()`，而不是只判 `== snapshot_index_`。
      // 哨兵不变量（logs_ 非空且 logs_[0].index == snapshot_index_）保证
      // LastLogIndexLocked() >= snapshot_index_ 恒成立，所以正常情况 pos 必定命中
      // logs_ 末位（打完快照只剩哨兵时 pos==0，取到 snapshot_term_，与旧逻辑等价）。
      // 换成同口径的意义：一旦该不变量被打破（将来某处 clear() 漏重建哨兵、跨版本
      // 读出 n=0 的空日志、或日志出现空洞），旧的 `==` 会漏判 —— prev < snapshot_index_
      // 时偏移为负、强转 size_t 成天文数字 → 段错误，且崩在 leader 的读心跳路径上
      // （整个集群的读全挂）。对齐后两处一致：异常时优雅降级为 snapshot_term_，不崩。
      size_t rb_pos = static_cast<size_t>(args.prev_log_index - snapshot_index_);
      args.prev_log_term =
          (rb_pos < logs_.size()) ? logs_[rb_pos].term : snapshot_term_;
      args.entries.clear();
      args.read_ctx = ctx;
    }
    std::shared_ptr<Raft> self = shared_from_this();
    const int server = static_cast<int>(s);
    peers_[s]->CallAsyncTyped<AppendEntriesArgs, AppendEntriesReply>(
        "Raft.AppendEntries", args,
        [self, server, args, ctx](bool ok, const AppendEntriesReply& reply) {
          (void)ctx;
          if (!ok) return;                   // 丢包 / 对方挂了，等下次或超时
          std::lock_guard<std::mutex> lk(self->mu_);
          if (self->killed_.load()) return;
          if (reply.term > self->current_term_) {
            self->ConvertToFollowerLocked(reply.term);
            self->last_heartbeat_ = raftcpp::Now();
            self->tick_cv_.notify_all();
            self->replicator_cv_.notify_all();
            return;
          }
          // 二次确认：迟到的回包作废（term 不符 / 已非 leader）
          if (args.term != self->current_term_ ||
              self->state_ != ServerState::kLeader) {
            return;
          }
          // 记下这次"联系"：刷新 CheckQuorum / Leader Lease 的 ack 时间
          self->last_ack_time_[server] = raftcpp::Now();
          if (server >= static_cast<int>(self->is_member_.size()) ||
              !IsVoter(self->is_member_[server])) {
            return;
          }
          // 按 ctx 把这一票记到对应的读请求上（普通心跳 ctx==0 会直接 return）
          self->RecordReadAckLocked(reply.read_ctx, server);
        },
        &cancel_rpcs_);
  }
}

void Raft::RecordReadAckLocked(int ctx, int server) {
  if (ctx == 0) return;                      // 普通心跳/普通快照，与读确认无关
  if (!active_read_ || active_read_->ctx != ctx) return;  // 无在飞轮/被新轮覆盖/超时，迟到回包忽略
  ReadIndexCtx& r = *active_read_;
  if (r.term != current_term_) return;       // term 变了，作废
  if (server < 0 || server >= static_cast<int>(r.acked_.size())) return;
  if (r.acked_[server]) return;  // 已记过：跳过 O(N) 重算（性能优化；正确性由下方现算保证）
  r.acked_[server] = 1;          // 标记该 server 已回 ack
  // 现算分子：当前 voter 中、回过 ack 的去重集合大小（与 CheckQuorum/commit/选举同构）
  int granted = 0;
  const int n = static_cast<int>(r.acked_.size());
  for (int i = 0; i < n; ++i) {
    if (r.acked_[i] && i < static_cast<int>(is_member_.size()) && IsVoter(is_member_[i])) {
      ++granted;
    }
  }
  int majority = MemberCountLocked() / 2 + 1;
  if (granted >= majority && !r.done) {
    r.done = true;
    // 共用 read_cv_：唤醒后各读线程查自己的 done 标志，互不干扰
    read_cv_.notify_all();
  }
}

// 若本节点是 follower：把读请求转发给当前认知的 leader（Raft.ReadIndex RPC），
// 拿回 leader 经多数派确认的 commit_index_ 作为自己的线性化读点（Follower ReadIndex）。
void Raft::ReadIndexRPC(const ReadIndexArgs& args, ReadIndexReply& reply) {
  reply.read_index = -1;  // 本地回复默认初值，不碰共享状态，无需持锁
  {
    std::lock_guard<std::mutex> lk(mu_);
    reply.term = current_term_;  // current_term_ 是普通 int(非 atomic)，写均在 mu_ 下，须持锁读
    // 防御:follower 转发时带自身 term。若它与本 leader 的 term 不一致(落后未追上,
    // 或它见过更高 term、本节点已非真 leader),直接拒。这样 ReadIndexArgs.term 从死字段
    // 变成有效守卫 —— follower 端会据 r.read_index<0 转 kWrongLeader 让 Clerk 重定向到真 leader。
    if (args.term != current_term_) return;
    if (state_ != ServerState::kLeader) return;
    if (pending_stepdown_ || !IsVoter(is_member_[me_])) return;
  }
  // 已退出临界区，ReadIndex() 自行加锁，不会死锁。超时 / 凑不齐多数派时返回 -1，
  // follower 端会据 r.read_index<0 转成 kWrongLeader 让客户端重定向重试。
  int ri = ReadIndex();
  if (ri >= 0) reply.read_index = ri;
}

// ReadIndex 的做法是每个读请求都重新验一次身份：
// 1.记录当前 commitIndex 作 readIndex；
// 2.给全体 voter 发一次带 ctx 的空心跳(同一在飞读轮内的并发读共享这一轮心跳,见下方合并逻辑)；
// 3.等多数派在当前 term 回 ack —— 拿不到就拒绝这个读（返回 -1）；
// 4.确认自己仍是合法 leader 后，等状态机 apply 到 readIndex 再返回。
int Raft::ReadIndex() {
  int ctx = 0;
  int my_read_index = 0;
  int my_term = 0;
  bool i_am_starter = false;  // 本调用是否开启了新的读心跳轮(合并轮里只有发起者广播)
  std::shared_ptr<ReadIndexCtx> my_round;  // 本调用挂入的轮对象(waiter 自持副本,不随 active_read_ 覆写失效)

  // 第 1 步：锁内判断身份、开租约、给本条读分配唯一 ctx 并登记上下文
  {
    std::unique_lock<std::mutex> lk(mu_);
    if (state_ != ServerState::kLeader) {
      // ---- Follower ReadIndex：转发给 leader 拿线性化读点 ----
      // 读负载因此能平摊到所有节点，不再集中于 leader 单点。安全性与 leader-only
      // ReadIndex 等价：leader 必须先用多数派心跳确认自己仍是合法 leader、read_index
      // 才有效；follower 再用该 read_index 等自己的状态机追平，杜绝读陈旧值。
      int lid = leader_id_;
      if (lid >= 0 && lid != me_) {
        ReadIndexArgs a;
        a.term = current_term_;
        ReadIndexReply r;
        lk.unlock();  // 发 RPC 不能持锁（可能阻塞在不可靠网络 / Kill 取消）
        // cancel_rpcs_ 让 Kill() 时本 RPC 在 50ms 内返回 false，避免线程卡死。
        if (peers_[lid]->CallTyped("Raft.ReadIndex", a, r, &cancel_rpcs_) &&
            r.read_index >= 0 ) {
          // RPC 返回后重新持锁取【最新】current_term_ 做最终裁决:
          // 若 RPC 期间发生了新选举(本节点 term 被抬高),旧 leader 的 r.term 会 < 新 term,
          // 这里判否 → 拒绝该 read_index,转 kWrongLeader 让 Clerk 重定向到新 leader。
          // 同时消除"解锁后无锁读 current_term_"的 data race(UB),并关闭 recency 窗口。
          std::lock_guard<std::mutex> lk2(mu_);
          if (r.term >= current_term_) {
            return r.read_index;
          }
        }
      }
      return -1;
    }

    // 与 Start() 的 pending_stepdown_ 守卫【对称】：事件驱动退位的宽限期内，
    // 本节点虽仍是 leader，但已被移除 / 降级出 voter，不得服务线性一致读 ——
    // 否则 learner 会以 leader 身份返回本地 commit_index_（脏读风险）。
    // 缺这条时：leader 把自己降级为 learner 后，ReadIndex 仍能凑齐多数派 ack
    // （AppendEntries 只校验 term、不校验 sender 是否 voter）而返回 >=0，
    // 正是 TestLearnerReadIndexRejected 偶发失败的根因。
    if (pending_stepdown_ || !IsVoter(is_member_[me_])) return -1;

    // 线性一致读硬性前提：本 leader 必须先提交一条自己 term 的 entry（no-op，
    // 见 become-leader 块），把 commit_index_ 抬到覆盖上任前已提交的 entry；
    // 否则 commit_index_ 是旧值（甚至 0），据此读会拿到 stale 状态
    // （分区用例偶发 wrong appends / 空读）。不满足就拒绝，让上层 Clerk 换台/重试，
    // 等 no-op 提交后 commit_index_ 自然越过 read_safe_commit_。
    if (commit_index_ < read_safe_commit_) return -1;

    // 快路径：LeaseRead（默认 kEnableLeaseRead=false，不走到这）
    if (kEnableLeaseRead && raftcpp::Now() < lease_expire_) {
      return commit_index_;
    }

    // 读合并:优先挂入"当前在飞"的读轮(未完成、同 term、仍是 leader),复用它的
    // read_index 与 ctx,无需再广播一轮心跳;否则开新轮(只广播一次心跳)。
    // !done 是安全性承重件:只允许"确认尚未完成"的在飞轮被挂入,保证每个挂入者的
    // 调用时刻都早于多数派 ack 完成时刻 → 领导权验证对它不过期。done 轮若再放人进,
    // 挂入者会复用一次发生在自己调用【之前】的 quorum 确认——分区+新主已 ACK 写后
    // 即陈旧读(线性一致性违例),见 patches/raft_readindex_merge_staleread_fix.patch 的教训。
    if (active_read_ && !active_read_->done &&
        active_read_->term == current_term_ && state_ == ServerState::kLeader) {
      my_round = active_read_;              // waiter 自持副本:之后 active_read_ 被新轮
      ctx = my_round->ctx;                  // 覆写也不影响本调用观察自己这一轮的 done
      my_term = my_round->term;
      my_read_index = commit_index_;  // 必须更新该轮开启时记录的 read_index，不然线性一致报错（挂入者已在 quorum 确认完成前加入,见上行 !done 守卫,安全）
      my_round->waiters++;                   // 挂入在飞读轮:per-ctx 计数 +1
    } else {
      // 开新轮:分配唯一 ctx、记 read_index、广播一次心跳(锁外执行,见下)。
      my_term = current_term_;
      my_read_index = commit_index_;
      ctx = next_read_ctx_++;
      my_round = std::make_shared<ReadIndexCtx>();
      my_round->term = my_term;
      my_round->read_index = my_read_index;
      my_round->acked_.assign(peers_.size(), 0);  // 去重表，大小对齐 peers_
      my_round->acked_[me_] = 1;            // leader 自己这一票（voter 默认已 ack 自己）
      my_round->done = false;
      my_round->waiters = 1;                // 开新轮:本调用是第一个挂入者(per-ctx 计数)
      my_round->ctx = ctx;                  // 自身身份号写入结构体
      active_read_ = my_round;              // 登记为当前在飞轮(旧轮对象由 waiter 副本续命)
      i_am_starter = true;
    }
  }

  // 第 2 步：仅当本调用是本轮发起者时,锁外发一轮带 ctx 的空心跳。
  // 合并轮里的其他挂入者不再重复广播,把读心跳数从 O(并发读数) 降到 O(心跳轮数)。
  if (i_am_starter) {
    BroadcastReadHeartbeat(ctx);
  }

  // 第 3 步：等多数派确认（锁外等待，绝不持锁阻塞）
  const auto deadline =
      raftcpp::Now() + std::chrono::milliseconds(kElectionTimeoutMin);
  {
    std::unique_lock<std::mutex> lk(mu_);
    int result = -1;
    while (!killed_.load()) {
      bool done = my_round->done;   // 查自己的轮对象:即使 active_read_ 已换新轮也不搁浅
      if (done) { result = my_read_index; break; }   // 本条读的 ctx 已攒够
      if (my_term != current_term_) break;           // term 变了 → 作废重试
      if (state_ != ServerState::kLeader) break;     // 已不是 leader
      if (pending_stepdown_ || !IsVoter(is_member_[me_])) break;
      auto remain = deadline - raftcpp::Now();
      if (remain <= std::chrono::milliseconds(0)) break;  // 超时
      read_cv_.wait_for(lk, remain);
    }
    if (result >= 0) {                               // 醒来再确认一遍
      if (!my_round->done || my_round->term != current_term_ ||
          state_ != ServerState::kLeader ||
          pending_stepdown_ || !IsVoter(is_member_[me_])) {
        result = -1;
      }
    }
    // per-ctx 引用计数清理(shared_ptr 版):waiter 持自己的轮对象副本,计数永远记在
    // 自己轮上,不存在"被新轮覆写后跳过清理"的问题。末位离开者把 active_read_ 撤下
    // (若它仍指向本轮):done 轮撤下好让位给新轮;超时轮撤下尤其关键——否则重试读者
    // 会不断挤进同一个凑不齐多数派的死轮同步超时(羊群效应,不可靠网下表现为成功数
    // 崩塌),复位后各自开新轮错峰重试。
    if (--my_round->waiters <= 0 && active_read_.get() == my_round.get()) {
      active_read_.reset();  // 撤下无人挂靠的轮;对象由残余 waiter 副本/此处释放
    }
    return result;           // 调用方拿到 readIndex 后等 apply 到它再读
  }
}

}  // namespace raft


namespace raft {

// §5.2 单条 AE 字节上限观测接口实现
long long Raft::AEByteTruncatedCount() const {
  return ae_byte_truncated_count_.load(std::memory_order_relaxed);
}
long long Raft::MaxAppendEntriesBytes() const {
  return max_ae_payload_bytes_.load(std::memory_order_relaxed);
}

}  // namespace raft
