// raft_protocol.h —— Raft 的线上协议数据结构（RPC 消息 / 日志条目 / apply 消息）
//
// 从 raft.h 拆出（代码重组 L1）。这里只放「线上格式」：
//   · 各 RPC struct 的 Serialize()/Deserialize() 是网络字节契约（实现在 raft.cpp），
//     任何一端加字段都必须两端同步改，否则解码错位——测试脚手架的 hand-rolled blob
//     与生产编解码是同一契约，改编码必须同轮换（编译期抓不住语义漂移）。
//   · LogEntry / ApplyMsg 是日志与状态机边界的载荷格式。
//   · MemberRole 三态 + IsRemovedFrozen 判据是成员变更的配置格式。
// 算法常量 / ServerState / Raft 类本身仍在 raft.h；CRC32 / blob 编解码 / Trace 在
// raft_util.h。
//
// 本文件【零项目内依赖】（只用标准库），可被任意模块安全 include。

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace raft {

// 日志里存的命令。Go 版是 interface{}，C++ 里我们用字符串，
// 到了 Lab 3（KV 服务）你可以把它塞成任意序列化的字节。
using Command = std::string;

// ---------------------------------------------------------------------------
// 成员角色（成员变更 / Learner 三态）
// ---------------------------------------------------------------------------
// 论文里"C_old 与 C_new 的多数派必有交集"的论证，落地点就是这个角色数组。
//   kRemoved : 不在当前配置 C 里（已被移除 / 尚未加入）→ 不收 RPC、不投票、不计入多数派
//   kLearner : 在 C 里但【只追数据不投票】（扩容零可用性损失，见文档 §2.6）
//   kVoter   : 正式成员，参与一切（选举计票 / 日志提交 / CheckQuorum / ReadIndex）
enum class MemberRole {
  kRemoved = 0,
  kLearner = 1,
  kVoter = 2,
};
// 只有 kVoter 计入多数派 / 能投票 / 能当选。
inline bool IsVoter(MemberRole r) { return r == MemberRole::kVoter; }
// Q2 隐私加固的"冻结判据"：leader 是否该停止向 s 复制【新数据】。
// 两个信息源必须取"或"——缺任何一个都会漏出一条真实路径：
//   (a) removed_at_index_[s] >= 0：提案时 / apply 时写入的"冻结点"下标。
//       覆盖【提案 → apply】这段窗口（此间 is_member_[s] 仍是 kVoter）。
//   (b) is_member_[s] == kRemoved：配置视图。
//       覆盖【经 InstallSnapshot 恢复配置】的场景：快照把 conf 条目截断了，
//       新 leader 上任扫描 logs_ 扫不到那条 conf，removed_at_index_ 重建不出来，
//       若只看 (a) 就会继续给 removed 节点发新数据（隐私泄漏）。
// 只用 (b) 是旧 bug（㉓ 偶发 40%）：提案到 apply 之间 cap 形同虚设。
inline bool IsRemovedFrozen(const std::vector<MemberRole>& is_member,
                            const std::vector<int64_t>& removed_at_index, int s) {
  if (s < 0) return false;
  if (s < static_cast<int>(removed_at_index.size()) &&
      removed_at_index[s] >= 0) {
    return true;
  }
  if (s < static_cast<int>(is_member.size()) &&
      is_member[s] == MemberRole::kRemoved) {
    return true;
  }
  return false;
}

enum class EntryType {
  kNormal,     // 普通客户端命令
  kConfChange, // 成员变更条目
  kNoOp        // leader 当选时为建立读安全点补的空日志
};

struct LogEntry {
  int64_t term = 0;
  int64_t index = 0;
  Command command;
  EntryType type = EntryType::kNormal;  // E7：统一 entry 类型，替代 is_conf 特判
  // ---- 成员变更配置条目 ----
  // 普通命令 type=kNormal；成员变更条目 type=kConfChange，并携带目标节点与加/删。
  // 节点必须【apply 到这条 entry 时】才切换本地 is_member_（不是收到就切），
  // 且同一时刻只允许一个 conf 变更在飞行（ProposeConfChange 里的 pending_conf_index_ 保证）。
  int conf_server = -1;  // 目标节点编号
  // 目标角色的整型值（MemberRole：0=kRemoved, 1=kLearner, 2=kVoter）。
  // 原来是 bool conf_add，只能表达"加 / 删"两态，塞不下 learner 这个第三态，
  // 所以升级成 int，直接存 MemberRole 的值。
  int conf_role = 0;
};

// 提交给上层状态机的消息
struct ApplyMsg {
  bool command_valid = false;
  Command command;
  int64_t command_index = 0;
  // E7-hardening：携带本 entry 的类型，让上层（KV）显式分流，
  // 不必再用「空串=no-op / 反序列化失败=配置」这类隐式 content heuristic。
  EntryType entry_type = EntryType::kNormal;

  // Lab 3（快照）才会用到，这里先留着
  bool snapshot_valid = false;
  std::string snapshot;
  int64_t snapshot_term = 0;
  int64_t snapshot_index = 0;
};

// ---- RPC 消息 ----
// 每个结构都有 Serialize()/Deserialize()，网络层靠它们把对象变成字节流。
// 顺序必须严格一致：编码写进去几个字段、什么顺序，解码就怎么读出来。

struct RequestVoteArgs {
  int64_t term = 0;
  int candidate_id = 0;
  int64_t last_log_index = 0;
  int64_t last_log_term = 0;

  std::string Serialize() const;
  bool Deserialize(const std::string& s);
};

struct RequestVoteReply {
  int64_t term = 0;
  bool vote_granted = false;

  std::string Serialize() const;
  bool Deserialize(const std::string& s);
};

// ---- 预投票 RPC（lab 增强：治 term 暴涨）----
struct RequestPreVoteArgs {
  int64_t term = 0;
  int candidate_id = 0;
  int64_t last_log_index = 0;
  int64_t last_log_term = 0;

  std::string Serialize() const;
  bool Deserialize(const std::string& s);
};

struct RequestPreVoteReply {
  int64_t term = 0;
  bool vote_granted = false;

  std::string Serialize() const;
  bool Deserialize(const std::string& s);
};

struct AppendEntriesArgs {
  int64_t term = 0;
  int leader_id = 0;
  int64_t prev_log_index = 0;
  int64_t prev_log_term = 0;
  std::vector<LogEntry> entries;   // 空 = 纯心跳
  int64_t leader_commit = 0;
  uint64_t read_ctx = 0;

  std::string Serialize() const;
  bool Deserialize(const std::string& s);
};

struct AppendEntriesReply {
  int64_t term = 0;
  bool success = false;
  // 快速回退优化：失败时告诉 leader "你下次从这个下标开始试"
  // 没有这个优化，TestBackup2B 会跑很久（leader 一次只退一格）
  int64_t next_index = 0;
  uint64_t read_ctx = 0;

  std::string Serialize() const;
  bool Deserialize(const std::string& s);
};

// Follower ReadIndex：follower 向 leader 索要一个「经多数派确认」的线性化读点。
// leader 在自己的 Raft 层做"多数派心跳确认"后，把当前 commit_index_ 回给 follower，
// follower 等自己的状态机 apply 到该点再本地读 —— 从而把线性一致读从 leader 单点
// 平摊到所有节点，解除 leader 读瓶颈（不依赖 lease，安全性与 leader-only ReadIndex 等价）。
// 对应 raft.cpp 的 Raft::ReadIndex()（follower 分支）与 Raft::ReadIndexRPC()（leader 端）。
struct ReadIndexArgs {
  int64_t term = 0;  // follower 的 currentTerm，leader 用来判断自己是否过时

  std::string Serialize() const;
  bool Deserialize(const std::string& s);
};

struct ReadIndexReply {
  int64_t term = 0;          // leader 的 currentTerm（follower 据此校验 leader 新鲜度）
  int64_t read_index = -1;   // leader 确认身份后的 commit_index_；<0 表示被拒

  std::string Serialize() const;
  bool Deserialize(const std::string& s);
};

// 快照 RPC（Lab 3 日志压缩）：leader 给落后太多的 follower 直接发整块快照，
// 跳过漫长的日志重放。data 是上层状态机的序列化 blob，Raft 不解析。
struct InstallSnapshotArgs {
  int64_t term = 0;               // leader 的 currentTerm
  int leader_id = 0;              // 便于 follower 重定向客户端
  int64_t last_included_index = 0;  // 快照覆盖的最后一条日志下标的逻辑 index
  int64_t last_included_term = 0;   // 该下标的 term
  int64_t offset = 0;                 // 本 chunk 在完整 blob 中的字节偏移
  int chunk_size = 0;             // 本 chunk 字节数
  bool done = false;              // 是否为最后一块
  int crc = 0;                    // 完整 blob 的 CRC32（截断成 int，比较用）
  int epoch = 0;                  // 本次分块传输的代号：每次 leader 起一帧新传输就 +1，
                                  // 跟随同一帧的所有 chunk 共享同一 epoch。follower 只接受
                                   // 当前期望 epoch 的片，旧 epoch 残片（乱序迟到）一律忽略，
                                   // 避免「上一帧没装成的残片」冲掉「当前在途新快照」的重组缓冲。
  std::string data;               // 本 chunk 的状态机 blob 切片
  // 成员配置随快照一起传：落后太多的 follower 装快照时本地 logs_ 可能已不含
  // 覆盖区间内的 conf 条目（被截断），必须按 leader 在快照点处的角色数组重建。
  std::vector<int> members;       // 各节点角色（MemberRole 的整型值）

  std::string Serialize() const;
  bool Deserialize(const std::string& s);
};

struct InstallSnapshotReply {
  int64_t term = 0;  // follower 当前 term，leader 据以退位
  bool ok_install = false;  // 仅当「本 chunk 是 done 且 follower 真正安装成功」时为 true；
                            // leader 据此推进 match_index_。中间 chunk / 校验失败均为 false。
  std::string Serialize() const;
  bool Deserialize(const std::string& s);
};

struct ReadIndexCtx {
  uint64_t ctx = 0;         // 自身身份号(0=无在飞读轮),复用作"无"哨兵,与旧 active_read_ctx_ 语义一致
  int64_t term = 0;        // 发起读时的 currentTerm，term 一变该 ctx 作废
  int64_t read_index = 0;  // 发起读那一刻的 commitIndex（本次读至少要看到它）
  std::vector<char> acked_;  // 按 server 下标去重：记录哪些节点已回过 ack（leader 自己那一票在 ReadIndex() 里置位）
  bool done = false;   // 是否已攒够多数派
  int waiters = 0;     // 【读合并】挂在该 ctx 上的并发读者计数(per-ctx 引用计数,最后一个离开者清理)
};

// Start() 的返回值。Go 版返回 (index, term, isLeader) 三元组，
// C++ 里没有多返回值，包成结构体更清楚。
struct StartResult {
  bool is_leader = false;
  int64_t index = -1;
  int64_t term = 0;
  // C1 背压标记：本节点【确实是 leader】，但因未提交 entry 已达上限而拒绝提案。
  // 与 is_leader=false 区分开 —— 后者可能是"我不是 leader"，本标记专指"我是
  // leader 但被限流了"，调用方可据此选择"稍后重试"而不是"换 leader 重试"。
  bool backpressure = false;
  // #2.2 磁盘水位保护：本节点【是 leader】，但 raft state 落盘体积已超阈值，
  // 拒绝本次新写入，逼上层去调 Snapshot() 压缩。与 backpressure 同构但语义不同：
  // backpressure=未提交过多（内存/复制压力）；disk_full=raft state 太大（磁盘水位）。
  // 调用方看到 disk_full 应触发快照后重试，而非换节点。
  bool disk_full = false;
};

}  // namespace raft
