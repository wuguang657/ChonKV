// segment_log_store.h —— SegmentLogStore：append-only 段文件日志存储（file_backed 模式专用）
// ---------------------------------------------------------------------------
// 解决「整文件重写」问题：原 PersistLocked 每次把全部 logs_ 序列化进一个 state
// blob 再整体重写 + fsync（O(日志长度)）。SegmentLogStore 把日志独立成「只追加」
// 的段文件 raft.log.seg，新条目永远只写在尾部，绝不重写历史；冲突/快照截断用
// ftruncate 。
//
// 记录格式：[uint32 len][uint32 crc][entry-bytes]，crc 覆盖 entry-bytes。
//   - 尾部半截写（掉电）：回放时 CRC 不符即停在最后一条好记录，安全丢弃。
//   - 中段损坏（bit rot）：索引不连续 → fail-stop（abort），与项目铁律一致。
//
// Group Commit：Append 只 pwrite()（数据进 OS 页缓存，不 fsync）；真正 fsync 由
// Sync() 合并——debounce 窗口内多次 append 共用一次 fsync；而 commit 推进点强制
// Sync(true) 保证「已提交条目在对外成功前已落盘」。
// ===========================================================================

#pragma once

#include <sys/types.h>  // off_t

#include <memory>
#include <string>
#include <vector>

#include "raft.h"  // LogEntry / Command / EntryType / labrpc::Encoder,Decoder

namespace raft {

// SegmentLogStore：file_backed 模式下把 Raft 日志镜像到 append-only 段文件
// raft.log.seg。内存模式（file_backed=false）下 Raft 不创建它（seg_log_ == nullptr），
// 读路径全走 logs_ 向量。完整实现见 segment_log_store.cpp。
class SegmentLogStore {
 public:
  ~SegmentLogStore();

  // 打开/创建 dir/raft.log.seg，回放已有记录重建内存索引。
  static std::unique_ptr<SegmentLogStore> Open(const std::string& dir);

  // 追加一条（假设 index 严格连续递增：e.index == last+1）。
  void Append(const LogEntry& e);

  // 保留 <= index 的条目，丢弃其后（冲突覆盖）。
  void TruncateSuffix(int index);

  // 丢弃 <= index 的条目（快照前缀截断）。
  void TruncatePrefix(int index);

  // 重放：哨兵(snap_term, snap_idx) + 全部已存条目 → 还原 logs_（不含哨兵以外的历史）。
  std::vector<LogEntry> Replay(int snap_term, int snap_idx) const;

  // Group Commit：合并 fsync。force=true 忽略去抖窗口（commit 推进点用）。
  void Sync(bool force);

  int LastIndex() const { return entries_.empty() ? 0 : entries_.back().index; }
  size_t Size() const { return entries_.size(); }
  size_t FileSize() const { return file_size_; }

  // 重启对账：以权威内存日志（logs，含哨兵）重建段文件镜像 entries_，
  // 保证「entries_ == logs 去掉哨兵」这一不变式在重启后恒成立。
  // 消除两类 entries_/logs_ 漂移（详见 segment_log_store.cpp 内注释）：
  //   1) InstallSnapshot 的 pos 分支会【保留】边界条目 entry@last_included_index
  //      进 logs_（与非段模式状态机结构一致），镜像端同步保留即可；
  //   2) 崩溃落在「写 blob 之后、提交 state 之前」的窗口，ReadPersist 的 blob
  //      追认会改掉 logs_ 的哨兵/截断点，段文件是另一路独立落盘，需在此追平。
  // 调用方：Raft::ReadPersist 末尾（此时 logs 已是最终权威结构）。
  void ResetFromLogs(const std::vector<LogEntry>& logs);

 private:
  SegmentLogStore();

  static void PutU32(uint8_t* p, uint32_t v);
  static uint32_t GetU32(const uint8_t* p);
  static std::string EncodeEntry(const LogEntry& e);
  static LogEntry DecodeEntry(const std::string& b);

  void WriteRecord(const LogEntry& e);
  void RewriteFile();
  void Rebuild();
  off_t FileSizeAtOpen() const;
  static int64_t NowNs();

  int fd_ = -1;
  std::vector<LogEntry> entries_;  // 内存镜像（与 logs_ 对齐，不含哨兵）
  int64_t last_sync_ns_ = 0;
  bool dirty_ = false;
  size_t unsynced_ = 0;
  size_t file_size_ = 0;
  static constexpr int64_t kGroupCommitNs = 2000000;  // 2ms 去抖窗口
  static constexpr size_t kMaxUnsynced = 1u << 20;     // 累积 1MB 强制刷
};

}  // namespace raft
