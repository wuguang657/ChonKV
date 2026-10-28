// segment_log_store.h —— 段文件日志后端（SegmentLogStorage，LogStorage 的 kFile 实现）
// ---------------------------------------------------------------------------
// 抽象接口见上一级 log_store.h；内存后端见同目录 memory_log_store.h。
//
// 段文件格式/崩溃恢复/Group Commit 细节见 segment_log_store.cpp。核心思路：
//   [uint32 len][uint32 crc][entry-bytes]，crc 覆盖 entry-bytes。
//   - 尾部半截写（掉电）：回放 CRC 不符即停，安全丢弃。
//   - 中段损坏（bit rot）：索引不连续 → fail-stop（abort），与项目铁律一致。
// Group Commit：Append 只 pwrite()（进 OS 页缓存，不 fsync）；真正 fsync 由 Sync() 合并。
// ===========================================================================

#pragma once

#include <sys/types.h>  // off_t

#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "../log_store.h"  // LogStorage 抽象接口 + LogEntry

namespace raft {

// SegmentLogStorage：file_backed 模式，日志镜像到 append-only 段文件 raft.log.seg。
// 完整实现见 segment_log_store.cpp。
class SegmentLogStorage : public LogStorage {
 public:
  ~SegmentLogStorage() override;

  static std::unique_ptr<SegmentLogStorage> Open(const std::string& dir);

  void Append(const LogEntry& e) override;
  void TruncateSuffix(int64_t index) override;
  void TruncatePrefix(int64_t index) override;
  void DropPrefix(int64_t index) override;
  std::vector<LogEntry> Replay(int64_t snap_term, int64_t snap_idx) const override;
  void Sync(bool force) override;
  int64_t LastIndex() const override { return entries_.empty() ? 0 : entries_.back().index; }
  size_t Size() const override { return entries_.size(); }
  size_t FileSize() const override { return file_size_; }
  bool KeepsLogExternally() const override { return true; }
  void ResetFromLogs(const std::vector<LogEntry>& logs) override;

 private:
  SegmentLogStorage();

  static void PutU32(uint8_t* p, uint32_t v);
  static uint32_t GetU32(const uint8_t* p);
  static std::string EncodeEntry(const LogEntry& e);
  static LogEntry DecodeEntry(const std::string& b);

  void WriteRecord(const LogEntry& e);
  void RewriteFile();
  void Rebuild();
  off_t FileSizeAtOpen() const;
  static int64_t NowNs();

  // Group Commit 专职刷盘线程：把分散的 fsync 合并到后台，按窗口批量落盘。
  //   · Sync(true)（提交点）：登记 epoch 并阻塞等待 flusher 把数据落盘后才返回
  //     → 调用方在 flusher 落盘完成前绝不对外 ack，durability 语义与「原地 fsync」等价。
  //   · Sync(false)（普通 append）：仅标记 pending 并唤醒 flusher，2ms 去抖窗口内合并，
  //     不阻塞调用方（fire-and-forget）。
  // flusher 在锁外（不持 raft mu_）做 ::fsync，心跳 / AE 回包 / 提案线程都不会被它堵住。
  void FlushLoop();

  int fd_ = -1;
  std::vector<LogEntry> entries_;  // 内存镜像（与 logs_ 对齐，不含哨兵）
  // 每条记录写完后在段文件中的【结束偏移】：offsets_[i] == entries_[i] 结束处。
  // 有了它，截断尾部 = 一次 ftruncate(offsets_[keep-1])，O(1)，不必重写整个文件
  // （与 braft Segment::truncate 的 _offset_and_term 表同构，见 log.cpp:608-637）。
  // 不变式：offsets_.size() == entries_.size()。
  std::vector<off_t> offsets_;
  bool dirty_ = false;             // 是否写过（析构前据此做一次兜底 fsync）
  size_t file_size_ = 0;
  static constexpr int64_t kGroupCommitNs = 2000000;  // 2ms 去抖窗口（普通 append）
  static constexpr int64_t kFlushUrgentNs = 200000;   // 200µs（提交点短窗口，尽快落盘）

  std::mutex flush_mu_;
  std::condition_variable flush_wake_cv_;  // 唤醒 flusher 干活
  std::condition_variable flush_done_cv_;  // 等待 fsync 完成的调用方在此等
  uint64_t flush_request_epoch_ = 0;      // 每次 force=true 自增，用于等待者配对
  uint64_t flush_done_epoch_ = 0;         // flusher 已完成到的最高 epoch
  bool flush_pending_ = false;
  bool flush_urgent_ = false;  // 有 force=true 在等 → 缩短去抖窗口
  bool flush_stop_ = false;
  std::thread flush_thread_;
};

}  // namespace raft
