// segment_log_store.h —— 日志存储抽象（LogStorage 接口 + 两后端）
// ---------------------------------------------------------------------------
// 依赖反转：Raft 核心只依赖抽象基类 LogStorage，不依赖具体后端。
//   - SegmentLogStorage：file_backed 模式，把日志镜像到 append-only 段文件 raft.log.seg
//   - MemoryLogStorage ：内存模式（等价于原 seg_log_ == nullptr），null-object 风格，
//                        不持久化、不分配后端镜像；Raft 权威日志仍在 raft.cpp 的 logs_。
//   - CreateLogStorage ：工厂，按 LogStorageKind 选后端，返回 unique_ptr<LogStorage>。
//
// 段文件格式/崩溃恢复/Group Commit 细节见 segment_log_store.cpp。核心思路：
//   [uint32 len][uint32 crc][entry-bytes]，crc 覆盖 entry-bytes。
//   - 尾部半截写（掉电）：回放 CRC 不符即停，安全丢弃。
//   - 中段损坏（bit rot）：索引不连续 → fail-stop（abort），与项目铁律一致。
// Group Commit：Append 只 pwrite()（进 OS 页缓存，不 fsync）；真正 fsync 由 Sync() 合并。
// ===========================================================================

#pragma once

#include <sys/types.h>  // off_t

#include <memory>
#include <string>
#include <vector>

#include "raft.h"  // LogEntry / Command / EntryType / labrpc::Encoder,Decoder

namespace raft {

// 日志后端种类：驱动工厂选哪个具体实现。预留 kRemote 给第三种存储
// （如远程对象存储 / LSM），新增后端时只需在此加枚举值 + 工厂加 case
// + 新类正确实现 KeepsLogExternally()，raft.cpp 无需任何改动。
enum class LogStorageKind { kMemory, kFile, kRemote };

// LogStorage：Raft 日志持久化后端的抽象契约。Raft 核心只认这个接口，
// 不认具体类。新增后端（如 LSM 版）只需继承并实现这 9 个方法。
class LogStorage {
 public:
  virtual ~LogStorage() = default;

  // 追加一条（假设 index 严格连续递增：e.index == last+1）。
  virtual void Append(const LogEntry& e) = 0;

  // 保留 <= index 的条目，丢弃其后（冲突覆盖）。
  virtual void TruncateSuffix(int index) = 0;

  // 丢弃 <= index 的条目（快照前缀截断）。
  virtual void TruncatePrefix(int index) = 0;

  // 重放：哨兵(snap_term, snap_idx) + 全部已存条目 → 还原 logs_。
  virtual std::vector<LogEntry> Replay(int snap_term, int snap_idx) const = 0;

  // 能力查询：本后端是否自己持久化日志（无需 Raft 把日志内联进 state blob）。
  //   - SegmentLogStorage（段文件）：true —— 日志独立落盘，blob 只记条数 0。
  //   - MemoryLogStorage（内存）：false —— 日志必须随 state blob 一起落盘才能重启恢复。
  // raft.cpp 的 EncodeRaftStateLocked / ReadPersist / ResetFromLogs 据此分流，
  // 不再依赖 persister 的 file_backed_ 布尔，加第三种后端时此处逻辑零改动。
  virtual bool KeepsLogExternally() const = 0;

  // Group Commit：合并 fsync。force=true 忽略去抖窗口（commit 推进点用）。
  virtual void Sync(bool force) = 0;

  virtual int LastIndex() const = 0;
  virtual size_t Size() const = 0;
  virtual size_t FileSize() const = 0;

  // 重启对账：以权威内存日志（logs，含哨兵）重建镜像。
  virtual void ResetFromLogs(const std::vector<LogEntry>& logs) = 0;
};

// SegmentLogStorage：file_backed 模式，日志镜像到 append-only 段文件 raft.log.seg。
// 完整实现见 segment_log_store.cpp。
class SegmentLogStorage : public LogStorage {
 public:
  ~SegmentLogStorage() override;

  static std::unique_ptr<SegmentLogStorage> Open(const std::string& dir);

  void Append(const LogEntry& e) override;
  void TruncateSuffix(int index) override;
  void TruncatePrefix(int index) override;
  std::vector<LogEntry> Replay(int snap_term, int snap_idx) const override;
  void Sync(bool force) override;
  int LastIndex() const override { return entries_.empty() ? 0 : entries_.back().index; }
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

  int fd_ = -1;
  std::vector<LogEntry> entries_;  // 内存镜像（与 logs_ 对齐，不含哨兵）
  int64_t last_sync_ns_ = 0;
  bool dirty_ = false;
  size_t unsynced_ = 0;
  size_t file_size_ = 0;
  static constexpr int64_t kGroupCommitNs = 2000000;  // 2ms 去抖窗口
  static constexpr size_t kMaxUnsynced = 1u << 20;     // 累积 1MB 强制刷
};

// MemoryLogStorage：内存模式后端（等价于原 seg_log_ == nullptr）。
// null-object 风格：所有操作均为空实现，不持久化、不分配后端镜像。
// Raft 权威日志仍在 raft.cpp 的 logs_ 向量，本类仅用于让"内存模式"也走统一接口，
// 避免 raft.cpp 里出现 nullptr 判空与具体类名。
// 若要让内存真正成为主存储（替代 logs_），需把 raft.cpp 的 logs_ 收进此类，
// 并为 LogStorage 接口增加单条读取方法（Get/GetTerm），属更大重构。
class MemoryLogStorage : public LogStorage {
 public:
  ~MemoryLogStorage() override = default;

  void Append(const LogEntry&) override {}
  void TruncateSuffix(int) override {}
  void TruncatePrefix(int) override {}
  std::vector<LogEntry> Replay(int, int) const override { return {}; }
  void Sync(bool) override {}
  int LastIndex() const override { return 0; }
  size_t Size() const override { return 0; }
  size_t FileSize() const override { return 0; }
  bool KeepsLogExternally() const override { return false; }
  void ResetFromLogs(const std::vector<LogEntry>&) override {}
};

// 工厂：按 LogStorageKind 选择后端，返回抽象接口指针。
// raft.cpp 调用：log_store_ = CreateLogStorage(persister_->FileBacked()
//                     ? LogStorageKind::kFile : LogStorageKind::kMemory, persister_->Dir());
std::unique_ptr<LogStorage> CreateLogStorage(LogStorageKind kind, const std::string& dir);

}  // namespace raft
