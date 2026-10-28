// log_store.h —— Raft 日志持久化后端的抽象契约（LogStorage 接口 + 工厂）
// ---------------------------------------------------------------------------
// 依赖反转：Raft 核心只依赖抽象基类 LogStorage，不依赖具体后端。
//   - SegmentLogStorage：file_backed 模式，把日志镜像到 append-only 段文件 raft.log.seg
//     （实现见 log/segment_log_store.{h,cpp}）
//   - MemoryLogStorage ：内存模式（等价于原 seg_log_ == nullptr），null-object 风格，
//                        不持久化、不分配后端镜像；Raft 权威日志仍在 raft.cpp 的 logs_。
//     （实现见 log/memory_log_store.h）
//   - CreateLogStorage ：工厂，按 LogStorageKind 选后端，返回 unique_ptr<LogStorage>。
//
// 未来新增第三种后端（远程对象存储 / LSM 等）：在本文件加枚举值，在 log/ 下加一个
// 目录放实现（类继承 LogStorage），工厂加一个 case —— raft.cpp 无需任何改动。
// ===========================================================================

#pragma once

#include <sys/types.h>  // off_t

#include <memory>
#include <string>
#include <vector>

#include "../raft/raft.h"  // LogEntry / Command / EntryType / labrpc::Encoder,Decoder

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
  virtual void TruncateSuffix(int64_t index) = 0;

  // 丢弃 <= index 的条目（快照前缀截断）。
  virtual void TruncatePrefix(int64_t index) = 0;

  // 只丢「内存镜像」里的前缀条目，不重写文件、不 fsync（默认 no-op）。
  //
  // 为什么需要它：快照压缩 / 安装快照时，Raft 会先推进 logs_ 与 snapshot_index_，
  // 而 state blob 的落盘（PersistLocked）在 file 模式下要 【释放 mu_ 等 flusher】。
  // 若段镜像要等落盘之后才截断，这段窗口里并发进来的 AppendEntries 就会看到
  // 「logs_ 已跳过快照点、段镜像还停在旧位置」，于是对段镜像追加一个跨号的 index
  // （典型：snapshot_index_=56、段镜像 back=55、AE 带 57）→ 连续性断言 abort。
  // 因此把「内存截断」提前到释放锁之前；「重写 + fsync」仍留在 state 落盘之后，
  // 以保持磁盘顺序 blob → state → 段文件（重启恢复依赖该顺序）。
  virtual void DropPrefix(int64_t index) { (void)index; }

  // 重放：哨兵(snap_term, snap_idx) + 全部已存条目 → 还原 logs_。
  virtual std::vector<LogEntry> Replay(int64_t snap_term, int64_t snap_idx) const = 0;

  // 能力查询：本后端是否自己持久化日志（无需 Raft 把日志内联进 state blob）。
  //   - SegmentLogStorage（段文件）：true —— 日志独立落盘，blob 只记条数 0。
  //   - MemoryLogStorage（内存）：false —— 日志必须随 state blob 一起落盘才能重启恢复。
  // raft.cpp 的 EncodeRaftStateLocked / ReadPersist / ResetFromLogs 据此分流，
  // 不再依赖 persister 的 file_backed_ 布尔，加第三种后端时此处逻辑零改动。
  virtual bool KeepsLogExternally() const = 0;

  // Group Commit：合并 fsync。force=true 忽略去抖窗口（commit 推进点用）。
  virtual void Sync(bool force) = 0;

  virtual int64_t LastIndex() const = 0;
  virtual size_t Size() const = 0;
  virtual size_t FileSize() const = 0;

  // 重启对账：以权威内存日志（logs，含哨兵）重建镜像。
  virtual void ResetFromLogs(const std::vector<LogEntry>& logs) = 0;
};

// 工厂：按 LogStorageKind 选择后端，返回抽象接口指针。
// raft.cpp 调用：log_store_ = CreateLogStorage(persister_->FileBacked()
//                     ? LogStorageKind::kFile : LogStorageKind::kMemory, persister_->Dir());
// 实现见 log/segment_log_store.cpp（依赖两个具体后端的完整定义，故不在此内联）。
std::unique_ptr<LogStorage> CreateLogStorage(LogStorageKind kind, const std::string& dir);

}  // namespace raft
