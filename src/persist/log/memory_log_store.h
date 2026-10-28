// memory_log_store.h —— 内存日志后端（MemoryLogStorage，LogStorage 的 kMemory 实现）
// ---------------------------------------------------------------------------
// null-object 风格：所有操作均为空实现，不持久化、不分配后端镜像。
// Raft 权威日志仍在 raft.cpp 的 logs_ 向量，本类仅用于让"内存模式"也走统一接口，
// 避免 raft.cpp 里出现 nullptr 判空与具体类名。
// 若要让内存真正成为主存储（替代 logs_），需把 raft.cpp 的 logs_ 收进此类，
// 并为 LogStorage 接口增加单条读取方法（Get/GetTerm），属更大重构。
//
// 纯头文件：全方法内联，无对应 .cpp。
// ===========================================================================

#pragma once

#include <sys/types.h>  // off_t

#include <cstddef>
#include <cstdint>
#include <vector>

#include "../log_store.h"  // LogStorage 抽象接口 + LogEntry

namespace raft {

class MemoryLogStorage : public LogStorage {
 public:
  ~MemoryLogStorage() override = default;

  void Append(const LogEntry&) override {}
  void TruncateSuffix(int64_t) override {}
  void TruncatePrefix(int64_t) override {}
  std::vector<LogEntry> Replay(int64_t, int64_t) const override { return {}; }
  void Sync(bool) override {}
  int64_t LastIndex() const override { return 0; }
  size_t Size() const override { return 0; }
  size_t FileSize() const override { return 0; }
  bool KeepsLogExternally() const override { return false; }
  void ResetFromLogs(const std::vector<LogEntry>&) override {}
};

}  // namespace raft
