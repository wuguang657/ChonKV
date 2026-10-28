// persister_factory.h —— RaftState 持久化后端的"工厂"（对标 transport_factory.h 的 MakeTransport）
//
// 与 persister.h 同属 persist/state/。
//
// 用法：config 里用 persist::MakePersister()（无参）即代表"按环境变量选后端"：
//   * 默认（LABRPC_PERSIST 未设）或 LABRPC_PERSIST=memory → 内存模式 Persister()（原行为，零回归）。
//   * LABRPC_PERSIST=file|disk|true                 → 文件模式 Persister(dir)，真正落盘。
//       - 目录：优先取 LABRPC_PERSIST_DIR；否则自动生成 /tmp/cpp6raft_<pid>_<seq>。
//     （显式指定目录的文件模式仍走 persister.h 的 MakePersister(dir)，本文件不重复实现。）
//
// 这样切换持久化后端就像切换网络后端一样：构造 Config 时一行不用改，上层也不感知，
// 想换真存储引擎只设个环境变量即可。新增第三种后端时，在这里加分支 + 一个继承 Persister
// 的类（或类似 SegmentLogStorage 那样在 raft 核心内分流）即可。

#pragma once

#include <atomic>
#include <cstdlib>
#include <memory>
#include <string>
#include <unistd.h>  // getpid

#include "persister.h"  // Persister, MakePersister(dir)

namespace persist {

// 跨 TU 共享的序列号，保证自动生成的持久化目录不撞名（C++17 inline 变量 = 单实例）。
inline std::atomic<uint64_t> g_persist_seq{0};

// 解析环境变量，决定最终是否走文件模式：
//   env 显式设置 → env 优先；未设置 → 回退到调用方传入的 fallback
//   （通常是 Config 构造的 file_backed 参数，保留旧用例的显式 file_backed=true 语义）。
// 未知取值 → 回退兜底，绝不擅自改变行为。
inline bool PersistFileBacked(bool fallback) {
  const char* e = std::getenv("LABRPC_PERSIST");
  if (!e || e[0] == '\0') return fallback;
  std::string m(e);
  if (m == "file" || m == "disk" || m == "true" || m == "1") return true;
  if (m == "memory" || m == "mem" || m == "inmem" || m == "false" || m == "0")
    return false;
  return fallback;
}

// 按环境变量构造 Persister：默认内存，env 指定 file 时落盘。
// 与 transport_factory.h 的 MakeTransport() 对称：无参 = "环境决定后端"。
inline std::shared_ptr<Persister> MakePersister() {
  if (!PersistFileBacked(false)) {
    return std::make_shared<Persister>();  // 内存模式（原行为）
  }
  // 文件模式：目录来自 LABRPC_PERSIST_DIR，否则自动唯一目录
  std::string dir;
  const char* d = std::getenv("LABRPC_PERSIST_DIR");
  std::string prefix =
      (d && d[0]) ? std::string(d) + "/node_"
                  : "/tmp/cpp6raft_" + std::to_string(::getpid()) + "_";
  // 撞名防御（与 persister.h CopyDir 的 access 换号逻辑对称）：
  // 目录已存在就换下一个序号，绝不静默复用。
  do {
    dir = prefix + std::to_string(++g_persist_seq);
  } while (::access(dir.c_str(), F_OK) == 0);
  return MakePersister(dir);  // persister.h 里的显式目录版
}

}  // namespace persist
