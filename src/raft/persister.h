// persister.h —— 模拟磁盘（Go 版 src/raft/persister.go 的移植）
//
// 在真实的 Raft 里，currentTerm / votedFor / log 必须写盘，
// 断电重启后还在。测试里用一块内存 + 一把锁来假装是磁盘。
//
// 关键点：Raft 崩了之后 Persister 里的内容必须还在，这样新起的 Raft
// 才能恢复到崩溃前的状态 —— 这就是 2C 要考的东西。

#pragma once

#include <memory>
#include <mutex>
#include <string>

namespace raft {

class Persister {
 public:
  Persister() = default;

  // 复制一份（快照语义）：copy 出的新 Persister 与原来的互不影响。
  // config 在重启一个 server 时会先 Copy()，防止旧实例继续往里写。
  std::shared_ptr<Persister> Copy() {
    auto np = std::make_shared<Persister>();
    std::lock_guard<std::mutex> lk(mu_);
    np->raft_state_ = raft_state_;
    np->snapshot_ = snapshot_;
    return np;
  }

  void SaveRaftState(const std::string& state) {
    std::lock_guard<std::mutex> lk(mu_);
    raft_state_ = state;
  }

  std::string ReadRaftState() {
    std::lock_guard<std::mutex> lk(mu_);
    return raft_state_;
  }

  int RaftStateSize() {
    std::lock_guard<std::mutex> lk(mu_);
    return static_cast<int>(raft_state_.size());
  }

  // 原子地同时保存 raft 状态和快照，避免两者对不上（Lab 3 用得上）
  void SaveStateAndSnapshot(const std::string& state, const std::string& snapshot) {
    std::lock_guard<std::mutex> lk(mu_);
    raft_state_ = state;
    snapshot_ = snapshot;
  }

  std::string ReadSnapshot() {
    std::lock_guard<std::mutex> lk(mu_);
    return snapshot_;
  }

  int SnapshotSize() {
    std::lock_guard<std::mutex> lk(mu_);
    return static_cast<int>(snapshot_.size());
  }

 private:
  std::mutex mu_;
  std::string raft_state_;
  std::string snapshot_;
};

inline std::shared_ptr<Persister> MakePersister() {
  return std::make_shared<Persister>();
}

}  // namespace raft
