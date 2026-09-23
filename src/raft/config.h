// config.h —— 测试脚手架（Go 版 src/raft/config.go 的移植）
//
// 这个文件【不用改】，它提供：
//   * 起一堆 Raft 实例、接进同一个模拟网络
//   * 断开 / 恢复某个节点、杀掉 / 重启某个节点
//   * one()     提交一条命令并等待多数派达成共识
//   * wait()    等某个下标的日志被 n 台机器提交
//   * checkOneLeader() / checkNoLeader() / checkTerms()
//   * 统计 RPC 次数、字节数
//   * 自动检查"同一条日志在不同机器上内容是否一致"（applier 线程干这事）
//
// 失败机制：Go 用 t.Fatalf() 抛异常终止当前测试。C++ 里我们抛
// TestFailure 异常，main 捕获后打印失败并继续跑下一个用例。

#pragma once

#include <atomic>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "../common/chan.h"
#include "../common/util.h"
#include "../labrpc/labrpc.h"
#include "persister.h"
#include "raft.h"

namespace raft {

// 快照自动压缩阈值（以日志条数为单位，对应 Lab 3 的 maxraftstate）。
// 状态机发现本节点日志超过这个条数就主动调 rf.Snapshot() 压缩。
static constexpr int kSnapshotThreshold = 20;

// 测试失败时抛出
//
// 必须继承 std::exception：ThreadTracker 的后台线程只 catch std::exception
// （thread_tracker.h:47），不继承的话 TestFailure 会掉进 catch (...) 分支，
// 只留一句 "unknown exception"，真实 msg 全丢。
//
// 注意：一旦有虚函数，本类就不再是 aggregate（C++17 起也不行），
// 所以必须给显式构造函数 —— 原来的 `TestFailure{msg}` 花括号初始化
// 会走这个构造函数，调用点无需改动。
struct TestFailure : std::exception {
  explicit TestFailure(std::string m) : msg(std::move(m)) {}
  const char* what() const noexcept override { return msg.c_str(); }
  std::string msg;
};

class Config {
 public:
  Config(int n, bool unreliable);
  ~Config();

  Config(const Config&) = delete;
  Config& operator=(const Config&) = delete;

  // ---------- 生命周期 ----------
  void Cleanup();
  void Begin(const std::string& description);
  void End();

  // ---------- 节点操作 ----------
  void Start1(int i);   // 启动/重启第 i 台（先 crash 再起新的）
  void Crash1(int i);   // 杀掉第 i 台，但持久化状态留在 saved_[i]
  void Connect(int i);
  void Disconnect(int i);

  // ---------- 断言 ----------
  int CheckOneLeader();       // 返回 leader 的下标
  int CheckTerms();           // 所有连通节点任期必须一致
  void CheckNoLeader();

  // ---------- 提交 / 等待 ----------
  // 返回 (有多少台认为该下标已提交, 该下标的值)
  std::pair<int, std::optional<Command>> NCommitted(int index);
  // 等 index 被 n 台提交；返回 nullopt 表示"大家已经进入更高任期，别等了"
  std::optional<Command> Wait(int index, int n, int start_term);
  // 提交一条命令，返回它落在的下标
  int One(const Command& cmd, int expected_servers, bool retry);

  // ---------- 网络控制 / 统计 ----------
  void SetUnreliable(bool b) { net_->Reliable(!b); }
  void SetLongReordering(bool b) { net_->LongReordering(b); }
  int RpcCount(int server) { return net_->GetCount(server); }
  int RpcTotal() { return net_->GetTotalCount(); }
  int64_t BytesTotal() { return net_->GetTotalBytes(); }

  // ---------- C3 单条消息字节上限（转发给 labrpc::Network）----------
  // net_ 是私有的，测试要压低闸门必须经这里转发，避免为此放宽封装。
  void SetMaxRpcMessageBytes(size_t n) { net_->SetMaxMessageBytes(n); }
  int64_t OversizedDropped() { return net_->OversizedDropped(); }

  // ---------- 给测试用例直接访问 ----------
  std::shared_ptr<Raft> GetRaft(int i);
  bool Connected(int i);
  int n() const { return n_; }

  // 读某节点"已 apply 的状态机状态"（index → command），用于校验快照再水化。
  std::map<int, Command> GetState(int i);

  // 打开"快照感知状态机"的自动压缩：applier 在日志过长时主动调 rf.Snapshot()。
  // 默认关闭，避免影响 2B/2C 原有测试；快照测试里显式打开。
  void EnableSnapshotCompaction() { snapshot_compaction_ = true; }

  [[noreturn]] void Fatal(const std::string& msg);
  void DumpState(const char* where);  // 排错：打印所有节点状态

 private:
  void CheckTimeout();
  void ApplyErrLocked(int i, const std::string& err);

  mutable std::mutex mu_;
  int n_ = 0;
  std::shared_ptr<labrpc::Network> net_;

  std::vector<std::shared_ptr<Raft>> rafts_;
  // 被 crash 掉的实例：它的后台线程可能还在跑，不能立刻析构，
  // 先放这儿，等 Cleanup() 里等线程收工再一起释放。
  std::vector<std::shared_ptr<Raft>> graves_;

  std::vector<std::string> apply_err_;
  std::vector<bool> connected_;
  std::vector<std::shared_ptr<Persister>> saved_;
  std::vector<std::vector<std::string>> endnames_;
  std::vector<std::map<int, Command>> logs_;  // 每台机器已提交的下标 → 命令
  std::vector<std::shared_ptr<raftcpp::Chan<ApplyMsg>>> apply_chs_;
  std::vector<std::thread> appliers_;
  int max_index_ = 0;

  // 快照感知状态机：applier 是否在日志过长时主动压缩（由 EnableSnapshotCompaction 打开）。
  bool snapshot_compaction_ = false;

  raftcpp::TimePoint start_;
  raftcpp::TimePoint t0_;
  int rpcs0_ = 0;
  int64_t bytes0_ = 0;
  int max_index0_ = 0;

  bool cleaned_ = false;
};

inline std::shared_ptr<Config> MakeConfig(int n, bool unreliable) {
  return std::make_shared<Config>(n, unreliable);
}

}  // namespace raft
