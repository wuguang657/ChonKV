// config.h —— KV 测试脚手架（对应 Go 版 src/kvraft/config.go）
//
// 和 raft/config.h 的约定保持一致：测试只管"编排故障 + 发命令"，
// 正确性检查（各副本状态是否一致）放在 End() 里做。

#pragma once

#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "../labrpc/labrpc.h"
#include "../raft/persister.h"
#include "client.h"
#include "server.h"

namespace kvraft {

class Config {
 public:
  // n：server 个数；unreliable：是否开不可靠网络；maxraftstate：快照阈值（-1 = 不快照）
  // linearizability：是否在 End() 时调用 porcupine 做线性一致性检查（耗时几十秒）
  Config(int n, bool unreliable, int maxraftstate, bool linearizability = false);
  ~Config();

  Config(const Config&) = delete;
  Config& operator=(const Config&) = delete;

  // ---- 生命周期 ----
  // 起第 i 台（先 ShutdownServer(i) 再 StartServer(i) 就是"崩溃重启"）
  void StartServer(int i);
  void ShutdownServer(int i);
  void Cleanup();

  // ---- 网络编排 ----
  void ConnectAll();
  // 把网络切成两半：p1 内部互通、p2 内部互通，p1 与 p2 之间不通
  void Partition(const std::vector<int>& p1, const std::vector<int>& p2);
  // 对应 Go 版 config.go:339 make_partition()：p1 固定 n/2+1 台（多数派），
  // p2 固定 n/2 台，并把当前 leader 放进 p2。
  // ⚠️ TestOnePartition3A 依赖"p1 一定是多数派、能在里面推进"这个语义，
  //    （实测：换成随机版会切出 2v3，p1 变少数派 → 写不进去 → 用例红）
  //    所以这里必须保持固定比例，不要改成抛硬币。
  void MakePartition(std::vector<int>* p1, std::vector<int>* p2);
  // 对应 Go 版 test_test.go:126 partitioner()：每台 server 独立 50%，
  // 形态随机（2^n 种），且允许空组（全 0 / 全 1 → 那一轮等价于全网连通）。
  // GenericTest / GenericTestLinearizability 的分区线程应该用这个。
  void MakePartitionRandom(std::vector<int>* p1, std::vector<int>* p2);

  // ---- 客户端 ----
  // ⚠️ `to` 为空表示【一台都不连】（对齐 Go 版 config.go:191 makeClient 的语义
  //    —— 早期版本写成"连所有"，方向正好相反，会让误传空 vector 的分区用例
  //    形同虚设）。想要"连所有"必须显式传 cfg.All()。
  std::shared_ptr<Clerk> MakeClient(const std::vector<int>& to = {});
  void ConnectClient(Clerk* ck, const std::vector<int>& to);
  // 断开某 client 与 `from` 这组服务器的连接（仅禁用 from 内的端点，其余不动）。
  // 对齐 Go 版 config.go 的 ConnectClient/DisconnectClientUnlocked 语义。
  void DisconnectClient(Clerk* ck, const std::vector<int>& from);
  // 拆除一个 client：禁用并注销它的全部端点，释放 Config 对 Clerk 的持有。
  // 对齐 Go 版 config.go 的 deleteClient（标准 labrpc 走 cfg.net.Remove，
  // 这个 fork 写成 os.Remove 实为 no-op；C++ 走 RemoveClientEnd 真正注销）。
  void DeleteClient(Clerk* ck);

  // ---- 度量 ----
  int LogSize() const;       // 所有 server 里最大的 raft 持久化字节数
  int SnapshotSize() const;  // 所有 server 里最大的快照字节数
  int RpcTotal();
  bool Leader(int* leader_id);  // 找当前 leader

  // ---- 测试计时 / 断言 ----
  void Begin(const std::string& desc);
  void Op();   // 每完成一个客户端操作计一次
  // 打印耗时 / RPC 数 / 操作数，并（在全网连通的前提下）做副本一致性检查
  void End();

  // 当前网络是否被切开。分区中的副本本来就不一致，End() 会据此跳过一致性检查。
  bool partitioned() const { return partitioned_.load(); }

  // 记录一次失败（不立刻退出，让 End() 汇总打印）
  void Fail(const std::string& msg);
  bool failed() const { return failed_.load(); }

  int n() const { return n_; }
  int maxraftstate() const { return maxraftstate_; }
  // 所有 server 的编号。测试里常写成 MakeClient(cfg.All()) 表示"能连所有机器"。
  std::vector<int> All() const;

  // 仅供测试用：直接拿第 i 台 KVServer 实例（白盒断言 reply.err / leader_id 用）。
  // ⚠️ 生产代码绝不该这么拿；这是测试脚手架的逃生舱口。
  std::shared_ptr<KVServer> kvserver(int i) const {
    return (i >= 0 && i < static_cast<int>(kvservers_.size())) ? kvservers_[i]
                                                               : nullptr;
  }

  // 校验所有副本的状态机是否一致（3A/3B 的核心断言）
  bool CheckConsistency(std::string* err);

  // 把所有 Clerk 的 op 历史收上来，跑 porcupine 做线性一致性检查。
  // 返回 true 表示线性一致；false / 超时在内部 Fail() 并打印。
  // （只对 Linearizable 用例调用，普通用例不动它 —— 一次检查可能几十秒）
  bool CheckLinearizability();

  // 是否启用了线性一致性检查
  bool linearizability() const { return check_linearizability_; }

 private:
  void ConnectUnlocked(int i, const std::vector<int>& to);
  void DisconnectUnlocked(int i, const std::vector<int>& from);
  // client 级断开（参数区别于上面的 server 级：这里传 Clerk* 而不是 server id）
  void DisconnectClientUnlocked(Clerk* ck, const std::vector<int>& from);

  mutable std::mutex mu_;

  std::shared_ptr<labrpc::Network> net_;
  int n_ = 0;
  int maxraftstate_ = 0;
  bool check_linearizability_ = false;

  std::vector<std::shared_ptr<KVServer>> kvservers_; // 当前活着的 KVServer 实例，下标就是 server 编号（0~n-1）。每台含一个 KVServer + 一个 Raft。
  // 延后析构列表：ShutdownServer 时旧 KVServer 不立即析构，而是存这里，
  // 等 Cleanup 的 ThreadTracker::WaitAll 之后（所有 detached 的 Raft 后台线程
  // 都已退出）才真正释放。否则旧 Raft 的 mu_ 一析构，还在跑的 ApplyLoop
  // 线程去 lock 已销毁的 mutex → "mutex lock failed"。 raft::Config 用完全
  // 一样的 graves_ 机制（raft/config.cpp:85）。
  std::vector<std::shared_ptr<KVServer>> graves_;
  std::vector<std::shared_ptr<raft::Persister>> saved_;
  std::vector<std::vector<std::string>> endnames_;

  // Clerk* -> 它那组 ClientEnd 的端点名（用于 ConnectClient 时 Enable/Disable）
  // 注意必须存 shared_ptr，否则 Clerk 会随测试栈帧析构，End() 里访问
  // 历史时撞到已销毁的 mutex（"mutex lock failed: Invalid argument"）。
  std::map<Clerk*, std::shared_ptr<Clerk>> clerks_;  // own the Clerk (client)，避免clerk被函数栈提前析构
  std::map<Clerk*, std::vector<std::string>> clerk_ends_; // 每个 Clerk 创建时 MakeClient 会给它建 n 个 ClientEnd（每台 server 一个端点），这些端点名全部记到 clerk_ends_[ck] 里。但端点的"开关"状态是另一回事

  // ---- 计时 / 统计 ----
  std::atomic<int> ops_{0};
  std::chrono::steady_clock::time_point t0_;
  int rpcs0_ = 0;
  std::atomic<bool> failed_{false};
  std::string fail_msg_;
  // 网络是否正处于被切开的状态（Partition 置 true，ConnectAll 置 false）。
  // End() 只在它为 false 时才校验副本一致性 —— 分区期间各副本本来就不一致。
  std::atomic<bool> partitioned_{false};
  bool cleaned_ = false;
  bool desc_printed_ = false;
  std::string desc_;
};

}  // namespace kvraft
