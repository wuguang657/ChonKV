// server.h —— KV 服务器（对应 Go 版 src/kvraft/server.go）
//
// ===========================================================================
// 【本文件解决 Lab 2 遗留的 T2/T3 窗口 —— 见文件末尾的详细说明】
// ===========================================================================
//
// 职责：
//   1. 把客户端的 Get/Put/Append 包成 Op，塞进 Raft 日志
//   2. 从 applyCh 读已提交的命令，应用到自己的 kv_store_（状态机）
//   3. exactly-once 语义：靠 (client_id, seq_id) 去重
//   4. 日志压缩：raft 状态超过 maxraftstate 就生成快照
//
// 【和 Go 版的几处关键差异 —— 都是为了不踩 C++ 的坑】
//   1. Go 版用 channel 做"等 apply 完成"的通知，配合 select + time.After 实现
//      超时。C++ 的 Chan 没有超时 Pop，所以改用 mutex + condition_variable 的
//      wait_for(100ms) 轮询唤醒。
//      ⚠️ 注意：这里【不】再在轮询时探测"我还是不是 leader"——锁外探测拿到的
//      只是一个会过期的快照，既不能保证正确性，也拦不住"被隔离却自认为仍是
//      leader"的少数派旧 leader（它 isLeader 恒为 true，探测根本不触发）。
//      真正兜底的是 Get / WaitOp 里的 1s 硬超时。
//   2. 绝不在持有 mu_ 时调用 Raft 的方法（ReadIndex / Snapshot / GetState ...）。
//      它们内部会抢 raft 的锁，"持 KV 锁跨进 raft 锁"是死锁温床
//      （config.cpp:178-179 的注释也强调过这一点）。
//      本实现一律先在锁内收集好数据，释放 mu_ 后再调 Raft。

#pragma once

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "../common/chan.h"
#include "../labrpc/labrpc.h"
#include "../raft/persister.h"
#include "../raft/raft.h"
#include "common.h"

namespace kvraft {

// 某个 index 上正在等待的客户端请求。
//
// 注：Get 已改走 ReadIndex 直读（不写 raft 日志、不进 apply 流程），所以这里不再
// 需要固化 value / err —— 只有 Put/Append 会创建 waiter。
//
// 身份校验（client_id/seq_id）是必需的：这个 index 上的命令可能已被新 leader
// 覆盖成别的内容，此时 done=true 但 ok=false，WaitOp 便返回 WrongLeader 让
// 客户端换台重试。
struct NotifyMsg {
  bool done = false;  // applier 已经处理过这个 index 了
  bool ok = false;    // 身份匹配 → 这条命令确实生效了
  int client_id = 0;
  int seq_id = 0;
};

class KVServer {
 public:
  KVServer(std::vector<std::shared_ptr<labrpc::ClientEnd>> peers, int me,
           std::shared_ptr<raft::Persister> persister, int maxraftstate);
  ~KVServer();

  KVServer(const KVServer&) = delete;
  KVServer& operator=(const KVServer&) = delete;

  // 启动 applier 后台线程（对应 Go 的 go kv.ReadRaftApplyCommandLoop()）。
  // 和 Raft 一样必须两步走：构造完 → 再 Start()，避免线程摸到半成品对象。
  void Start();

  void Kill();
  bool killed() const { return dead_.load(); }

  // ---- RPC handler：由网络线程并发调用，内部自己加锁 ----
  void Get(const GetArgs& args, GetReply& reply);
  void PutAppend(const PutAppendArgs& args, PutAppendReply& reply);

  // ---- 给测试框架用的访问器 ----
  int me() const { return me_; }
  int LogSize() const { return rf_ ? rf_->LogSize() : 0; }
  int RaftStateSize() const {
    return persister_ ? persister_->RaftStateSize() : 0;
  }
  int SnapshotIndex() const { return rf_ ? rf_->SnapshotIndex() : 0; }
  // 直接读状态机（测试校验一致性用）。调用方需要自己保证并发安全，
  // 测试里只在"没有客户端在跑"的时候调。
  std::map<std::string, std::string> SnapshotStore() const;

  // ---- 仅供测试脚手架使用 ----
  // 脚手架需要把 Raft 也挂到同一个网络节点上（Go 版是 srv.AddService(rfsvc)）
  std::shared_ptr<raft::Raft> raft_for_test() const { return rf_; }
  bool is_leader_for_test() const {
    return rf_ ? rf_->GetState().second : false;
  }

 private:
  // applier 后台线程：消费 applyCh，把已提交的命令应用到状态机
  void ApplierLoop();

  // 把一条 Op 交给 raft，等它被 apply。
  // 返回 kOK 表示这条命令确实生效了；kWrongLeader 表示得换台机器重试。
  //
  // （早期版本有两个 out_value / out_err 出参，用来给 Get 回传"apply 那瞬间"
  //  的状态机值。Get 改走 ReadIndex 直读后不再需要，已删除。）
  Err WaitOp(const Op& op);

  // ---- 以下三个都要求调用方【持有 mu_】（名字以 Locked 结尾）----
  // 把状态机编码成快照字节：kv_store_ + last_seq_
  std::string EncodeSnapshotLocked() const;
  // 把快照字节装回状态机（幂等，可重复调用）
  void ApplySnapshotLocked(const std::string& blob);
  // 判断是否需要生成快照，需要就把数据准备好（在锁内编码）
  bool PrepareSnapshotLocked(int index, int* snap_index, std::string* blob);

  mutable std::mutex mu_;
  std::condition_variable apply_cv_;  // applier 完成某个 index 后通知等待者

  int me_ = 0;
  int maxraftstate_ = 0;  // -1 表示不生成快照
  std::shared_ptr<raft::Raft> rf_;
  std::shared_ptr<raft::Persister> persister_;
  std::shared_ptr<raftcpp::Chan<raft::ApplyMsg>> apply_ch_;

  // ---- 状态机：快照要持久化的就是这两个 ----
  std::map<std::string, std::string> kv_store_;
  std::map<int, int> last_seq_;  // clientId -> 已 apply 的最大 seqId

  // applier 已应用到状态机的最高 cmd index。
  // 注意与 raft.last_applied_ 的区别：raft 的在【派发时】推进，
  // 这个在【KVServer 真正 apply 后】更新，两者差 = applyCh 积压。
  //
  // ⚠️ 它是 ReadIndex 读路径的关键：Get 拿到线性化点 ri 后，必须等到
  // last_cmd_index_ >= ri 才允许读本地状态机（见 Get 的实现）。
  // 因此 applier 的【每一个】分支（普通命令 / no-op / 装快照）都要推进它，
  // 漏掉任何一个分支都会让 Get 永久卡在那个下标上。
  int last_cmd_index_ = 0;

  // index -> 正在等这个 index 被 apply 的请求
  std::map<int, NotifyMsg> msg_replies_;

  std::atomic<bool> dead_{false};
  std::thread applier_;
};

// 建一个 KVServer（内含 Raft）。对应 Go 的 StartKVServer。
std::shared_ptr<KVServer> StartKVServer(
    std::vector<std::shared_ptr<labrpc::ClientEnd>> peers, int me,
    std::shared_ptr<raft::Persister> persister, int maxraftstate);

// 把 KVServer 包装成 labrpc 的 Service。对应 Go 的 labrpc.MakeService(kv)。
inline std::shared_ptr<labrpc::Service> MakeKVServerService(
    std::shared_ptr<KVServer> kv) {
  using labrpc::Service;

  Service::Handler get = [kv](const std::string& args) -> std::string {
    GetArgs a;
    a.Deserialize(args);
    GetReply r;
    kv->Get(a, r);
    return r.Serialize();
  };
  Service::Handler put_append = [kv](const std::string& args) -> std::string {
    PutAppendArgs a;
    a.Deserialize(args);
    PutAppendReply r;
    kv->PutAppend(a, r);
    return r.Serialize();
  };
  return std::make_shared<Service>(
      "KVServer", std::unordered_map<std::string, Service::Handler>{
                      {"Get", get},
                      {"PutAppend", put_append},
                  });
}

}  // namespace kvraft
