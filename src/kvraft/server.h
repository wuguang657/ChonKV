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
// 【和 Go 版的两处关键差异 —— 都是为了不踩 C++ 的坑】
//   1. Go 版用 channel 做"等 apply 完成"的通知，配合 select + time.After 实现
//      超时。C++ 的 Chan 没有超时 Pop，所以这里改用 mutex + condition_variable
//      的 wait_for(100ms)，效果一样：既能被及时唤醒，又能定期查"我还是不是 leader"。
//   2. 绝不在持有 mu_ 时调用 Raft 的方法（Snapshot / CondInstallSnapshot）。
//      那两个函数内部会抢 raft 的锁，"持 KV 锁跨进 raft 锁"是死锁温床
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

// 某个 index 上正在等待的客户端请求
//
// ⚠️ value / err 字段是关键修复：
//   Get 命令的 value 必须在 ApplyLoop 持有 KV 锁时被捕获进这个结构，
//   WaitOp 再从这个结构读出来返回给调用方。
//   如果照 Go 版 server.go 的写法（WaitOp 返回后再读 kv_store_），
//   释放锁期间另一个 Put 可能已经改变状态，Get 就会读到比它自己的
//   linearization point 更晚的值，破坏线性一致性 —— porcupine 会判 Illegal。
struct NotifyMsg {
  bool done = false;       // applier 已经处理过这个 index 了
  bool ok = false;         // 且身份匹配（client_id/seq_id 对得上）→ 这条命令真的生效了
  int client_id = 0;
  int seq_id = 0;
  std::string value;       // Get 的返回值（在 apply 时刻从状态机读出，固化下来）
  Err err = Err::kOK;      // Get 的 err（kNoKey 或 kOK）
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
  // 把当前状态机编码成快照字节（测试用来校验快照大小和 round-trip）
  std::string EncodeSnapshot() const;

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

  // 把一条 Op 交给 raft，等它被 apply。返回 kWrongLeader 表示得换台机器重试。
  //
  // 【为什么 out_value / out_err 是指针】
  //   Get 必须在线性一致意义上读到"apply 那一瞬间"的状态机值。
  //   所以 apply 时刻 ApplierLoop 把 value + err 写入 msg_replies_[index]，
  //   WaitOp 在自己的临界区内把这两个值拷给调用方。
  //   调用方【禁止】在 WaitOp 返回后再去读 kv_store_，因为中间可能被并发 apply
  //   改写（这正是 Go 版 server.go 的隐藏 bug，porcupine 抓得到）。
  //   对 Put/Append 来说这两个参数 nullptr 即可，不关心。
  Err WaitOp(const Op& op, std::string* out_value, Err* out_err);

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

  // ---- 排错用：applier 已应用到状态机的最高 cmd index ----
  // 注意与 raft.last_applied_ 的区别：raft 的在【派发时】推进，
  // 这个在【KVServer 真正 apply 后】更新，两者差 = applyCh 积压。
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
