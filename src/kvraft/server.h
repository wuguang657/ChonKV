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
#include <deque>       // std::deque（fence_fifo_）：以前靠传递包含才编过，显式补上
#include <functional>  // std::function（Reap/Evict 的 on_evict 回调）
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>  // std::pair（FenceOrderedForTest 的返回值）
#include <vector>
#include <unordered_map>
#include <list>

#include "../common/chan.h"
#include "../labrpc/labrpc.h"
#include "../labgob/persister.h"
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

class SessionTable {
  public:
    static constexpr uint64_t kNoIdleLimit = ~static_cast<uint64_t>(0);  // 一个会话空闲多少条日志未活动就淘汰, ~是按位取反
    explicit SessionTable(size_t cap = 1024 * 1024,
                          uint64_t max_idle_index = 100000):
        cap_(cap), max_idle_index_(max_idle_index) {}

    int Get(int cid, uint64_t now_index){
      auto it = entries_.find(cid);
      if (it == entries_.end()) return 0;
      Touch(it, now_index);
      return it->second.last_seq;
    }
    // 写入新的最大 seq（apply 了更大 seq 之后调用），并刷新为"刚访问"。
   void Put(int cid, int seq, uint64_t now_index) {
      auto it = entries_.find(cid);
      if (it == entries_.end()) {
        SessionEntry e;
        e.last_seq = seq;
        e.last_index = now_index;
        e.lru_it = lru_.insert(lru_.begin(), cid);
        entries_.emplace(cid, std::move(e)); // 原地构造，省深拷贝
      } else{
        it->second.last_seq = seq;
        Touch(it, now_index);
      }
   }
  // 快照恢复专用：按给定的 last_index 装载，【不】走 Touch。
  // 调用方必须按快照里的顺序（最旧 -> 最新）依次调用，这样 push_front 重建出的
  // LRU 队列与"从日志逐条 apply 过来"的副本完全一致。
  void Load(int cid, int seq, uint64_t last_index) {
    auto it = entries_.find(cid);
    if (it == entries_.end()) {
      SessionEntry e;
      e.last_seq = seq;
      e.last_index = last_index;
      e.lru_it = lru_.insert(lru_.begin(), cid);
      entries_.emplace(cid, std::move(e));
    } else {
      it->second.last_seq = seq;
      it->second.last_index = last_index;  // 已在队首，位置不变
    }
  }

  // 回收：先按逻辑时钟淘汰空闲过久的，再按容量淘汰最久未访问的。
  // now_index = 当前 apply 的 command_index，即逻辑时钟的"现在"。
  // on_evict：每淘汰一个 client_id 时回调（KVServer 用它做 fencing），可为空。
  // on_evict 装一个lambda表达式， 例如：[this](int cid) { this->fence(cid); }
  void Reap(uint64_t now_index, std::function<void(int,int)> on_evict = {}) {
    // 1) 空闲淘汰：lru_.back() 恒为 last_index 最小者，它不满足就能整段跳出。
    while (!lru_.empty()) {
      auto it = entries_.find(lru_.back());
      if (it == entries_.end()) {
        lru_.pop_back();  // 防御性清理（正常不会出现）
        continue;
      }
      uint64_t idle = now_index >= it->second.last_index
                          ? now_index - it->second.last_index
                          : 0;
      if (idle <= max_idle_index_) break;
      Evict(it, on_evict);
    }
    // 2) 容量淘汰：插入只由日志触发，故 size 的演化对所有副本完全一致。
    while (!lru_.empty() && entries_.size() > cap_) {
      auto it = entries_.find(lru_.back());
      if (it == entries_.end()) {
        lru_.pop_back();
        continue;
      }
      Evict(it, on_evict);
    }
  }

  void Clear() {
    entries_.clear();
    lru_.clear();
  }
  size_t Size() const { return entries_.size(); }
  size_t Capacity() const { return cap_; }
  uint64_t Evicted() const { return evicted_; }

  template <typename F>
  void ForEach(F f) const {
    for (auto it = lru_.rbegin(); it != lru_.rend(); ++it) {
      auto e = entries_.find(*it);
      if (e == entries_.end()) continue;
      f(*it, e->second.last_seq, e->second.last_index);
    }
  }

  void SetLimit(size_t cap, uint64_t max_idle_index) {
    cap_ = cap;
    max_idle_index_ = max_idle_index;
  }

  private:
    struct SessionEntry {
      int last_seq = 0;
      uint64_t last_index = 0; // 最后一次访问的 index,长时间没访问就淘汰
      std::list<int>::iterator lru_it;  // lru_ 中的迭代器
    };
    void Touch(std::unordered_map<int, SessionEntry>::iterator it,
               uint64_t now_index) {
      it->second.last_index = now_index;   // ？
      lru_.splice(lru_.begin(), lru_, it->second.lru_it);
    }
    void Evict(std::unordered_map<int, SessionEntry>::iterator it,
             std::function<void(int,int)> on_evict = {}) {
      int cid = it->first;
      int last_seq = it->second.last_seq;   // 淘汰前取出，供墓碑使用
      lru_.erase(it->second.lru_it);
      entries_.erase(it);
      evicted_++;
      if (on_evict) on_evict(cid, last_seq);
    } 

    size_t cap_;
    uint64_t max_idle_index_;
    uint64_t evicted_ = 0;  // 累计淘汰次数，压测可观测
    std::unordered_map<int, SessionEntry> entries_;
    std::list<int> lru_;  // front = 最近访问（last_index 最大），back = 最久未访问
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

  // 测试专用：返回 ForceSnapshotOnDiskFull 真正落盘（CAS 抢到快照资格）的次数。
  // 用于并发突发护栏用例断言"节流生效、没形成 fsync 风暴"——该计数应被钉死在
  // ≤ 测试耗时/50ms，而非正比于 disk_full 突发事件数。
  int64_t ForcedSnapCountForTest() const {
    return forced_snap_count_.load(std::memory_order_relaxed);
  }

  // ---- 会话表淘汰 / fencing 调试用（白盒）----
  // ⚠️ 这几个都加锁：ApplierLoop 会在另一个线程里并发改 sessions_ / fenced_，
  //    测试线程无锁直读是 data race（TSan 会报），必须走这里的访问器。
  size_t SessionSizeForTest() const {
    std::lock_guard<std::mutex> lk(mu_);
    return sessions_.Size();
  }
  uint64_t SessionEvictedForTest() const {
    std::lock_guard<std::mutex> lk(mu_);
    return sessions_.Evicted();
  }
  // 该 client_id 是否已被 fencing（会话被淘汰后，迟到重试应被拒绝）。
  bool IsFencedForTest(int cid) const {
    std::lock_guard<std::mutex> lk(mu_);
    return fenced_.count(cid) > 0;
  }
  size_t FenceSizeForTest() const {
    std::lock_guard<std::mutex> lk(mu_);
    return fenced_.size();
  }
  // 读某个被 fencing 的 cid 的【墓碑值】（淘汰那一刻的 last_seq）。
  // 未 fence 返回 false。这条是验证"墓碑 tombstone"正确性的唯一入口：
  // 若 Evict 回调漏传 last_seq、或快照编解码丢 seq，这里就会读到错误的值。
  bool FenceLastSeqForTest(int cid, int* out_seq) const {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = fenced_.find(cid);
    if (it == fenced_.end()) return false;
    if (out_seq) *out_seq = it->second;
    return true;
  }
  // 按 fence_fifo_（淘汰先后）导出整个 fenced_，用于比对【副本间顺序一致性】。
  // 只比对 size 是不够的：若各副本编码/回放顺序不同，封顶 kFenceCap 时会踢掉
  // 不同的 cid → 状态机随时间发散，而 size 恰恰相等。
  std::vector<std::pair<int, int>> FenceOrderedForTest() const {
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<std::pair<int, int>> out;
    out.reserve(fence_fifo_.size());
    for (int cid : fence_fifo_) {
      auto it = fenced_.find(cid);
      if (it != fenced_.end()) out.emplace_back(cid, it->second);
    }
    return out;
  }
  // cap = 容量上限；max_idle_index = 空闲多少条日志未活动即淘汰
  // （传 SessionTable::kNoIdleLimit 关闭空闲淘汰）。
  void SetSessionLimitForTest(size_t cap, uint64_t max_idle_index) {
    std::lock_guard<std::mutex> lk(mu_);
    sessions_.SetLimit(cap, max_idle_index);
  }
  // 已应用到状态机的最高 cmd index（逻辑时钟读数），供副本一致性比对。
  int LastCmdIndexForTest() const {
    std::lock_guard<std::mutex> lk(mu_);
    return last_cmd_index_;
  }

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
  // 返回 kOK 表示这条命令确实生效了；kWrongLeader 表示得换台机器重试；
  // kBusy 表示"我是 leader 但被背压限流，稍后重试同一台即可"。
  // out_leader_id：当返回 kWrongLeader 时，回填"本节点认知到的 leader 编号"
  // （供客户端重定向直连）；kOK/kBusy/未知时为 -1。
  //
  // （早期版本有两个 out_value / out_err 出参，用来给 Get 回传"apply 那瞬间"
  //  的状态机值。Get 改走 ReadIndex 直读后不再需要，已删除。）
  Err WaitOp(const Op& op, int* out_leader_id);

  // ---- 以下三个都要求调用方【持有 mu_】（名字以 Locked 结尾）----
  // 把状态机编码成快照字节：kv_store_ + last_seq_
  std::string EncodeSnapshotLocked() const;
  // 把快照字节装回状态机（幂等，可重复调用）
  void ApplySnapshotLocked(const std::string& blob);
  // 判断是否需要生成快照，需要就把数据准备好（在锁内编码）
  bool PrepareSnapshotLocked(int index, int* snap_index, std::string* blob);
  // #2.2 磁盘水位：disk_full 时主动压缩一次（锁内编码 + 锁外 Snapshot）。
  void ForceSnapshotOnDiskFull();

  mutable std::mutex mu_;
  std::condition_variable apply_cv_;  // applier 完成某个 index 后通知等待者

  int me_ = 0;
  int maxraftstate_ = 0;  // -1 表示不生成快照

  // #2.2 磁盘水位节流：上次强制快照的 steady_clock 纳秒戳（原子，无锁）。
  // 并发 disk_full 突发下用 CAS 限流——每 kForceSnapMinInterval 窗口仅一个线程真落盘，
  // 其余直接跳过（状态已被抢到资格的线程的快照压缩过，再压一次是纯浪费 I/O、反令磁盘更满）。
  // 见 server.cpp ForceSnapshotOnDiskFull 的限流实现。
  std::atomic<int64_t> last_forced_snap_ns_{0};

  // 测试专用计数器：ForceSnapshotOnDiskFull 真正落盘（CAS 抢到资格）的累计次数。
  // 仅在 CAS 成功路径递增，跳过（被节流）的路径不计数。
  std::atomic<int64_t> forced_snap_count_{0};

  std::shared_ptr<raft::Raft> rf_;
  std::shared_ptr<raft::Persister> persister_;
  std::shared_ptr<raftcpp::Chan<raft::ApplyMsg>> apply_ch_;

  // ---- 状态机：快照要持久化的就是这两个 ----
  std::map<std::string, std::string> kv_store_;
  // std::map<int, int> last_seq_;  // clientId -> 已 apply 的最大 seqId   改为定期清理会话
  SessionTable sessions_;
  // fenced_ 同时是"淘汰挡板"和"last_seq 墓碑"：key=被淘汰 client_id，
  // value=淘汰时记录的 last_seq。apply 去重时若会话记录已被擦除，仍可用墓碑
  // 里的 last_seq 去重，堵住"入口 fence 有 TOCTOU、重试滑过、记录擦除后才 apply"
  // 导致的双执行窄窗。受 kFenceCap + fence_fifo_ 约束，内存有界。
  std::unordered_map<int, int> fenced_;
  std::deque<int> fence_fifo_;
  static constexpr size_t kFenceCap = 4 * 1024 * 1024;   // 保证黑名单有界

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
