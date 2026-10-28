// network.h —— in-proc 传输后端（Transport 的可插拔实现之一）
//
// 即原 labrpc.h 内的 labrpc::Network 类（重组时拆到 inproc/ 子目录）。
// 它是「同进程内、用共享内存 ReplySlot 来回包」的模拟网络，也是默认后端
// （MakeTransport() 未设 LABRPC_TRANSPORT 时返回它）。
//
// 这个网络能做的事（和 Go 版一一对应）：
//   * 丢请求 / 丢回复（Reliable(false) 时各 10% 概率）
//   * 延迟消息（不可靠模式随机 0~27ms；端点仍通但 server 已删时随机 0~7000ms）
//   * 乱序（LongReordering(true) 时，2/3 概率把回复延迟 200~2200ms）
//   * 整机断网 / 恢复（Enable(endname, false/true)）
//   * 统计：总 RPC 次数、总字节数、每台的入站 RPC 次数
//
// ---------------------------------------------------------------------------
// 【和 Go 版的实现差异，务必先读这段】
//
// Go 版给每个 RPC 开一个 goroutine，阻塞、睡觉都很便宜。C++ 不能这么干
// （一秒钟几千个 RPC，开几千个 std::thread 会爆内存）。所以这里用两个线程池：
//
//   1) 定时器线程（1 个）：管"什么时候该做下一步"。
//      内部是一个按时间排序的最小堆（std::multimap）。
//      —— 它只做瞬时动作，绝不阻塞。
//
//   2) 工作线程池（N 个）：真正执行 server 的 RPC handler。
//      handler 会抢 Raft 的锁，可能阻塞一小会儿，所以放在独立线程里。
//
// 一个 RPC 的生命周期：
//
//   Call() ──► SendReq()（计数、决定是否可用）──► 定时器线程（到点了）
//        ──► 工作线程执行 handler ──► 定时器线程（延迟后）──► 唤醒 Call()
//
// ---------------------------------------------------------------------------
// 【C++ 知识点】
//   * std::condition_variable 的 wait 必须配一个 while 循环（防虚假唤醒），
//     或者用带谓词的 wait(lock, pred) —— 后者等价于前者，更省心。
//   * 所有共享状态都要在锁里读；出了锁就只能用"自己那一份拷贝"。

#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "../labrpc.h"      // ClientEnd / Server / ReqMsg / ReplySlot / kMaxRpcMessageBytesDefault
#include "../transport.h"   // Transport 抽象基类：本类是它的 in-proc 实现
#include "../../common/util.h"  // raftcpp::TimePoint / Now / RandInt

namespace labrpc {

// ---------------------------------------------------------------------------
// 中央交换机（in-proc 后端）
// ---------------------------------------------------------------------------
// 继承 Transport —— 它既是"原模拟网络"这个后端，也是 Transport 的一个实现。
// 默认路径（MakeTransport() 未设 LABRPC_TRANSPORT 时返回它）行为与原 Network
// 逐字节一致。
// 注意：这里【不】再提供 MakeNetwork()。早先那个 inline 工厂直接 new 一个
// Network（in-proc），完全绕开 transport_factory.h 的后端选择 ——
// LABRPC_TRANSPORT=socket 时调它仍会静默拿到 in-proc，net 用例"假过"。
// 建网络一律走 labrpc::MakeTransport()。
class Network : public Transport {
 public:
  Network();
  ~Network();

  Network(const Network&) = delete;
  Network& operator=(const Network&) = delete;

  // 建一个发信端点（默认禁用，要先 Connect 再 Enable）
  std::shared_ptr<ClientEnd> MakeEnd(const std::string& endname) override;
  void AddServer(int servername, std::shared_ptr<Server> srv) override;
  void DeleteServer(int servername) override;
  void Connect(const std::string& endname, int servername) override;
  void Enable(const std::string& endname, bool enabled) override;
  // 注销一个发信端点（从 enabled_/connections_ 移除）。
  // 对齐 Go 版 labrpc 的 Network.Remove(endname)，由 config.deleteClient 调用：
  // 一个 client 被删除后，它那组 ClientEnd 不再存在于网络里，发往它们的
  // RPC 一律找不到目标而失败。仅删这两项即可（network 不持有 ClientEnd 本体，
  // 它的 shared_ptr 随调用方释放而析构）。
  void RemoveClientEnd(const std::string& endname) override;

  void Reliable(bool yes) override;
  void LongReordering(bool yes) override;
  void LongDelays(bool yes) override;

  int GetCount(int servername) override;   // 某台的入站 RPC 数
  int GetTotalCount() const override;      // 全网发出的 RPC 总数
  int64_t GetTotalBytes() const override;  // 全网传输的总字节数

  // ---- C3 单条消息字节上限 ----
  // 设置单条 RPC 请求的字节上限（0 表示不限制）。默认 kMaxRpcMessageBytesDefault。
  void SetMaxMessageBytes(size_t n) override;
  size_t MaxMessageBytes() const override;
  // 因超过字节上限而被拒收的消息条数（观测/测试用：证明闸门真的在拦，
  // 而不是"碰巧没发出去"）。
  int64_t OversizedDropped() const override;

  void Cleanup() override;  // 停线程、唤醒所有还在等的人

  // ---- 以下给 ClientEnd 用 ----
  void SendReq(std::shared_ptr<ReqMsg> req) override;
  bool IsStopped() const override { return stopped_.load(); }

 private:
  enum class EventKind { kDispatch, kDeliverReply }; // 投递请求还是投递回包

  struct TimerEvent {
    EventKind kind = EventKind::kDispatch;
    std::shared_ptr<ReplySlot> slot;
    ReplyCallback cb;
    std::atomic<bool>* cancel = nullptr;

    // kDispatch 用
    std::string endname;
    std::string svc_meth;
    std::string args;
    int servername = -1;
    std::shared_ptr<Server> server;
    bool reliable = true;
    bool longreordering = false;
    bool longdelays = false;  // 失败路径的长延迟：仅"端点 enabled 但 server 已 DeleteServer"
                              // 这一种（0~6999ms）；端点被 Disconnect 是立即失败（见 SendReq）

    // kDeliverReply 用
    bool ok = false;
    std::string reply;
  };

  void TimerLoop();
  void WorkerLoop();
  // 新建一个 worker 线程。调用前必须持有 mu_。
  void SpawnWorkerLocked();
  void Schedule(std::shared_ptr<TimerEvent> e, int64_t delay_ms);
  // 收尾：同步调用填 slot，异步调用把回调丢进线程池
  void Finish(std::shared_ptr<TimerEvent> e);
  bool IsServerDead(const TimerEvent& e);
  // 把一个任务丢进线程池（服务器端的 handler、客户端的回信回调都走这儿）
  void Post(std::function<void()> task);
  struct EndInfo;
  EndInfo ReadEndInfo(const std::string& endname);

  mutable std::mutex mu_;

  // 网络属性
  bool reliable_ = true;
  bool long_delays_ = false;
  bool long_reordering_ = false;

  std::map<std::string, bool> enabled_;             // endname -> 是否通
  std::map<std::string, int> connections_;          // endname -> servername
  std::unordered_map<int, std::shared_ptr<Server>> servers_;

  std::atomic<int> count_{0};
  std::atomic<int64_t> bytes_{0};

  // ---- C3 单条消息字节上限 ----
  // 运行期可改（默认 kMaxRpcMessageBytesDefault），故用 atomic 而非 constexpr，
  // 这样测试能把闸门压到很小来断言"超限被拒"，而不必改代码重编译。
  std::atomic<size_t> max_message_bytes_{kMaxRpcMessageBytesDefault};
  // 被字节闸门拒收的消息条数。
  std::atomic<int64_t> oversized_dropped_{0};

  // 定时器线程
  std::multimap<raftcpp::TimePoint, std::shared_ptr<TimerEvent>> timers_;
  std::condition_variable timer_cv_;

  // 工作线程池：跑"需要占用线程"的活儿
  //   * 服务端：执行 RPC handler（会抢 Raft 的锁，所以不能放在定时器线程里）
  //   * 客户端：执行 CallAsync 的回信回调
  //
  // ⚠️ 池子必须能【动态扩容】，不能是定长。原因：handler 是同步执行的，
  // 而 KV 层的 Get/PutAppend 会阻塞等待 raft 提交（最长 1 秒，见
  // kvraft/server.cpp 的 WaitOp）。定长池（哪怕 16 个）在 20 个客户端并发时
  // 会被这类阻塞型 handler 全部占满 —— 心跳、日志复制的 handler 就排不进
  // 队列，而它们恰恰是让那些阻塞的 handler 得以返回的前提 → 整个集群停摆。
  //
  // Go 版没这个问题：它每个 RPC handler 各跑一个 goroutine，阻塞不占线程。
  // C++ 要等价于"每 handler 一线程"的语义，就用"全忙即扩容"来逼近。
  // 详见 doc/06-Lab3-KV服务与测试.md 的「线程池饥饿」一节。
  std::deque<std::function<void()>> tasks_;
  std::condition_variable task_cv_;
  size_t busy_workers_ = 0;   // 正在执行任务（含阻塞）的 worker 数
  size_t total_workers_ = 0;  // 已创建的 worker 总数
  static constexpr size_t kMaxWorkers = 512;

  std::vector<std::thread> threads_;
  std::atomic<bool> stopped_{false};
  bool cleaned_ = false;
};

}  // namespace labrpc
