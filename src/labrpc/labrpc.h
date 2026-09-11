// labrpc.h —— 模拟网络（Go 版 src/labrpc/labrpc.go 的 C++ 移植）
//
// 这个网络能做的事（和 Go 版一一对应）：
//   * 丢请求 / 丢回复（Reliable(false) 时各 10% 概率）
//   * 延迟消息（不可靠模式随机 0~27ms；断连时随机 0~7000ms）
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

#include "../common/util.h"
#include "codec.h"

namespace labrpc {

// 异步调用的回信处理函数：ok=false 表示丢包/断连/服务器挂了
using ReplyCallback = std::function<void(bool ok, const std::string& reply)>;

// 同步调用的"收件箱"。Call() 在这儿等结果；网络线程拿到回复后填进去并通知。
struct ReplySlot {
  std::mutex mu;
  std::condition_variable cv;
  bool done = false;   // 已经有结论了（成功或失败）
  bool ok = false;     // true = 服务端真的执行了
  std::string reply;   // 编码后的回复
};

// 客户端发出的一封"信"
struct ReqMsg {
  std::string endname;               // 发信端点名
  std::string svc_meth;              // 例如 "Raft.AppendEntries"
  std::string args;                  // 编码后的参数
  std::shared_ptr<ReplySlot> slot;   // 同步调用：回信投递到这儿
  ReplyCallback cb;                  // 异步调用：直接回调（slot 为 null）
  std::atomic<bool>* cancel = nullptr;  // 紧急刹车（Raft::Kill 时置 true）
};

class Network;

// ---------------------------------------------------------------------------
// ClientEnd：一个"发信端点"，Raft 通过它给别的 server 发 RPC
// ---------------------------------------------------------------------------
class ClientEnd {
 public:
  ClientEnd(Network* net, std::string endname)
      : net_(net), endname_(std::move(endname)) {}

  // 发一个 RPC，阻塞等回复。
  // 返回 true 表示服务端执行了、reply 有效；false 表示丢包/断连/服务器挂了。
  //
  // cancel：可选的"紧急刹车"。Go 版靠随时可丢弃的 goroutine 天然不会卡住；
  // C++ 的线程必须能自己醒来退出，否则 Kill() 之后线程可能还要等 7 秒
  // （断连时网络会随机延迟 0~7000ms）。传入一个 atomic<bool>，
  // 把它置 true 就能让所有在途的 Call() 在 50ms 内返回 false。
  bool Call(const std::string& svc_meth, const std::string& args,
            std::string& reply, std::atomic<bool>* cancel = nullptr);

  // 泛型包装：自动帮你序列化 / 反序列化
  template <typename ArgsT, typename ReplyT>
  bool CallTyped(const std::string& svc_meth, const ArgsT& args, ReplyT& reply,
                 std::atomic<bool>* cancel = nullptr) {
    std::string encoded_args = args.Serialize();
    std::string encoded_reply;
    if (!Call(svc_meth, encoded_args, encoded_reply, cancel)) return false;
    if (!reply.Deserialize(encoded_reply)) {
      // 解码失败说明收发两端结构对不上，属于代码 bug，直接炸掉比默默出错好。
      std::fprintf(stderr, "ClientEnd::CallTyped: decode reply failed\n");
      std::abort();
    }
    return true;
  }

  // ---------------------------------------------------------------------
  // 异步版本：发出就返回，回信到了再调 cb
  // ---------------------------------------------------------------------
  // 为什么必须用异步？
  //   Go 版到处是 `go sendRPC(...)`，一个 RPC 卡住不影响下一个心跳。
  //   如果用同步 Call，往"已断连节点"发的 RPC 会被模拟网络拖住 0~7000ms
  //   （config 里开了 LongDelays），整条复制线程就卡死了 —— 节点重连后
  //   要等最多 7 秒才收得到心跳。TestReElection2A 就是专门抓这个的。
  //   C++ 又不能像 Go 那样随便开几万个 goroutine，所以：
  //   等待不占线程（挂在定时器队列里），回信时才占用一个池线程跑回调。
  void CallAsync(const std::string& svc_meth, std::string args,
                 ReplyCallback cb, std::atomic<bool>* cancel = nullptr);

  template <typename ArgsT, typename ReplyT>
  void CallAsyncTyped(const std::string& svc_meth, const ArgsT& args,
                      std::function<void(bool, const ReplyT&)> cb,
                      std::atomic<bool>* cancel = nullptr) {
    CallAsync(svc_meth, args.Serialize(),
              [cb](bool ok, const std::string& data) {
                ReplyT reply;
                if (ok && !reply.Deserialize(data)) {
                  std::fprintf(stderr,
                               "ClientEnd::CallAsyncTyped: decode failed\n");
                  std::abort();
                }
                cb(ok, reply);
              },
              cancel);
  }

  const std::string& name() const { return endname_; }

 private:
  Network* net_;  // 裸指针即可：Network 的生命周期长于所有 ClientEnd
  std::string endname_;
};

// ---------------------------------------------------------------------------
// Service：一组可以被 RPC 调用的方法（对应 Go 的 Raft / KVServer 这类对象）
// ---------------------------------------------------------------------------
class Service {
 public:
  // 方法签名：输入编码后的参数，返回编码后的回复
  using Handler = std::function<std::string(const std::string&)>;
  // 移动构造
  Service(std::string name, std::unordered_map<std::string, Handler> methods)
      : name_(std::move(name)), methods_(std::move(methods)) {}

  std::string Dispatch(const std::string& method, const std::string& args);
  const std::string& name() const { return name_; }

 private:
  std::string name_;
  std::unordered_map<std::string, Handler> methods_;
};

// ---------------------------------------------------------------------------
// Server：一个网络节点，可以挂多个 Service（比如同时有 Raft 和 KVServer）
// ---------------------------------------------------------------------------
class Server {
 public:
  void AddService(std::shared_ptr<Service> svc);
  // svc_meth 形如 "Raft.AppendEntries"，args 是编码后的参数
  std::string Dispatch(const std::string& svc_meth, const std::string& args);
  int GetCount() const;

 private:
  mutable std::mutex mu_;
  std::unordered_map<std::string, std::shared_ptr<Service>> services_;
  int count_ = 0;
};

// ---------------------------------------------------------------------------
// Network：整个模拟网络
// ---------------------------------------------------------------------------
class Network {
 public:
  Network();
  ~Network();

  Network(const Network&) = delete;
  Network& operator=(const Network&) = delete;

  // 建一个发信端点（默认禁用，要先 Connect 再 Enable）
  std::shared_ptr<ClientEnd> MakeEnd(const std::string& endname);
  void AddServer(int servername, std::shared_ptr<Server> srv);
  void DeleteServer(int servername);
  void Connect(const std::string& endname, int servername);
  void Enable(const std::string& endname, bool enabled);
  // 注销一个发信端点（从 enabled_/connections_ 移除）。
  // 对齐 Go 版 labrpc 的 Network.Remove(endname)，由 config.deleteClient 调用：
  // 一个 client 被删除后，它那组 ClientEnd 不再存在于网络里，发往它们的
  // RPC 一律找不到目标而失败。仅删这两项即可（network 不持有 ClientEnd 本体，
  // 它的 shared_ptr 随调用方释放而析构）。
  void RemoveClientEnd(const std::string& endname);

  void Reliable(bool yes);
  void LongReordering(bool yes);
  void LongDelays(bool yes);

  int GetCount(int servername);   // 某台的入站 RPC 数
  int GetTotalCount() const;      // 全网发出的 RPC 总数
  int64_t GetTotalBytes() const;  // 全网传输的总字节数

  void Cleanup();  // 停线程、唤醒所有还在等的人

  // ---- 以下给 ClientEnd 用 ----
  void SendReq(std::shared_ptr<ReqMsg> req);
  bool IsStopped() const { return stopped_.load(); }

 private:
  enum class EventKind { kDispatch, kDeliverReply };

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

inline std::shared_ptr<Network> MakeNetwork() {
  return std::make_shared<Network>();
}

}  // namespace labrpc
