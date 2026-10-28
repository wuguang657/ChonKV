// labrpc.h —— RPC 框架（Go 版 src/labrpc/labrpc.go 的 C++ 移植）
//
// 这里放的是【传输无关】的框架部分，被两个传输后端共用：
//   * ClientEnd：一个"发信端点"，Raft 通过它给别的 server 发 RPC；
//   * Service / Server：一组可被 RPC 调用的方法 / 一个网络节点；
//   * ReqMsg / ReplySlot：一封"信"和同步调用的"收件箱"。
//
// 传输后端（都实现 transport.h 的 Transport 抽象）：
//   * inproc/network.{h,cpp}：同进程模拟网络（丢包/延迟/乱序/断网），默认后端；
//   * socket/socket_transport.{h,cpp}：真实 TCP（默认 127.0.0.1 loopback）。
// 选择哪一个由 transport_factory.h 的 MakeTransport() 决定（读环境变量
// LABRPC_TRANSPORT），上层（raft::Config / kvraft::Config）只依赖 Transport。
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
#include "transport.h"  // 传输层抽象基类：ClientEnd 持有 Transport*，具体后端见 inproc/ 与 socket/

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
  std::atomic<bool> delivered{false};  // socket 后端：保证 DeliverResult 恰好一次（防并发失败路径双投）
};

// ---------------------------------------------------------------------------
// ClientEnd：一个"发信端点"，Raft 通过它给别的 server 发 RPC
// ---------------------------------------------------------------------------
class ClientEnd {
 public:
  ClientEnd(Transport* net, std::string endname)
      : net_(net), endname_(std::move(endname)) {}

  // 发一个 RPC，阻塞等回复。
  // 返回 true 表示服务端执行了、reply 有效；false 表示丢包/断连/服务器挂了。
  //
  // cancel：可选的"紧急刹车"。Go 版靠随时可丢弃的 goroutine 天然不会卡住；
  // C++ 的线程必须能自己醒来退出，否则 Kill() 之后线程可能还要等 7 秒
  // （断连是立即失败，但"端点仍通、目标 server 已删"的失败回包会随机延迟
  //   0~7000ms，见 labrpc.cpp 的 SendReq——config 里开着 LongDelays）。
  // 传入一个 atomic<bool>，把它置 true 就能让所有在途的 Call() 在 50ms 内返回 false。
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
  //   如果用同步 Call，往"端点仍通但 server 已删"的节点发的 RPC 会被模拟网络
  //   拖住 0~7000ms（config 里开了 LongDelays），整条复制线程就卡死了 —— 节点
  //   重连后要等最多 7 秒才收得到心跳。TestReElection2A 就是专门抓这个的。
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
  Transport* net_;  // 裸指针即可：Transport 的生命周期长于所有 ClientEnd
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

  // Dispatch：找不到方法就 std::abort() —— 适合【本方代码】直接调用的场景
  // （方法名是编译期常量，写错了就是程序 bug，早点炸出来最好）。
  std::string Dispatch(const std::string& method, const std::string& args);

  // TryDispatch：找不到方法只返回 false，不 abort。
  // 【专供传输层使用】——socket 后端的方法名是从网线上读出来的【远程输入】
  // （可能来自别的进程/机器，也可能因极端帧错位变成垃圾）。远程输入绝不能让
  // 进程 std::abort()：那等于任何人连上端口发 30 字节就能杀掉整个测试/服务进程
  // （实测复现：一帧 "Echo.NoSuchMethod" → SIGABRT 134）。
  bool TryDispatch(const std::string& method, const std::string& args,
                   std::string* reply);
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
  // svc_meth 形如 "Raft.AppendEntries"，args 是编码后的参数。
  // Dispatch：未知 service / 方法名不含 '.' → std::abort()（本方调用用）。
  std::string Dispatch(const std::string& svc_meth, const std::string& args);
  // TryDispatch：同上但【不 abort】，不认识就返回 false（传输层用，见 Service::TryDispatch）。
  bool TryDispatch(const std::string& svc_meth, const std::string& args,
                   std::string* reply);
  int GetCount() const;

 private:
  mutable std::mutex mu_;
  std::unordered_map<std::string, std::shared_ptr<Service>> services_;
  int count_ = 0;
};

// ---------------------------------------------------------------------------
// C3 生产加固：单条 RPC 消息字节上限
// ---------------------------------------------------------------------------
// 限制【单条 RPC 请求】序列化后的字节数。超限的消息不会被投递给服务端，
// 而是立即以失败回信（ok=false）返回调用方，形成"拒绝服务"而非"内存被打爆"。
//
// 为什么要这条：Raft 的 InstallSnapshot 载荷就是整个状态机快照，一旦上层
// 状态机膨胀（或收到畸形的超大条目），单条消息可以轻易到 GB 级 —— 发送侧
// 要整块序列化、接收侧要整块反序列化，两侧内存同时被顶穿，且没有任何
// 协议层面的兜底。etcd 的 --max-request-bytes（默认 1.5MiB）就是同一道闸门。
//
// 默认 64MiB 是刻意放宽的：lab 的快照用例远小于此，现有 67 个用例零行为变更；
// 生产部署或测试可用 SetMaxMessageBytes() 收紧。
constexpr size_t kMaxRpcMessageBytesDefault = 64u * 1024u * 1024u;  // 64 MiB

}  // namespace labrpc
