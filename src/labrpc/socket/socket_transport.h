// socket_transport.h —— Transport 的"真网络"后端（对标 Persister 的文件模式）
//
// 与 inproc/network.h（同进程共享内存）并列，是 Transport 的两个实现之一。
//
// 与 InProcessNetwork（同进程共享内存）不同，这里走真实的 TCP socket：
//   * 每个 server 在 host_:base_port+servername 上监听；
//   * 每个 ClientEnd 第一次发 RPC 时，懒连接到目标 server，之后复用这条 TCP 连接；
//   * 请求/回包都带一个 correlation id（corr_id），客户端用它在"同一连接"上把
//     回包匹配回当初那次 SendReq 的 ReqMsg（替代 in-proc 的共享内存 ReplySlot）。
//
// 这就把 labrpc 从"只能同进程模拟"变成了"可以跨进程 / 跨机"——默认 127.0.0.1
// loopback（本机网络），地址是标准的 host:port。
//
// host 的边界（写清楚，别让注释比代码更乐观）：
//   * 只支持 IPv4；host 在【构造期】用 getaddrinfo 解析一次，数字地址与可解析的
//     域名/主机名都行；解析失败会立刻报错，所有 RPC 立即失败 —— 绝不静默退化
//     成 loopback（那个坑见 .cpp 里 ResolveIPv4 的注释）。
//   * 一个 SocketNetwork 只有【一个】host：所有 server 都绑在这同一个地址上、
//     靠端口区分。所以"把 server 分散到多台机器"不能只改 host 实现 —— 那需要
//     每台机器各自持有自己的网络/端口表（本后端不做这件事）。
//
// 混沌旋钮语义与 in-proc 对齐：
//   * Reliable(false)：10% 丢请求（客户端直接判失败）+ 10% 丢回包（服务端回一个
//     "失败帧"，客户端立即失败，而不是干等超时——和 in-proc 的 ok=false 等价）。
//   * LongReordering(true)：2/3 概率把回包延迟 200~2200ms 再发，制造真实网络乱序。
//   * LongDelays：本后端不实现（真实网络里"端点通但 server 没了"由 connect 失败
//     直接暴露，不需要人为拖长；上层异步 RPC 天然消化这类失败）。
//
// ⚠️ 这是教学/可插拔示范实现，目标是"接口对齐 + 基本可用"，不是工业级网络栈：
//   单连接串行读写、无连接池、无 TLS、无流控。跑通 raft/kv 的基本用例足够；
//   不可靠模式下的极端时序稳定性以 in-proc 为准，socket 模式用于"真网络"验证。

#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "../labrpc.h"    // ClientEnd / Server / ReqMsg / ReplySlot / kMaxRpcMessageBytesDefault
#include "../transport.h" // Transport 抽象基类
#include "../../common/util.h"  // raftcpp::TimePoint（延迟帧队列）

namespace labrpc {

class SocketNetwork : public Transport {
 public:
  // host：监听/连接地址（默认 127.0.0.1）。构造期用 getaddrinfo 解析一次，
  //       可以是数字地址（"10.0.0.5"）或可解析的域名/主机名（"node-a.cluster"）；
  //       只支持 IPv4；解析失败会【明确报错并让所有 RPC 立即失败】——
  //       绝不静默退化成 loopback（见 .cpp 里 ResolveIPv4 的血案注释）。
  // base_port：server i 监听在 base_port + i；同机多实例靠端口区分。
  // 也可通过环境变量覆盖：LABRPC_SOCKET_HOST / LABRPC_SOCKET_BASE。
  explicit SocketNetwork(const std::string& host = "127.0.0.1",
                         int base_port = 19090);
  ~SocketNetwork() override;

  SocketNetwork(const SocketNetwork&) = delete;
  SocketNetwork& operator=(const SocketNetwork&) = delete;

  // ---- Transport 接口实现 ----
  std::shared_ptr<ClientEnd> MakeEnd(const std::string& endname) override;
  void AddServer(int servername, std::shared_ptr<Server> srv) override;
  void DeleteServer(int servername) override;
  void Connect(const std::string& endname, int servername) override;
  void Enable(const std::string& endname, bool enabled) override;
  void RemoveClientEnd(const std::string& endname) override;

  void Reliable(bool yes) override;
  void LongReordering(bool yes) override;
  void LongDelays(bool yes) override;

  int GetCount(int servername) override;
  int GetTotalCount() const override;
  int64_t GetTotalBytes() const override;

  // 某台 server 当前【滞留未回收】的 handler 线程数（观测/测试用）。
  // 只有本后端有这个概念（in-proc 是固定容量线程池 + 动态扩容），所以不进
  // Transport 接口。它存在的意义：把"连接 churn 会不会无限堆线程资源"变成
  // 一条可断言的事实（selftest TestHandlerThreadsReaped）。
  int GetHandlerThreadCount(int servername);

  // 客户端当前【滞留未回收】的 reader 线程数（观测/测试用）。
  // 与 GetHandlerThreadCount 对称：把"反复重连会不会无限堆线程资源"变成一条
  // 可断言的事实（selftest TestClientReaderThreadsReaped）。
  int GetReaderThreadCount();

  void SetMaxMessageBytes(size_t n) override;
  size_t MaxMessageBytes() const override;
  int64_t OversizedDropped() const override;

  void Cleanup() override;

  void SendReq(std::shared_ptr<ReqMsg> req) override;
  bool IsStopped() const override { return stopped_.load(); }

  // 客户端连接缓存（conns_）的复用判据。三条缺一不可：
  //   ① !closed        —— 没被 ShutdownConn 宣布死亡（fd 可能还没置 -1！）
  //   ② fd >= 0        —— 连接还没被关掉
  //   ③ conn_servername == want_servername —— 端点没被 Connect 改指向别的 server
  //
  // 为什么抽成一个可见的纯函数：这条判据曾经少写 ①（第 32 轮），而端到端的
  // 症状是【间歇】的（reader 被调度得快就测不到），用成功率当断言只会变成
  // flaky 赌局。抽成纯函数后，selftest 的 TestConnReusePredicate 可以逐条钉住
  // 三个子句，任何一条被删掉都会立刻红。
  static bool ConnReusable(int fd, bool closed, int conn_servername,
                           int want_servername) {
    return !closed && fd >= 0 && conn_servername == want_servername;
  }

 private:
  // 客户端（ClientEnd 侧）每条连到某个 server 的 TCP 连接
  struct Conn {
    int fd = -1;
    // 这条 TCP 连接当初连的是哪台 server（base_port_ + servername）。
    // 复用判据必须是「fd 有效【且】目标 server 没变」：Transport::Connect(endname, x)
    // 的语义是"这个端点指向 x"，改了目标之后旧连接必须废弃，否则 RPC 会沿着
    // 旧连接串台发到上一台 server（in-proc 后端每次 SendReq 都重读 connections_，
    // 天然没有这个问题 → 两个后端语义不一致，selftest TestRetargetServer 抓这个）。
    int servername = -1;
    // 注意：读回包线程不再挂在 Conn 上（否则 ~Conn() 可能在仍 joinable 的线程上
    // 触发 std::terminate）。线程统一收归 SocketNetwork::reader_threads_ 管理。
    std::mutex mu;                  // 保护 pending / 写串行化用 write_mu
    std::mutex write_mu;            // 同一 fd 的并发读写串行化（避免帧交错）
    std::unordered_map<uint64_t, std::shared_ptr<ReqMsg>> pending;  // corr_id -> 在途请求
    bool closed = false;
    // reader_exited：ConnReader 【函数最后一行】置位，表示"这个线程马上就要 return，
    // join 它不会阻塞"。只给 ReapFinishedReaderThreads 用，故是 atomic（reaper 在锁内
    // 读、reader 在锁外写，若复用 conn->mu/write_mu 判断反而要额外锁嵌套）。
    //
    // 为什么不用"fd 已置 -1"当判据：置 -1 之后线程还要给 pending 里的在途请求
    // 逐个投递失败结果（可能进 cb_q_ 抢 mu_），此时 join 会挡住 SendReq 的调用方。
    // （早先这里还有一个 reader_done 字段专门记"fd 已关"，但唯一的读点
    //  ——ShutdownConn 的兜底 close——已被证明是死代码删掉了；之后它变成只写
    //  不读的死状态，第 30 轮一并删除，免得后来人以为有地方依赖它。）
    std::atomic<bool> reader_exited{false};
  };

  // 服务端（Server 侧）每条客户端连接。
  // 存在的意义：LongReordering 的延迟回包由 DelayLoop 在【独立线程】里发送，
  // 与 HandleClient 的"读请求→派发→回包"循环并发写同一 fd → 必须有一把
  // 写锁 + 统一的 fd 关闭点（否则延迟帧可能写到已 close 又被复用的 fd 上，
  // 即 P0 的 fd-reuse 串台在服务端的重演）。
  struct SConn {
    int fd = -1;
    std::mutex write_mu;   // 帧发送 / fd 关闭互斥（HandleClient、DelayLoop、ShutdownServerThreads 同锁）
    // 没有单独的 closed 标志：fd >= 0 就是"还活着"的唯一判据，且所有读写都在
    // write_mu 下。DelayLoop 发延迟帧前检查 fd，HandleClient 退出时同锁把 fd 置 -1，
    // 于是"连接已废"与"帧可以发"由同一个值表达，不会出现两个标志互相打架。
    //
    // done：HandleClient 把 fd 关掉之后置位，表示"这个 handler 线程随时可以 join"。
    // 只给 ReapFinishedHandlers 用（AcceptLoop 定期回收）。语义上它与 fd 无关：
    // fd < 0 表示"连接不能再用"，done = true 表示"线程已经干完活、join 不会阻塞"。
    std::atomic<bool> done{false};
  };

  // 一条"延迟发送"的回包帧（LongReordering 用）
  struct DelayedFrame {
    std::shared_ptr<SConn> conn;
    uint64_t corr;
    std::string reply;
  };

  // 服务端（Server 侧）：监听 + 派发
  struct ServerState {
    int servername = -1;
    std::string addr;
    int listen_fd = -1;
    std::shared_ptr<Server> srv;
    std::thread acceptor;           // accept 循环
    std::atomic<bool> accept_stopped{false};  // B1: 让 poll 化 AcceptLoop 可被唤醒退出
    // accept() 瞬时错误（EMFILE/ENFILE/ECONNABORTED…）的日志限流计数：fd 长期打满
    // 时 poll 会不停返回、accept 会不停失败，不限制就是刷屏（每条几十字节 ×
    // 每秒 50 条）。只影响日志，不影响重试（重试永远继续，见 AcceptLoop）。
    std::atomic<int> accept_err_logged{0};
    // B1b: 唤醒管道（self-pipe）。只靠 accept_stopped 标志是不够的 —— poll() 只认
    // "被监视的 fd 上有事件"，而 close(lfd) 在 macOS 上【不能】唤醒正阻塞在
    // poll(lfd) 的线程（POSIX 对"close 与 poll 并发"也是未定义行为）。于是
    // DeleteServer/Cleanup 只能干等 AcceptLoop 的 500ms 超时才有机会看到标志。
    // 实测（/tmp/probe_split.cpp）：
    //   无连接、acceptor 已在 poll 中 → DeleteServer 固定 297~500ms；
    //   3 台 server 的 Cleanup → 501ms（并行等，所以只付一次）。
    // 而 raft 的每个 crash/restart（config.cpp 的 crash1/start1）都走 DeleteServer，
    // 每个 Config 析构都走 Cleanup → 全是纯死等。写一个字节到 wake_w，poll 立刻返回。
    // 写端设 O_NONBLOCK：万一管道被写满也不会把关停线程卡住（这里每个 server
    // 生命周期只会写一次）。
    int wake_r = -1;
    int wake_w = -1;
    // 注意：client_fds / handlers 统一由全局 mu_ 保护（与 AcceptLoop 推入时的锁一致）
    // 存 shared_ptr<SConn> 而非裸 fd：HandleClient 退出 / DelayLoop 发延迟帧
    // 都要安全地读写 fd，fd 的关闭与写入必须同锁（防 P0 的 fd-reuse 串台）。
    std::vector<std::shared_ptr<SConn>> client_conns;  // 当前活跃连接（shutdown 时唤醒 handler）
    // 每条连接一个处理线程（需 join，不能 detach）。
    // 为什么成对存 SConn：线程结束后 std::thread 仍然持有【已退出线程的栈】和
    // callable 里的 shared_ptr<SConn>，直到 join 才释放。早先只有裸 std::thread、
    // 且只在 DeleteServer/Cleanup 时统一 join —— 一个长期存活的 server 每被
    // "连一下就关"一次就永久多留一份线程资源（实测 3000 次短连接 → RSS +48MiB，
    // 且 DeleteServer 一 join 就回落到 +0.3MiB）。成对存之后由
    // ReapFinishedHandlers 按 done 标志及时回收。
    std::vector<std::pair<std::shared_ptr<SConn>, std::thread>> handlers;
  };

  // 懒连接：返回 endname 对应的 Conn（必要时建连并起 reader）
  std::shared_ptr<Conn> GetOrCreateConn(const std::string& endname,
                                        int servername);

  // 读线程：一个 Conn 一个，循环读回包帧并投递
  void ConnReader(std::shared_ptr<Conn> conn, std::string endname);

  // 服务端 accept 循环（poll 化，靠 per-server accept_stopped 退出）
  // listen_fd 由 AddServer 按值捕获传入，AcceptLoop 不再读 st->listen_fd，
  // 以规避与 DeleteServer 同字段写操作的数据竞争（TSan 已抓到）。
  void AcceptLoop(std::shared_ptr<ServerState> st, int listen_fd);
  // 单条客户端连接的处理循环（连接对象化：fd 生命周期受 SConn::write_mu 保护）
  void HandleClient(int servername, std::shared_ptr<SConn> conn);
  // B1b: 往 st 的唤醒管道写一个字节，让阻塞在 poll() 里的 AcceptLoop 立刻返回。
  // 必须在 st->accept_stopped 置位【之后】调用（acceptor 醒来会复查标志）。
  void WakeAcceptor(ServerState& st);
  // 关掉 st 的唤醒管道。必须在该 server 的 acceptor 线程 join 之后调用 ——
  // 否则 acceptor 可能还在 poll 这两个 fd（close 正在被 poll 的 fd 是未定义行为）。
  void CloseAcceptorWakePipe(ServerState& st);
  // 回收 st->handlers 里 conn->done 已置位的线程（join 会释放线程栈与 callable）。
  // 由 AcceptLoop 定期调用（≤250ms 一次）；持 mu_ 摘下、锁外 join。
  void ReapFinishedHandlers(std::shared_ptr<ServerState> st);
  // 回收 reader_threads_ 里 reader_exited 已置位的线程（join 释放线程栈 + Conn）。
  // 由 GetOrCreateConn 在每次新建连接后调用；持 mu_ 摘下、锁外 join。
  // 注意 join 对象是"已经执行到最后一行"的线程，所以不会阻塞调用方
  // （SendReq 可能跑在 Raft 的 replicator / ticker 线程上）。
  void ReapFinishedReaderThreads();
  // 关掉某 server 的所有活跃连接（shutdown 唤醒阻塞的 recv）+ join 其 handler
  void ShutdownServerThreads(std::shared_ptr<ServerState> st);

  // ---- LongReordering：延迟回包 ----
  // 把一次回包帧挂进延迟队列（不阻塞 HandleClient 的处理循环），
  // 由 DelayLoop 线程到点后发送。语义与 in-proc Network 对齐：
  // 延迟的是【回包投递】，不是服务端处理能力。
  void ScheduleDelayedReply(std::shared_ptr<SConn> conn, uint64_t corr,
                            std::string reply, int64_t delay_ms);
  // 延迟帧 sender 线程主体（kDelayWorkers 个）
  void DelayLoop();

  // 把一次结果（成功/失败）投递回 ClientEnd（填 ReplySlot 或跑 cb）。
  // 注意：cb 绝不内联执行——DeliverResult 的调用方可能是持锁的 Raft 线程
  // （SendReq 的快速失败路径），也可能是 reader 线程。cb 一律进 cb_q_
  // 由 cb_worker_ 执行，与 in-proc Network::Finish 把回调 Post 进线程池
  // 的语义对齐（in-proc 明确"绝不能在定时器线程里直接跑回调"）。
  void DeliverResult(std::shared_ptr<ReqMsg> req, bool ok, std::string reply);

  // 通知连接关闭并唤醒阻塞的 I/O；fd 的实际 close 由 ConnReader 负责
  // （防跨线程 close 引发 fd 复用竞态，对应 P0）
  void ShutdownConn(std::shared_ptr<Conn> conn);

  std::string host_;
  int base_port_;
  // host_ 解析一次后的 IPv4 地址（网络字节序）+ 是否解析成功。
  //
  // 为什么要在构造期解析并缓存在这里：早先 bind / connect 两处各自调
  // ::inet_addr(host_)，而 inet_addr 只认【数字点分】字面量 —— 域名、主机名、
  // 或打错一位的数字地址（"127.0.0.999"）一律返回 INADDR_NONE，旧代码遇到它
  // 就兜底成 INADDR_LOOPBACK，于是配置被【静默改写】成 127.0.0.1：
  // 单机测试看不出问题，多机部署则每台机器都在跟自己的 loopback 说话，
  // 集群永远收不到对方的 RPC 且零报错（实测 /tmp/probe_host.cpp）。
  // 现在解析失败 → host_ok_ = false → AddServer 不监听、GetOrCreateConn 直接
  // 返回 nullptr（RPC 立即失败），错误在构造期就打出来了。
  uint32_t host_addr_be_ = 0;
  bool host_ok_ = true;

  // base_port_（以及 base_port_ + servername）是否是一个合法端口。
  // 与 host_ok_ 同一纪律：非法配置【明确拒绝 + 报错】，绝不截断/回绕成另一个端口
  // —— atoi("abc")==0、atoi("-5")==-5、70000 & 0xFFFF == 4464，三者都会让
  // "监听在 19090" 悄悄变成"监听在某个别的端口"（实测 /tmp/probe_baseport.cpp：
  // base=70000 时 RPC 居然成功，因为它和客户端一起回绕到了 4464）。
  bool port_ok_ = true;

  std::atomic<bool> stopped_{false};

  std::mutex mu_;  // 保护下面所有 map / 标志
  std::map<std::string, bool> enabled_;
  std::map<std::string, int> connections_;  // endname -> servername
  std::map<int, std::shared_ptr<ServerState>> servers_;
  std::map<std::string, std::shared_ptr<Conn>> conns_;  // endname -> 客户端连接
  // 客户端读回包线程统一收归此处（受 mu_ 保护），由 Cleanup 统一 join。
  // 不放进 Conn，是为了避免 ~Conn() 在仍 joinable 的 reader 线程上触发 terminate。
  //
  // 为什么成对存 Conn：和 ServerState::handlers 是【同一个坑的另一半】——
  // std::thread 在 join 之前一直持有已退出线程的栈 + callable 里按值捕获的
  // shared_ptr<Conn>。早先这里只有裸 std::thread 且只在 Cleanup 时统一 join，
  // 于是"客户端反复断开重连"（RemoveClientEnd / DeleteServer / Connect 改指向
  // 都会撕掉旧连接）每发生一次就永久多留一份线程资源。实测（/tmp/probe_reader_leak
  // .cpp：3000 轮解绑重连，server 全程存活）：
  //   RSS +65.0 MiB（≈16 KiB/轮，线性）；静置 1s 仍是 +47.6 MiB；
  //   Cleanup 一 join 立刻回落到 +0.6 MiB ← 是"join 前不释放"，不是真泄漏。
  // 成对存之后由 ReapFinishedReaderThreads 在"下次建连时"顺手回收（客户端没有
  // 常驻轮询线程，建连点就是天然的回收点：不建新连接就不会产生新的滞留）。
  std::vector<std::pair<std::shared_ptr<Conn>, std::thread>> reader_threads_;

  std::atomic<bool> reliable_{true};
  std::atomic<bool> long_reordering_{false};
  // 没有 long_delays_ 成员：LongDelays 在本后端是【真正的空操作】（真实网络里
  // "端点通但 server 没了"由 connect/EOF 直接暴露，不需要人为拖延）。留一个
  // 永不读写的 atomic 字段只会让后来人以为"这里其实支持、只是没接线"——它跟
  // 第 16/19 轮清掉的死旋钮是同一类病。

  std::atomic<int> count_{0};
  std::atomic<int64_t> bytes_{0};
  std::atomic<size_t> max_message_bytes_{kMaxRpcMessageBytesDefault};
  std::atomic<int64_t> oversized_dropped_{0};

  std::atomic<uint64_t> next_corr_{1};

  // ---- LongReordering：延迟回包队列（mu_ 保护）----
  // 为什么不能像最初那样在 HandleClient 里 sleep：handler 线程被睡住，
  // 同一连接上的后续请求全部队头阻塞（Figure8 里 leader 的心跳/复制全走
  // 一条 TCP 连接 → 复制停顿 → 反复选举 → 用例超时，实测 net 模式
  // TestFigure8Unreliable2C 稳定失败）。in-proc 的等价语义是"定时器事件
  // 延迟"（等待不占线程），这里用独立 sender 线程逼近同一语义。
  std::multimap<raftcpp::TimePoint, DelayedFrame> delay_q_;
  std::condition_variable delay_cv_;
  std::vector<std::thread> delay_workers_;  // 由 Cleanup 统一 join
  static constexpr size_t kDelayWorkers = 4;

  // ---- 异步回调投递队列（mu_ 保护）----
  // cb 的唯一执行者是 cb_worker_，绝不内联跑在调用方/reader 线程上。
  // 旧实现在 SendReq 的快速失败路径（断连/丢请求/无连接/超大帧/连接已废）
  // 里【直接】在调用方线程上调 cb——而调用方可能是 Raft 的 replicator /
  // ticker 线程。当前所有 Raft 回调恰好都在 !ok 时立即返回、且调用点都在
  // 锁外，内联暂未致命；但"回调跑在持锁的调用方线程上"是个将来谁加个会
  // 阻塞 / 会重入的回调就死锁的陷阱，且与 in-proc（Finish 里 Post 到
  // 线程池）语义不一致。统一进队列后：cb 永远跑在本后端的投递线程上。
  // stopped_ 即退、残留回调丢弃——与 in-proc 的 Post 在 stopped_ 后丢弃一致。
  void CbWorkerLoop();
  std::deque<std::function<void()>> cb_q_;
  std::condition_variable cb_cv_;
  std::thread cb_worker_;  // 由 Cleanup 统一 join
};

}  // namespace labrpc
