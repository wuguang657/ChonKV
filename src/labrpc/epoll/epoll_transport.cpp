// epoll_transport.cpp —— EpollNetwork 骨架（你要实现的部分都标了 TODO(n)）
//
// ===========================================================================
// 这个文件想教会你什么：一张"阻塞 I/O 思维"到"事件驱动思维"的翻译表
// ===========================================================================
//
//   SocketNetwork（每连接一线程）      EpollNetwork（单线程事件循环）
//   -------------------------------    -------------------------------------
//   read(fd) 阻塞直到有数据            read(fd) 只在 epoll 说"可读"之后调，
//                                     而且必须一直读到 EAGAIN 为止
//   RecvAll 读满 n 字节才返回          FrameReader 攒半帧，切得出才处理
//   write(fd) 阻塞直到写完             write 只写内核愿意收的部分，剩下的进
//                                      输出队列，等 EPOLLOUT 再写
//   "一条连接一个线程，天然串行"        "所有连接共享一个线程，靠状态机串行"
//   线程回收 = join                    没有线程要回收，只有 fd 要注销
//   shutdown() 唤醒阻塞的 read         没有阻塞的 read，唤醒只用于"别的线程
//                                      有新写入任务"（eventfd）
//
// ===========================================================================
// 练习清单（搜索 "TODO(" 可全部定位；建议按顺序做）
// ===========================================================================
//   TODO(1) Start()          —— 建 poller / 唤醒 fd / 起事件循环与 worker
//   TODO(2) LoopMain()       —— epoll_wait + 按 token 三路分派 + 跨线程任务
//   TODO(3) OnAccept()       —— 一个 token 上 accept 到 EAGAIN
//   TODO(4) OnReadable()     —— read 到 EAGAIN + 切帧 + 分发
//   TODO(5) FlushWrites()    —— 输出队列 + EPOLLOUT 开关（回压）
//   TODO(6) OnClientFrame()  —— corr 匹配在途请求 → DeliverResult
//   TODO(7) OnServerFrame()  —— 派给 worker → TryDispatch → 回包入队
//   TODO(8) SendReq()        —— 线程安全入口：入队 + 唤醒
//   TODO(9) GetOrCreateConn()—— 非阻塞 connect（EINPROGRESS）与复用判据
//   TODO(10) Cleanup()       —— 有序关停（顺序错了就是 use-after-free / 挂死）
//
// 已经送给你的部分（不需要你写，但建议读一遍理解契约）：
//   * wire.h：帧编解码 + 半帧解析（纯内存，macOS 上也能测）
//   * poller.{h,cpp}：epoll 的 4 个动作 + eventfd 唤醒
//   * 本文件里的：连接簿记 CloseConn/FailPending、worker 池、DeliverResult、
//     统计与旋钮、AddServer 里的 socket/bind/listen 样板代码
//
// ===========================================================================
// 两把锁的模型（写事件循环之前先把这张表记牢，否则会死锁）
// ===========================================================================
//   Impl::mu     保护"全局表"：enabled_ / connections_ / servers_ / conns_
//                以及"待处理跨线程请求队列"。持有时间必须极短（只做 map 操作）。
//   EpConn::mu   保护"单连接状态"：pending（在途请求表）+ out（输出队列）。
//
//   两条铁律：
//     1) 绝不在持有 EpConn::mu 时去拿 Impl::mu，反之亦然 —— 要拿就先放。
//     2) 绝不在持锁时跑用户回调（DeliverResult 的 cb）或 TryDispatch ——
//        用户代码可能回头调 SendReq，而那要拿锁 → 非递归锁自死锁。
//        SocketNetwork 就是踩了这个坑之后才把 cb 全部赶进 cb_worker_ 的。
//
// ===========================================================================
// fd 归属权（这是 epoll 后端与 socket 后端【差别最大】的地方）
// ===========================================================================
//   socket 后端：fd 由"持有它的那个线程"关闭（ConnReader/HandleClient 退出块），
//                因为阻塞 read 只有本线程退出了才安全。
//   epoll 后端：fd 只有【一个】关闭点 —— 事件循环线程（CloseConn）。
//                别的线程发现连接坏了（SendReq 写失败、Cleanup）只能：
//                  (a) 置 conn->dead = true，(b) WakeUp()，(c) 立刻返回。
//                绝不在非 loop 线程里 ::close —— close 之后 fd 号会被下一个
//                socket() 立刻复用，而 loop 线程手里还有这个号码（经典的
//                fd-reuse 串台：写到别人的连接上，且没有任何报错）。

#include "epoll_transport.h"

#include <fcntl.h>   // fcntl(F_SETFL, O_NONBLOCK)
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "../../common/util.h"  // raftcpp::RandInt（不可靠模式用）
#include "../labrpc.h"       // ClientEnd / Server / ReqMsg / ReplySlot
#include "poller.h"
#include "wire.h"

namespace labrpc {

namespace {

// 只在第一次调用时打印一次。用途：TODO 还没实现时给出"看得见"的反馈，
// 而不是让测试静默挂死（挂死是最贵的失败模式：看门狗 600 秒之后才报一个
// 与真正原因无关的 timeout）。
void WarnOnce(const char* what) {
  static std::mutex mu;
  static std::unordered_map<const char*, int> seen;
  std::lock_guard<std::mutex> lk(mu);
  int& n = seen[what];
  if (n++ == 0) std::fprintf(stderr, "[labrpc/epoll] %s\n", what);
}

// 严格解析端口（十进制、整串、1..65535）。理由见 socket_transport.cpp 里
// ParsePortStrict 的注释：atoi("abc")==0 会让 bind(0) 变成"内核随便挑一个端口"，
// 而客户端仍然去连 base_port+i → 全部失败且零日志。
bool ParsePortStrict(const char* s, int* out) {
  if (s == nullptr || *s == '\0') return false;
  errno = 0;
  char* end = nullptr;
  const long v = std::strtol(s, &end, 10);
  if (errno != 0 || end == s || *end != '\0') return false;
  if (v < 1 || v > 65535) return false;
  *out = static_cast<int>(v);
  return true;
}

bool PortsInRange(int base_port, int servername) {
  return base_port >= 1 && servername >= 0 && base_port + servername <= 65535;
}

// host -> IPv4（网络字节序）。必须在构造期解析一次并缓存，失败就明确报错：
// 若退回 ::inet_addr + "失败就 loopback"，多机部署会变成"每台机器都在跟自己的
// 环回说话"，集群起得来但永远收不到对方的 RPC，且零报错。
bool ResolveIPv4(const std::string& host, uint32_t* out_be) {
  addrinfo hints{};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* res = nullptr;
  const int rc = ::getaddrinfo(host.c_str(), nullptr, &hints, &res);
  if (rc != 0 || res == nullptr) {
    std::fprintf(stderr, "[labrpc/epoll] getaddrinfo(\"%s\") 失败: %s\n",
                 host.c_str(), rc != 0 ? ::gai_strerror(rc) : "no result");
    if (res != nullptr) ::freeaddrinfo(res);
    return false;
  }
  bool ok = false;
  for (addrinfo* p = res; p != nullptr; p = p->ai_next) {
    if (p->ai_family == AF_INET && p->ai_addr != nullptr &&
        p->ai_addrlen >= sizeof(sockaddr_in)) {
      *out_be = reinterpret_cast<sockaddr_in*>(p->ai_addr)->sin_addr.s_addr;
      ok = true;
      break;
    }
  }
  ::freeaddrinfo(res);
  return ok;
}

// 非阻塞化。注意不要用 socket(..., SOCK_NONBLOCK) 或 accept4()：
// 那两个是 Linux 专属，用了这个文件在 macOS 上就编不过，而"能在 macOS 上
// 编过"正是本骨架的设计目标（见 poller.h 的注释）。
bool SetNonBlocking(int fd) {
  const int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0) return false;
  return ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

}  // namespace

// ===========================================================================
// Impl：所有实现细节。头文件里只有一句 `std::unique_ptr<Impl>`。
// ===========================================================================

struct EpollNetwork::Impl {
  // ---- 一条连接（客户端侧与服务端侧共用同一个结构）----
  // 为什么合并：两侧的"读帧/切帧/写队列/回压"完全相同，只有"帧该交给谁"不同
  // （客户端交给 pending 表，服务端交给 worker）。分开写两份会有一份长歪。
  struct EpConn {
    int fd = -1;

    // 这条连接属于谁：
    //   servername >= 0 && endname.empty() → 服务端侧（accept 出来的）
    //   servername >= 0 && !endname.empty() → 客户端侧（连到 servername 的）
    int servername = -1;
    std::string endname;  // 客户端侧：属于哪个发信端点
    std::shared_ptr<Server> srv;  // 服务端侧：这条连接归属的 Server 对象

    // ---- 读：半帧状态机 ----
    wire::FrameReader reader{kMaxRpcMessageBytesDefault};

    // ---- 写：输出队列 + 回压 ----
    // 为什么必须有队列：非阻塞 write 可能只写进去一部分（内核发送缓冲满），
    // 剩下的必须留着等 EPOLLOUT。这也顺手解决了 socket 后端靠 write_mu 串行化
    // 的那个问题 —— 事件循环本来就是单线程写，天然不会帧交错。
    std::deque<std::string> out;
    size_t out_bytes = 0;
    bool registered = false;  // 是否已经出现在 poller 的关注集里
    bool want_write = false;  // 当前关注集里有没有 EPOLLOUT（用来省掉无谓的 epoll_ctl）

    // ---- 生命周期 ----
    // dead 只能由非 loop 线程置位（表示"我发现它坏了，请 loop 收尾"）；
    // closed 只能由 loop 线程在 CloseConn 里置位。两者都不是原子的理由：
    // dead 用 atomic（跨线程），closed 只 loop 线程读写。
    std::atomic<bool> dead{false};
    bool closed = false;
    uint64_t token = 0;

    // ---- 单连接状态锁 ----
    // 保护 pending 与 out。持有时间必须极短（只做容器操作，绝不在锁里 write）。
    std::mutex mu;
    std::unordered_map<uint64_t, std::shared_ptr<ReqMsg>> pending;  // corr -> 在途请求
    // 正在非阻塞 connect 的连接：客户端会在 EPOLLOUT/EPOLLERR 时据此判断
    bool connecting = false;
  };

  struct ServerRec {
    int servername = -1;
    int listen_fd = -1;
    uint64_t token = 0;
    std::shared_ptr<Server> srv;
  };

  // ---- token 空间 ----
  // epoll 只回传 token，不回传 fd，所以 token 必须能反查"这是什么"。
  // 分段是为了让分派是三次比较，而不是查一张表。
  static constexpr uint64_t kWakeToken = 0;
  static constexpr uint64_t kListenTokenBase = 1000;    // +servername
  static constexpr uint64_t kConnTokenBase = 1000000;   // 连接，自增分配

  // ---- 配置与统计 ----
  std::string host;
  int base_port = 0;
  uint32_t host_addr_be = 0;
  bool host_ok = true;
  bool port_ok = true;

  std::atomic<bool> stopped{false};
  std::atomic<bool> open_ok{false};  // poller 是否起来了（false 时一切 RPC 立即失败）
  std::atomic<bool> reliable{true};
  std::atomic<bool> long_reordering{false};
  std::atomic<size_t> max_message_bytes{kMaxRpcMessageBytesDefault};
  std::atomic<int64_t> oversized_dropped{0};
  std::atomic<int> total_count{0};
  std::atomic<int64_t> total_bytes{0};
  std::atomic<uint64_t> next_corr{1};
  std::atomic<uint64_t> next_conn_token{kConnTokenBase};

  // ---- 事件循环 ----
  Poller poller;
  std::thread loop_thread;

  // ---- 全局表（Impl::mu 保护）----
  std::mutex mu;
  std::map<std::string, bool> enabled;       // endname -> 是否可发
  std::map<std::string, int> connections;    // endname -> servername
  std::map<int, ServerRec> servers;          // servername -> 监听信息
  std::unordered_map<uint64_t, std::shared_ptr<EpConn>> conns;  // token -> 连接
  // 【跨线程投递区】别的线程不能直接改连接状态（那会与 loop 线程竞争），
  // 只能把"要做的事"塞进来 + WakeUp()，由 loop 线程独占执行。
  //   待建连请求：SendReq 发现没有可用连接时塞一个进来
  //   待唤醒标志：见 TODO(2) 里的 drain 逻辑
  std::deque<std::shared_ptr<ReqMsg>> pending_send;
  // 待关闭的监听 fd（DeleteServer 委托给 loop 线程关）。
  // 为什么不在 DeleteServer 里直接 Del+close：那样会在【非 loop 线程】关 fd，
  // 而 loop 线程此刻可能刚拿到这个 fd 的就绪事件、正要 accept 它 —— 一个
  // close 之后 fd 号被下一个 socket() 复用，accept 就接到了别人的连接上。
  // 这类竞态不会报错、只会串台，事后再查基本查不动。
  std::deque<int> pending_close_listen;

  // ---- worker 池 ----
  // 为什么必须有：TryDispatch 跑的是【真 RPC handler】（Raft::AppendEntries 之类
  // ——它会抢 raft 的 mu_、可能等 fsync）。这种调用一旦放进事件循环，一个慢
  // handler 就会把【所有】连接一起堵死（socket 后端靠"每连接一线程"天然隔离，
  // 事件循环里没有这层隔离，只能靠线程池补回来）。
  // 顺便：回包回调（DeliverResult 的 cb）也丢这里跑 —— 这正是 epoll 后端能比
  // socket 后端快的结构性原因：那边所有 cb 挤在【一个】cb_worker_ 上。
  std::vector<std::thread> workers;
  std::deque<std::function<void()>> tasks;
  std::mutex task_mu;
  std::condition_variable task_cv;
  static constexpr size_t kWorkers = 4;

  // =========================================================================
  // 已实现部分
  // =========================================================================

  void PostTask(std::function<void()> fn) {
    {
      std::lock_guard<std::mutex> lk(task_mu);
      if (stopped.load()) return;
      tasks.push_back(std::move(fn));
    }
    task_cv.notify_one();
  }

  void WorkerLoop() {
    std::unique_lock<std::mutex> lk(task_mu);
    while (true) {
      task_cv.wait(lk, [this] { return stopped.load() || !tasks.empty(); });
      if (tasks.empty()) {
        if (stopped.load()) return;  // 已停且队列空：退出
        continue;
      }
      auto fn = std::move(tasks.front());
      tasks.pop_front();
      lk.unlock();   // 铁律②：任务在锁外跑
      fn();
      lk.lock();
    }
  }

  // 把结果投递回 ClientEnd（填 ReplySlot 或跑 cb）。语义与 in-proc Network::Finish
  // 和 SocketNetwork::DeliverResult 逐条对齐，三处必须一致否则后端之间会打架。
  void DeliverResult(const std::shared_ptr<ReqMsg>& req, bool ok, std::string reply) {
    if (!req) return;
    if (req->cancel && req->cancel->load()) return;   // 被 Kill：结果作废
    if (req->delivered.exchange(true)) return;        // 恰好一次投递
    if (ok) total_bytes.fetch_add(static_cast<int64_t>(reply.size()));

    if (req->cb) {
      // cb 绝不内联：本函数的调用者可能是事件循环线程（绝对不能跑用户代码），
      // 也可能是 SendReq 的快速失败路径（调用方是 Raft 的 replicator 线程）。
      std::function<void()> task;
      if (ok) {
        std::string r = std::move(reply);
        task = [cb = std::move(req->cb), r]() { cb(true, r); };
      } else {
        task = [cb = std::move(req->cb)]() { cb(false, std::string()); };
      }
      PostTask(std::move(task));
      return;
    }
    if (!req->slot) return;
    std::lock_guard<std::mutex> lk(req->slot->mu);
    req->slot->ok = ok;
    req->slot->reply = std::move(reply);
    req->slot->done = true;
    req->slot->cv.notify_all();
  }

  // 把连接上所有在途请求判失败（锁外投递，避免持锁跑用户 cb 自死锁）。
  void FailPending(const std::shared_ptr<EpConn>& c, std::vector<std::shared_ptr<ReqMsg>>* out) {
    std::lock_guard<std::mutex> lk(c->mu);
    for (auto& kv : c->pending) out->push_back(kv.second);
    c->pending.clear();
    c->out.clear();
    c->out_bytes = 0;
  }

  // 【唯一的关闭点】只能在事件循环线程里调用。
  // 顺序有讲究，别乱：
  //   1) poller.Del(fd)   —— 先从 epoll 里摘掉，否则下一次 Wait 还会给你事件，
  //                          而你手里这个 fd 可能已经变成了别人的。
  //   2) ::close(fd)      —— 再关。
  //   3) 把 pending 判失败 —— 最后投递（锁外），这步可能触发用户 cb，
  //                          绝不能在持任何锁时做。
  void CloseConn(const std::shared_ptr<EpConn>& c, const char* why) {
    if (!c || c->closed) return;
    c->closed = true;
    if (c->fd >= 0) {
      poller.Del(c->fd);
      ::close(c->fd);
      c->fd = -1;
    }
    std::vector<std::shared_ptr<ReqMsg>> failed;
    FailPending(c, &failed);
    {
      std::lock_guard<std::mutex> lk(mu);
      auto it = conns.find(c->token);
      if (it != conns.end() && it->second.get() == c.get()) conns.erase(it);
      // 客户端连接还要从 connections 表里摘掉吗？不 —— connections 是"端点指向
      // 哪台 server"的配置，不是连接本身。连接断了，端点仍然指向那台，下次
      // SendReq 会重建连接。这就是 ConnReusable 判据里 fd>=0 那一条的意义。
    }
    if (why != nullptr && std::strcmp(why, "eof") != 0) {
      std::fprintf(stderr, "[labrpc/epoll] 关闭连接 token=%llu: %s\n",
                   static_cast<unsigned long long>(c->token), why);
    }
    for (auto& r : failed) DeliverResult(r, false, "");
  }

  // 给连接注册/更新关注集。want_read 恒为 true（连接总要读），
  // 写方向按 out 是否为空决定 —— 这就是"回压"的全部：没人要写就别让内核为
  // EPOLLOUT 反复叫你（把内核当忙等轮询器用，是 epoll 最经典的浪费 CPU 方式）。
  bool UpdateInterest(const std::shared_ptr<EpConn>& c) {
    if (!c || c->fd < 0) return false;
    const bool need_write = !c->out.empty() || c->connecting;
    const uint32_t mask = Poller::kReadable | (need_write ? Poller::kWritable : 0);
    if (!c->registered) {
      c->registered = true;
      c->want_write = need_write;
      return poller.Add(c->fd, c->token, mask);
    }
    if (c->want_write == need_write) return true;  // 关注集没变：省掉一次 epoll_ctl
    c->want_write = need_write;
    return poller.Mod(c->fd, c->token, mask);
  }

  // 别的线程怎么让事件循环立刻干活：塞任务 + 踢醒。
  void WakeLoop() { poller.WakeUp(); }

  // =========================================================================
  // TODO 区：下面这些是你来实现的
  // =========================================================================

  // 起 poller、唤醒 fd、事件循环线程、worker 池。
  bool Start() {
    // TODO(1): 按这个顺序做，顺序错了会出现"fd 已注册但没人 Wait"或
    //          "Wait 在跑但 poller 还没建"这类难查的启动竞态。
    //
    //   a) poller.Open()                 失败 → return false（打印 errno）
    //   b) poller.WakeOpen()             失败 → return false
    //   c) poller.Add(poller.wake_fd(), kWakeToken, Poller::kReadable)
    //      —— 让"唤醒"本身也成为一个 fd 事件，否则别的线程没有任何办法叫醒
    //         一个正阻塞在 Wait 里的循环。
    //   d) open_ok = true; 起 kWorkers 个 std::thread 跑 WorkerLoop()
    //   e) 起 1 个 std::thread 跑 LoopMain()
    //   f) 全部成功才 return true。
    WarnOnce("TODO(1) Start() 还没实现：事件循环没起来，所有 RPC 都会失败");
    return false;
  }

  // 事件循环主体。
  void LoopMain() {
    // TODO(2): 结构如下（照抄这个形状就行，细节自己想）：
    //
    //   PollEvent evs[64];
    //   while (!stopped.load()) {
    //     int timeout_ms = NextTimeoutMs();     // 没有定时器就传 -1
    //     int n = poller.Wait(evs, 64, timeout_ms);
    //     if (n < 0) { 打印 errno; break; }      // 真错误才退出
    //     for (int i = 0; i < n; i++) { Dispatch(evs[i]); }
    //     DrainPendingCrossThreadWork();         // 见下面两段注释
    //   }
    //
    // 三路分派（按 token 段）：
    //   token == kWakeToken                        → poller.DrainWake()
    //   token in [kListenTokenBase, kConnTokenBase) → OnAccept(servername = token - kListenTokenBase)
    //   其它                                        → 查 conns[token]，按 mask 调
    //                                                OnReadable / FlushWrites / CloseConn
    //
    // 「跨线程工作」为什么不能只靠唤醒 fd 就完事：
    //   SendReq 在别的线程里把请求塞进 pending_send、DeleteServer 把待关 fd 塞进
    //   pending_close_listen，然后 WakeUp()。循环醒来后【必须】把这两个队列取空
    //   并执行 —— 唤醒只是"让你醒",不是"帮你做事"。忘了这一步的典型症状：
    //   单线程测试全过，一上并发就全部超时。
    //
    // 取任务时不要在持 mu 的时候执行：先 swap 到一个局部 deque，放锁，再逐个执行。
    WarnOnce("TODO(2) LoopMain() 还没实现：没有任何 fd 会被处理");
  }

  // 监听 fd 就绪：accept 到 EAGAIN 为止。
  void OnAccept(int servername) {
    (void)servername;
    // TODO(3): 三个坑，按顺序都会撞上：
    //   a) 一次事件可能对应【多个】连接排队，accept 必须循环到 EAGAIN
    //      （只 accept 一次 = 剩下的连接要等下一次事件，高并发下会明显掉吞吐）。
    //   b) 新 fd 必须立刻 SetNonBlocking，否则一次 write 就能把整个事件循环卡住
    //      —— 这是"事件驱动"最容易被自己破坏的地方。
    //   c) Server 可能已经被 DeleteServer 换掉/删掉：accept 出来之后要复查
    //      servers 里那台 server 还是不是同一个 Server 对象（对齐 socket 后端
    //      HandleClient 里 `it->second->srv == srv` 的判断），否则请求会派发到
    //      一个已经销毁的对象上。
    WarnOnce("TODO(3) OnAccept() 还没实现：监听 fd 有事件但没人接");
  }

  // 可读：读到 EAGAIN 为止，边读边切帧。
  void OnReadable(const std::shared_ptr<EpConn>& c) {
    // TODO(4): 关键点：
    //   a) 用一个栈上缓冲（8~64KiB）循环 read，直到 read 返回 EAGAIN（errno 判断）
    //      才算"这次读干净了"。LT 模式下不读干净 → 下一轮 Wait 立刻又返回它，
    //      你会看到一个 100% CPU 的空转循环。
    //   b) read 返回 0 = 对端正常关闭；返回 -1 且 errno != EAGAIN/EWOULDBLOCK
    //      = 出错。两者都要 CloseConn（但注意：先处理完已读到的字节再关，
    //      否则对端关闭前发来的最后一帧会丢）。
    //   c) 每读一段就喂给 c->reader.Append()，然后 while (reader.Next(body)) 分发。
    //      reader.broken() 为真说明对端给了非法的长度头 → 直接 CloseConn。
    //   d) 分帧之后按"这条连接是谁的"分发：客户端侧 → OnClientFrame，
    //      服务端侧 → OnServerFrame。
    //   e) 别忘了 kError：mask 里有 kError 时说明对端半关/出错，处理方式是
    //      "把当前能读的读完，然后再关"。
    //
    // 踩坑预警：不要在循环里用同一个 string 反复 Append（FrameReader 内部已经攒了），
    // 也不要在 read 返回 EAGAIN 之后继续 read（那就是忙等）。
    (void)c;
    WarnOnce("TODO(4) OnReadable() 还没实现：连接上有数据但没人读");
  }

  // 可写：把输出队列尽量刷出去。
  void FlushWrites(const std::shared_ptr<EpConn>& c) {
    // TODO(5): 关键点：
    //   a) 从 out 队头取一帧，用非阻塞 write 写；写不完就把"剩下那半截"留在队头
    //      （不要整帧扔掉，也不要重写已经发出去的字节 —— 那会造出撕裂的帧，
    //      对端会看到长度头对不上的垃圾，然后两边一起断连）。
    //   b) 写完一帧就 pop，直到 write 返回 EAGAIN 或队列空。
    //   c) 队列空了 → UpdateInterest() 把 EPOLLOUT 摘掉（否则你会一直被叫醒）。
    //   d) 队列长时间不空怎么办？这就是"回压"：现在先不管（raft 的 RPC 都很小），
    //      但要在注释里写下你将来会加的上限（比如 out_bytes > 8MiB 就断连，
    //      与 C3 那条 max_message_bytes_ 是同一个思路：宁可拒绝，不要 OOM）。
    (void)c;
    WarnOnce("TODO(5) FlushWrites() 还没实现：回包写不出去（客户端会全部超时）");
  }

  // 客户端侧收到一帧回包。
  void OnClientFrame(const std::shared_ptr<EpConn>& c, const std::string& body) {
    // TODO(6):
    //   a) wire::DecodeReply(body, corr, ok, reply)，失败 → CloseConn（帧坏了）
    //   b) 在 c->mu 下从 pending 里摘出 req（没有就丢弃：可能是已取消的请求）
    //   c) 【放锁之后】再 DeliverResult(req, ok, reply)
    //      —— 持锁调可能让上层回调重入 SendReq → 自死锁。
    (void)c;
    (void)body;
    WarnOnce("TODO(6) OnClientFrame() 还没实现：回包到了但没人匹配");
  }

  // 服务端侧收到一帧请求。
  void OnServerFrame(const std::shared_ptr<EpConn>& c, const std::string& body) {
    // TODO(7):
    //   a) wire::DecodeRequest(body, corr, svc, args)，失败 → CloseConn
    //   b) 【不要在这里 TryDispatch】。把它 PostTask 到 worker 池：
    //        保持顺序：同一连接上的请求必须按到达顺序处理吗？
    //        —— 对 raft 来说不需要（每个 RPC 独立），但你要在注释里说明为什么。
    //   c) worker 里跑 srv->TryDispatch(svc, args, &reply)：
    //        失败 → 回一个 ok=false 的空帧（对齐 socket 后端：远程方法名不认识
    //        绝不能 abort，那等于谁连上端口发 30 字节就能杀掉整个进程）
    //   d) 回包时别忘了那两件事：入 c->out 后要 UpdateInterest/Mod 加 EPOLLOUT，
    //      【且】要 WakeUp() —— 因为此刻事件循环可能正阻塞在 Wait 里，
    //      没人叫它，这个"有新数据要写"就永远没人看见。
    //   e) 不可靠模式的 10% 丢回包、2/3 概率延迟回包，都在这一层实现（与
    //      socket 后端 HandleClient 里的阈值一致）。
    (void)c;
    (void)body;
    WarnOnce("TODO(7) OnServerFrame() 还没实现：请求到了但没人处理");
  }

  // 建/取一条到 servername 的客户端连接。
  std::shared_ptr<EpConn> GetOrCreateConn(const std::string& endname, int servername) {
    // TODO(9):
    //   a) 复用判据：fd >= 0 且 !closed 且 servername 没变（对齐 socket 后端的
    //      ConnReusable）。注意 dead 也算不可用。
    //   b) 没有就新建：socket() → SetNonBlocking() → connect()。
    //      非阻塞 connect 会立刻返回 -1 且 errno == EINPROGRESS —— 这不是失败，
    //      是"握手在后台进行"。此时注册 EPOLLOUT（可写 == 握手完成），
    //      等事件到了用 getsockopt(SO_ERROR) 取真实结果。
    //      懒人版：也可以先阻塞 connect（loopback 上只要几十微秒），但要在
    //      注释里写明"这在真实网络上会阻塞调用方线程，属于已知取舍"。
    //   c) 新连接要 Add 进 poller（token 自增）并放进 conns 表。
    //   d) 【重要】这个函数会被 SendReq 在【任意线程】调用。所以它内部碰的
    //      共享状态必须持 mu，而 poller.Add 只能在 loop 线程里做（epoll_ctl 是
    //      线程安全的，但"token 分配 + 注册 + 入表"必须原子，否则会出现
    //      "fd 已注册但表里查不到 token"的事件 → 空指针）。
    //      最干净的做法：SendReq 只把 ReqMsg 塞进 pending_send 并 WakeUp，
    //      真正的建连全部在 loop 线程里做。
    (void)endname;
    (void)servername;
    WarnOnce("TODO(9) GetOrCreateConn() 还没实现：一条连接都建不出来");
    return nullptr;
  }
};

// ===========================================================================
// 对外接口
// ===========================================================================

bool EpollNetwork::Supported() { return Poller::Supported(); }

int EpollNetwork::ConnCount() const {
  if (!impl_) return 0;
  std::lock_guard<std::mutex> lk(impl_->mu);
  return static_cast<int>(impl_->conns.size());
}

EpollNetwork::EpollNetwork(const std::string& host, int base_port)
    : impl_(new Impl()) {
  impl_->host = host;
  impl_->base_port = base_port;

  // 环境变量覆盖（与 socket 后端同名，方便同一套脚本切后端）
  if (const char* e = std::getenv("LABRPC_SOCKET_HOST")) impl_->host = e;
  if (const char* e = std::getenv("LABRPC_SOCKET_BASE")) {
    int v = 0;
    if (ParsePortStrict(e, &v)) {
      impl_->base_port = v;
    } else {
      std::fprintf(stderr, "[labrpc/epoll] LABRPC_SOCKET_BASE=\"%s\" 不是合法端口\n", e);
      impl_->port_ok = false;
    }
  }

  if (!Poller::Supported()) {
    // 绝不静默降级：说清楚为什么不能用、以及该用什么。
    std::fprintf(stderr,
                 "[labrpc/epoll] 本平台不支持 epoll（当前 Poller 后端：%s）。\n"
                 "              epoll 是 Linux 专属；在 macOS 上请用 inproc 或 socket 后端。\n"
                 "              这个后端仍会被构造出来，但所有 RPC 会立即失败。\n",
                 Poller::BackendName());
    return;
  }

  impl_->host_ok = ResolveIPv4(impl_->host, &impl_->host_addr_be);
  if (!impl_->host_ok) return;
  if (!impl_->port_ok) return;
  if (!impl_->Start()) return;
  impl_->open_ok.store(true);
}

EpollNetwork::~EpollNetwork() { Cleanup(); }

// ---- 端点管理 ----

std::shared_ptr<ClientEnd> EpollNetwork::MakeEnd(const std::string& endname) {
  // 与 in-proc / socket 对齐：新建端点默认【禁用 + 未连接】。否则同名端点重建时
  // 上一世代的 enabled/connections 残留，新端点"没 Connect 就能发 RPC"。
  {
    std::lock_guard<std::mutex> lk(impl_->mu);
    impl_->enabled[endname] = false;
    impl_->connections.erase(endname);
  }
  return std::make_shared<ClientEnd>(this, endname);
}

void EpollNetwork::AddServer(int servername, std::shared_ptr<Server> srv) {
  DeleteServer(servername);  // 先清同名旧实例（它内部会自己上锁，不能持锁调用）

  if (!impl_->open_ok.load()) return;      // poller 没起来：不监听（RPC 会立即失败）
  if (impl_->stopped.load()) return;       // 停机后的网络是终态，绝不再建 server
  if (!impl_->host_ok || !impl_->port_ok) return;
  if (!PortsInRange(impl_->base_port, servername)) {
    std::fprintf(stderr,
                 "[labrpc/epoll] AddServer(%d): base_port=%d + servername=%d 超出 65535\n",
                 servername, impl_->base_port, servername);
    return;
  }

  const int port = impl_->base_port + servername;
  const int lfd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (lfd < 0) {
    std::fprintf(stderr, "[labrpc/epoll] AddServer(%d): socket() 失败: %s\n",
                 servername, std::strerror(errno));
    return;
  }
  int yes = 1;
  ::setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

  sockaddr_in sa{};
  sa.sin_family = AF_INET;
  sa.sin_port = htons(static_cast<uint16_t>(port));
  sa.sin_addr.s_addr = impl_->host_addr_be;
  if (::bind(lfd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) < 0) {
    const int e = errno;
    std::fprintf(stderr, "[labrpc/epoll] AddServer(%d): bind(%s:%d) 失败: %s\n",
                 servername, impl_->host.c_str(), port, std::strerror(e));
    ::close(lfd);
    return;
  }
  // backlog 128：测试里 20 个客户端首跳并发 connect 是常态，backlog 太小时
  // 多出来的连接会被内核直接 RST（客户端 connect 成功、write 成功，然后
  // reader 读到 ECONNRESET，在途 RPC 全判失败）。
  if (::listen(lfd, 128) < 0) {
    std::fprintf(stderr, "[labrpc/epoll] AddServer(%d): listen 失败: %s\n",
                 servername, std::strerror(errno));
    ::close(lfd);
    return;
  }
  if (!SetNonBlocking(lfd)) {
    std::fprintf(stderr, "[labrpc/epoll] AddServer(%d): 设置非阻塞失败\n", servername);
    ::close(lfd);
    return;
  }

  Impl::ServerRec rec;
  rec.servername = servername;
  rec.listen_fd = lfd;
  rec.token = Impl::kListenTokenBase + static_cast<uint64_t>(servername);
  rec.srv = std::move(srv);
  {
    std::lock_guard<std::mutex> lk(impl_->mu);
    impl_->servers[servername] = rec;
  }
  // 监听 fd 直接注册进 poller。注意这里【没有】任何线程来阻塞 accept ——
  // 就绪时事件循环会调 OnAccept。对比 socket 后端：那里每个 server 一个 acceptor
  // 线程 + 一个 self-pipe 唤醒 fd + 500ms 超时兜底，一整套机制在这里都不需要。
  impl_->poller.Add(lfd, rec.token, Poller::kReadable);
}

void EpollNetwork::DeleteServer(int servername) {
  Impl::ServerRec rec;
  bool found = false;
  {
    std::lock_guard<std::mutex> lk(impl_->mu);
    auto it = impl_->servers.find(servername);
    if (it != impl_->servers.end()) {
      rec = it->second;
      impl_->servers.erase(it);
      found = true;
    }
  }
  if (!found) return;
  // Del 是幂等且线程安全的（epoll_ctl），可以先做 —— 这样从这一刻起 loop
  // 不会再为这个 fd 返回事件；真正的 close 交给 loop 线程（见 pending_close_listen
  // 的注释）。不做 Del 就直接排队等 loop 关也行，只是多几次无意义的事件。
  impl_->poller.Del(rec.listen_fd);
  {
    std::lock_guard<std::mutex> lk(impl_->mu);
    impl_->pending_close_listen.push_back(rec.listen_fd);
  }
  impl_->WakeLoop();
  // 挂在这台 server 上的连接：只置 dead + 唤醒，真正的 close 由 loop 线程做。
  std::vector<std::shared_ptr<Impl::EpConn>> victims;
  {
    std::lock_guard<std::mutex> lk(impl_->mu);
    for (auto& kv : impl_->conns) {
      if (kv.second->servername == servername && kv.second->endname.empty()) {
        victims.push_back(kv.second);
      }
    }
  }
  for (auto& c : victims) c->dead.store(true);
  if (!victims.empty()) impl_->WakeLoop();
}

void EpollNetwork::Connect(const std::string& endname, int servername) {
  std::shared_ptr<Impl::EpConn> stale;
  {
    std::lock_guard<std::mutex> lk(impl_->mu);
    impl_->connections[endname] = servername;
    // 改指向：沿用旧连接的 RPC 会串台发到上一台 server（in-proc 后端每次
    // SendReq 都重读 connections_，天然没这个问题 → 两个后端必须对齐语义）
    for (auto& kv : impl_->conns) {
      auto& c = kv.second;
      if (c->endname == endname && c->servername != servername) stale = c;
    }
  }
  if (stale) {
    // TODO(9) 的配套：这里只置 dead，由 loop 线程 CloseConn。
    stale->dead.store(true);
    impl_->WakeLoop();
  }
}

void EpollNetwork::Enable(const std::string& endname, bool enabled) {
  std::lock_guard<std::mutex> lk(impl_->mu);
  impl_->enabled[endname] = enabled;
}

void EpollNetwork::RemoveClientEnd(const std::string& endname) {
  std::shared_ptr<Impl::EpConn> conn;
  {
    std::lock_guard<std::mutex> lk(impl_->mu);
    impl_->enabled.erase(endname);
    impl_->connections.erase(endname);
    for (auto& kv : impl_->conns) {
      if (kv.second->endname == endname) conn = kv.second;
    }
  }
  if (conn) {
    conn->dead.store(true);
    impl_->WakeLoop();
  }
}

// ---- 旋钮 / 统计（全部与 socket 后端逐字对齐）----

void EpollNetwork::Reliable(bool yes) { impl_->reliable.store(yes); }
void EpollNetwork::LongReordering(bool yes) { impl_->long_reordering.store(yes); }
// 空操作是有意为之：真实网络里"端点通但 server 没了"由 connect 失败/EOF 直接
// 暴露，不需要人为拖长（与 socket 后端的取舍相同）。
void EpollNetwork::LongDelays(bool /*yes*/) {}

int EpollNetwork::GetCount(int servername) {
  std::shared_ptr<Server> srv;
  {
    std::lock_guard<std::mutex> lk(impl_->mu);
    auto it = impl_->servers.find(servername);
    if (it != impl_->servers.end()) srv = it->second.srv;
  }
  return srv ? srv->GetCount() : 0;
}

int EpollNetwork::GetTotalCount() const { return impl_->total_count.load(); }
int64_t EpollNetwork::GetTotalBytes() const { return impl_->total_bytes.load(); }

void EpollNetwork::SetMaxMessageBytes(size_t n) {
  impl_->max_message_bytes.store(n);
  // 接收侧闸门也要跟着换：SetMaxMessageBytes 之后，一个"合法小请求 + 大回包"
  // 必须仍然能过（请求上限和接收上限不是同一个数，理由见 socket 后端
  // RecvFrameLenCap 的注释）。这里取 max(运行期上限, 默认上限)，0 = 不设限。
  size_t cap = (n == 0) ? 0 : std::max(n, kMaxRpcMessageBytesDefault);
  std::lock_guard<std::mutex> lk(impl_->mu);
  for (auto& kv : impl_->conns) kv.second->reader.SetMaxBody(cap);
}
size_t EpollNetwork::MaxMessageBytes() const { return impl_->max_message_bytes.load(); }
int64_t EpollNetwork::OversizedDropped() const { return impl_->oversized_dropped.load(); }

// ---- 收发 ----

void EpollNetwork::SendReq(std::shared_ptr<ReqMsg> req) {
  // TODO(8): 这个函数是客户端侧唯一的入口，会从【任意线程】调用
  //          （Raft 的 replicator / ticker / applier 都可能）。顺序建议：
  //
  //   a) 字节上限：max_message_bytes>0 && args.size()>上限 → oversized_dropped++,
  //      DeliverResult(false) 后 return（和 in-proc / socket 同一处拦截）
  //   b) total_count++ ; total_bytes += args.size()
  //   c) 在 mu 下快照 enabled[endname] 与 connections[endname]
  //   d) !enabled || servername < 0 → DeliverResult(false) return
  //   e) !reliable && RandInt(1000) < 100 → DeliverResult(false) return（丢请求）
  //   f) 取/建连接（GetOrCreateConn）；拿不到 → DeliverResult(false) return
  //   g) corr = next_corr++；在 c->mu 下 pending[corr] = req
  //   h) 编码请求帧，塞进 c->out；UpdateInterest 之类要在 loop 线程里做，
  //      所以这里只需要 WakeLoop()（唤醒之后由 loop 线程把"队列里有东西"变成
  //      "关注集里有 EPOLLOUT"）
  //   i) 任何一步失败都只调 DeliverResult(false)，绝不在这里跑用户 cb
  //
  // 最容易写错的一步是 (h)：如果你在调用方线程里直接 poller.Mod()，那么
  // epoll_ctl 和 loop 线程的 epoll_wait 之间没有顺序保证 —— 事件可能先到、
  // 数据还没入队。统一"数据入队 + 唤醒，全部动作由 loop 线程做"最省心。

  // 骨架阶段的行为：**【快速失败】，绝不静默挂住**。
  // 这一点很关键：SendReq 什么都不做的话，Call() 会永远等 ReplySlot 的 cv，
  // 整个测试进程表现为"卡住"，最后被外层看门狗当成"某个不相干的用例超时"。
  // 失败要快、要带原因，你实现完 (a)~(i) 之后把下面这两段删掉即可。
  if (!impl_->open_ok.load()) {
    WarnOnce("后端未就绪（平台无 epoll，或 TODO(1) Start() 未实现）：RPC 立即失败");
    impl_->DeliverResult(req, false, "");
    return;
  }
  WarnOnce("TODO(8) SendReq() 还没实现：RPC 立即失败（有意不挂住）");
  impl_->DeliverResult(req, false, "");
}

bool EpollNetwork::IsStopped() const { return impl_->stopped.load(); }

void EpollNetwork::Cleanup() {
  // TODO(10): 关停顺序错一步就是挂死或 use-after-free。建议：
  //   a) stopped.exchange(true)；不是首次就直接返回（幂等）
  //   b) WakeLoop() —— 让正阻塞在 Wait 的循环立刻醒来看到 stopped
  //   c) join 事件循环线程（它退出前应把 listen fd、所有连接都 CloseConn）
  //   d) join worker 线程（先让它们看到 stopped 并清空队列，否则还会跑 cb）
  //   e) poller.Close()
  //   f) 把剩余的 pending_send / 所有连接的 pending 全部 DeliverResult(false)
  //       —— 否则那些正在 Call() 里等的人会一直等到各自的上层超时
  //
  // 注意 (c) 与 (f) 的先后：DeliverResult 可能触发用户 cb（走 worker 池），
  // 所以 (f) 必须在 worker 还活着的时候做，或者改成同步填 slot。
  // 这里给你一个"当前最小可用"的收尾，先保证不崩：
  impl_->stopped.store(true);
  impl_->task_cv.notify_all();
  impl_->WakeLoop();
  if (impl_->loop_thread.joinable()) impl_->loop_thread.join();
  for (auto& t : impl_->workers) {
    if (t.joinable()) t.join();
  }
  impl_->workers.clear();
  impl_->poller.Close();
  impl_->open_ok.store(false);
}

}  // namespace labrpc
