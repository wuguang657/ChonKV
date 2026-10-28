// socket_transport.cpp —— Transport 的"真网络"后端实现（详见 socket_transport.h）

#include "socket_transport.h"

#include <arpa/inet.h>
#include <csignal>  // ::signal / SIGPIPE（P7：写已关 fd 不再杀进程）
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <netdb.h>   // getaddrinfo：把 host 解析成 IPv4（inet_addr 不认域名）
#include <netinet/in.h>
#include <poll.h>        // B1: poll 化 accept 循环，避免依赖 close() 唤醒阻塞的 accept()
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>

#include "../../common/util.h"  // raftcpp::RandInt

namespace labrpc {

// ===========================================================================
// 帧编解码辅助（长度前缀 + 大端；和具体 RPC 语义无关，纯传输管线）
// ===========================================================================

static void PutU8(std::string& b, uint8_t v) { b.push_back(static_cast<char>(v)); }

static void PutU32(std::string& b, uint32_t v) {
  char tmp[4];
  tmp[0] = static_cast<char>((v >> 24) & 0xFF);
  tmp[1] = static_cast<char>((v >> 16) & 0xFF);
  tmp[2] = static_cast<char>((v >> 8) & 0xFF);
  tmp[3] = static_cast<char>(v & 0xFF);
  b.append(tmp, 4);
}

static void PutU64(std::string& b, uint64_t v) {
  char tmp[8];
  for (int i = 7; i >= 0; i--) {
    tmp[i] = static_cast<char>(v & 0xFF);
    v >>= 8;
  }
  b.append(tmp, 8);
}

static bool GetU8(const char*& p, const char* end, uint8_t& v) {
  if (p + 1 > end) return false;
  v = static_cast<uint8_t>(*p++);
  return true;
}

static bool GetU32(const char*& p, const char* end, uint32_t& v) {
  if (p + 4 > end) return false;
  v = (static_cast<uint8_t>(p[0]) << 24) | (static_cast<uint8_t>(p[1]) << 16) |
      (static_cast<uint8_t>(p[2]) << 8) | static_cast<uint8_t>(p[3]);
  p += 4;
  return true;
}

static bool GetU64(const char*& p, const char* end, uint64_t& v) {
  if (p + 8 > end) return false;
  v = 0;
  for (int i = 0; i < 8; i++) {
    v = (v << 8) | static_cast<uint8_t>(p[i]);
  }
  p += 8;
  return true;
}

// 把 buf 整块写进 fd（处理短写 / EINTR）
static bool SendAll(int fd, const char* buf, size_t n) {
  size_t off = 0;
  while (off < n) {
    ssize_t w = ::write(fd, buf + off, n - off);
    if (w < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (w == 0) return false;  // n > 0 时 write 不该返回 0；写成 continue 会死循环
    off += static_cast<size_t>(w);
  }
  return true;
}

// 从 fd 整块读 n 字节（处理短读 / EINTR；对端关闭返回 false）
static bool RecvAll(int fd, char* buf, size_t n) {
  size_t off = 0;
  while (off < n) {
    ssize_t r = ::read(fd, buf + off, n - off);
    if (r < 0) {
      if (errno == EINTR) continue;   // 被信号打断重试，该线程正在睡眠中，叫醒继续处理
      return false;
    }
    if (r == 0) return false;  // 对端关闭
    off += static_cast<size_t>(r);
  }
  return true;
}

// 帧长冗余量：请求/回包帧在 max_message_bytes_ 之外还有定长头
// （corr 8B + 各长度字段 4B×3）和极短的方法名，留 1KiB 足够，
// 避免把合法帧误杀。
static constexpr size_t kFrameLenSlack = 1024;

// 接收侧帧长上限：
//   * 0（SetMaxMessageBytes(0) = 不限制，见 labrpc.h:235 承诺）必须【透传】为 0；
//   * 否则取 max(运行期上限, 默认上限) + slack。
// 为什么不能直接用运行期上限：传输层这道闸门只负责"防错乱/恶意的 4 字节长度头
// 触发一次超大分配"，它【不是】C3 那条"单条 RPC【请求】上限"契约（labrpc.h:191）。
// 把两者混成一个值会出事：SetMaxMessageBytes 调小之后，一个【合法的小请求 +
// 超过该值的回包】（经典例子：Get 的 key 只有几十字节，回包是 megabytes 的 value）
// 会被 RecvReplyFrame 判成"帧非法" → ConnReader 视作连接断开 → 撕掉 TCP 连接，
// 在途请求全判失败；重连后下一次调用还会撞同一个大回包 → 该 RPC 在 socket 后端
// 【永久不可用】。in-proc 后端没有接收侧闸门，同一份代码照常成功 → 两个后端语义
// 分歧（selftest TestReplyLargerThanRequestCap 抓这个：修复前 socket 4 条断言全红）。
// 请求方向的上限由发送侧 SendReq 负责，接收侧只保底到默认上限即可。
static size_t RecvFrameLenCap(const std::atomic<size_t>& max_bytes) {
  size_t m = max_bytes.load();
  if (m == 0) return 0;
  return std::max(m, kMaxRpcMessageBytesDefault) + kFrameLenSlack;
}

// 请求帧：[u32 总长][u64 corr][u32 svc_len][svc][u32 args_len][args]
static bool SendRequestFrame(int fd, uint64_t corr, const std::string& svc,
                             const std::string& args) {
  std::string payload;
  PutU64(payload, corr);
  PutU32(payload, static_cast<uint32_t>(svc.size()));
  payload += svc;
  PutU32(payload, static_cast<uint32_t>(args.size()));
  payload += args;
  std::string frame;
  PutU32(frame, static_cast<uint32_t>(payload.size()));
  frame += payload;
  return SendAll(fd, frame.data(), frame.size());
}

static bool RecvRequestFrame(int fd, uint64_t& corr, std::string& svc,
                             std::string& args, size_t max_len) {
  char lenbuf[4];
  if (!RecvAll(fd, lenbuf, 4)) return false;
  const char* p = lenbuf;
  const char* end = lenbuf + 4;
  uint32_t len = 0;
  if (!GetU32(p, end, len)) return false;
  // 接收侧长度闸门：错乱/恶意的 4 字节长度头最多触发一次 (max_len) 的
  // 分配，而不是 4GiB。与发送侧 SendReq 的 max_message_bytes_ 同源；
  // max_len==0 表示不设限（SetMaxMessageBytes(0) 语义）。
  // 触发即判连接断开（RecvAll 失败同理）——客户端会重建连接重试。
  if (max_len > 0 && static_cast<size_t>(len) > max_len) return false;
  std::string buf(len, '\0');
  if (!RecvAll(fd, &buf[0], len)) return false;
  p = buf.data();
  end = buf.data() + len;
  uint32_t svc_len = 0, args_len = 0;
  if (!GetU64(p, end, corr)) return false;
  if (!GetU32(p, end, svc_len)) return false;
  if (p + svc_len > end) return false;
  svc.assign(p, svc_len);
  p += svc_len;
  if (!GetU32(p, end, args_len)) return false;
  if (p + args_len > end) return false;
  args.assign(p, args_len);
  return true;
}

// 回包帧：[u32 总长][u64 corr][u8 ok][u32 reply_len][reply]
static bool SendReplyFrame(int fd, uint64_t corr, bool ok,
                           const std::string& reply) {
  std::string payload;
  PutU64(payload, corr);
  PutU8(payload, ok ? 1 : 0);
  PutU32(payload, static_cast<uint32_t>(reply.size()));
  payload += reply;
  std::string frame;
  PutU32(frame, static_cast<uint32_t>(payload.size()));
  frame += payload;
  return SendAll(fd, frame.data(), frame.size());
}

// static限制该函数可见性
static bool RecvReplyFrame(int fd, uint64_t& corr, bool& ok,
                           std::string& reply, size_t max_len) {
  char lenbuf[4];    // 无论初不初始化，4字节都会被填满
  if (!RecvAll(fd, lenbuf, 4)) return false;   // 4字节长度头
  const char* p = lenbuf;
  const char* end = lenbuf + 4;
  uint32_t len = 0;
  if (!GetU32(p, end, len)) return false;       // 解码后真正的帧长度，但只算4字节后的帧长度不包含头的4个字节
  // 接收侧长度闸门：同 RecvRequestFrame，防错乱长度头打爆分配
  if (max_len > 0 && static_cast<size_t>(len) > max_len) return false;
  std::string buf(len, '\0');   // 预分配len长度的缓冲区
  if (!RecvAll(fd, &buf[0], len)) return false;
  p = buf.data();
  end = buf.data() + len;
  uint32_t reply_len = 0;
  uint8_t ok_byte = 0;
  if (!GetU64(p, end, corr)) return false;  // corr 是每次 RPC 请求的唯一关联 id（原子计数器生成、uint64 不重复）8字节
  if (!GetU8(p, end, ok_byte)) return false;  // 1字节当ok标志
  if (!GetU32(p, end, reply_len)) return false; // 扣4字节当回复内容的长度
  // 游标p移动到reply真实内容处
  if (p + reply_len > end) return false;
  reply.assign(p, reply_len);  // 从 p 拷 n 个字节进来，不管中间有没有 \0
  ok = (ok_byte != 0);
  return true;
}

// 把结果投递回 ClientEnd（填 ReplySlot 或跑 cb）。与 in-proc Network::Finish 等价。
void SocketNetwork::DeliverResult(std::shared_ptr<ReqMsg> req, bool ok,
                                 std::string reply) {
  if (req->cancel && req->cancel->load()) return;  // 被 Kill：结果作废
  // 恰好一次投递守卫：SendReq 的 !sent 失败分支 与 ConnReader 连接断开退出块，
  // 可能并发地对同一个 req 调 DeliverResult(false)（见 SendReq ~704 与 ConnReader ~609）。
  // 用原子 CAS 保证只真正投递一次，避免异步 cb 被重复调用（同步 slot 双写 done 无害，
  // 但 cb 模式下重复调用会让上层误判为两次独立失败）。
  if (req->delivered.exchange(true)) return;
  // 回包字节只在【真正投递】时记账，与 in-proc Finish（labrpc.cpp:406-412）的顺序
  // 完全一致：先看 cancel，再算字节。
  // 旧实现把这一行放在 ConnReader 里（读帧之后、DeliverResult 之前），于是
  // "Raft 已经 Kill（cancel_rpcs_=true）之后才到达的回包"仍然被计入 —— 而
  // in-proc 明确"结果作废，一个字节都不算"。两个后端于是对 GetTotalBytes()
  // 给出不同答案（实测 /tmp/probe_cancel_bytes.cpp：同一条被取消的 4 字节回包，
  // in-proc 记 4，socket 记 8）。这种差异最坏情况是让上层"用字节数反推
  // 有没有重复发送"的断言（TestRPCBytes2B 那类）在 net 模式下偏松。
  // 放在 delivered CAS 之后还有一个好处：并发重复投递也只算一次。
  if (ok) bytes_.fetch_add(static_cast<int64_t>(reply.size()));
  // 这封信有没有装一个回调函数
  if (req->cb) {
    // 异步调用：cb 一律【不内联】，压入 cb_q_ 交给 cb_worker_ 执行。
    // 为什么：DeliverResult 有两个调用线程——ConnReader（网络线程）和
    // SendReq 的快速失败路径（【调用方线程】，可能是 Raft 的 replicator /
    // ticker）。旧实现在后者里直接跑 cb，而 in-proc 后端的等价路径
    // （Network::Finish）明确"绝不能在定时器线程里直接跑回调"而是 Post
    // 到线程池。当前所有 Raft 回调恰好都在 !ok 时立即返回、且调用点都在
    // 锁外，内联暂未致命；但这是个将来谁加个会阻塞 / 会重入的回包处理
    // 就死锁的陷阱，且两个后端语义不一致。统一进队列后：cb 永远跑在
    // 本后端的投递线程上，与 in-proc 对齐。
    // 队列 FIFO：cb 的执行顺序 = 入队顺序（reader 线程串行读帧 → 顺序天然确定）。
    std::function<void()> task;
    if (ok) {
      std::string r = std::move(reply);
      task = [cb = std::move(req->cb), r]() { cb(true, r); };
    } else {
      task = [cb = std::move(req->cb)]() { cb(false, std::string()); };
    }
    {
      std::lock_guard<std::mutex> lk(mu_);
      cb_q_.push_back(std::move(task));
    }
    cb_cv_.notify_one();
    return;
  }

  if (!req->slot) return;
  std::lock_guard<std::mutex> lk(req->slot->mu);
  req->slot->ok = ok;
  req->slot->reply = std::move(reply);
  req->slot->done = true;
  req->slot->cv.notify_all();
}

// ===========================================================================
// 构造 / 析构 / 生命周期
// ===========================================================================

// 严格解析端口号（十进制、整串、1..65535）。
//
// 为什么不能沿用 std::atoi：它对三种输入都【不报错】，而是给出一个"看起来能用"
// 的数字，于是端口配置被静默改写成另一个端口（与 ResolveIPv4 注释里 inet_addr
// 那桩血案是同一类病，只是维度从地址换成了端口）：
//   atoi("abc")      == 0      → bind(0) = 让内核随便挑一个临时端口，而客户端
//                                 connect 的仍是 0+servername → 全部失败且零日志
//   atoi("-5")       == -5     → static_cast<uint16_t> → 65531（一个完全无关的端口）
//   atoi("19090xyz") == 19090  → 拼错的配置被当成合法值接受
// 另外 base_port + servername 超过 65535 时，htons(uint16_t) 也会静默回绕：
// 两个不同 worker 的端口段一旦回绕到同一段，两个测试进程就会抢同一个端口
// （test_part.sh 的 w_socket_base = base + slot*1000 + ... 正是这么算的）。
static bool ParsePortStrict(const char* s, int* out) {
  if (s == nullptr || *s == '\0') return false;
  errno = 0;
  char* end = nullptr;
  const long v = std::strtol(s, &end, 10);
  if (errno != 0 || end == s || *end != '\0') return false;  // 非数字 / 尾随垃圾
  if (v < 1 || v > 65535) return false;                      // 0 与越界都不接受
  *out = static_cast<int>(v);
  return true;
}

// base + servername 是否仍是一个合法端口（server i 监听在 base_port + i）
static bool PortsInRange(int base_port, int servername) {
  return base_port >= 1 && servername >= 0 && base_port + servername <= 65535;
}

// 把 host 解析成 IPv4 地址（网络字节序，失败返回 false）。
//
// 【为什么需要这一步，以及为什么必须在构造期做一次】
// 早先 AddServer 的 bind 和 GetOrCreateConn 的 connect 各自调 ::inet_addr(host_)，
// 两处都跟着一句 `if (sin_addr.s_addr == INADDR_NONE) sin_addr.s_addr = INADDR_LOOPBACK;`。
// 而 ::inet_addr 只接受【数字点分】的 IPv4 字面量：传域名（"node-a.cluster"）、
// 主机名、或打错一位的数字地址（"127.0.0.999"）一律返回 INADDR_NONE，于是那句
// 兜底把地址【静默改写】成 127.0.0.1：
//   * 单机跑测试：一切正常（本来就是 loopback），看不出问题；
//   * 多机部署：每台机器都 bind/connect 自己的 loopback → 集群"看起来起来了"，
//     但永远收不到对方的 RPC，且【零报错】。"配置被静默改写"这类故障，排查
//     成本远高于启动时直接报错。
// 实测（/tmp/probe_host.cpp，修复前）：
//   host="no-such-host.invalid" → RPC 成功、reply="HELLO"
//   host="example.com"          → RPC 成功（同样被打回 loopback）
// 现在改成：构造期用 getaddrinfo 解析一次（域名 / 主机名 / 数字地址都支持），
// 失败就把 host_ok_ 置 false 并打一条明确错误；后续 AddServer 不建监听、
// GetOrCreateConn 直接返回 nullptr → 表现为"RPC 全部立即失败"，
// 而不是"悄悄跑 loopback"。
static bool ResolveIPv4(const std::string& host, uint32_t* out_be) {
  addrinfo hints{};
  hints.ai_family = AF_INET;  // 本后端的帧/连接只用 IPv4
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* res = nullptr;
  const int rc = ::getaddrinfo(host.c_str(), nullptr, &hints, &res);
  if (rc != 0 || res == nullptr) {
    if (res != nullptr) ::freeaddrinfo(res);
    std::fprintf(stderr, "[labrpc] getaddrinfo(\"%s\") 失败: %s\n", host.c_str(),
                 rc != 0 ? ::gai_strerror(rc) : "no result");
    return false;
  }
  bool ok = false;
  for (addrinfo* p = res; p != nullptr; p = p->ai_next) {
    if (p->ai_family == AF_INET && p->ai_addr != nullptr &&
        p->ai_addrlen >= sizeof(sockaddr_in)) {
      *out_be =
          reinterpret_cast<const sockaddr_in*>(p->ai_addr)->sin_addr.s_addr;
      ok = true;
      break;
    }
  }
  ::freeaddrinfo(res);
  if (!ok) {
    std::fprintf(stderr, "[labrpc] host \"%s\" 没有可用的 IPv4 地址\n",
                 host.c_str());
  }
  return ok;
}

SocketNetwork::SocketNetwork(const std::string& host, int base_port) {
  // 服务端的tcp关闭后，本端再::write这个fd，内核会向进程投递 SIGPIPE 信号，默认动作是直接杀死整个进程
  // 加上这个就返回false
  ::signal(SIGPIPE, SIG_IGN);
  const char* e = std::getenv("LABRPC_SOCKET_HOST");
  host_ = e && e[0] ? e : host;
  const char* b = std::getenv("LABRPC_SOCKET_BASE");
  base_port_ = base_port;
  if (b != nullptr && b[0] != '\0') {
    // 严格解析：解析不了就【明确拒绝】，绝不猜一个端口（见 ParsePortStrict 注释）
    int v = 0;
    if (ParsePortStrict(b, &v)) {
      base_port_ = v;
    } else {
      port_ok_ = false;
    }
    if (!port_ok_) {
      std::fprintf(stderr,
                   "[labrpc] SocketNetwork: LABRPC_SOCKET_BASE=\"%s\" 不是合法端口 —— "
                   "要求 1..65535 的十进制整数（当前值不会被截断/回绕成别的端口）。\n"
                   "         本网络不会监听也不会连接任何地址（所有 RPC 立即失败）。\n",
                   b);
    }
  } else if (!PortsInRange(base_port_, 0)) {
    // 显式构造参数（非环境变量）同样要校验：SocketNetwork("127.0.0.1", 0) 这类
    // 调用以前会 bind 到内核分配的临时端口，客户端却连 base+servername → 全黑。
    port_ok_ = false;
    std::fprintf(stderr,
                 "[labrpc] SocketNetwork: base_port=%d 不是合法端口（要求 1..65535）—— "
                 "本网络不会监听也不会连接任何地址。\n",
                 base_port_);
  }
  // host 只在这里解析一次（见 ResolveIPv4 的注释）。注意环境变量覆盖发生在这
  // 之前：脚本用 LABRPC_SOCKET_HOST 统一改地址时，解析的也是被改过的值。
  host_ok_ = ResolveIPv4(host_, &host_addr_be_);
  if (!host_ok_) {
    std::fprintf(stderr,
                 "[labrpc] SocketNetwork: 主机 \"%s\" 无法解析为 IPv4 地址 —— "
                 "本网络不会监听也不会连接任何地址（所有 RPC 立即失败）。\n"
                 "         请用 LABRPC_SOCKET_HOST 指定一个 IPv4 地址或可解析的"
                 "域名（本后端不支持 IPv6）。\n",
                 host_.c_str());
  }
  stopped_.store(false);
  // 延迟回包 sender 线程池：到点发送 LongReordering 挂起的帧（不占 handler）
  for (size_t i = 0; i < kDelayWorkers; i++) {
    delay_workers_.emplace_back([this] { DelayLoop(); });
  }
  // 异步回调投递线程：cb 的唯一执行者（绝不内联在调用方/reader 线程上跑，
  // 见 DeliverResult 的注释）。与 in-proc 的"回调 Post 到线程池"语义对齐。
  cb_worker_ = std::thread([this] { CbWorkerLoop(); });
}

SocketNetwork::~SocketNetwork() { Cleanup(); }

void SocketNetwork::Cleanup() {
  if (stopped_.exchange(true)) return;  // 幂等

  // 1) 关掉所有监听 socket 和活跃连接 → 让阻塞在 recv/accept 的线程自然退出
  std::map<int, std::shared_ptr<ServerState>> servers;
  std::map<std::string, std::shared_ptr<Conn>> conns;
  {
    std::lock_guard<std::mutex> lk(mu_);
    // 【必须先把 servers_ 摘空，再关连接/join handler】：
    // acceptor 可能在"poll 返回 POLLIN → accept 返回 → 等 mu_"的途中被
    // Cleanup 抢走 mu_。若此时 servers_ 还挂着本 server，这次迟到的 accept
    // 会走正常分支 push 一个新 handler——而 ShutdownServerThreads 已经把
    // handlers vector 移走 join 完了，这个新 handler 没人 join，最终随
    // ServerState 析构 → ~thread 遇到 joinable → std::terminate → SIGABRT
    // （实测 net 模式 selftest ~10% 概率崩）。先摘空，迟到 accept 命中
    // 下面的 dead 分支直接 close，不会再有新 handler 入队。
    // （DeleteServer 本来就是"先 erase 再 shutdown"，顺序正确；这里对齐。）
    servers = std::move(servers_);
    servers_.clear();
    conns = conns_;
  }
  for (auto& kv : servers) {
    auto& st = kv.second;
    st->accept_stopped = true;   // 让 poll 化 acceptor 自行退出
    WakeAcceptor(*st);           // B1b：踢醒 poll（否则 join 要干等 ≤500ms）
    if (st->listen_fd >= 0) {
      ::close(st->listen_fd);
      st->listen_fd = -1;
    }
  }
  // 唤醒并 join 各 server 的处理线程（shutdown 让阻塞的 recv 返回）
  for (auto& kv : servers) {
    ShutdownServerThreads(kv.second);
  }
  // 等 acceptor 线程收工
  for (auto& kv : servers) {
    if (kv.second->acceptor.joinable()) kv.second->acceptor.join();
  }
  // acceptor 都已退出 → 关掉唤醒管道（否则每个 Config 析构都漏 2 个 fd）
  for (auto& kv : servers) CloseAcceptorWakePipe(*kv.second);

  // 2) 客户端 reader 线程：唤醒后它们会因 EOF 退出并自行关闭 fd，再统一 join
  for (auto& kv : conns) ShutdownConn(kv.second);
  // 把 reader 线程集合移出，避免 join 期间持锁；这些线程关 fd 后会在本轮末退出。
  // 元素是 pair<Conn, thread>，join 之后 callable 里的 shared_ptr<Conn> 一起释放。
  std::vector<std::pair<std::shared_ptr<Conn>, std::thread>> readers;
  {
    std::lock_guard<std::mutex> lk(mu_);
    readers.swap(reader_threads_);
  }
  for (auto& r : readers) {
    if (r.second.joinable()) r.second.join();
  }

  // 3) 延迟回包 sender 线程：丢弃所有还没发的帧（等价丢包），唤醒并 join
  {
    std::lock_guard<std::mutex> lk(mu_);
    delay_q_.clear();
  }
  delay_cv_.notify_all();
  for (auto& t : delay_workers_) {
    if (t.joinable()) t.join();
  }
  delay_workers_.clear();

  // 3.5) 异步回调投递线程：丢弃还没跑的 cb（同 in-proc Post 的 stopped_ 语义），
  // 唤醒并 join。必须 join：cb_worker_ 会访问 mu_/cb_q_，SocketNetwork 析构后
  // 还在跑就是 use-after-free（round-14 的 terminate 教训）。
  {
    std::lock_guard<std::mutex> lk(mu_);
    cb_q_.clear();
  }
  cb_cv_.notify_all();
  if (cb_worker_.joinable()) cb_worker_.join();

  // 4) 清空状态
  {
    std::lock_guard<std::mutex> lk(mu_);
    servers_.clear();
    conns_.clear();
    enabled_.clear();
    connections_.clear();
  }
}

// ===========================================================================
// 端点 / 服务端管理
// ===========================================================================

std::shared_ptr<ClientEnd> SocketNetwork::MakeEnd(const std::string& endname) {
  // 与 in-proc Network::MakeEnd 对齐：新建端点默认【禁用 + 未连接】。
  // 否则同名 endname 重建端点时，上一世代的 enabled_/connections_ 残留下来，
  // 新端点"没 Connect 就能发 RPC"，而 in-proc 会把它打回未启用状态 —— 同一份
  // 测试代码在两个后端表现不同，属于契约漂移。
  {
    std::lock_guard<std::mutex> lk(mu_);
    enabled_[endname] = false;
    connections_.erase(endname);
  }
  return std::make_shared<ClientEnd>(this, endname);
}

void SocketNetwork::AddServer(int servername, std::shared_ptr<Server> srv) {
  // 先清掉同名旧 server。注意：DeleteServer 内部自己上锁并 join 处理线程，
  // 不能在持 mu_ 时调用（std::mutex 非递归，会自死锁）。in-proc Network::AddServer
  // 是直接覆盖，本后端因为要回收 listen_fd / 线程，必须先干净地 shutdown 旧实例。
  DeleteServer(servername);

  std::lock_guard<std::mutex> lk(mu_);
  // 已 Cleanup 的网络是【终态】：绝不能再建 server。
  // 为什么必须挡住（实测复现）：Cleanup 是幂等的（stopped_.exchange 早退），所以
  //   net.Cleanup(); net.AddServer(0, ...);  // 误用 / 与 Cleanup 竞态
  // 会在这里重新 socket()+bind()+listen() 并起一个 acceptor；而 Cleanup 已经跑完、
  // 不会再来 join 它 → listen fd + acceptor 线程永久滞留（AcceptLoop 的退出条件
  // 原本只看 per-server 的 accept_stopped）。后果不是"多一个空转线程"这么轻：
  // 那个 socket 一直占着端口，同进程后续从零新建的 SocketNetwork（测试里每个
  // Config 都用同一个 base_port）bind 直接 EADDRINUSE —— 打印出来是"端口被别的
  // 进程占用"，实际是【自己上一个网络泄漏的】；server 起不来 → 所有 RPC 发黑洞
  // → 用例卡到看门狗。返回空操作即可：停机后的网络不接受任何新工作。
  if (stopped_.load()) return;
  // host 解析失败：构造期已经打过错因，这里保持"不监听"（宁可 RPC 立即失败，
  // 也不要把整个集群静默搬到 loopback 上）。
  if (!host_ok_) return;
  // 端口配置非法：同上，构造期已打过错因（见 ParsePortStrict 的注释）。
  if (!port_ok_) return;
  // base_port + servername 越界：htons(static_cast<uint16_t>(port)) 会静默回绕到
  // 一个完全无关的端口（如 65534+3 → 1），两个 worker 的端口段因此可能撞在一起。
  // 宁可这台 server 明确起不来，也不要"起在别的端口上"。
  if (!PortsInRange(base_port_, servername)) {
    std::fprintf(stderr,
                 "[labrpc] AddServer(%d): base_port=%d + servername=%d 超出 65535 —— "
                 "这台 server 不监听（请把 LABRPC_SOCKET_BASE 调小或减少 server 数）。\n",
                 servername, base_port_, servername);
    return;
  }

  int port = base_port_ + servername;
  int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd < 0) {
    std::fprintf(stderr, "[labrpc] AddServer(%d): socket() 失败: %s\n",
                 servername, std::strerror(errno));
    return;
  }
  int yes = 1;
  // SO_REUSEADDR允许端口被close后立刻可以被复用，通常关闭后有60s冷却期
  ::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

  sockaddr_in sa{};
  sa.sin_family = AF_INET;
  sa.sin_port = htons(static_cast<uint16_t>(port));
  // 用构造期解析好的地址（不再每次 inet_addr + "猜不到就 loopback" 兜底）
  sa.sin_addr.s_addr = host_addr_be_;

  if (::bind(listen_fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) < 0) {
    // 失败原因必须分清，否则报错会把排查方向带歪：
    //   EADDRINUSE    —— 端口被占（同机残留进程）；
    //   EADDRNOTAVAIL —— 这个地址根本不属于本机。
    // 后者是 host 现在能被解析成真实地址之后才会出现的新情况：以前 inet_addr
    // 认不出域名就把地址打回 loopback，永远不会 EADDRNOTAVAIL；现在
    // LABRPC_SOCKET_HOST=example.com 会真的去绑 93.184.x.x → 必然失败。
    // 若还按旧文案报"端口被占用"，人会去 pkill，然后发现完全没用。
    // 反正都要打印 —— 目标是"1 秒可见的明确错误"，而不是"10 分钟神秘卡死"。
    const int e = errno;
    if (e == EADDRNOTAVAIL) {
      std::fprintf(stderr,
                   "[labrpc] AddServer(%d): bind(%s:%d) 失败: %s —— "
                   "这个地址不属于本机（host 解析结果 %s）。\n"
                   "         跨机部署时每台机器要绑【自己】的地址；"
                   "检查 LABRPC_SOCKET_HOST。\n",
                   servername, host_.c_str(), port, std::strerror(e),
                   host_.c_str());
    } else if (e == EADDRINUSE) {
      std::fprintf(stderr,
                   "[labrpc] AddServer(%d): bind(%s:%d) 失败: %s —— "
                   "端口被别的进程占用（旧测试残留？跑前 pkill 清一下）\n",
                   servername, host_.c_str(), port, std::strerror(e));
    } else {
      std::fprintf(stderr, "[labrpc] AddServer(%d): bind(%s:%d) 失败: %s\n",
                   servername, host_.c_str(), port, std::strerror(e));
    }
    ::close(listen_fd);
    return;
  }
  // backlog 必须远大于"可能同时发起 connect 的客户端数"：测试里 20 个客户端
  // 首跳并发 connect 是常态（TestConcurrentCalls/Many），acceptor 又可能因
  // mu_ 争用/调度延迟几十毫秒才排空队列。backlog=16 时队列溢出，多出来的
  // 连接在 macOS 上直接吃 RST——客户端 connect() 成功、write() 成功、然后
  // reader 读到一个 ECONNRESET，在途 RPC 全判失败（实测 selftest net 模式
  // ~2% RPC 丢失）。128 是 macOS somaxconn，Linux 亦安全。
  if (::listen(listen_fd, 128) < 0) {
    std::fprintf(stderr, "[labrpc] AddServer(%d): listen(port=%d) 失败: %s\n",
                 servername, port, std::strerror(errno));
    ::close(listen_fd);
    return;
  }

  auto st = std::make_shared<ServerState>();
  st->servername = servername;
  st->addr = host_ + ":" + std::to_string(port);
  st->listen_fd = listen_fd;
  st->srv = srv;
  // B1b: 唤醒管道（见 socket_transport.h 里 wake_r/wake_w 的注释）。
  // pipe() 失败不是致命错误：AcceptLoop 会退回"只 poll listen_fd + 500ms 超时"
  // 的旧行为（功能正确、只是 DeleteServer 又变成最多等 500ms）。
  int wp[2] = {-1, -1};
  if (::pipe(wp) == 0) {
    ::fcntl(wp[0], F_SETFL, O_NONBLOCK);
    ::fcntl(wp[1], F_SETFL, O_NONBLOCK);
    st->wake_r = wp[0];
    st->wake_w = wp[1];
  } else {
    std::fprintf(stderr,
                 "[labrpc] AddServer(%d): pipe() 失败: %s（回退到 500ms 超时轮询）\n",
                 servername, std::strerror(errno));
  }
  int lfd = listen_fd;  // 按值捕获进 acceptor 线程：AcceptLoop 不再读 st->listen_fd，
                        // 避免与 DeleteServer 对 st->listen_fd 的写形成数据竞争（TSan 已抓到）
  st->acceptor = std::thread([this, st, lfd]() {
    AcceptLoop(st, lfd);
  });
  servers_[servername] = st;
}

void SocketNetwork::DeleteServer(int servername) {
  std::shared_ptr<ServerState> st;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = servers_.find(servername);
    if (it == servers_.end()) return;
    st = it->second;
    servers_.erase(it);
  }
  // 标记 acceptor 停止（poll 化后 ≤500ms 内自行退出，不再依赖 close 唤醒 accept
  // 的平台语义——macOS 上 close() 不一定能唤醒阻塞的 accept()），再关监听。
  // B1b：光置标志还不够，必须往唤醒管道写一字节把 poll 踢醒 —— 否则这里
  // join 时要干等最多 500ms（实测每次 DeleteServer 297~500ms，raft 每个 crash
  // 都走这条路）。
  st->accept_stopped = true;
  WakeAcceptor(*st);
  if (st->listen_fd >= 0) {
    ::close(st->listen_fd); // 关闭fd后 accept/poll 立即返回 → 双保险退出
    st->listen_fd = -1;
  }
  ShutdownServerThreads(st);
  // 主动撕掉所有「连向本 server」的 client 连接：server 重启（DeleteServer+AddServer
  // 同端口）后，旧 TCP 连接对端已死，但 EOF 的异步传播在某些平台/调度下会延迟或不可靠，
  // 导致 client 复用死连接、写成功却永远收不到回包 → 用例卡死到看门狗 abort（net 模式
  // 间歇 FAIL 的真凶之一）。这里立刻标记 closed + shutdown，让 ConnReader 立即退出、
  // pending 判失败，下次 RPC 重建连接，不等 EOF。
  {
    std::vector<std::shared_ptr<Conn>> to_shutdown;
    {
      std::lock_guard<std::mutex> lk(mu_);
      for (auto& kv : conns_) {
        auto cit = connections_.find(kv.first);
        if (cit != connections_.end() && cit->second == servername) {
          to_shutdown.push_back(kv.second);
        }
      }
    }
    for (auto& c : to_shutdown) ShutdownConn(c);
  }
  if (st->acceptor.joinable()) st->acceptor.join();  // 等待唯一的accpetor结束
  // acceptor 已退出 → 此刻没有线程在 poll 这两个 fd，才能安全关闭。
  CloseAcceptorWakePipe(*st);
}

void SocketNetwork::Connect(const std::string& endname, int servername) {
  std::lock_guard<std::mutex> lk(mu_);
  connections_[endname] = servername;
}

void SocketNetwork::Enable(const std::string& endname, bool enabled) {
  std::lock_guard<std::mutex> lk(mu_);
  enabled_[endname] = enabled;
}

void SocketNetwork::RemoveClientEnd(const std::string& endname) {
  std::shared_ptr<Conn> conn;
  {
    std::lock_guard<std::mutex> lk(mu_);
    enabled_.erase(endname);
    connections_.erase(endname);
    auto it = conns_.find(endname);
    if (it != conns_.end()) {
      conn = it->second;
      conns_.erase(it);
    }
  }
  if (conn) ShutdownConn(conn);  // 只唤醒 + 置 closed，fd 由 ConnReader 关闭
}

// 通知连接关闭并唤醒阻塞的 I/O；fd 的实际 close 由 ConnReader 负责，
// 避免 RemoveClientEnd/Cleanup 跨线程 close 同一 fd 后该 fd 号被 socket() 复用，
// 导致 ConnReader/SendReq 误读写到别人的连接上（P0：fd-reuse 竞态）。
void SocketNetwork::ShutdownConn(std::shared_ptr<Conn> conn) {
  if (!conn) return;
  {
    std::lock_guard<std::mutex> lk(conn->mu);
    conn->closed = true;  // 标志仍留 conn->mu（SendReq 同锁读），用于拦截后续 SendReq
  }
  // shutdown 收归 write_mu，与 ConnReader 退出关 fd、SendReq 写 fd 互斥，
  // 杜绝 conn->fd 的跨锁 data race（含潜在的双重 close + fd 复用误伤）。
  {
    std::lock_guard<std::mutex> lw(conn->write_mu);
    if (conn->fd >= 0) ::shutdown(conn->fd, SHUT_RDWR);  // 唤醒阻塞的 read/write，使其返回
    // 这里【不做】兜底 close。早先有一句
    //     if (conn->reader_done && conn->fd >= 0) { ::close(conn->fd); ... }
    // 是可证明的死代码：reader_done 与 fd=-1 在 ConnReader 退出块的【同一个】
    // write_mu 临界区里一起写，所以 "reader_done == true" 必然蕴含 "fd == -1"，
    // 且 fd 之后再也不会变回 >= 0。留着它的害处不是多几行，而是让人以为
    // "fd 泄漏在这里兜住了"，从而不敢动真正的唯一关闭点（ConnReader 退出块）。
    // 第 30 轮把这条死代码连同 reader_done 字段一起删了（删完之后它变成
    // "只写不读"的死状态，正是本项目一直在清的那类病）。本函数只负责 shutdown 唤醒。
  }
}

// ===========================================================================
// 混沌旋钮 / 统计 / 字节上限
// ===========================================================================

void SocketNetwork::Reliable(bool yes) { reliable_.store(yes); }
void SocketNetwork::LongReordering(bool yes) { long_reordering_.store(yes); }
// 空操作【是有意为之】，不是漏接线：见 socket_transport.h 的成员注释。
// 接口是纯虚的，所以这个 overrider 必须存在；但参数确实被忽略，
// 形参名注释掉以防 -Wunused-parameter。
void SocketNetwork::LongDelays(bool /*yes*/) {}

int SocketNetwork::GetCount(int servername) {
  std::shared_ptr<Server> srv;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = servers_.find(servername);
    if (it != servers_.end()) srv = it->second->srv;
  }
  return srv ? srv->GetCount() : 0;
}

int SocketNetwork::GetHandlerThreadCount(int servername) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = servers_.find(servername);
  if (it == servers_.end()) return 0;
  return static_cast<int>(it->second->handlers.size());
}

int SocketNetwork::GetReaderThreadCount() {
  std::lock_guard<std::mutex> lk(mu_);
  return static_cast<int>(reader_threads_.size());
}

int SocketNetwork::GetTotalCount() const { return count_.load(); }
int64_t SocketNetwork::GetTotalBytes() const { return bytes_.load(); }
void SocketNetwork::SetMaxMessageBytes(size_t n) { max_message_bytes_.store(n); }
size_t SocketNetwork::MaxMessageBytes() const { return max_message_bytes_.load(); }
int64_t SocketNetwork::OversizedDropped() const {
  return oversized_dropped_.load();
}

// ===========================================================================
// 服务端：accept / 派发
// ===========================================================================
// 让客户端自己关闭自己的fd，并等待所有客户端处理线程结束
void SocketNetwork::ShutdownServerThreads(std::shared_ptr<ServerState> st) {
  std::vector<std::pair<std::shared_ptr<SConn>, std::thread>> hs;
  {
    std::lock_guard<std::mutex> lk(mu_);
    // shutdown 唤醒客户端阻塞在 RecvRequestFrame 底层的 ::read(cfd) 上；
    // fd 的关闭统一由 HandleClient 退出块在本线程里做（持 write_mu），
    // 这里只 wakeup 不 close —— 跨线程 close 会让 fd 号被 socket() 立刻复用（P0）。
    for (auto& c : st->client_conns) {
      // 持 write_mu shutdown：与 HandleClient 的帧发送 / DelayLoop 的延迟帧
      // 发送同锁，避免动一个正在被写的 fd（P0 家族）。
      std::lock_guard<std::mutex> lw(c->write_mu);
      if (c->fd >= 0) ::shutdown(c->fd, SHUT_RDWR);
    }
    hs = std::move(st->handlers);
  }
  // 等所有 waiter 下班；它们退出时已把各自 conn->fd 置 -1 并 close，
  // 所以此后 DelayLoop 拿到这些连接只会丢弃（fd < 0），不需要额外的 closed 标志。
  for (auto& h : hs) {
    if (h.second.joinable()) h.second.join();
  }
}

// 回收"已经干完活"的 handler 线程。
//
// 为什么必须有它：std::thread 在 join 之前一直持有两样东西 ——
//   ① 已退出线程的栈（macOS/Linux 都要等 join 才真正释放）；
//   ② callable（lambda 里按值捕获的 shared_ptr<SConn>）。
// 早先 handlers 只在 DeleteServer/Cleanup 时统一 join，于是"长期存活的服务端 +
// 短连接 churn"会无限堆资源。实测（/tmp/probe_conn_leak2.cpp，3000 次 connect→close）：
//   before DeleteServer: RSS +48.3 MiB（≈16 KiB/连接，线性）
//   after  DeleteServer: RSS +0.3 MiB      ← 证明是 join 前未释放，不是真泄漏
// 在 AcceptLoop 里定期调用后，滞留量被压到"最近 250ms 内结束的连接"。
//
// 只在 done 置位后 join（绝不阻塞 acceptor），且持 mu_ 只做"摘下 + 移走"，
// join 一律在锁外 —— 与 ShutdownServerThreads 同一套纪律。
void SocketNetwork::ReapFinishedHandlers(std::shared_ptr<ServerState> st) {
  std::vector<std::thread> finished;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto& hs = st->handlers;
    size_t w = 0;
    for (size_t r = 0; r < hs.size(); r++) {
      if (hs[r].first && hs[r].first->done.load(std::memory_order_acquire)) {
        finished.push_back(std::move(hs[r].second));
      } else {
        if (w != r) hs[w] = std::move(hs[r]);
        w++;
      }
    }
    hs.resize(w);
  }
  for (auto& t : finished) {
    if (t.joinable()) t.join();
  }
}

// 回收已经跑完的客户端 reader 线程。
//
// 与 ReapFinishedHandlers 完全同构（同一个坑的另一半，见 socket_transport.h 里
// reader_threads_ 的注释）。差别只有两点：
//   * 触发点不是"常驻的 accept 循环"，而是 GetOrCreateConn —— 客户端没有轮询线程，
//     而滞留量只在"新建连接"时才会增长，所以建连点就是最自然的回收点；
//   * 判据用 conn->reader_exited（ConnReader 最后一行置位）而不是 reader_done：
//     reader_done 置位之后线程还要给在途请求逐个投递失败（可能进 cb_q_ 抢 mu_），
//     此时 join 会把 SendReq 的调用方（Raft replicator / ticker）挡住。
void SocketNetwork::ReapFinishedReaderThreads() {
  std::vector<std::thread> finished;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto& v = reader_threads_;
    size_t w = 0;
    for (size_t r = 0; r < v.size(); r++) {
      if (v[r].first &&
          v[r].first->reader_exited.load(std::memory_order_acquire)) {
        finished.push_back(std::move(v[r].second));
      } else {
        if (w != r) v[w] = std::move(v[r]);
        w++;
      }
    }
    v.resize(w);
  }
  // join 必须在锁外：reader 线程退出前还要取 mu_ 摘 conns_ 槽位。
  for (auto& t : finished) {
    if (t.joinable()) t.join();
  }
}

// B1b: 唤醒 acceptor。语义要求：调用方【已经】把 st->accept_stopped 置为 true，
// 本函数只负责"踢一脚"让 poll 立刻返回。多写或少写一个字节都无妨（acceptor
// 醒来后先复查标志，根本不读管道内容）；管道满（EAGAIN）也无妨。
void SocketNetwork::WakeAcceptor(ServerState& st) {
  if (st.wake_w < 0) return;
  const char b = 1;
  ssize_t n = ::write(st.wake_w, &b, 1);
  (void)n;  // EAGAIN/EPIPE 都可以忽略：poll 的 500ms 超时是最终兜底
}

void SocketNetwork::CloseAcceptorWakePipe(ServerState& st) {
  if (st.wake_r >= 0) {
    ::close(st.wake_r);
    st.wake_r = -1;
  }
  if (st.wake_w >= 0) {
    ::close(st.wake_w);
    st.wake_w = -1;
  }
}

void SocketNetwork::AcceptLoop(std::shared_ptr<ServerState> st, int lfd) {
  // lfd 是 AddServer 创建线程时按值捕获的，本函数不再读取 st->listen_fd，
  // 从而与 DeleteServer 中对 st->listen_fd 的写无并发访问（消除 TSan 报告的 data race）。
  // 退出条件有两个，缺一不可：
  //   ① per-server 的 accept_stopped —— DeleteServer 只想停【这一台】的 acceptor，
  //      不能误停其它 server（所以不能用全局 stopped_ 替代它）；
  //   ② 全局 stopped_ —— Cleanup 之后整张网络是终态，acceptor 必须跟着退。
  //      早先只检查 ①，于是"Cleanup 之后才被 AddServer 建起来"的 acceptor 会一直
  //      空转到进程结束（见 AddServer 里 stopped_ 守卫的注释；两处是同一个洞的
  //      两道闸，缺任何一个都会留下占着端口的监听 socket）。
  // 即便平台不保证 close() 能唤醒阻塞的 accept()（macOS 经典坑），poll 500ms
  // 超时也会让循环自行醒来、查标志、退出。
  //
  // 顺带在这个循环里定期回收已经结束的 handler 线程（见 ReapFinishedHandlers）：
  // ① poll 超时分支 —— 连接静默后也能把最后一批收掉；
  // ② 每次 accept 之后（带 250ms 节流）—— 连接风暴期间 poll 永不超时，
  //    光靠 ① 会让滞留量涨到风暴全量。
  auto last_reap = raftcpp::Now();
  while (!stopped_.load() && !st->accept_stopped.load(std::memory_order_acquire) &&
         lfd >= 0) {
    // 同时等两个 fd：
    //   [0] listen_fd —— 有新连接；
    //   [1] 唤醒管道 —— DeleteServer/Cleanup 写一个字节，让本线程立刻醒来。
    // 为什么不能只靠 close(lfd)：见 socket_transport.h 里 wake_r/wake_w 的注释
    // （macOS 上 close 不保证唤醒 poll，实测量化到 297~500ms/次）。
    // wake_r 为 -1 时 poll 会忽略该槽位（POSIX：负 fd 的 revents 置 0），
    // 于是自动退回"只等 listen_fd + 500ms 超时"的旧行为。
    pollfd pfds[2];
    pfds[0].fd = lfd;
    pfds[0].events = POLLIN;
    pfds[0].revents = 0;
    pfds[1].fd = st->wake_r;
    pfds[1].events = POLLIN;
    pfds[1].revents = 0;
    int pr = ::poll(pfds, 2, 500);  // 500ms 超时仍是兜底（唤醒管道不可用/极端平台）
    if (pr < 0) {
      if (errno == EINTR) continue;
      break;  // listen_fd 已关闭（EBADF 等）→ 退出
    }
    // ★ 醒来后【先复查退出条件、再 accept】。这一步不只是省时间：
    //   poll 可能因为 listen_fd 被关掉（POLLNVAL）或唤醒字节而返回，此时若直接
    //   accept(lfd)，而 lfd 的 fd 号刚被同进程下一个 socket() 复用，就会对着
    //   【别人的 fd】调 accept()（两代 acceptor 抢同一 fd 的窗口）。
    //   先复查标志就从这个窗口里彻底跳出去。
    if (stopped_.load() || st->accept_stopped.load(std::memory_order_acquire)) break;
    if (pr == 0) {  // 超时：回到循环顶查 accept_stopped
      ReapFinishedHandlers(st);
      last_reap = raftcpp::Now();
      continue;
    }
    int cfd = ::accept(lfd, nullptr, nullptr);  // 有连接到达（或 listen_fd 关闭）
    if (cfd < 0) {
      if (errno == EINTR) continue;
      // ★ accept() 的失败必须分两类，绝不能一律 break。
      //
      // 本循环 break 的代价是"这台 server 永久死亡"，而且是最难查的那种死法：
      // 监听 fd 还开着 → 客户端 connect() 仍然成功（内核在 backlog 里完成握手）
      // → 请求写出去也成功 → 然后【永远没有回包】。上层看到的是"集群活着但
      // 节点失联"，不是"进程崩了"。
      //
      // 实测（/tmp/probe_accept_err.cpp，把进程 fd 表填满后接一条连接）：
      //   accept<0 errno=24(EMFILE) → break → 释放 fd 之后同一条用例的 RPC
      //   2042ms 超时失败（修复前），修复后 0ms 成功。
      // ECONNABORTED 同理：客户端 connect 后立刻 close/RST，BSD/macOS 的 accept
      // 会返回 ECONNABORTED —— 这是【对端的行为】，不是本机的故障。
      //
      // 判据：只有"监听 fd 本身没了"才是真退出理由（DeleteServer/Cleanup 关掉
      // 它之后 poll 会先返回 POLLNVAL，正常路径在循环顶就 break 了）；其余一律
      // 视为瞬时错误 —— 记日志（每台 server 限流，避免 fd 长期打满时刷屏）+
      // 短暂退避 + continue。
      const int e = errno;
      if (e == EBADF || e == ENOTSOCK || e == EINVAL || e == ENOENT) break;
      if (st->accept_err_logged.fetch_add(1) < 8) {
        std::fprintf(stderr,
                     "[labrpc] AddServer(%d): accept() 失败: %s —— "
                     "瞬时错误，已退避重试（监听未停止）\n",
                     st->servername, std::strerror(e));
      }
      // 退避 20ms：fd 打满/pending 队列里挂着坏连接时 poll 会立刻再次返回，
      // 不退避就是纯自旋（50 次/秒仍远小于自旋，同时保证恢复足够快）。
      raftcpp::SleepMs(20);
      continue;
    }
    auto conn = std::make_shared<SConn>();
    conn->fd = cfd;
    {
      std::lock_guard<std::mutex> lk(mu_);
      auto it = servers_.find(st->servername);
      if (it == servers_.end()) {   // 这里可能已经被deleteserver删掉了
        ::close(cfd);
        continue;
      }
      it->second->client_conns.push_back(conn);
      // 跟踪 handler 线程（不再 detach）：Cleanup / DeleteServer 负责 join，
      // 否则线程会在 SocketNetwork 析构后访问已销毁的 mu_ → use-after-free。
      // 成对保存 SConn：线程结束后由 ReapFinishedHandlers 按 done 及时 join，
      // 不必等到整个 server 关停（否则短连接 churn 会无限积累线程栈）。
      it->second->handlers.emplace_back(
          conn, std::thread([this, srv = st->servername, conn]() {
            HandleClient(srv, conn);
          }));
    }
    if (raftcpp::Now() - last_reap >= std::chrono::milliseconds(250)) {
      ReapFinishedHandlers(st);
      last_reap = raftcpp::Now();
    }
  }
}

void SocketNetwork::HandleClient(int servername, std::shared_ptr<SConn> conn) {
  std::shared_ptr<Server> srv;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = servers_.find(servername);
    if (it != servers_.end()) srv = it->second->srv;
  }

  // fd 快照：读请求全程用这一份。本线程是唯一读者，关闭只发生在下面的
  // 退出块（同线程），ShutdownServerThreads 只 shutdown 不 close，
  // 所以快照值在整个循环内始终有效。
  int cfd = -1;
  {
    std::lock_guard<std::mutex> lw(conn->write_mu);
    cfd = conn->fd;
  }

  while (!stopped_ && cfd >= 0) {
    uint64_t corr = 0;
    std::string svc, args;
    if (!RecvRequestFrame(cfd, corr, svc, args, RecvFrameLenCap(max_message_bytes_)))
      break;  // 连接断开

    // 服务端被删 / 被同名新实例替换？这条请求作废（对应 in-proc 的 IsServerDead）
    bool alive = false;
    {
      std::lock_guard<std::mutex> lk(mu_);
      auto it = servers_.find(servername);
      // 必须是「同一个 Server 对象」：DeleteServer 后再 AddServer 同名实例时，
      // in-proc 靠 sit->second != e.server 判死；这里对齐同一语义，避免把请求
      // 派发给已被换掉的旧 server 对象（否则会落到过期的 srv 指针上）。
      if (it != servers_.end() && it->second->srv == srv) alive = true;
    }
    if (!alive) break;
    // 调用具体rpc方法。
    // ⚠️ 这里【不能】用 Server::Dispatch：它对未知 service / 未知方法 / 名字里没有
    // '.' 都是 std::abort()。那在 in-proc 后端是合理的"编程错误断言"（方法名是
    // 本方编译期常量），但本后端的方法名是【从 socket 读出来的远程输入】：
    //   * 对端可能是另一个进程/机器（这个后端的设计目标就是能跨机）；
    //   * 极端情况下帧错位也会把方法名变成垃圾。
    // 一行 38 字节的脏帧杀掉整个进程（实测：发 "Echo.NoSuchMethod" → SIGABRT 134），
    // 属于把"对方发错一个包"升级成"我这边全挂"。改为 TryDispatch：不认识就回一个
    // ok=false 的失败帧，连接继续可用（后面的合法请求照常处理）。
    std::string reply;
    bool served = (srv != nullptr) && srv->TryDispatch(svc, args, &reply);
    if (!served) {
      std::lock_guard<std::mutex> lw(conn->write_mu);
      if (conn->fd >= 0) SendReplyFrame(conn->fd, corr, false, "");
      continue;
    }
    // 模拟不可靠网丢包乱序
    bool drop = (!reliable_.load() && raftcpp::RandInt(1000) < 100);
    bool reorder = (long_reordering_.load() && raftcpp::RandInt(900) < 600);
    if (drop) {
      // 丢回包：发一个"失败帧"，让客户端立即失败（与 in-proc ok=false 等价）
      std::lock_guard<std::mutex> lw(conn->write_mu);
      if (conn->fd >= 0) SendReplyFrame(conn->fd, corr, false, "");
    } else if (reorder) {
      // 乱序：把回包挂进延迟队列【立即返回】，不睡本线程。
      // 旧实现在这里 sleep 200~2200ms，把整条连接的处理循环堵住
      // （后续请求全部队头阻塞）→ net 模式 TestFigure8Unreliable2C 稳定失败。
      int64_t d = 200 + raftcpp::RandInt(1 + raftcpp::RandInt(2000));
      ScheduleDelayedReply(conn, corr, std::move(reply), d);
    } else {
      std::lock_guard<std::mutex> lw(conn->write_mu);
      if (conn->fd >= 0) SendReplyFrame(conn->fd, corr, true, reply);
    }
  }

  {
    std::lock_guard<std::mutex> lk(mu_);
    auto sit = servers_.find(servername);
    if (sit != servers_.end()) {
      auto& v = sit->second->client_conns;
      // 砍掉所有等于本连接的（handlers 里的线程句柄由 ReapFinishedHandlers 回收）
      v.erase(std::remove_if(v.begin(), v.end(),
                             [&conn](const std::shared_ptr<SConn>& c) {
                               return c.get() == conn.get();
                             }),
              v.end());
    }
  }
  // 连接断了：关闭本端 fd（持 write_mu，与 DelayLoop 的延迟帧发送互斥；
  // 之后 DelayLoop 看到 fd=-1 会丢弃还没发的帧，等价丢包）
  {
    std::lock_guard<std::mutex> lw(conn->write_mu);
    if (conn->fd >= 0) {
      ::close(conn->fd);
      conn->fd = -1;
    }
  }
  // 最后一步：宣布"本线程随时可 join"。必须在关 fd 之后、且在不再触碰任何
  // 共享状态之前置位 —— ReapFinishedHandlers 看到 done 就会立刻 join，
  // 而 join 只保证"线程已返回"，不保证"资源已按某个顺序释放"。
  conn->done.store(true, std::memory_order_release);
}

// ===========================================================================
// 异步回调投递（cb 绝不内联）
// ===========================================================================

// cb 的唯一执行线程。语义对齐 in-proc Network::WorkerLoop 对回调的处理：
//   * stopped_ 即退，残留回调丢弃（与 in-proc 的 Post 在 stopped_ 后丢弃一致）；
//   * 任务在【锁外】执行——cb 可能重新调 SendReq 把新 cb 压回 cb_q_（不死锁，
//     只是排队），也可能抢 Raft 的锁（持 mu_ 跑会把全局 map 访问全堵死）。
void SocketNetwork::CbWorkerLoop() {
  std::unique_lock<std::mutex> lk(mu_);
  while (!stopped_.load()) {
    cb_cv_.wait(lk, [this] { return stopped_.load() || !cb_q_.empty(); });
    if (stopped_.load()) return;
    auto task = std::move(cb_q_.front());
    cb_q_.pop_front();
    lk.unlock();
    task();
    lk.lock();
  }
}

// ===========================================================================
// LongReordering：延迟回包（sender 线程池）
// ===========================================================================

void SocketNetwork::ScheduleDelayedReply(std::shared_ptr<SConn> conn,
                                         uint64_t corr, std::string reply,
                                         int64_t delay_ms) {
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (stopped_.load()) return;  // 网络已停：不排了（客户端侧会收到失败）
    delay_q_.emplace(raftcpp::Now() + std::chrono::milliseconds(delay_ms),
                     DelayedFrame{std::move(conn), corr, std::move(reply)});
  }
  delay_cv_.notify_one();
}

void SocketNetwork::DelayLoop() {
  std::unique_lock<std::mutex> lk(mu_);
  while (!stopped_.load()) {
    if (delay_q_.empty()) {
      delay_cv_.wait_for(lk, std::chrono::milliseconds(50));
      continue;
    }
    auto due_in = delay_q_.begin()->first - raftcpp::Now();
    if (due_in > std::chrono::milliseconds(0)) {
      delay_cv_.wait_for(lk, due_in);
      continue;  // 醒来后重新判断（可能又是空队列/新的更早帧）
    }
    // 取出所有已到期的帧
    auto now = raftcpp::Now();
    std::vector<DelayedFrame> due;
    while (!delay_q_.empty() && delay_q_.begin()->first <= now) {
      due.push_back(delay_q_.begin()->second);
      delay_q_.erase(delay_q_.begin());
    }
    // 先释放 mu_ 再持 conn->write_mu：
    // ① 全局唯一嵌套方向是 mu_ → write_mu（ShutdownServerThreads :485、
    //    GetOrCreateConn :686/:717），没有任何反向持锁者，构成不了 ABBA
    //    （旧注释把方向写反了，已纠正）；
    // ② 真正的原因：SendReplyFrame 可能阻塞（对端不读 → TCP 发送缓冲打满），
    //    持着 mu_ 发送会把 AcceptLoop 注册 / HandleClient 存活检查 /
    //    GetOrCreateConn / SendReq 快照全堵死 → 系统级停顿。
    lk.unlock();
    for (auto& f : due) {
      std::lock_guard<std::mutex> lw(f.conn->write_mu);
      if (f.conn->fd >= 0) {
        SendReplyFrame(f.conn->fd, f.corr, true, f.reply);
      }
      // fd<0：连接已关，丢弃（乱序+不可靠网下等价于丢包）
    }
    lk.lock();
  }
}

// ===========================================================================
// 客户端：懒连接 + 发送 + 读回包
// ===========================================================================

std::shared_ptr<SocketNetwork::Conn> SocketNetwork::GetOrCreateConn(
    const std::string& endname, int servername) {
  if (stopped_.load(std::memory_order_acquire)) return nullptr;  // Cleanup 后不再建连
  // host 解析失败：不建连（构造期已打过错因）。返回 nullptr → SendReq 立即
  // 以失败投递，绝不会"悄悄连到 loopback 上假装成功"。
  if (!host_ok_) return nullptr;

  // 端口配置非法 / base+servername 越界：不建连（构造期或 AddServer 已打过错因）。
  // 与 host_ok_ 同一纪律：宁可 RPC 立即失败，也不要"连到被回绕出来的那个端口上"。
  if (!port_ok_ || !PortsInRange(base_port_, servername)) return nullptr;

  // 一条缓存连接还能不能复用。判据三条，缺一不可（实现见头文件 ConnReusable）：
  //   ① !closed        —— 没被 ShutdownConn 宣布死亡；
  //   ② fd >= 0        —— 连接还没被关掉；
  //   ③ servername 相同 —— 端点没被 Connect 改指向别的 server。
  //
  // 为什么 ① 必须单独列出来（第 32 轮发现的漏判）：
  //   ShutdownConn 先置 closed=true（在 conn->mu 下），fd 的 close/置 -1 由
  //   ConnReader 稍后在 write_mu 下做 —— 这中间有一段窗口，**fd 仍然 >= 0**。
  //   DeleteServer 正是这样撕掉"连向本 server"的客户端连接的（见 :641 注释），
  //   于是 crash→restart 之后紧接着的那条 RPC 会捞到这条已死的连接：
  //     GetOrCreateConn 判"可复用" → 返回 → SendReq 在 conn->closed 处直接
  //     DeliverResult(false) → 调用方收到一次凭空的失败（上层只能靠重试兜）。
  //   in-proc 没有连接缓存，天然没有这个洞 → 两个后端语义分歧。
  //
  // 证据强度（如实记录，别把间歇现象说成必现）：
  //   * 空载、100 轮同端口重启（/tmp/probe_restart_first_call.cpp）：10 次里
  //     有 2 次出现失败（94/100），其余 100/100，冷启动那次 86/100 —— 窗口
  //     宽度取决于 reader 被调度的快慢，所以【间歇】而非必现；
  //   * 4 个 hammer 线程压着重启（/tmp/probe_restart_racy.cpp）：偶发同样现象。
  //   修复后：两个探针各跑数轮，全部 100/100（0 失败）。
  //   因为间歇，这里【不】拿端到端成功率当断言（会变成 flaky 赌局），
  //   改为把判据本身固化成 selftest 的用例 TestConnReusePredicate。
  //
  // 注：先读 closed 再读 fd，两次读之间仍有纳秒级窗口；那种情况由 SendReq
  // 自己的 closed 检查兜底（只会失败，不会写坏 fd），不在本函数的职责内。
  auto conn_reusable = [servername](const std::shared_ptr<Conn>& c) {
    bool closed = true;
    {
      std::lock_guard<std::mutex> lc(c->mu);
      closed = c->closed;
    }
    // 读 conn->fd 须持 write_mu（与 ConnReader/ShutdownConn 写 fd 同锁，消 data race）
    std::lock_guard<std::mutex> lw(c->write_mu);
    return ConnReusable(c->fd, closed, c->servername, servername);
  };

  std::shared_ptr<Conn> stale;  // 目标变了/连接已废：需要在锁外 shutdown 的旧连接
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = conns_.find(endname);     // 有没有找到这个客户端的这条 TCP 连接的文件描述符
    if (it != conns_.end()) {
      if (conn_reusable(it->second)) return it->second;
      // 不能在这里 ShutdownConn：它要再取 write_mu，std::mutex 非递归 → 自死锁。
      // 也不 erase：ConnReader 退出时会比对 self 再 erase，这里马上要覆盖槽位。
      stale = it->second;
    }
  }
  if (stale) ShutdownConn(stale);  // 撤掉串台的旧连接（在途请求随之判失败，上层重试）

  int port = base_port_ + servername;        // 端口号
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);   // ipv4+tcp协议，准备一个文件描述符，用来作为socket与内核通信的号码牌
  if (fd < 0) return nullptr;
  sockaddr_in sa{};   // c风格的struct
  sa.sin_family = AF_INET;
  sa.sin_port = htons(static_cast<uint16_t>(port));
  sa.sin_addr.s_addr = host_addr_be_;  // 构造期解析好的地址（见 ResolveIPv4）
  // os隐式给客户端生成出的fd随机分配一个端口bind到server端指定端口
  if (::connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) < 0) {
    ::close(fd);
    return nullptr;
  }

  auto conn = std::make_shared<Conn>();
  conn->fd = fd;
  conn->servername = servername;
  std::shared_ptr<Conn> replaced;  // 槽位里被我们覆盖掉的对手（指向别的目标 / 已废）
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (stopped_.load()) {  // Cleanup 已启动，别再建连/起线程
      ::close(fd);
      return nullptr;
    }
    // 并发下另一线程可能已建好【指向同一台 server】的连接，复用之，多个rpc并发发送
    auto it = conns_.find(endname);
    if (it != conns_.end()) {
      if (conn_reusable(it->second)) {
        ::close(fd);
        return it->second;
      }
      replaced = it->second;  // 目标不同（并发 Connect 改指向）/ 已 closed：锁外撤掉，避免 fd/线程泄漏
    }
    conns_[endname] = conn;
  }
  if (replaced) ShutdownConn(replaced);
  // reader 线程按值捕获 conn（保证 Conn 存活至 reader 返回），但线程对象收归
  // reader_threads_ 由 Cleanup 统一 join；Conn 自身不再持有 std::thread，避免
  // ~Conn() 在仍 joinable 的 reader 线程上触发 std::terminate。
  // 循环从 conn->fd 读回包帧、用 corr_id 匹配在途请求、调 DeliverResult 交回结果
  std::thread th([this, conn, endname]() { ConnReader(conn, endname); });  // 构造即启动线程
  bool orphan = false;
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (stopped_.load()) {
      // Cleanup 已把 reader_threads_ swap 走并 join，这个线程不能塞进去（否则漏 join → 悬空）
      orphan = true;
    } else {
      // 成对存 Conn：线程结束后由 ReapFinishedReaderThreads 及时 join（详见头文件
      // reader_threads_ 的注释），move 仅移不可拷只能交的资源
      reader_threads_.emplace_back(conn, std::move(th));
    }
  }
  if (orphan) {
    th.join();  // 就地 join，reader 会因 stopped_ 立刻退出并关闭 fd
  } else {
    // 建连点就是回收点：把此前"已经断掉、reader 已退出"的线程在这里 join 掉。
    // 放在锁外、且对象都是已执行到最后一行的线程 → 不会挡住 SendReq 的调用方。
    ReapFinishedReaderThreads();
  }
  return conn;
}

void SocketNetwork::ConnReader(std::shared_ptr<Conn> conn,
                               std::string endname) {
  while (!stopped_) {
    uint64_t corr = 0;
    bool ok = false;
    std::string reply;
    // 读 fd 前先持 write_mu 快照：conn->fd 的全部写（关闭/置 -1）都在 write_mu 下，
    // 这里若无锁读，会和 ShutdownConn 对 conn->fd 的读形成「跨锁访问同一字段」→ TSan 报 race。
    // 关键：快照后必须释放锁再阻塞读——否则 ShutdownConn 拿不到 write_mu 去 shutdown() 唤醒本线程，
    // 会死锁。读期间 fd 不会被真正关闭（client 侧 fd 的唯一关闭点是本函数退出块，
    // 且与这里的快照同锁 write_mu），所以快照值在本轮迭代内始终有效。
    int rfd = -1;
    {
      std::lock_guard<std::mutex> lk(conn->write_mu);
      rfd = conn->fd;
    }
    if (rfd < 0) break;
    if (!RecvReplyFrame(rfd, corr, ok, reply, RecvFrameLenCap(max_message_bytes_)))
      break;  // 连接断开

    std::shared_ptr<ReqMsg> req;
    {
      std::lock_guard<std::mutex> lk(conn->mu);
      auto it = conn->pending.find(corr);  // corr可能损坏
      if (it == conn->pending.end()) continue;
      req = it->second;
      conn->pending.erase(it);  // 清除工单
    }
    // 字节记账在 DeliverResult 里（见那里的注释）：这里加会因为"cancel 之后
    // 才发现要作废"而多算一个回包长度，与 in-proc 后端不一致。
    DeliverResult(req, ok, std::move(reply));
  }
  // 连接断了：把还在等的请求全部判失败；并关闭本端 fd（client 侧 fd 的唯一关闭点）
  std::vector<std::shared_ptr<ReqMsg>> pending_fail;
  {
    std::lock_guard<std::mutex> lk(conn->mu);
    pending_fail.reserve(conn->pending.size());
    for (auto& kv : conn->pending) pending_fail.push_back(kv.second);
    conn->pending.clear();
    conn->closed = true;
  }
  {
    // fd 关闭统一收归 write_mu：与 ShutdownConn 的 shutdown、SendReq 的写 fd 互斥，
    // 让 conn->fd 的全部读写落在同一把锁下，根除跨锁 data race。
    std::lock_guard<std::mutex> lw(conn->write_mu);
    if (conn->fd >= 0) {
      ::close(conn->fd);
      conn->fd = -1;
    }
  }
  // 锁外投递失败结果：避免持 conn->mu 跑用户 cb（cb 内若再发同 conn 的 RPC，
  // 会在 SendReq 处再次取 conn->mu → 非递归锁自死锁）
  for (auto& req : pending_fail) DeliverResult(req, false, "");
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = conns_.find(endname); //  一个 endname 在 conns_ 里只绑一个 conn
    if (it != conns_.end() && it->second.get() == conn.get()) conns_.erase(it);
  }
  // 最后一行：宣布"本线程随时可 join"。必须是本函数【最后一条语句】——
  // ReapFinishedReaderThreads 看到它就会 join，而 join 只保证"线程已返回"，
  // 不保证其它顺序；放在这里才能保证 join 时本线程不再触碰任何共享状态。
  conn->reader_exited.store(true, std::memory_order_release);
}

void SocketNetwork::SendReq(std::shared_ptr<ReqMsg> req) {
  // ---- 单条消息字节上限：和 in-proc 同一处拦截 ----
  size_t limit = max_message_bytes_.load();
  if (limit > 0 && req->args.size() > limit) {
    oversized_dropped_.fetch_add(1);
    DeliverResult(req, false, "");
    return;
  }
  count_.fetch_add(1);  // 发送次数+1
  bytes_.fetch_add(static_cast<int64_t>(req->args.size()));   // 总字节数+本条长度

  // 快照 endname 状态（是否通、连到哪台）
  int servername = -1;
  bool enabled = false;
  bool reliable = reliable_.load();
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto eit = enabled_.find(req->endname);
    enabled = (eit != enabled_.end()) && eit->second;
    auto cit = connections_.find(req->endname);
    if (cit != connections_.end()) servername = cit->second;
  }

  // 断连 / 目标不存在：快速返回失败（模拟网络不通）
  if (!enabled || servername < 0) {
    DeliverResult(req, false, "");
    return;
  }

  // 不可靠模式：10% 丢请求（直接判失败，不等超时） 丢请求：在真正写 socket 之前，直接 return
  if (!reliable && raftcpp::RandInt(1000) < 100) {
    DeliverResult(req, false, "");
    return;
  }

  // 取/建连接，并启动一个读线程一帧一帧读取回包内容
  auto conn = GetOrCreateConn(req->endname, servername);
  if (!conn) {
    DeliverResult(req, false, "");
    return;
  }

  uint64_t corr = next_corr_.fetch_add(1);  // 生成唯一请求票号
  bool conn_closed = false;
  {
    std::lock_guard<std::mutex> lk(conn->mu);
    if (conn->closed) {  // 连接已被 teardown，别再往死 fd 写
      conn_closed = true;
    } else {
      conn->pending[corr] = req;
    }
  }
  if (conn_closed) {
    DeliverResult(req, false, "");  // 锁外投递，避免持 conn->mu 跑用户 cb 自死锁
    return;
  }
  // 写请求帧：在 write_mu 下重读 conn->fd。reader 关闭 fd 同样持 write_mu（见下方
  // ConnReader 退出块），二者互斥；若 reader 已把 fd 置 -1 则跳过，从根上杜绝
  // "快照 fd 被同进程 socket() 复用"导致的 P0 残余串台。
  bool sent = false;
  {
    std::lock_guard<std::mutex> lk(conn->write_mu);  // 同一 fd 写串行化，且与 reader 关 fd 互斥
    if (conn->fd >= 0) {
      sent = SendRequestFrame(conn->fd, corr, req->svc_meth, req->args);
    }
  }
  if (!sent) {
    {
      std::lock_guard<std::mutex> lk(conn->mu);
      conn->pending.erase(corr);
    }
    DeliverResult(req, false, "");  // 锁外投递，避免持 conn->mu 跑用户 cb 自死锁
  }
}

}  // namespace labrpc
