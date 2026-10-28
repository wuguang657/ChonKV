// poller.h —— "等哪些 fd 就绪"的最小抽象（epoll 的唯一落脚点）
//
// 为什么要单独抽这一层，而不是把 epoll 直接写进事件循环：
//   1) 整个工程只在 macOS 上开发/跑 80 条 raft 用例，而 epoll 是 Linux 专属。
//      把 sys/epoll.h 关在这一个文件里，事件循环（epoll_transport.cpp）就能在
//      macOS 上被【完整编译 + 类型检查】—— 你写的状态机在本机就能编过，
//      只有这 140 行平台胶水要到 Linux 上才编。
//   2) 想加 kqueue 时只改本文件，事件循环一行不动。这不是"为了抽象而抽象"：
//      抽象的成本这里刚好是 0（4 个动作），收益是"逻辑可跨平台验证"。
//
// 语义约定（很重要，实现事件循环时按这个来）：
//   * 只支持【水平触发（LT）】：Wait 返回某个 fd 就绪后，只要你不把它读完/写完，
//     下次 Wait 还会返回它。ET（边缘触发）要求你一次读到 EAGAIN，写错就是"事件
//     永久丢失 + 连接假死"，是 epoll 最经典的坑——练习时先用 LT 把逻辑跑对，
//     再考虑要不要换 ET（换的成本见 epoll_transport.cpp 里 OnReadable 的注释）。
//   * Wait 永远不因 EINTR 失败：内部把 EINTR 折成"本轮 0 个事件"，让调用方
//     不必区分"被信号打断"和"真没事件"。
//   * token 是唯一能穿过 epoll 带回给你的东西（epoll 不告诉你 fd）。所以事件
//     循环必须自己维护 token -> 连接 的映射，见 epoll_transport.cpp 的 token 空间。

#pragma once

#include <cstdint>

namespace labrpc {

// 一个就绪事件。注意【没有 fd 字段】：epoll_wait 只回传你注册时塞进去的 data，
// 不保证回传 fd（尤其在 Del 之后 fd 可能已被复用）。想拿 fd 就自己从 token 查。
struct PollEvent {
  uint64_t token = 0;  // 注册时给的 id：唤醒 fd / 监听 fd / 某条连接
  uint32_t mask = 0;   // Poller::kReadable | kWritable | kError
};

class Poller {
 public:
  // 事件掩码。故意用"我自己的一份"，而不是把 EPOLLIN/EPOLLOUT 泄漏到调用方：
  // 否则事件循环里会到处出现 #ifdef __linux__，抽象就白做了。
  static constexpr uint32_t kReadable = 0x1;
  static constexpr uint32_t kWritable = 0x2;
  // kError 覆盖：对端关闭（EOF/HUP）、RST、以及 epoll 自己报的 ERR。
  // 事件循环收到 kError 时应当"读完剩余数据再关"，而不是立刻 close —— 立刻 close
  // 会把对端刚发来还没读的那半个回包一起丢掉（上层会看到一次凭空的 RPC 失败）。
  static constexpr uint32_t kError = 0x4;

  // 当前平台是否支持。false 时本类所有动作都返回失败，且 EpollNetwork 会在
  // 构造期明确报错（绝不静默回退成别的后端 —— 那会造出"测试全绿但一条都没跑"）。
  static bool Supported();
  static const char* BackendName();  // "epoll" / "unsupported"

  Poller() = default;
  ~Poller();
  Poller(const Poller&) = delete;
  Poller& operator=(const Poller&) = delete;

  // ---- 主循环用的四个动作 ----
  bool Open();                                     // 建 epoll fd
  bool Add(int fd, uint64_t token, uint32_t mask);  // 注册（fd 必须是非阻塞的）
  bool Mod(int fd, uint64_t token, uint32_t mask);  // 改关注集（读写切换 / 摘掉 EPOLLOUT）
  bool Del(int fd);                                 // 注销（fd 关闭前必须 Del，见下）
  // 返回就绪事件数；0 = 超时；-1 = 真错误。timeout_ms < 0 表示永久等待。
  int Wait(PollEvent* out, int max_events, int timeout_ms);

  // ---- 唤醒：事件循环在 Wait 里睡着时，别的线程怎么让它立刻醒 ----
  // why：SendReq 可能跑在 Raft 的 replicator 线程上，它要把"这条连接有新数据要写"
  // 告诉事件循环。只往队列里塞是不够的 —— 循环正阻塞在 Wait 里，没人叫它就是
  // 永远不醒。标准做法：一个 eventfd/pipe 也注册进 epoll，写入 8 字节即唤醒。
  bool WakeOpen();
  void WakeUp();      // 任意线程可调，幂等，不阻塞
  void DrainWake();   // 只能在事件循环线程里调（唤醒后把计数读掉）
  int wake_fd() const { return wake_fd_; }

  // 关闭 epoll fd 与唤醒 fd。调用前必须保证没有线程还在 Wait 里。
  // 顺序很关键：先 Del 所有已注册 fd 再 close(epfd_) —— close 一个仍被监听的 fd
  // 在 Linux 上会把它从 epoll 里静默摘掉，但 Del 更明确，也避免 EBADF 噪声。
  void Close();

 private:
  int epfd_ = -1;
  int wake_fd_ = -1;
};

}  // namespace labrpc
