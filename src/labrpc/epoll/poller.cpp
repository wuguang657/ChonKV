// poller.cpp —— Poller 的两种实现：Linux=epoll，其它平台=明确不可用
//
// 这个文件是【全工程唯一】包含 <sys/epoll.h> 的地方。想换 kqueue 就只改这里。

#include "poller.h"

#if defined(__linux__)
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <cerrno>
#endif

namespace labrpc {

#if defined(__linux__)

// ---------------------------------------------------------------------------
// Linux：真 epoll
// ---------------------------------------------------------------------------
bool Poller::Supported() { return true; }
const char* Poller::BackendName() { return "epoll"; }

Poller::~Poller() { Close(); }

bool Poller::Open() {
  // EPOLL_CLOEXEC：exec 出来的子进程不继承这个 fd。忘了写它，一次 fork+exec
  // （测试里没有，但生产里很常见）就会让 epoll fd 泄漏到子进程，父进程退出后
  // 端口仍被占的样子，排查方向会被带偏。
  epfd_ = ::epoll_create1(EPOLL_CLOEXEC);
  return epfd_ >= 0;
}

// 我的事件掩码 -> epoll 的掩码。集中在一处转换，是"抽象不漏出底层常量"的关键。
static uint32_t ToEpollEvents(uint32_t mask) {
  uint32_t e = 0;
  if (mask & Poller::kReadable) e |= EPOLLIN;
  if (mask & Poller::kWritable) e |= EPOLLOUT;
  // EPOLLRDHUP：对端 shutdown(SHUT_WR)（半关闭）也会立刻上报。没有它，你会
  // 在"对端不再发数据但连接还开着"的情况下白等（对端已 close，你却收不到事件）。
  // 这里一律加上，代价只是多几条事件。
  e |= EPOLLRDHUP;
  return e;
}

bool Poller::Add(int fd, uint64_t token, uint32_t mask) {
  epoll_event ev{};
  ev.events = ToEpollEvents(mask);
  ev.data.u64 = token;  // 唯一能穿过内核回来的东西
  return ::epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) == 0;
}

bool Poller::Mod(int fd, uint64_t token, uint32_t mask) {
  epoll_event ev{};
  ev.events = ToEpollEvents(mask);
  ev.data.u64 = token;
  // 注意：Mod 用的是 ADD 时那个 fd。Del 之后再 Mod 会得到 ENOENT。
  return ::epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev) == 0;
}

bool Poller::Del(int fd) {
  // Linux 允许给一个已经 close 掉的 fd 调 Del（返回 EBADF），这里把它当成成功：
  // 调用方的意图是"这个 fd 别再给我事件了"，而这个目的已经达成。
  if (::epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr) == 0) return true;
  return errno == EBADF || errno == ENOENT;
}

int Poller::Wait(PollEvent* out, int max_events, int timeout_ms) {
  if (max_events <= 0) return 0;
  // 一次最多取 64 个：再多对 3 节点场景没有意义，只会撑大栈。调用方传更大的
  // max_events 时按 64 截断，返回值也只会 <= 64，不会撒谎。
  static constexpr int kMaxBatch = 64;
  epoll_event evs[kMaxBatch];
  int want = max_events < kMaxBatch ? max_events : kMaxBatch;

  const int n = ::epoll_wait(epfd_, evs, want, timeout_ms);
  if (n < 0) {
    // EINTR：被信号打断。对调用方来说等价于"这一轮没有事件"，不该让它当成错误
    // 去退出循环（否则一收到 SIGCHLD/SIGUSR1 就把整个网络停了）。
    if (errno == EINTR) return 0;
    return -1;
  }
  for (int i = 0; i < n; i++) {
    out[i].token = evs[i].data.u64;
    uint32_t m = 0;
    if (evs[i].events & EPOLLIN) m |= kReadable;
    if (evs[i].events & EPOLLOUT) m |= kWritable;
    if (evs[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) m |= kError;
    out[i].mask = m;
  }
  return n;
}

bool Poller::WakeOpen() {
  // eventfd 比 pipe 省一半 fd，而且读写都是 8 字节计数器，天然可以合并多次唤醒
  // （pipe 写满 64KB 后会阻塞/EAGAIN，还得处理"谁负责清空"）。
  wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  return wake_fd_ >= 0;
}

void Poller::WakeUp() {
  if (wake_fd_ < 0) return;
  const uint64_t one = 1;
  // 返回值和 errno 都不用管：
  //   * 计数器已经很大（理论上要 2^64 次）才会 EAGAIN；
  //   * EINTR 说明这次没写进去，但循环最多再睡一个 timeout 就会自己醒。
  // 换句话说：唤醒是"尽力而为的加速",不是正确性依赖。
  const ssize_t n = ::write(wake_fd_, &one, sizeof(one));
  (void)n;
}

void Poller::DrainWake() {
  if (wake_fd_ < 0) return;
  uint64_t v = 0;
  // 一次 read 就把计数器清零（eventfd 语义：读出来的就是累计值）。
  const ssize_t n = ::read(wake_fd_, &v, sizeof(v));
  (void)n;
}

void Poller::Close() {
  if (wake_fd_ >= 0) {
    ::close(wake_fd_);
    wake_fd_ = -1;
  }
  if (epfd_ >= 0) {
    ::close(epfd_);
    epfd_ = -1;
  }
}

#else  // ===================== 非 Linux：明确不可用 =====================

// 为什么不做"优雅降级成 select/poll"：
//   * 这份后端的目的就是练 epoll 的语义（LT/ET、EPOLLOUT 回压、eventfd 唤醒）。
//     降级成 poll 会让它在 macOS 上"看起来能用"，于是同一份代码在两套语义下
//     表现不同 —— 那是比"直接不支持"更贵的问题。
//   * 而且要的是 Linux 岗，本机验证用 in-proc / socket 两个后端已经够了。

bool Poller::Supported() { return false; }
const char* Poller::BackendName() { return "unsupported"; }
Poller::~Poller() = default;

bool Poller::Open() { return false; }
bool Poller::Add(int, uint64_t, uint32_t) { return false; }
bool Poller::Mod(int, uint64_t, uint32_t) { return false; }
bool Poller::Del(int) { return false; }
int Poller::Wait(PollEvent*, int, int) { return -1; }
bool Poller::WakeOpen() { return false; }
void Poller::WakeUp() {}
void Poller::DrainWake() {}
void Poller::Close() {
  // 非 Linux 分支不会真的建过 fd，但把成员复位一下：这样两个分支的
  // "Close 之后状态"是一致的，也避免 -Wunused-private-field 噪声。
  epfd_ = -1;
  wake_fd_ = -1;
}

#endif

}  // namespace labrpc
