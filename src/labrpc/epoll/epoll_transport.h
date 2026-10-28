// epoll_transport.h —— Transport 的第三个后端：单线程 epoll 事件循环 + worker 池
//
// 和另外两个后端的关系（三者实现同一份 transport.h 契约）：
//
//   InProcessNetwork  同进程共享内存，无 socket。测协议逻辑最快，默认后端。
//   SocketNetwork     真 TCP，但【每连接 2 个常驻阻塞线程】+ 阻塞 read/write。
//                     ~1400 行，能跑通全部用例，实测天花板 ~3.4k ops/s
//                     （瓶颈是单 cb 线程 + 每连接 write_mu）。
//   EpollNetwork      真 TCP，1 个事件循环线程管所有 fd，连接是非阻塞 fd +
//                     半帧状态机 + 输出队列。线程数与连接数【无关】。
//
// 为什么值得单独写一个而不是改 SocketNetwork：
//   SocketNetwork 里那些"线程回收 / shutdown 唤醒阻塞 recv / fd 归属线程"的机制
//   （ReapFinishedHandlers、reader_exited、ServerState::handlers…）全都是
//   "每连接一线程"的产物。改成事件循环后它们全部作废 —— 那不是重构，是重写。
//   新开一个后端可以让老后端一行不动（80 条用例继续在 macOS 上跑），
//   新后端只在 Linux 上被启用和验证。
//
// 选择的语义（与 SocketNetwork 对齐，别自己发明）：
//   * Reliable(false)    ：10% 丢请求 + 10% 丢回包（服务端回一个 ok=false 的帧，
//                          客户端立即失败，而不是干等超时）。两处阈值必须与
//                          socket 后端一致，否则同一份用例在两个后端表现不同。
//   * LongReordering(true)：2/3 概率把回包延迟 200~2200ms 再发，制造真实乱序。
//   * LongDelays         ：空操作（与 socket 后端一致）。
//
// ⚠️ 平台边界：本后端【只在 Linux 上可用】。macOS 上它照样能编译、能构造，
//    但 Supported()==false，构造期会打一条明确错误、之后所有 RPC 立即失败。
//    绝不做"悄悄降级成 in-proc"——那会让 LABRPC_TRANSPORT=epoll 的用例全绿
//    而其实一条 epoll 都没跑（transport_factory.h 里那段"拼错后端名假绿"
//    的注释就是这个坑）。

#pragma once

#include <memory>
#include <string>

#include "../transport.h"  // Transport 抽象基类

namespace labrpc {

class EpollNetwork : public Transport {
 public:
  // host：监听/连接地址（默认 127.0.0.1），只支持 IPv4；base_port：server i
  // 监听在 base_port + i。两者语义与 SocketNetwork 完全一致，便于横向对照。
  explicit EpollNetwork(const std::string& host = "127.0.0.1",
                        int base_port = 19090);
  ~EpollNetwork() override;

  EpollNetwork(const EpollNetwork&) = delete;
  EpollNetwork& operator=(const EpollNetwork&) = delete;

  // ---- Transport 接口 ----
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

  void SetMaxMessageBytes(size_t n) override;
  size_t MaxMessageBytes() const override;
  int64_t OversizedDropped() const override;

  void Cleanup() override;

  void SendReq(std::shared_ptr<ReqMsg> req) override;
  bool IsStopped() const override;

  // ---- 本后端独有 ----
  // 当前平台能不能真的跑（Linux true / 其它 false）。
  // selftest 与 factory 都靠它，而不是靠 #ifdef 猜。
  static bool Supported();
  // 当前注册在 epoll 里的连接数（观测用）。它替代了 SocketNetwork 的
  // GetReaderThreadCount —— 事件循环下"线程数"不再是资源指标，"连接数"才是。
  int ConnCount() const;

 private:
  struct Impl;                    // 全部平台/实现细节（epoll fd、连接表、队列…）
  std::unique_ptr<Impl> impl_;    // pimpl：让本头文件不出现 sys/epoll.h
};

}  // namespace labrpc
