// transport.h —— 网络传输层的可插拔抽象（对标 Persister 的"内存/文件"可切换）
//
// 为什么要有这一层？
//   原 labrpc::Network 是一个"同进程内、用共享内存 ReplySlot 来回包"的模拟网络。
//   它和 raft / kvraft 的耦合方式是：Config 持有 std::shared_ptr<Network>，
//   通过 MakeEnd / AddServer / Connect / Enable / SendReq 这一组公开接口使用它。
//
//   这一组接口，本质上就是"传输层契约"。我们把这个契约抽成抽象基类 Transport，
//   于是底下可以换不同的实现：
//     * InProcessNetwork（即原 labrpc::Network）：同进程内模拟，共享内存回包。
//     * SocketNetwork（socket_transport.*）：真实 TCP（默认 127.0.0.1 loopback，
//       可指定 IP/端口），用 correlation id 关联回包。
//
//   上层（raft::Config / kvraft::Config）只依赖 Transport，不关心底下是内存还是
//   真实网络。切换方式：MakeTransport() 读环境变量 LABRPC_TRANSPORT
//   （默认 "inproc"，设 "socket" 走真实网络）。
//
// 设计要点：
//   * ClientEnd / Server / Service / ReqMsg / ReplySlot 这些"数据结构"是传输无关的
//     （服务端 dispatch、客户端等回包的槽位），仍留在 labrpc.h，被两个后端共用。
//   * Transport 只声明"行为接口"。这里对 ClientEnd/Server/ReqMsg 只做前向声明，
//     避免 transport.h 和 labrpc.h 形成循环包含。
//   * 默认路径（inproc）行为与原 Network 逐字节一致，保证 67 个测试用例零回归。

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace labrpc {

class ClientEnd;   // 发送端点（labrpc.h 定义，两个后端共用）
class Server;      // 接收端（labrpc.h 定义，两个后端共用）
struct ReqMsg;     // 一封"信"（labrpc.h 定义，两个后端共用）

// 传输层抽象：raft / kvraft 只认这个。
class Transport {
 public:
  virtual ~Transport() = default;

  // ---- 端点 / 服务端管理（与旧 Network 公开接口一一对应）----
  virtual std::shared_ptr<ClientEnd> MakeEnd(const std::string& endname) = 0;
  virtual void AddServer(int servername, std::shared_ptr<Server> srv) = 0;
  virtual void DeleteServer(int servername) = 0;
  virtual void Connect(const std::string& endname, int servername) = 0;
  virtual void Enable(const std::string& endname, bool enabled) = 0;
  // 注销一个发信端点（从 enabled_/connections_ 移除）。
  virtual void RemoveClientEnd(const std::string& endname) = 0;

  // ---- 混沌旋钮 ----
  virtual void Reliable(bool yes) = 0;       // false：10% 丢请求 + 10% 丢回包
  virtual void LongReordering(bool yes) = 0; // true：2/3 概率把回包延迟 200~2200ms
  virtual void LongDelays(bool yes) = 0;     // 失败回包的长延迟（仅 in-proc：端点仍通但 server 已删时 0~7000ms；断连立即失败。socket 后端为空操作）

  // ---- 统计 ----
  virtual int GetCount(int servername) = 0;          // 某台入站 RPC 数
  virtual int GetTotalCount() const = 0;             // 全网发出的 RPC 总数
  virtual int64_t GetTotalBytes() const = 0;         // 全网传输的总字节数

  // ---- C3 单条消息字节上限 ----
  virtual void SetMaxMessageBytes(size_t n) = 0;
  virtual size_t MaxMessageBytes() const = 0;
  virtual int64_t OversizedDropped() const = 0;

  // ---- 生命周期 ----
  virtual void Cleanup() = 0;  // 停线程、唤醒所有还在等的人

  // ---- 以下给 ClientEnd 用 ----
  // 发送入口（同步 Call 与异步 CallAsync 的唯一共同入口）。
  virtual void SendReq(std::shared_ptr<ReqMsg> req) = 0;
  // 网络是否已停止（Cleanup 后置 true；Call 轮询用）。
  virtual bool IsStopped() const = 0;
};

}  // namespace labrpc
