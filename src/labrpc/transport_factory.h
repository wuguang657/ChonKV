// transport_factory.h —— 网络后端的"工厂"（对标 Persister 的 MakePersister）
//
// 用法：config 里 `net_ = labrpc::MakeTransport();`
//   * 默认返回 InProcessNetwork（原 labrpc::Network）：同进程模拟，零回归。
//   * 设环境变量 LABRPC_TRANSPORT=socket 时，返回 SocketNetwork：真实 TCP。
//
// 这样切换网络后端就像切换 Persister 的内存/文件模式一样，上层 Config 一行不用改。
// 将来要加第三个后端（比如"跨机真实集群"），只要再写一个继承 Transport 的类，
// 在这里加一个分支即可。

#pragma once

#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>

#include "epoll/epoll_transport.h"   // EpollNetwork (事件循环后端，仅 Linux 可用)
#include "inproc/network.h"      // Network (inproc 后端)
#include "socket/socket_transport.h" // SocketNetwork (真实 TCP 后端)
#include "transport.h"        // Transport 抽象基类

namespace labrpc {

// 后端名写错时的"我回退过"标志（一次性，进程级）。
//
// 为什么需要它：MakeTransport 不能对拼错的环境变量 abort（它是库函数，而且
// 拼错的是【测试配置】不是程序 bug），所以只能回退成 in-proc 并打一行 warning。
// 但"回退"如果不【可验证】，就会变成最坏的那种假绿：
//   $ LABRPC_TRANSPORT=socekt ./test_part.sh 2A
//   ...
//   OK：2A 全部 2 条用例 × 1 轮通过（后端 net=socekt persist=memory）
// 横幅照抄用户写的 "socekt"，实际一条 socket 用例都没跑（那行 warning 还被
// run_one 重定向进了每个用例的日志文件里，控制台根本看不见）。
//
// 修法：回退时置位本标志，由 labrpc_selftest 的 main 末尾断言它为 false ——
// 后端名拼错 → 自测红 → build.sh 的闸门红。生产代码不读它。
inline bool& TransportKindFallbackFlag() {
  static bool fell_back = false;
  return fell_back;
}
inline bool TransportKindFellBack() { return TransportKindFallbackFlag(); }

inline std::shared_ptr<Transport> MakeTransport(const std::string& kind = "") {
  std::string k = kind;
  if (k.empty()) {
    const char* e = std::getenv("LABRPC_TRANSPORT");
    k = e ? e : "inproc";
  }
  if (k == "socket" || k == "tcp" || k == "net") {
    return std::make_shared<SocketNetwork>();
  }
  // 第三个后端：单线程 epoll 事件循环（见 epoll/ 目录）。
  //
  // 在非 Linux 平台上【刻意不静默回退】：返回的对象能正常构造，但构造期会打
  // 一条明确错误、之后所有 RPC 立即失败。于是 LABRPC_TRANSPORT=epoll 在 macOS
  // 上会"红得很快"，而不是悄悄跑成 in-proc —— 后者正是下面那段"拼错后端名
  // 假绿"注释要防的同一类病（后端名错了，用例却全绿，一条都没真跑）。
  if (k == "epoll") {
    return std::make_shared<EpollNetwork>();
  }
  if (!k.empty() && k != "inproc" && k != "memory") {
    // 拼错环境变量（如 LABRPC_TRANSPORT=socekt）会静默跑成 in-proc——
    // net 模式的测试全部"假过"。至少吼一声，把假过变成可见。
    // 光吼不够（stderr 常被重定向进日志），所以同时置位"回退过"标志，
    // 让 labrpc_selftest 能把这件事断言成红色（见上面的注释）。
    TransportKindFallbackFlag() = true;
    std::fprintf(stderr,
                 "[labrpc] LABRPC_TRANSPORT=\"%s\" 无法识别，回退为 inproc "
                 "（可用值：inproc / socket / epoll）\n",
                 k.c_str());
  }
  // 默认：同进程模拟网络（原 labrpc::Network）
  return std::make_shared<Network>();
}

}  // namespace labrpc
