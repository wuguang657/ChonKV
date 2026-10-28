// labrpc.cpp —— RPC 框架的实现（Service / Server / ClientEnd）
//
// 传输后端的实现不在这里：
//   * inproc/network.cpp —— 同进程模拟网络（默认后端，labrpc::Network）
//   * socket/socket_transport.cpp —— 真实 TCP 后端（labrpc::SocketNetwork）

#include "labrpc.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>

namespace labrpc {

// ===========================================================================
// Service / Server
// ===========================================================================

std::string Service::Dispatch(const std::string& method,
                              const std::string& args) {
  std::string reply;
  if (!TryDispatch(method, args, &reply)) std::abort();
  return reply;
}

bool Service::TryDispatch(const std::string& method, const std::string& args,
                          std::string* reply) {
  auto it = methods_.find(method);
  if (it == methods_.end()) {
    // 只打印不 abort：调用者（传输层）会把这个请求判为失败并回 ok=false。
    // 打日志是必须的 —— 名字写错在本方代码里仍要靠这行字发现，
    // 只是不再让远程输入把进程带走。
    std::fprintf(stderr, "labrpc: unknown method %s in service %s\n",
                 method.c_str(), name_.c_str());
    return false;
  }
  std::string r = it->second(args);
  if (reply) *reply = std::move(r);
  return true;
}

void Server::AddService(std::shared_ptr<Service> svc) {
  std::lock_guard<std::mutex> lk(mu_);
  services_[svc->name()] = std::move(svc);
}

std::string Server::Dispatch(const std::string& svc_meth,
                             const std::string& args) {
  std::string reply;
  if (!TryDispatch(svc_meth, args, &reply)) std::abort();
  return reply;
}

bool Server::TryDispatch(const std::string& svc_meth,
                         const std::string& args, std::string* reply) {
  std::string svc_name, method_name;
  std::shared_ptr<Service> svc;
  {
    std::lock_guard<std::mutex> lk(mu_);
    count_++;  // 入站计数，TestCount2B 会看这个

    // "Raft.AppendEntries" → 服务名 "Raft" + 方法名 "AppendEntries"
    size_t dot = svc_meth.rfind('.');
    if (dot == std::string::npos) {
      std::fprintf(stderr, "labrpc: bad svcMeth %s\n", svc_meth.c_str());
      return false;
    }
    svc_name = svc_meth.substr(0, dot);
    method_name = svc_meth.substr(dot + 1);

    auto it = services_.find(svc_name);
    if (it != services_.end()) svc = it->second;
  }
  if (!svc) {
    std::fprintf(stderr, "labrpc: unknown service %s\n", svc_name.c_str());
    return false;
  }
  return svc->TryDispatch(method_name, args, reply);
}

int Server::GetCount() const {
  std::lock_guard<std::mutex> lk(mu_);
  return count_;
}

// ===========================================================================
// ClientEnd
// ===========================================================================

bool ClientEnd::Call(const std::string& svc_meth, const std::string& args,
                     std::string& reply, std::atomic<bool>* cancel) {
  auto req = std::make_shared<ReqMsg>();
  req->endname = endname_;
  req->svc_meth = svc_meth;
  req->args = args;
  req->slot = std::make_shared<ReplySlot>();

  net_->SendReq(req);

  // 等结果。用 50ms 的超时轮询，是为了在网络被 Cleanup() 掉的时候
  // 也能及时醒来返回 false，不会永远卡住。
  std::unique_lock<std::mutex> lk(req->slot->mu);
  while (!req->slot->done) {
    if (net_->IsStopped()) return false;
    if (cancel != nullptr && cancel->load()) return false;  // 被取消
    req->slot->cv.wait_for(lk, std::chrono::milliseconds(50));
  }
  if (!req->slot->ok) return false;
  reply = std::move(req->slot->reply);
  return true;
}

void ClientEnd::CallAsync(const std::string& svc_meth, std::string args,
                          ReplyCallback cb, std::atomic<bool>* cancel) {
  auto req = std::make_shared<ReqMsg>();
  req->endname = endname_;
  req->svc_meth = svc_meth;
  req->args = std::move(args);
  req->cb = std::move(cb);
  req->cancel = cancel;
  net_->SendReq(req);
}

}  // namespace labrpc
