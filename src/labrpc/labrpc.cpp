// labrpc.cpp —— 模拟网络的实现
//
// 阅读顺序建议：Network::SendReq → TimerLoop → WorkerLoop → Deliver。
// 这四步就是一个 RPC 从发出到结果回来的完整路径。

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
  auto it = methods_.find(method);
  if (it == methods_.end()) {
    std::fprintf(stderr, "labrpc: unknown method %s in service %s\n",
                 method.c_str(), name_.c_str());
    std::abort();
  }
  return it->second(args);
}

void Server::AddService(std::shared_ptr<Service> svc) {
  std::lock_guard<std::mutex> lk(mu_);
  services_[svc->name()] = std::move(svc);
}

std::string Server::Dispatch(const std::string& svc_meth,
                             const std::string& args) {
  std::string svc_name, method_name;
  std::shared_ptr<Service> svc;
  {
    std::lock_guard<std::mutex> lk(mu_);
    count_++;  // 入站计数，TestCount2B 会看这个

    // "Raft.AppendEntries" → 服务名 "Raft" + 方法名 "AppendEntries"
    size_t dot = svc_meth.rfind('.');
    if (dot == std::string::npos) {
      std::fprintf(stderr, "labrpc: bad svcMeth %s\n", svc_meth.c_str());
      std::abort();
    }
    svc_name = svc_meth.substr(0, dot);
    method_name = svc_meth.substr(dot + 1);

    auto it = services_.find(svc_name);
    if (it != services_.end()) svc = it->second;
  }
  if (!svc) {
    std::fprintf(stderr, "labrpc: unknown service %s\n", svc_name.c_str());
    std::abort();
  }
  return svc->Dispatch(method_name, args);
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

// ===========================================================================
// Network
// ===========================================================================

struct Network::EndInfo {
  bool enabled = false;
  int servername = -1;
  std::shared_ptr<Server> server;
  bool reliable = true;
  bool longreordering = false;
  bool longdelays = false;
};

Network::Network() {
  stopped_.store(false);

  // 初始 worker 数：只覆盖 Raft 自身那些"快速"handler（RequestVote /
  // AppendEntries / InstallSnapshot —— 都是抢一下锁就返回）。
  // KV 那种会阻塞等 raft 提交的 handler 由 Post() 按需扩容来兜。
  unsigned hw = std::thread::hardware_concurrency();
  if (hw == 0) hw = 4;
  size_t n_workers = std::min<size_t>(16, std::max<size_t>(8, hw));

  threads_.emplace_back([this] { TimerLoop(); });
  {
    std::lock_guard<std::mutex> lk(mu_);
    for (size_t i = 0; i < n_workers; i++) SpawnWorkerLocked();
  }
}

void Network::SpawnWorkerLocked() {
  total_workers_++;
  threads_.emplace_back([this] { WorkerLoop(); });
}

Network::~Network() { Cleanup(); }

Network::EndInfo Network::ReadEndInfo(const std::string& endname) {
  EndInfo info;
  info.reliable = reliable_;
  info.longreordering = long_reordering_;
  info.longdelays = long_delays_;

  auto eit = enabled_.find(endname);
  info.enabled = (eit != enabled_.end()) && eit->second;

  auto cit = connections_.find(endname);
  if (cit != connections_.end()) {
    info.servername = cit->second;
    auto sit = servers_.find(info.servername);
    if (sit != servers_.end()) info.server = sit->second;  // 可能是 nullptr
  }
  return info;
}

// ---- C3 单条消息字节上限（访问器）----
void Network::SetMaxMessageBytes(size_t n) { max_message_bytes_.store(n); }
size_t Network::MaxMessageBytes() const { return max_message_bytes_.load(); }
int64_t Network::OversizedDropped() const { return oversized_dropped_.load(); }

void Network::SendReq(std::shared_ptr<ReqMsg> req) {
  // ---- C3 单条消息字节上限：超限直接拒收，不投递给服务端 ----
  // 放在 SendReq 而不是 ClientEnd::Call 里，是因为 SendReq 是同步 Call 与
  // 异步 CallAsync 的【唯一共同入口】—— 一处拦住即覆盖全网全部 RPC。
  // 拒收方式沿用下面"断连/服务器不存在"的既有失败路径：投递一个 ok=false
  // 的回信事件，调用方（Call 返回 false / CallAsync 回调收到 ok=false）
  // 与"网络不通"表现一致，Raft 侧无需任何改动即可自然重试或放弃。
  //
  // 注意闸门必须在 count_/bytes_ 累加【之前】：被拒收的消息从未真正上网，
  // 不该计入"全网传输字节数"（TestRPCBytes2B 之类断言的是真实传输量）。
  size_t limit = max_message_bytes_.load();
  if (limit > 0 && req->args.size() > limit) {
    oversized_dropped_.fetch_add(1);
    auto dropped = std::make_shared<TimerEvent>();
    dropped->slot = req->slot;
    dropped->cb = req->cb;
    dropped->cancel = req->cancel;
    dropped->kind = EventKind::kDeliverReply;
    dropped->ok = false;
    Schedule(dropped, 0);
    return;
  }

  count_.fetch_add(1);
  bytes_.fetch_add(static_cast<int64_t>(req->args.size()));

  // 在锁里读一次快照，出了锁就只能相信这份快照（Go 版也是这么干的）。
  EndInfo info;
  {
    std::lock_guard<std::mutex> lk(mu_);
    info = ReadEndInfo(req->endname);
  }

  auto e = std::make_shared<TimerEvent>();
  e->slot = req->slot;
  e->cb = req->cb;
  e->cancel = req->cancel;
  e->endname = req->endname;
  e->svc_meth = req->svc_meth;
  e->args = req->args;
  e->servername = info.servername;
  e->server = info.server;
  e->reliable = info.reliable;
  e->longreordering = info.longreordering;

  if (info.enabled && info.server) {
    // 通路正常，准备投递给服务端
    int64_t delay = info.reliable ? 0 : raftcpp::RandInt(27);
    Schedule(e, delay);
  } else {
    // 断连 / 服务器不存在：快速返回失败
    int64_t delay = raftcpp::RandInt(100);
    e->kind = EventKind::kDeliverReply;
    e->ok = false;
    Schedule(e, delay);
  }
}

void Network::Schedule(std::shared_ptr<TimerEvent> e, int64_t delay_ms) {
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto at = raftcpp::Now() + std::chrono::milliseconds(delay_ms);
    timers_.emplace(at, std::move(e));
  }
  timer_cv_.notify_one();
}

void Network::TimerLoop() {
  std::unique_lock<std::mutex> lk(mu_);
  while (!stopped_.load()) {
    if (timers_.empty()) {
      timer_cv_.wait_for(lk, std::chrono::milliseconds(50));
      continue;
    }
    auto first = timers_.begin();
    // ⚠️ macOS libc++ 兼容性修复（不改变 Lab 2 语义）。
    // 不能直接 wait_until(绝对 time_point)：当 first->first 已经"刚刚过去"时，
    // libc++ 会转出 tv_sec 为负的非法 timespec，pthread_cond_timedwait 返回
    // EINVAL → 抛 system_error("condition_variable timed_wait failed: Invalid argument")。
    // KV 测试（3B 重启用例）高频重建定时器时极易踩中。改用相对 wait_for 并先做
    // "已过期则跳过"的保护，逻辑完全等价。
    auto due_in = first->first - raftcpp::Now();
    if (due_in > std::chrono::milliseconds(0)) {
      timer_cv_.wait_for(lk, due_in);
    }

    // 取出所有已经到期的事件
    auto now = raftcpp::Now();
    std::vector<std::shared_ptr<TimerEvent>> due;
    while (!timers_.empty() && timers_.begin()->first <= now) {
      due.push_back(timers_.begin()->second);
      timers_.erase(timers_.begin());
    }
    if (due.empty()) continue;

    lk.unlock();
    for (auto& e : due) {
      if (e->kind == EventKind::kDeliverReply) {
        Finish(e);  // 收尾：填 slot 或跑回调
      } else {
        // 不可靠模式下，10% 概率丢请求
        if (!e->reliable && raftcpp::RandInt(1000) < 100) {
          e->ok = false;
          Finish(e);
          continue;
        }
        // 执行 handler 会抢 Raft 的锁，丢进线程池，别堵住定时器线程
        Post([this, e]() {
          // 真正执行 RPC handler（会短暂抢 Raft 的锁）
          std::string reply = e->server->Dispatch(e->svc_meth, e->args);

          // 执行期间服务器被 DeleteServer 了 → 结果作废。
          // 【必须】给客户端回一个"失败"，否则调用方永远等不到回音。
          if (IsServerDead(*e)) {
            e->kind = EventKind::kDeliverReply;
            e->ok = false;
            Finish(e);
            return;
          }

          // 不可靠模式下，10% 概率丢回复。
          // 丢的是"回复"，但客户端依然要收到一个失败结果（相当于超时）。
          if (!e->reliable && raftcpp::RandInt(1000) < 100) {
            e->kind = EventKind::kDeliverReply;
            e->ok = false;
            Finish(e);
            return;
          }

          // 乱序模式：2/3 概率把回复延迟 200~2200ms 再送回去
          int64_t delay = 0;
          if (e->longreordering && raftcpp::RandInt(900) < 600) {
            delay = 200 + raftcpp::RandInt(1 + raftcpp::RandInt(2000));
          }
          e->kind = EventKind::kDeliverReply;
          e->ok = true;
          e->reply = std::move(reply);
          Schedule(e, delay);
        });
      }
    }
    lk.lock();
  }
}

void Network::Post(std::function<void()> task) {
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (stopped_.load()) return;
    tasks_.push_back(std::move(task));

    // ---- 按需扩容：逼近 Go 版"每个 handler 一个 goroutine"的语义 ----
    //
    // 判定条件：所有 worker 都在忙，且还有任务在排队。
    // 说明池子不够用了 —— 多半是撞上了阻塞型 handler（KV 的 WaitOp
    // 会阻塞最长 1 秒等 raft 提交）。此时必须补线程，否则心跳 / 日志复制
    // 的 handler 永远排不进来，而它们正是让阻塞的 handler 得以返回的前提，
    // 于是整个集群停摆（实测：20 客户端并发下吞吐掉到 1/66，且用例随机失败）。
    //
    // 一次补到"够用 + 4 个余量"，避免突发 20 个请求时逐个扩容的抖动。
    if (busy_workers_ >= total_workers_ && total_workers_ < kMaxWorkers) {
      size_t need = busy_workers_ + tasks_.size();
      size_t want = std::min<size_t>(
          kMaxWorkers, std::max<size_t>(need, total_workers_ + 4));
      while (total_workers_ < want) SpawnWorkerLocked();
    }
  }
  task_cv_.notify_one();
}

void Network::WorkerLoop() {
  while (true) {
    std::function<void()> task;
    {
      std::unique_lock<std::mutex> lk(mu_);
      task_cv_.wait(lk, [this] { return stopped_.load() || !tasks_.empty(); });
      if (stopped_.load()) return;
      task = std::move(tasks_.front());
      tasks_.pop_front();
      busy_workers_++;
    }

    // ⚠️ 必须在【锁外】执行：handler 可能长时间阻塞（KV 的 WaitOp 最长 1 秒）。
    // 阻塞期间 busy_workers_ 保持计数，Post() 才能据此判断"池子满了要扩容"。
    task();

    {
      std::lock_guard<std::mutex> lk(mu_);
      busy_workers_--;
    }
  }
}

bool Network::IsServerDead(const TimerEvent& e) {
  std::lock_guard<std::mutex> lk(mu_);
  auto eit = enabled_.find(e.endname);
  bool enabled = (eit != enabled_.end()) && eit->second;
  if (!enabled) return true;
  auto sit = servers_.find(e.servername);
  // server 被换掉（DeleteServer 后再 AddServer 一个新的）也算"死过"
  return sit == servers_.end() || sit->second != e.server;
}

void Network::Finish(std::shared_ptr<TimerEvent> e) {
  // 对方（Raft）已经被 Kill：这次结果作废，一个字节都不算。
  if (e->cancel != nullptr && e->cancel->load()) return;

  if (e->ok) {
    bytes_.fetch_add(static_cast<int64_t>(e->reply.size()));
  }

  if (e->cb) {
    // 异步调用：把回调丢进线程池执行。
    // 绝不能在定时器线程里直接跑 —— 回调会去抢 Raft 的锁，
    // 一旦卡住，整个网络的定时系统就停摆了。
    if (e->ok) {
      std::string reply = std::move(e->reply);
      Post([cb = std::move(e->cb), reply]() { cb(true, reply); });
    } else {
      Post([cb = std::move(e->cb)]() { cb(false, std::string()); });
    }
    return;
  }

  if (!e->slot) return;
  std::lock_guard<std::mutex> lk(e->slot->mu);
  e->slot->ok = e->ok;
  e->slot->reply = std::move(e->reply);
  e->slot->done = true;
  e->slot->cv.notify_all();
}

// ---- 对外接口 ----

std::shared_ptr<ClientEnd> Network::MakeEnd(const std::string& endname) {
  std::lock_guard<std::mutex> lk(mu_);
  enabled_[endname] = false;
  connections_.erase(endname);
  return std::make_shared<ClientEnd>(this, endname);
}

void Network::AddServer(int servername, std::shared_ptr<Server> srv) {
  std::lock_guard<std::mutex> lk(mu_);
  servers_[servername] = std::move(srv);
}

void Network::DeleteServer(int servername) {
  std::lock_guard<std::mutex> lk(mu_);
  servers_[servername] = nullptr;
}

void Network::Connect(const std::string& endname, int servername) {
  std::lock_guard<std::mutex> lk(mu_);
  connections_[endname] = servername;
}

void Network::Enable(const std::string& endname, bool enabled) {
  std::lock_guard<std::mutex> lk(mu_);
  enabled_[endname] = enabled;
}

void Network::RemoveClientEnd(const std::string& endname) {
  std::lock_guard<std::mutex> lk(mu_);
  enabled_.erase(endname);
  connections_.erase(endname);
}

void Network::Reliable(bool yes) {
  std::lock_guard<std::mutex> lk(mu_);
  reliable_ = yes;
}

void Network::LongReordering(bool yes) {
  std::lock_guard<std::mutex> lk(mu_);
  long_reordering_ = yes;
}

void Network::LongDelays(bool yes) {
  std::lock_guard<std::mutex> lk(mu_);
  long_delays_ = yes;
}

int Network::GetCount(int servername) {
  std::shared_ptr<Server> srv;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = servers_.find(servername);
    if (it != servers_.end()) srv = it->second;
  }
  return srv ? srv->GetCount() : 0;
}

int Network::GetTotalCount() const { return count_.load(); }

int64_t Network::GetTotalBytes() const { return bytes_.load(); }

void Network::Cleanup() {
  if (cleaned_) return;
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (cleaned_) return;
    cleaned_ = true;
    stopped_.store(true);
    timers_.clear();
    tasks_.clear();
  }
  timer_cv_.notify_all();
  task_cv_.notify_all();
  for (auto& t : threads_) {
    if (t.joinable()) t.join();
  }
}

}  // namespace labrpc
