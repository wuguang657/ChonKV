// network.cpp —— in-proc 传输后端的实现（labrpc::Network）
//
// 阅读顺序建议：Network::SendReq → TimerLoop → WorkerLoop → Finish。
// 这四步就是一个 RPC 从发出到结果回来的完整路径。

#include "network.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>

#include "../../common/metrics.h"  // 性能埋点（未定义 RAFT_BENCH_METRICS 时为空头）

namespace labrpc {

// ===========================================================================
// Network（in-proc 后端）
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
    Schedule(dropped, 0);  // 0延迟，把失败回信事件立即塞进TimerLoop中
    return;
  }

  count_.fetch_add(1);
  bytes_.fetch_add(static_cast<int64_t>(req->args.size()));

  // 在锁里读一次快照，返回当前endname网线状态。里面包含该endname连接到的server
  // 网线状态（三个混沌开关），是否enabled
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
  e->longreordering = info.longreordering; // 是否延迟回包
  e->longdelays = info.longdelays;         // 失败路径的回复延迟（见 SendReq else 分支）

  if (info.enabled && info.server) {
    // 通路正常，准备投递给服务端
    int64_t delay = info.reliable ? 0 : raftcpp::RandInt(27);
    Schedule(e, delay);
  } else if (!info.enabled) {
    // 端点被 Disconnect（网线拔了）：请求根本没发出去，立即失败（delay=0）。
    //
    // 为什么不能在这里套 longDelays（0~6999ms）：raft 的 CheckQuorum 退位判定
    // 依赖"失败回包【到达】后累加 cq_consec_fail_"（见 raft.cpp 的 AppendEntries
    // 回调），而 TestRemovedNodeExcludedFromQuorum 只等 2*ElectionTimeout（≈2s）。
    // 若断连失败回包被随机拖到 7 秒，2s 窗口内 cq 累加不到 kCQMaxConsecFail(5)
    // → leader 误判被断连的节点仍存活 → 不退位 → 用例时序 flaky（4 路并发下必现，
    // 单跑调度宽松时偶过）。端点已断连就不该再模拟"回包在路上"。
    e->kind = EventKind::kDeliverReply;
    e->ok = false;
    Schedule(e, 0);
  } else {
    // 端点仍通（enabled），但目标 server 已被 DeleteServer（进程没了 / 端口空着）：
    // 这封 RPC 在真实网络里会真的发出去、只是永远没人回 —— 这正是 longDelays
    // 要模拟的"让 leader 干等回包"场景（惩罚在持锁 / 主线程里同步发 RPC 的实现）：
    //   longDelays 开 → 随机 0~6999ms 才拿到失败；
    //   longDelays 关 → 随机 0~99ms（kv 测试要求客户端快速换服务器重试）。
    // 注意：raft 的 Crash1 是"先 Disconnect 再 DeleteServer"，走上面那条立即失败
    // 分支；本分支覆盖的是"只删 server、端点仍通"（如 kvraft crash 的删服务端步骤），
    // 所以长延迟不会影响 CheckQuorum 那类"断连后必须快速退位"的用例。
    e->kind = EventKind::kDeliverReply;
    e->ok = false;
    int64_t delay =
        info.longdelays ? raftcpp::RandInt(7000) : raftcpp::RandInt(100);
    Schedule(e, delay);
  }
}

void Network::Schedule(std::shared_ptr<TimerEvent> e, int64_t delay_ms) {
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto at = raftcpp::Now() + std::chrono::milliseconds(delay_ms);
    timers_.emplace(at, std::move(e));    // multimap  xx时刻有一个发信/回信事件
  }
  timer_cv_.notify_one();     // 唤醒notify_one立即投递
}

// 一张 TimerEvent 工单，塞进按时间排序的 multimap；回包时同一张工单翻成"送回顾客"状态再塞回同一个 multimap；请求侧有一道 10% 丢包闸，回包侧在塞回前有一道 10% 丢包闸 + 一道乱序延迟闸；全在本地进程内模拟。
// 每塞一张，就按截止时间自动插到正确的位置
void Network::TimerLoop() {
  std::unique_lock<std::mutex> lk(mu_);
  while (!stopped_.load()) {
    if (timers_.empty()) {
      timer_cv_.wait_for(lk, std::chrono::milliseconds(50));
      continue;
    }
    auto first = timers_.begin();

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
          e->kind = EventKind::kDeliverReply;  // 与下面两个失败路径同口径（Finish 不读 kind，保持一致防后人误判）
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
#ifdef RAFT_BENCH_METRICS
      // 埋点：工作池水位（busy/total）。每个任务执行时采样一次；
      // 纯原子加无锁，不扭曲被测时序。busy 贴 total 且 total 贴 512
      // 说明池子接近饱和 —— 此时的延迟数字含"线程不够"的伪影。
      raftcpp::Metrics::Instance().Record(raftcpp::kWorkerBusy, busy_workers_);
      raftcpp::Metrics::Instance().Record(raftcpp::kWorkerTotal, total_workers_);
#endif
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
  // cleaned_ 是普通 bool，唯一的读写都在 mu_ 里：不能在锁外先读一次
  // （早先的 `if (cleaned_) return;` 在锁外读、在锁内写 → TSan 会报
  //  data race；虽然后果只是"重复 join"，但这是同一把锁下该守的规矩）。
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
