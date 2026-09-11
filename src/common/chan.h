// chan.h —— Go channel 的极简 C++ 版
//
// Go 里 `ch := make(chan ApplyMsg)` 然后 `ch <- msg` / `for m := range ch`。
// C++ 用 mutex + condition_variable + deque 就能做出来，而且是无界缓冲，
// 这样 Raft 往里塞消息时永远不会被卡住（避免死锁）。

#pragma once

#include <condition_variable>
#include <deque>
#include <mutex>
#include <utility>

namespace raftcpp {

template <typename T>
class Chan {
 public:
  // 放入一条消息（永不阻塞）
  void Push(T v) {
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (closed_) return;  // 关了就丢掉，别让发送方崩
      q_.push_back(std::move(v));
    }
    cv_.notify_one();
  }

  // 取出一条消息。返回 false 表示 channel 已关闭且已取空。
  bool Pop(T& out) {
    std::unique_lock<std::mutex> lk(mu_);
    cv_.wait(lk, [this] { return closed_ || !q_.empty(); });
    if (q_.empty()) return false;  // 已关闭且空
    out = std::move(q_.front());
    q_.pop_front();
    return true;
  }

  // 关闭 channel。正在阻塞的 Pop 会被唤醒并返回 false。
  void Close() {
    {
      std::lock_guard<std::mutex> lk(mu_);
      closed_ = true;
    }
    cv_.notify_all();
  }

  size_t Size() {
    std::lock_guard<std::mutex> lk(mu_);
    return q_.size();
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<T> q_;
  bool closed_ = false;
};

}  // namespace raftcpp
